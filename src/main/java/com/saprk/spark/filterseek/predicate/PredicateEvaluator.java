package com.saprk.spark.filterseek.predicate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Exact evaluation of a {@link SeekPredicate} against one row of phase-1 column values.
 *
 * <p>Compile once per partition with {@link #compile}. IN lists are turned into a hash set and
 * LIKE patterns into a single compiled {@link Pattern}, so per-row cost no longer grows with the
 * number of IN values and no regex is recompiled per row.
 */
public final class PredicateEvaluator {

  private final SeekPredicate predicate;
  private final boolean ignoreCase;
  private final Map<SeekPredicate.In, InLookup> inLookups = new IdentityHashMap<>();
  private final Map<SeekPredicate.Like, Pattern> likePatterns = new IdentityHashMap<>();

  private PredicateEvaluator(SeekPredicate predicate, boolean ignoreCase) {
    this.predicate = predicate;
    this.ignoreCase = ignoreCase;
    collect(predicate);
  }

  /** Build a reusable, pre-compiled evaluator. */
  public static PredicateEvaluator compile(SeekPredicate predicate, boolean ignoreCase) {
    return new PredicateEvaluator(predicate, ignoreCase);
  }

  /** Convenience matcher with case-sensitive defaults; prefer {@link #compile} in scans. */
  public static boolean test(SeekPredicate predicate, Map<String, Object> row) {
    return compile(predicate, false).matches(row);
  }

  /** Test one row of phase-1 column values. */
  public boolean matches(Map<String, Object> row) {
    return matches(predicate, row);
  }

  private boolean matches(SeekPredicate predicate, Map<String, Object> row) {
    if (predicate == null) {
      return true;
    }
    if (predicate instanceof SeekPredicate.And) {
      for (SeekPredicate child : ((SeekPredicate.And) predicate).children()) {
        if (!matches(child, row)) {
          return false;
        }
      }
      return true;
    }
    if (predicate instanceof SeekPredicate.Or) {
      for (SeekPredicate child : ((SeekPredicate.Or) predicate).children()) {
        if (matches(child, row)) {
          return true;
        }
      }
      return false;
    }
    if (predicate instanceof SeekPredicate.Not) {
      return !matches(((SeekPredicate.Not) predicate).child(), row);
    }
    if (predicate instanceof SeekPredicate.Comparison) {
      SeekPredicate.Comparison c = (SeekPredicate.Comparison) predicate;
      Object actual = value(row, c.column());
      if (actual == null || c.value().value() == null) {
        return false;
      }
      int cmp = PredicateCase.compare(actual, c.value().value(),
          PredicateCase.effective(c, ignoreCase));
      switch (c.op()) {
        case EQ:
          return cmp == 0;
        case NE:
          return cmp != 0;
        case GT:
          return cmp > 0;
        case GTE:
          return cmp >= 0;
        case LT:
          return cmp < 0;
        case LTE:
          return cmp <= 0;
        default:
          return false;
      }
    }
    if (predicate instanceof SeekPredicate.In) {
      SeekPredicate.In in = (SeekPredicate.In) predicate;
      Object actual = value(row, in.column());
      if (actual == null) {
        return false;
      }
      boolean found = inLookups.get(in).contains(actual);
      return in.negated() != found;
    }
    if (predicate instanceof SeekPredicate.Like) {
      SeekPredicate.Like like = (SeekPredicate.Like) predicate;
      Object actual = value(row, like.column());
      if (actual == null) {
        return false;
      }
      boolean matches = likePatterns.get(like).matcher(actual.toString()).matches();
      return like.negated() != matches;
    }
    throw new IllegalArgumentException("Unknown predicate type: " + predicate.getClass());
  }

  private void collect(SeekPredicate predicate) {
    if (predicate == null) {
      return;
    }
    if (predicate instanceof SeekPredicate.And) {
      for (SeekPredicate child : ((SeekPredicate.And) predicate).children()) {
        collect(child);
      }
    } else if (predicate instanceof SeekPredicate.Or) {
      for (SeekPredicate child : ((SeekPredicate.Or) predicate).children()) {
        collect(child);
      }
    } else if (predicate instanceof SeekPredicate.Not) {
      collect(((SeekPredicate.Not) predicate).child());
    } else if (predicate instanceof SeekPredicate.In) {
      SeekPredicate.In in = (SeekPredicate.In) predicate;
      inLookups.put(in, buildLookup(in));
    } else if (predicate instanceof SeekPredicate.Like) {
      SeekPredicate.Like like = (SeekPredicate.Like) predicate;
      likePatterns.put(like, likePattern(like.pattern(),
          PredicateCase.effective(like, ignoreCase)));
    }
  }

  private InLookup buildLookup(SeekPredicate.In in) {
    List<Object> raw = new ArrayList<>();
    for (LiteralValue candidate : in.values()) {
      raw.add(candidate.value());
    }
    boolean stringDomain = PredicateCase.needsStringDomain(raw);
    boolean caseInsensitive = PredicateCase.effective(in, ignoreCase);
    Set<Object> keys = new HashSet<>();
    for (Object value : raw) {
      Object key = PredicateCase.canonical(value, stringDomain, caseInsensitive);
      if (key != null) {
        keys.add(key);
      }
    }
    return new InLookup(stringDomain, caseInsensitive, keys);
  }

  /** Row map keys are column names, matched case-insensitively. */
  private static Object value(Map<String, Object> row, String column) {
    if (row.containsKey(column)) {
      return row.get(column);
    }
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      if (entry.getKey() != null && entry.getKey().toLowerCase(Locale.ROOT).equals(column)) {
        return entry.getValue();
      }
    }
    return null;
  }

  private static final class InLookup {
    private final boolean stringDomain;
    private final boolean caseInsensitive;
    private final Set<Object> keys;

    InLookup(boolean stringDomain, boolean caseInsensitive, Set<Object> keys) {
      this.stringDomain = stringDomain;
      this.caseInsensitive = caseInsensitive;
      this.keys = keys;
    }

    boolean contains(Object actual) {
      return keys.contains(PredicateCase.canonical(actual, stringDomain, caseInsensitive));
    }
  }

  /** SQL LIKE -> regex. '%' matches any sequence, '_' matches exactly one character. */
  public static boolean likeMatches(String value, String pattern) {
    if (pattern == null) {
      return false;
    }
    return likePattern(pattern, false).matcher(value).matches();
  }

  public static Pattern likePattern(String pattern) {
    return likePattern(pattern, false);
  }

  public static Pattern likePattern(String pattern, boolean ignoreCase) {
    if (pattern == null) {
      throw new IllegalArgumentException("LIKE pattern must not be null");
    }
    StringBuilder regex = new StringBuilder(pattern.length() + 8);
    regex.append('^');
    boolean escaped = false;
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (escaped) {
        regex.append(Pattern.quote(String.valueOf(c)));
        escaped = false;
        continue;
      }
      if (c == '\\') {
        escaped = true;
      } else if (c == '%') {
        regex.append(".*");
      } else if (c == '_') {
        regex.append('.');
      } else {
        regex.append(Pattern.quote(String.valueOf(c)));
      }
    }
    if (escaped) {
      regex.append(Pattern.quote("\\"));
    }
    regex.append('$');
    int flags = Pattern.DOTALL | (ignoreCase ? Pattern.CASE_INSENSITIVE : 0);
    return Pattern.compile(regex.toString(), flags);
  }

  /** Flatten a predicate tree into a readable one-line description for logs. */
  public static String describe(SeekPredicate predicate) {
    return predicate == null ? "<none>" : predicate.toString();
  }
}
