package com.saprk.spark.filterseek.predicate;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/** Exact evaluation of a {@link SeekPredicate} against one row of phase-1 column values. */
public final class PredicateEvaluator {

  private PredicateEvaluator() {
  }

  /** Row map keys are column names, matched case-insensitively. */
  public static boolean test(SeekPredicate predicate, Map<String, Object> row) {
    if (predicate == null) {
      return true;
    }
    if (predicate instanceof SeekPredicate.And) {
      for (SeekPredicate child : ((SeekPredicate.And) predicate).children()) {
        if (!test(child, row)) {
          return false;
        }
      }
      return true;
    }
    if (predicate instanceof SeekPredicate.Or) {
      for (SeekPredicate child : ((SeekPredicate.Or) predicate).children()) {
        if (test(child, row)) {
          return true;
        }
      }
      return false;
    }
    if (predicate instanceof SeekPredicate.Not) {
      return !test(((SeekPredicate.Not) predicate).child(), row);
    }
    if (predicate instanceof SeekPredicate.Comparison) {
      SeekPredicate.Comparison c = (SeekPredicate.Comparison) predicate;
      Object actual = value(row, c.column());
      if (actual == null || c.value().value() == null) {
        return false;
      }
      int cmp = LiteralValue.compare(actual, c.value().value());
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
      boolean found = false;
      for (LiteralValue candidate : in.values()) {
        if (candidate.value() != null && LiteralValue.compare(actual, candidate.value()) == 0) {
          found = true;
          break;
        }
      }
      return in.negated() != found;
    }
    if (predicate instanceof SeekPredicate.Like) {
      SeekPredicate.Like like = (SeekPredicate.Like) predicate;
      Object actual = value(row, like.column());
      if (actual == null) {
        return false;
      }
      boolean matches = likeMatches(actual.toString(), like.pattern());
      return like.negated() != matches;
    }
    throw new IllegalArgumentException("Unknown predicate type: " + predicate.getClass());
  }

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

  /** SQL LIKE -> regex. '%' matches any sequence, '_' matches exactly one character. */
  public static boolean likeMatches(String value, String pattern) {
    if (pattern == null) {
      return false;
    }
    return likePattern(pattern).matcher(value).matches();
  }

  public static Pattern likePattern(String pattern) {
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
    return Pattern.compile(regex.toString(), Pattern.DOTALL);
  }

  /** Flatten a predicate tree into a readable one-line description for logs. */
  public static String describe(SeekPredicate predicate) {
    return predicate == null ? "<none>" : predicate.toString();
  }

  private static boolean allFalse(List<SeekPredicate> children,
      java.util.function.Predicate<SeekPredicate> checker) {
    for (SeekPredicate child : children) {
      if (checker.test(child)) {
        return false;
      }
    }
    return true;
  }
}
