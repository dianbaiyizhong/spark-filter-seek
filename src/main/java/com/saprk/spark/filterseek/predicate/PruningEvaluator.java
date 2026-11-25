package com.saprk.spark.filterseek.predicate;

/**
 * Conservative statistics-based pruning. Returning {@code true} means "this block may contain a
 * match", so a false negative is never produced: when in doubt we keep the block.
 */
public final class PruningEvaluator {

  private PruningEvaluator() {
  }

  /** Min/max bounds of one column in one ORC stripe or Parquet row group. */
  public interface ColumnStats {
    Object min();

    Object max();

    boolean hasNull();
  }

  public interface ColumnStatsProvider {
    /** Returns null when statistics are unavailable for the column. */
    ColumnStats stats(String column);
  }

  public static boolean mayMatch(SeekPredicate predicate, ColumnStatsProvider provider) {
    return mayMatch(predicate, provider, false);
  }

  /** Case-aware variant; string bounds and values are folded together before range checks. */
  public static boolean mayMatch(SeekPredicate predicate, ColumnStatsProvider provider,
      boolean globalIgnoreCase) {
    if (predicate == null) {
      return true;
    }
    if (predicate instanceof SeekPredicate.And) {
      for (SeekPredicate child : ((SeekPredicate.And) predicate).children()) {
        if (!mayMatch(child, provider, globalIgnoreCase)) {
          return false;
        }
      }
      return true;
    }
    if (predicate instanceof SeekPredicate.Or) {
      for (SeekPredicate child : ((SeekPredicate.Or) predicate).children()) {
        if (mayMatch(child, provider, globalIgnoreCase)) {
          return true;
        }
      }
      return false;
    }
    if (predicate instanceof SeekPredicate.Not) {
      // NOT cannot be decided from min/max without a full domain description.
      return true;
    }
    if (predicate instanceof SeekPredicate.Comparison) {
      SeekPredicate.Comparison c = (SeekPredicate.Comparison) predicate;
      ColumnStats stats = provider.stats(c.column());
      if (stats == null || stats.min() == null || stats.max() == null
          || c.value().value() == null) {
        return true;
      }
      if (c.op() == SeekPredicate.Op.NE) {
        return true;
      }
      Object[] folded = fold(stats.min(), stats.max(), c.value().value(),
          PredicateCase.effective(c, globalIgnoreCase));
      return rangeMayMatch(c.op(), folded[0], folded[1], folded[2]);
    }
    if (predicate instanceof SeekPredicate.In) {
      SeekPredicate.In in = (SeekPredicate.In) predicate;
      if (in.negated()) {
        return true;
      }
      ColumnStats stats = provider.stats(in.column());
      if (stats == null || stats.min() == null || stats.max() == null) {
        return true;
      }
      boolean fold = PredicateCase.effective(in, globalIgnoreCase);
      if (fold) {
        java.util.List<Object> raw = new java.util.ArrayList<>();
        for (LiteralValue value : in.values()) {
          raw.add(value.value());
        }
        fold = PredicateCase.needsStringDomain(raw);
      }
      Object blockMin = fold ? PredicateCase.text(stats.min(), true) : stats.min();
      Object blockMax = fold ? PredicateCase.text(stats.max(), true) : stats.max();
      Object valueMin = null;
      Object valueMax = null;
      for (LiteralValue value : in.values()) {
        if (value.value() == null) {
          continue;
        }
        Object normalized = fold ? PredicateCase.text(value.value(), true) : value.value();
        if (valueMin == null || LiteralValue.compare(normalized, valueMin) < 0) {
          valueMin = normalized;
        }
        if (valueMax == null || LiteralValue.compare(normalized, valueMax) > 0) {
          valueMax = normalized;
        }
      }
      if (valueMin == null) {
        return false;
      }
      // The block can only match if its [min,max] intersects the IN list's [min,max].
      return LiteralValue.compare(valueMax, blockMin) >= 0
          && LiteralValue.compare(valueMin, blockMax) <= 0;
    }
    if (predicate instanceof SeekPredicate.Like) {
      SeekPredicate.Like like = (SeekPredicate.Like) predicate;
      if (like.negated()) {
        return true;
      }
      ColumnStats stats = provider.stats(like.column());
      if (stats == null || stats.min() == null || stats.max() == null) {
        return true;
      }
      String prefix = literalPrefix(like.pattern());
      if (prefix == null || prefix.isEmpty()) {
        return true;
      }
      boolean fold = PredicateCase.effective(like, globalIgnoreCase);
      Object blockMax = fold ? PredicateCase.text(stats.max(), true) : stats.max();
      Object foldedPrefix = fold ? prefix.toLowerCase(java.util.Locale.ROOT) : prefix;
      // A string matching a prefix-start pattern is lexicographically >= that prefix.
      return LiteralValue.compare(blockMax, foldedPrefix) >= 0;
    }
    return true;
  }

  /** Fold string bounds/values to lower case only when a string is actually involved. */
  private static Object[] fold(Object min, Object max, Object value, boolean ignoreCase) {
    if (ignoreCase && (min instanceof CharSequence || max instanceof CharSequence
        || value instanceof CharSequence)) {
      return new Object[] {
          PredicateCase.text(min, true), PredicateCase.text(max, true),
          PredicateCase.text(value, true)
      };
    }
    return new Object[] {min, max, value};
  }

  private static boolean rangeMayMatch(SeekPredicate.Op op, Object min, Object max, Object value) {
    int cmpMin = LiteralValue.compare(value, min);
    int cmpMax = LiteralValue.compare(value, max);
    switch (op) {
      case EQ:
        return cmpMin >= 0 && cmpMax <= 0;
      case GT:
        return cmpMax < 0;
      case GTE:
        return cmpMax <= 0;
      case LT:
        return cmpMin > 0;
      case LTE:
        return cmpMin >= 0;
      default:
        return true;
    }
  }

  /** Returns the fixed prefix of a LIKE pattern, or null when it starts with a wildcard. */
  private static String literalPrefix(String pattern) {
    StringBuilder prefix = new StringBuilder();
    boolean escaped = false;
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (escaped) {
        prefix.append(c);
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == '%' || c == '_') {
        break;
      } else {
        prefix.append(c);
      }
    }
    return prefix.toString();
  }
}
