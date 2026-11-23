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
    if (predicate == null) {
      return true;
    }
    if (predicate instanceof SeekPredicate.And) {
      for (SeekPredicate child : ((SeekPredicate.And) predicate).children()) {
        if (!mayMatch(child, provider)) {
          return false;
        }
      }
      return true;
    }
    if (predicate instanceof SeekPredicate.Or) {
      for (SeekPredicate child : ((SeekPredicate.Or) predicate).children()) {
        if (mayMatch(child, provider)) {
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
      return rangeMayMatch(c.op(), stats.min(), stats.max(), c.value().value());
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
      for (LiteralValue value : in.values()) {
        if (value.value() == null
            || rangeMayMatch(SeekPredicate.Op.EQ, stats.min(), stats.max(), value.value())) {
          return true;
        }
      }
      return false;
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
      // A string matching a prefix-start pattern is lexicographically >= that prefix.
      return LiteralValue.compare(stats.max(), prefix) >= 0;
    }
    return true;
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
