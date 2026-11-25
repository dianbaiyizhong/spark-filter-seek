package com.saprk.spark.filterseek.predicate;

import java.util.Locale;

/**
 * Case-sensitivity rules shared by exact row evaluation and statistics pruning.
 *
 * <p>A leaf may force case-insensitive comparison (for example when the SQL used
 * {@code lower(name) = 'x'} or {@code ILIKE}). Otherwise the session-level setting applies.
 */
public final class PredicateCase {

  private PredicateCase() {
  }

  /** Effective case mode for one leaf of the predicate tree. */
  public static boolean effective(SeekPredicate predicate, boolean globalIgnoreCase) {
    Boolean override = null;
    if (predicate instanceof SeekPredicate.Comparison) {
      override = ((SeekPredicate.Comparison) predicate).ignoreCase();
    } else if (predicate instanceof SeekPredicate.In) {
      override = ((SeekPredicate.In) predicate).ignoreCase();
    } else if (predicate instanceof SeekPredicate.Like) {
      override = ((SeekPredicate.Like) predicate).ignoreCase();
    }
    return override == null ? globalIgnoreCase : override.booleanValue();
  }

  /** Compare two values, folding string case when requested. */
  public static int compare(Object left, Object right, boolean ignoreCase) {
    if (ignoreCase && (left instanceof CharSequence || right instanceof CharSequence)) {
      return LiteralValue.compare(text(left, true), text(right, true));
    }
    return LiteralValue.compare(left, right);
  }

  /**
   * Canonical key used by IN lookups. When the literal list is in the string domain, numbers are
   * rendered to strings so mixed literal types behave like {@link LiteralValue#compare}.
   */
  public static Object canonical(Object value, boolean stringDomain, boolean ignoreCase) {
    if (value == null) {
      return null;
    }
    if (stringDomain) {
      return text(value, ignoreCase);
    }
    if (value instanceof CharSequence) {
      String s = value.toString().trim();
      try {
        return new java.math.BigDecimal(s);
      } catch (NumberFormatException e) {
        return ignoreCase ? s.toLowerCase(Locale.ROOT) : s;
      }
    }
    Comparable<?> comparable = LiteralValue.comparable(value);
    return comparable == null ? text(value, ignoreCase) : comparable;
  }

  public static boolean needsStringDomain(Iterable<Object> values) {
    for (Object value : values) {
      if (value instanceof CharSequence) {
        return true;
      }
    }
    return false;
  }

  public static String text(Object value, boolean ignoreCase) {
    if (value == null) {
      return null;
    }
    String s = value instanceof CharSequence ? value.toString() : String.valueOf(value);
    return ignoreCase ? s.toLowerCase(Locale.ROOT) : s;
  }
}
