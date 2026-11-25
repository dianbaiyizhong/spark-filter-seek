package com.saprk.spark.filterseek.predicate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Internal filter tree. Both Druid SQL parsing and Spark Filter pushdown produce this model. */
public abstract class SeekPredicate implements Serializable {

  private static final long serialVersionUID = 1L;

  public abstract Set<String> referencedColumns();

  /** AND of several predicates, all of which must hold. */
  public static final class And extends SeekPredicate {
    private static final long serialVersionUID = 1L;
    private final List<SeekPredicate> children;

    public And(List<SeekPredicate> children) {
      this.children = new ArrayList<>(children);
    }

    public And(SeekPredicate... children) {
      this(Arrays.asList(children));
    }

    public List<SeekPredicate> children() {
      return children;
    }

    @Override
    public Set<String> referencedColumns() {
      Set<String> result = new LinkedHashSet<>();
      for (SeekPredicate child : children) {
        result.addAll(child.referencedColumns());
      }
      return result;
    }

    @Override
    public String toString() {
      return "And" + children;
    }
  }

  /** OR of several predicates. */
  public static final class Or extends SeekPredicate {
    private static final long serialVersionUID = 1L;
    private final List<SeekPredicate> children;

    public Or(List<SeekPredicate> children) {
      this.children = new ArrayList<>(children);
    }

    public Or(SeekPredicate... children) {
      this(Arrays.asList(children));
    }

    public List<SeekPredicate> children() {
      return children;
    }

    @Override
    public Set<String> referencedColumns() {
      Set<String> result = new LinkedHashSet<>();
      for (SeekPredicate child : children) {
        result.addAll(child.referencedColumns());
      }
      return result;
    }

    @Override
    public String toString() {
      return "Or" + children;
    }
  }

  /** Logical NOT; evaluation is exact, statistic-based pruning is conservative. */
  public static final class Not extends SeekPredicate {
    private static final long serialVersionUID = 1L;
    private final SeekPredicate child;

    public Not(SeekPredicate child) {
      this.child = child;
    }

    public SeekPredicate child() {
      return child;
    }

    @Override
    public Set<String> referencedColumns() {
      return child.referencedColumns();
    }

    @Override
    public String toString() {
      return "Not(" + child + ")";
    }
  }

  public enum Op {
    EQ, NE, GT, GTE, LT, LTE;

    public static Op fromSql(String symbol) {
      switch (symbol) {
        case "=":
        case "==":
          return EQ;
        case "!=":
        case "<>":
          return NE;
        case ">":
          return GT;
        case ">=":
          return GTE;
        case "<":
          return LT;
        case "<=":
          return LTE;
        default:
          throw new IllegalArgumentException("Unsupported comparison operator: " + symbol);
      }
    }
  }

  /** column OP literal. */
  public static final class Comparison extends SeekPredicate {
    private static final long serialVersionUID = 1L;
    private final String column;
    private final Op op;
    private final LiteralValue value;
    /** null means "use the configured default", TRUE/FORCE overrides it for this leaf. */
    private final Boolean ignoreCase;

    public Comparison(String column, Op op, LiteralValue value) {
      this(column, op, value, null);
    }

    public Comparison(String column, Op op, LiteralValue value, Boolean ignoreCase) {
      this.column = normalizeColumn(column);
      this.op = op;
      this.value = value;
      this.ignoreCase = ignoreCase;
    }

    public String column() {
      return column;
    }

    public Op op() {
      return op;
    }

    public LiteralValue value() {
      return value;
    }

    public Boolean ignoreCase() {
      return ignoreCase;
    }

    @Override
    public Set<String> referencedColumns() {
      return new LinkedHashSet<>(Arrays.asList(column));
    }

    @Override
    public String toString() {
      return column + " " + op + " " + value + caseSuffix(ignoreCase);
    }
  }

  /** IN / NOT IN list. */
  public static final class In extends SeekPredicate {
    private static final long serialVersionUID = 1L;
    private final String column;
    private final List<LiteralValue> values;
    private final boolean negated;
    private final Boolean ignoreCase;

    public In(String column, List<LiteralValue> values, boolean negated) {
      this(column, values, negated, null);
    }

    public In(String column, List<LiteralValue> values, boolean negated, Boolean ignoreCase) {
      this.column = normalizeColumn(column);
      this.values = new ArrayList<>(values);
      this.negated = negated;
      this.ignoreCase = ignoreCase;
    }

    public String column() {
      return column;
    }

    public List<LiteralValue> values() {
      return values;
    }

    public boolean negated() {
      return negated;
    }

    public Boolean ignoreCase() {
      return ignoreCase;
    }

    @Override
    public Set<String> referencedColumns() {
      return new LinkedHashSet<>(Arrays.asList(column));
    }

    @Override
    public String toString() {
      return column + (negated ? " NOT IN " : " IN ") + values + caseSuffix(ignoreCase);
    }
  }

  /** SQL LIKE / NOT LIKE with '%' and '_' wildcards. */
  public static final class Like extends SeekPredicate {
    private static final long serialVersionUID = 1L;
    private final String column;
    private final String pattern;
    private final boolean negated;
    private final Boolean ignoreCase;

    public Like(String column, String pattern, boolean negated) {
      this(column, pattern, negated, null);
    }

    public Like(String column, String pattern, boolean negated, Boolean ignoreCase) {
      this.column = normalizeColumn(column);
      this.pattern = pattern;
      this.negated = negated;
      this.ignoreCase = ignoreCase;
    }

    public String column() {
      return column;
    }

    public String pattern() {
      return pattern;
    }

    public boolean negated() {
      return negated;
    }

    public Boolean ignoreCase() {
      return ignoreCase;
    }

    @Override
    public Set<String> referencedColumns() {
      return new LinkedHashSet<>(Arrays.asList(column));
    }

    @Override
    public String toString() {
      return column + (negated ? " NOT LIKE " : " LIKE ") + pattern + caseSuffix(ignoreCase);
    }
  }

  /** Combine predicates with AND; null values are ignored. */
  public static SeekPredicate and(SeekPredicate... items) {
    List<SeekPredicate> flat = new ArrayList<>();
    for (SeekPredicate item : items) {
      if (item == null) {
        continue;
      }
      if (item instanceof And) {
        flat.addAll(((And) item).children());
      } else {
        flat.add(item);
      }
    }
    if (flat.isEmpty()) {
      return null;
    }
    if (flat.size() == 1) {
      return flat.get(0);
    }
    return new And(flat);
  }

  private static String normalizeColumn(String name) {
    return name == null ? null : name.toLowerCase(Locale.ROOT);
  }

  private static String caseSuffix(Boolean ignoreCase) {
    if (ignoreCase == null) {
      return "";
    }
    return " [ignoreCase=" + ignoreCase + "]";
  }
}
