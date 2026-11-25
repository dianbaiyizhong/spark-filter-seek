package com.saprk.spark.filterseek.sql;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLUnaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.saprk.spark.filterseek.predicate.LiteralValue;
import com.saprk.spark.filterseek.predicate.SeekPredicate;

/**
 * Parses the WHERE clause of a SQL statement with Alibaba Druid and converts the supported
 * operators (=, &lt;&gt;, &gt;, &gt;=, &lt;, &lt;=, IN, LIKE, AND/OR/NOT) into the internal
 * {@link SeekPredicate} tree. Anything else is rejected instead of being silently ignored.
 */
public final class DruidSqlParser {

  /**
   * Wrappers that only fold case. Unwrapping them is safe as long as the resulting leaf is
   * evaluated case-insensitively, regardless of the session-level setting.
   */
  private static final Set<String> CASE_FOLDING_FUNCTIONS = new java.util.HashSet<>(
      java.util.Arrays.asList("lower", "lcase", "upper", "ucase"));

  private final Set<String> knownColumns;
  private final java.util.Map<String, DataType> dataTypeByName = new java.util.HashMap<>();

  /** A resolved column plus whether a case-folding wrapper was seen on the left-hand side. */
  private static final class ColumnRef {
    final String name;
    final boolean caseFolded;

    ColumnRef(String name, boolean caseFolded) {
      this.name = name;
      this.caseFolded = caseFolded;
    }
  }

  public DruidSqlParser(StructType schema) {
    this.knownColumns = new LinkedHashSet<>();
    for (StructField field : schema.fields()) {
      String lower = field.name().toLowerCase(Locale.ROOT);
      knownColumns.add(lower);
      dataTypeByName.put(lower, field.dataType());
    }
  }

  /** Parse the WHERE clause of one SELECT statement. Returns null when there is no WHERE. */
  public SeekPredicate parseWhere(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return null;
    }
    SQLSelect select = parseSelect(sql, "seek.sql must be a SELECT statement");
    SQLSelectQuery query = select.getQuery();
    if (!(query instanceof SQLSelectQueryBlock)) {
      throw new IllegalArgumentException("Only simple SELECT ... FROM ... WHERE ... is supported");
    }
    SQLExpr where = ((SQLSelectQueryBlock) query).getWhere();
    return where == null ? null : convert(where);
  }

  /** Parse a bare expression, e.g. {@code "status = 'PAID' AND amount >= 100"}. */
  public SeekPredicate parseExpression(String expression) {
    if (expression == null || expression.trim().isEmpty()) {
      return null;
    }
    SQLSelect select = parseSelect("SELECT * FROM __seek__ WHERE " + expression,
        "Invalid seek expression: " + expression);
    SQLSelectQuery query = select.getQuery();
    SQLExpr where = query instanceof SQLSelectQueryBlock
        ? ((SQLSelectQueryBlock) query).getWhere() : null;
    return where == null ? null : convert(where);
  }

  private static SQLSelect parseSelect(String sql, String errorMessage) {
    List<SQLStatement> statements = SQLUtils.parseStatements(sql, DbType.mysql);
    if (statements.size() != 1) {
      throw new IllegalArgumentException(errorMessage);
    }
    SQLStatement statement = statements.get(0);
    if (statement instanceof SQLSelectStatement) {
      return ((SQLSelectStatement) statement).getSelect();
    }
    if (statement instanceof SQLSelect) {
      return (SQLSelect) statement;
    }
    throw new IllegalArgumentException(errorMessage);
  }

  private SeekPredicate convert(SQLExpr expr) {
    if (expr instanceof SQLBinaryOpExpr) {
      return convertBinary((SQLBinaryOpExpr) expr);
    }
    if (expr instanceof SQLInListExpr) {
      SQLInListExpr in = (SQLInListExpr) expr;
      ColumnRef ref = columnRef(in.getExpr());
      List<LiteralValue> values = new ArrayList<>();
      for (SQLExpr target : in.getTargetList()) {
        values.add(literal(target, columnType(ref.name)));
      }
      return new SeekPredicate.In(ref.name, values, in.isNot(),
          forcedIgnoreCase(ref.caseFolded, false));
    }
    if (expr instanceof SQLUnaryExpr) {
      SQLUnaryExpr unary = (SQLUnaryExpr) expr;
      if (unary.getOperator() == SQLUnaryOperator.Not) {
        return new SeekPredicate.Not(convert(unary.getExpr()));
      }
      throw unsupported("unary operator " + unary.getOperator());
    }
    if (expr instanceof SQLLiteralExpr) {
      // A bare TRUE/FALSE literal is not a seek predicate; reject it explicitly.
      throw unsupported("literal used as a whole predicate");
    }
    throw unsupported(expr == null ? "null expression" : expr.getClass().getSimpleName());
  }

  private SeekPredicate convertBinary(SQLBinaryOpExpr binary) {
    SQLBinaryOperator op = binary.getOperator();
    if (op == SQLBinaryOperator.BooleanAnd) {
      return new SeekPredicate.And(convert(binary.getLeft()), convert(binary.getRight()));
    }
    if (op == SQLBinaryOperator.BooleanOr) {
      return new SeekPredicate.Or(convert(binary.getLeft()), convert(binary.getRight()));
    }
    if (op == SQLBinaryOperator.Like || op == SQLBinaryOperator.NotLike
        || op == SQLBinaryOperator.ILike || op == SQLBinaryOperator.NotILike) {
      ColumnRef ref = columnRef(binary.getLeft());
      String pattern = stringValue(binary.getRight(), ref.name);
      boolean iLike = op == SQLBinaryOperator.ILike || op == SQLBinaryOperator.NotILike;
      return new SeekPredicate.Like(ref.name, pattern,
          op == SQLBinaryOperator.NotLike || op == SQLBinaryOperator.NotILike,
          forcedIgnoreCase(iLike, ref.caseFolded));
    }
    SeekPredicate.Op cmp = comparison(op);
    if (cmp == null) {
      throw unsupported("operator " + op.getName());
    }
    ColumnRef ref = columnRef(binary.getLeft());
    DataType dataType = columnType(ref.name);
    return new SeekPredicate.Comparison(ref.name, cmp,
        literal(binary.getRight(), dataType), forcedIgnoreCase(false, ref.caseFolded));
  }

  private SeekPredicate.Op comparison(SQLBinaryOperator op) {
    // Compare by name: Druid added a few aliases across releases (e.g. EqEq).
    switch (op.getName()) {
      case "=":
      case "==":
        return SeekPredicate.Op.EQ;
      case "!=":
      case "<>":
        return SeekPredicate.Op.NE;
      case ">":
        return SeekPredicate.Op.GT;
      case ">=":
        return SeekPredicate.Op.GTE;
      case "<":
        return SeekPredicate.Op.LT;
      case "<=":
        return SeekPredicate.Op.LTE;
      default:
        return null;
    }
  }

  private ColumnRef columnRef(SQLExpr expr) {
    boolean caseFolded = false;
    while (expr instanceof SQLMethodInvokeExpr) {
      SQLMethodInvokeExpr call = (SQLMethodInvokeExpr) expr;
      String method = call.getMethodName() == null ? "" : call.getMethodName().toLowerCase(Locale.ROOT);
      if (!CASE_FOLDING_FUNCTIONS.contains(method)
          || call.getArguments() == null || call.getArguments().size() != 1) {
        throw unsupported("function " + method + " on a filter column");
      }
      caseFolded = true;
      expr = call.getArguments().get(0);
    }
    String name;
    if (expr instanceof SQLIdentifierExpr) {
      name = ((SQLIdentifierExpr) expr).getName();
    } else if (expr instanceof SQLPropertyExpr) {
      name = ((SQLPropertyExpr) expr).getName();
    } else {
      throw unsupported("filter left-hand side must be a simple column "
          + "(optionally wrapped in lower/upper), got "
          + (expr == null ? "null" : expr.getClass().getSimpleName()));
    }
    String lower = name.toLowerCase(Locale.ROOT);
    if (!knownColumns.contains(lower)) {
      throw new IllegalArgumentException("Unknown or unsupported filter column: " + name);
    }
    return new ColumnRef(lower, caseFolded);
  }

  private static Boolean forcedIgnoreCase(boolean explicit, boolean caseFolded) {
    return explicit || caseFolded ? Boolean.TRUE : null;
  }

  private DataType columnType(String column) {
    // DruidSqlParser is constructed with a schema; columnType is resolved lazily via knownColumns.
    return dataTypeByName.get(column);
  }

  private LiteralValue literal(SQLExpr expr, DataType dataType) {
    if (expr instanceof SQLValuableExpr) {
      Object raw = ((SQLValuableExpr) expr).getValue();
      if (raw instanceof BigDecimal || raw instanceof Number || raw instanceof String
          || raw instanceof Boolean) {
        return LiteralValue.of(raw, dataType);
      }
    }
    if (expr instanceof SQLLiteralExpr) {
      return LiteralValue.of(expr.toString(), dataType);
    }
    throw unsupported("right-hand side must be a literal, got "
        + (expr == null ? "null" : expr.getClass().getSimpleName()));
  }

  private String stringValue(SQLExpr expr, String column) {
    if (expr instanceof SQLValuableExpr) {
      Object raw = ((SQLValuableExpr) expr).getValue();
      return raw == null ? null : raw.toString();
    }
    if (expr instanceof SQLLiteralExpr) {
      return expr.toString();
    }
    throw unsupported("LIKE pattern must be a literal string");
  }

  private static IllegalArgumentException unsupported(String what) {
    return new IllegalArgumentException(
        "Unsupported predicate for seek scan: " + what
            + ". Supported: =, <>, >, >=, <, <=, IN, LIKE, AND, OR, NOT on primitive columns, "
            + "optionally wrapped in lower()/upper().");
  }
}
