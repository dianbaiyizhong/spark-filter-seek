package com.saprk.spark.filterseek;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import com.saprk.spark.filterseek.predicate.PredicateEvaluator;
import com.saprk.spark.filterseek.predicate.PruningEvaluator;
import com.saprk.spark.filterseek.predicate.SeekPredicate;
import com.saprk.spark.filterseek.spark.SparkFilterConverter;
import com.saprk.spark.filterseek.sql.DruidSqlParser;

public class SeekPredicateTest {

  private static StructType schema() {
    return DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField("region", DataTypes.StringType, true),
        DataTypes.createStructField("amount", DataTypes.LongType, true),
        DataTypes.createStructField("status", DataTypes.StringType, true)
    });
  }

  private static Map<String, Object> row(String region, long amount, String status) {
    Map<String, Object> row = new HashMap<>();
    row.put("region", region);
    row.put("amount", amount);
    row.put("status", status);
    return row;
  }

  @Test
  public void druidParsesSupportedOperators() {
    SeekPredicate predicate = new DruidSqlParser(schema()).parseWhere(
        "SELECT * FROM t WHERE region = 'CN' AND amount >= 100 "
            + "AND status IN ('PAID', 'NEW') AND region LIKE 'C%'");

    assertTrue(PredicateEvaluator.test(predicate, row("CN", 120L, "PAID")));
    assertFalse(PredicateEvaluator.test(predicate, row("CN", 90L, "PAID")));
    assertFalse(PredicateEvaluator.test(predicate, row("US", 120L, "PAID")));
    assertFalse(PredicateEvaluator.test(predicate, row("CN", 120L, "CLOSED")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void druidRejectsUnsupportedOperator() {
    new DruidSqlParser(schema()).parseWhere(
        "SELECT * FROM t WHERE amount + 1 > 100");
  }

  @Test
  public void sparkFiltersAreConvertedToSameModel() {
    Filter[] filters = new Filter[] {
        new EqualTo("region", "CN"),
        new GreaterThanOrEqual("amount", 100L),
        new In("status", new Object[] {"PAID", "NEW"}),
        new StringContains("region", "C")
    };
    SparkFilterConverter.Result result = SparkFilterConverter.convert(filters, schema());
    assertEquals(4, result.accepted.length);
    assertEquals(0, result.remaining.length);
    assertTrue(PredicateEvaluator.test(result.predicate, row("CN", 100L, "NEW")));
    assertFalse(PredicateEvaluator.test(result.predicate, row("US", 100L, "NEW")));
  }

  @Test
  public void statisticsPruningNeverDropsPossibleMatches() {
    final Object min = 10L;
    final Object max = 20L;
    PruningEvaluator.ColumnStatsProvider provider = column -> new PruningEvaluator.ColumnStats() {
      @Override
      public Object min() {
        return min;
      }

      @Override
      public Object max() {
        return max;
      }

      @Override
      public boolean hasNull() {
        return false;
      }
    };

    SeekPredicate gte = new DruidSqlParser(schema()).parseWhere(
        "SELECT * FROM t WHERE amount >= 25");
    assertFalse(PruningEvaluator.mayMatch(gte, provider));

    SeekPredicate lte = new DruidSqlParser(schema()).parseWhere(
        "SELECT * FROM t WHERE amount <= 5");
    assertFalse(PruningEvaluator.mayMatch(lte, provider));

    SeekPredicate in = new DruidSqlParser(schema()).parseWhere(
        "SELECT * FROM t WHERE amount IN (30, 40)");
    assertFalse(PruningEvaluator.mayMatch(in, provider));

    SeekPredicate possible = new DruidSqlParser(schema()).parseWhere(
        "SELECT * FROM t WHERE amount >= 15");
    assertTrue(PruningEvaluator.mayMatch(possible, provider));
  }
}
