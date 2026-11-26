package com.saprk.spark.filterseek;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import com.saprk.spark.filterseek.predicate.PredicateEvaluator;
import com.saprk.spark.filterseek.sql.DruidSqlParser;

/** seek.sql edge cases must fail with a clear error, never with a NullPointerException. */
public class SeekSqlNullSafetyTest {

  private static final StructType SCHEMA = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[] {
      DataTypes.createStructField("id", DataTypes.IntegerType, false),
      DataTypes.createStructField("name", DataTypes.StringType, true)
  });

  @Test
  public void likeNullIsRejectedClearly() {
    try {
      new DruidSqlParser(SCHEMA).parseWhere("SELECT * FROM t WHERE name LIKE NULL");
      fail("LIKE NULL must be rejected");
    } catch (NullPointerException e) {
      fail("LIKE NULL must not throw NPE");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("LIKE pattern"));
    }
  }

  @Test
  public void likePatternNullIsRejectedByEvaluator() {
    try {
      PredicateEvaluator.likePattern(null, false);
      fail("null LIKE pattern must be rejected");
    } catch (NullPointerException e) {
      fail("null LIKE pattern must not throw NPE");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("LIKE pattern"));
    }
  }

  @Test
  public void inSubQueryIsRejectedClearly() {
    try {
      new DruidSqlParser(SCHEMA).parseWhere("SELECT * FROM t WHERE id IN (SELECT id FROM u)");
      fail("IN subquery must be rejected");
    } catch (NullPointerException e) {
      fail("IN subquery must not throw NPE");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("IN"));
    }
  }

  @Test
  public void selectWithoutWhereHasNoPredicate() {
    assertNull(new DruidSqlParser(SCHEMA).parseWhere("SELECT * FROM t"));
  }
}
