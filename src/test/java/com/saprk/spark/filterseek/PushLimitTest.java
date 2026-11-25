package com.saprk.spark.filterseek;

import static org.junit.Assert.assertFalse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import com.saprk.spark.filterseek.spark.SeekScanBuilder;
import com.saprk.spark.filterseek.util.ConfUtils;

public class PushLimitTest {

  private static SeekScanBuilder builder(Map<String, String> options) {
    StructType schema = DataTypes.createStructType(Collections.singletonList(
        DataTypes.createStructField("id", DataTypes.IntegerType, false)));
    return new SeekScanBuilder(schema, "orc", Collections.<String>emptyList(), options, null);
  }

  @Test
  public void limitIsNotPushedWithoutRedis() {
    assertFalse("LIMIT must be left to Spark when Redis is not configured",
        builder(new HashMap<String, String>()).pushLimit(7));
  }

  @Test
  public void negativeLimitIsRejected() {
    Map<String, String> options = new HashMap<>();
    options.put(ConfUtils.REDIS_ADDRESS_OPTION, "redis://127.0.0.1:6379");
    assertFalse(builder(options).pushLimit(-1));
  }
}
