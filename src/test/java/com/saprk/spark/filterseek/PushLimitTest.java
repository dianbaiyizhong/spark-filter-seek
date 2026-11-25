package com.saprk.spark.filterseek;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    Map<String, String> options = enabledRedisOptions();
    assertFalse(builder(options).pushLimit(-1));
  }

  @Test
  public void redisAddressAloneDoesNotEnableTheFeature() {
    Map<String, String> options = new HashMap<>();
    options.put(ConfUtils.REDIS_ADDRESS_OPTION, "redis://127.0.0.1:6379");
    assertFalse("Redis control is opt-in, an address alone must not push LIMIT",
        builder(options).pushLimit(7));
  }

  @Test
  public void explicitDisableWinsOverAddress() {
    Map<String, String> options = enabledRedisOptions();
    options.put(ConfUtils.REDIS_ENABLED_OPTION, "false");
    assertFalse("Explicit false must disable Redis even with a valid address",
        builder(options).pushLimit(7));
  }

  @Test
  public void enabledWithoutAddressIsNotPushed() {
    Map<String, String> options = new HashMap<>();
    options.put(ConfUtils.REDIS_ENABLED_OPTION, "true");
    assertFalse("No address means no global counter, LIMIT stays with Spark",
        builder(options).pushLimit(7));
  }

  @Test
  public void explicitEnableAndAddressPushesLimit() {
    assertTrue(builder(enabledRedisOptions()).pushLimit(7));
  }

  private static Map<String, String> enabledRedisOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(ConfUtils.REDIS_ENABLED_OPTION, "true");
    options.put(ConfUtils.REDIS_ADDRESS_OPTION, "redis://127.0.0.1:6379");
    return options;
  }
}
