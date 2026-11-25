package com.saprk.spark.filterseek.spark;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import com.saprk.spark.filterseek.limit.RedissonHolder;
import com.saprk.spark.filterseek.predicate.SeekPredicate;
import com.saprk.spark.filterseek.util.ConfUtils;

/** Accepts only =, IN, LIKE and range filters; everything else is left to Spark. */
public final class SeekScanBuilder implements ScanBuilder,
    SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsPushDownLimit {

  private final StructType tableSchema;
  private final String format;
  private final List<String> files;
  private final Map<String, String> options;
  private final SeekPredicate sqlPredicate;

  private StructType requiredSchema;
  private SeekPredicate predicate;
  private Filter[] acceptedFilters = new Filter[0];
  private Filter[] remainingFilters = new Filter[0];
  /** -1 means no limit was accepted; LIMIT is accepted only when Redis control is explicitly on. */
  private int limit = -1;
  private String redisKey;

  public SeekScanBuilder(StructType tableSchema, String format, List<String> files,
      Map<String, String> options, SeekPredicate sqlPredicate) {
    this.tableSchema = tableSchema;
    this.requiredSchema = tableSchema;
    this.format = format;
    this.files = files;
    this.options = options;
    this.sqlPredicate = sqlPredicate;
    this.predicate = sqlPredicate;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    SparkFilterConverter.Result result = SparkFilterConverter.convert(filters, tableSchema);
    this.acceptedFilters = result.accepted;
    this.remainingFilters = result.remaining;
    this.predicate = SeekPredicate.and(sqlPredicate, result.predicate);
    return remainingFilters;
  }

  @Override
  public Filter[] pushedFilters() {
    return acceptedFilters;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.requiredSchema = requiredSchema;
  }

  @Override
  public boolean pushLimit(int value) {
    // Redis control is disabled by default. When it is off (or no address is configured) there
    // is no way to coordinate a global cap across partitions, so leave LIMIT to Spark's own
    // GlobalLimit: correct, just no early stop inside the scan and zero Redis traffic.
    if (value < 0 || !ConfUtils.redisEnabled(options) || ConfUtils.redisAddress(options) == null) {
      return false;
    }
    this.limit = value;
    return true;
  }

  @Override
  public Scan build() {
    if (limit >= 0) {
      // A unique key per query avoids two concurrent queries resetting each other's counter.
      redisKey = ConfUtils.redisKey(options);
      if (redisKey == null || redisKey.isEmpty()) {
        redisKey = "filterseek:limit:" + java.util.UUID.randomUUID().toString();
      }
      RedissonHolder.init(ConfUtils.redisAddress(options), ConfUtils.redisPassword(options),
          ConfUtils.redisDatabase(options), ConfUtils.redisPoolSize(options), redisKey,
          ConfUtils.redisTtlSeconds(options));
    }
    return new SeekScan(format, files, requiredSchema, predicate, acceptedFilters, options,
        limit, redisKey);
  }
}
