package com.saprk.spark.filterseek.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import com.saprk.spark.filterseek.predicate.PredicateEvaluator;
import com.saprk.spark.filterseek.predicate.SeekPredicate;
import com.saprk.spark.filterseek.util.ConfUtils;

/** Logical scan with pushed filters and pruned columns. */
public final class SeekScan implements Scan {

  private final String format;
  private final List<String> files;
  private final StructType readSchema;
  private final SeekPredicate predicate;
  private final Filter[] pushedFilters;
  private final Map<String, String> options;
  /** -1 means LIMIT is handled by Spark, not by this scan. */
  private final int limit;
  private final String redisKey;

  public SeekScan(String format, List<String> files, StructType readSchema,
      SeekPredicate predicate, Filter[] pushedFilters, Map<String, String> options) {
    this(format, files, readSchema, predicate, pushedFilters, options, -1, null);
  }

  public SeekScan(String format, List<String> files, StructType readSchema,
      SeekPredicate predicate, Filter[] pushedFilters, Map<String, String> options,
      int limit, String redisKey) {
    this.format = format;
    this.files = new ArrayList<>(files);
    this.readSchema = readSchema;
    this.predicate = predicate;
    this.pushedFilters = pushedFilters;
    this.options = options;
    this.limit = limit;
    this.redisKey = redisKey;
  }

  @Override
  public StructType readSchema() {
    return readSchema;
  }

  @Override
  public String description() {
    return "SeekScan(format=" + format + ", files=" + files.size()
        + ", predicate=" + PredicateEvaluator.describe(predicate)
        + ", pushedFilters=" + pushedFilters.length
        + (limit >= 0 ? ", limit=" + limit + ", redisKey=" + redisKey : "") + ")";
  }

  @Override
  public Batch toBatch() {
    return new SeekBatch();
  }

  private final class SeekBatch implements Batch {

    @Override
    public InputPartition[] planInputPartitions() {
      Configuration conf = ConfUtils.hadoopConf(options);
      Map<String, String> confMap = ConfUtils.confToMap(conf);
      // Resolve on the driver (SparkConf lives here) and ship the decision to executors.
      confMap.put(ConfUtils.IGNORE_CASE_OPTION, String.valueOf(ConfUtils.ignoreCase(options)));
      if (limit >= 0) {
        // Resolve all limit settings on the driver and ship them as plain strings, so executors
        // never need a SparkSession or a Redisson config of their own.
        confMap.put(ConfUtils.REDIS_ADDRESS_OPTION, ConfUtils.redisAddress(options));
        confMap.put(ConfUtils.REDIS_PASSWORD_OPTION, ConfUtils.redisPassword(options));
        confMap.put(ConfUtils.REDIS_DB_OPTION, String.valueOf(ConfUtils.redisDatabase(options)));
        confMap.put(ConfUtils.REDIS_POOL_OPTION, String.valueOf(ConfUtils.redisPoolSize(options)));
        confMap.put(ConfUtils.REDIS_BATCH_OPTION, String.valueOf(ConfUtils.redisBatch(options)));
        confMap.put(ConfUtils.REDIS_KEY_OPTION, redisKey);
      }
      InputPartition[] partitions = new InputPartition[files.size()];
      for (int i = 0; i < files.size(); i++) {
        partitions[i] = new SeekInputPartition(files.get(i), format, predicate, readSchema,
            confMap, limit);
      }
      return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      return new SeekPartitionReaderFactory();
    }
  }
}
