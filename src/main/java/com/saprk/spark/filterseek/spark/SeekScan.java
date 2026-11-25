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

  public SeekScan(String format, List<String> files, StructType readSchema,
      SeekPredicate predicate, Filter[] pushedFilters, Map<String, String> options) {
    this.format = format;
    this.files = new ArrayList<>(files);
    this.readSchema = readSchema;
    this.predicate = predicate;
    this.pushedFilters = pushedFilters;
    this.options = options;
  }

  @Override
  public StructType readSchema() {
    return readSchema;
  }

  @Override
  public String description() {
    return "SeekScan(format=" + format + ", files=" + files.size()
        + ", predicate=" + PredicateEvaluator.describe(predicate)
        + ", pushedFilters=" + pushedFilters.length + ")";
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
      InputPartition[] partitions = new InputPartition[files.size()];
      for (int i = 0; i < files.size(); i++) {
        partitions[i] = new SeekInputPartition(files.get(i), format, predicate, readSchema, confMap);
      }
      return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      return new SeekPartitionReaderFactory();
    }
  }
}
