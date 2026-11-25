package com.saprk.spark.filterseek.spark;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

import com.saprk.spark.filterseek.predicate.SeekPredicate;

/** One input file per Spark task. Phase 1 and phase 2 both run inside the task. */
public final class SeekInputPartition implements InputPartition, Serializable {

  private static final long serialVersionUID = 1L;

  private final String file;
  private final String format;
  private final SeekPredicate predicate;
  private final StructType readSchema;
  private final Map<String, String> conf;
  /** -1 means no pushed limit; all partitions share the same Redis counter otherwise. */
  private final int limit;

  public SeekInputPartition(String file, String format, SeekPredicate predicate,
      StructType readSchema, Map<String, String> conf) {
    this(file, format, predicate, readSchema, conf, -1);
  }

  public SeekInputPartition(String file, String format, SeekPredicate predicate,
      StructType readSchema, Map<String, String> conf, int limit) {
    this.file = file;
    this.format = format;
    this.predicate = predicate;
    this.readSchema = readSchema;
    this.conf = conf;
    this.limit = limit;
  }

  public String file() {
    return file;
  }

  public String format() {
    return format;
  }

  public SeekPredicate predicate() {
    return predicate;
  }

  public StructType readSchema() {
    return readSchema;
  }

  public Map<String, String> conf() {
    return conf;
  }

  public int limit() {
    return limit;
  }

  @Override
  public String toString() {
    return "SeekInputPartition{" + format + " " + file
        + (limit >= 0 ? " sharedLimit=" + limit : "") + '}';
  }
}
