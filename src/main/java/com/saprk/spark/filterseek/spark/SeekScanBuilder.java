package com.saprk.spark.filterseek.spark;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import com.saprk.spark.filterseek.predicate.SeekPredicate;

/** Accepts only =, IN, LIKE and range filters; everything else is left to Spark. */
public final class SeekScanBuilder implements ScanBuilder,
    SupportsPushDownFilters, SupportsPushDownRequiredColumns {

  private final StructType tableSchema;
  private final String format;
  private final List<String> files;
  private final Map<String, String> options;
  private final SeekPredicate sqlPredicate;

  private StructType requiredSchema;
  private SeekPredicate predicate;
  private Filter[] acceptedFilters = new Filter[0];
  private Filter[] remainingFilters = new Filter[0];

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
  public Scan build() {
    return new SeekScan(format, files, requiredSchema, predicate, acceptedFilters, options);
  }
}
