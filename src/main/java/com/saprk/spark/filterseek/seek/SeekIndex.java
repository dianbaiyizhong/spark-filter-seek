package com.saprk.spark.filterseek.seek;

import java.io.IOException;
import java.util.Map;

import org.apache.spark.sql.types.StructType;

import com.saprk.spark.filterseek.predicate.SeekPredicate;

/** Phase 1: cheap read of filter columns only, producing exact row positions. */
public interface SeekIndex {

  SeekPlan build(String file, StructType tableSchema, SeekPredicate predicate,
      Map<String, String> conf) throws IOException;
}
