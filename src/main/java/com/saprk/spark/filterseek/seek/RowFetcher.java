package com.saprk.spark.filterseek.seek;

import java.io.Closeable;
import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;

/** Phase 2: pull required columns at the positions produced by {@link SeekIndex}. */
public interface RowFetcher extends Closeable {

  /** Returns the next matched row, or null when this fetcher is exhausted. */
  InternalRow next() throws IOException;

  @Override
  void close() throws IOException;
}
