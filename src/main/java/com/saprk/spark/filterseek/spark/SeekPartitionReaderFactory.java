package com.saprk.spark.filterseek.spark;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/** Creates the two-phase reader for a seek input partition. */
public final class SeekPartitionReaderFactory implements PartitionReaderFactory {

  private static final long serialVersionUID = 1L;

  @Override
  @SuppressWarnings("rawtypes")
  public PartitionReader createReader(InputPartition partition) {
    return new SeekPartitionReader((SeekInputPartition) partition);
  }
}
