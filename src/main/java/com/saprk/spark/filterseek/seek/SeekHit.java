package com.saprk.spark.filterseek.seek;

import java.io.Serializable;

/** One matched row located down to stripe/row-group granularity. */
public final class SeekHit implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String file;
  private final int blockIndex;
  private final long rowInBlock;
  private final long globalRowId;

  public SeekHit(String file, int blockIndex, long rowInBlock, long globalRowId) {
    this.file = file;
    this.blockIndex = blockIndex;
    this.rowInBlock = rowInBlock;
    this.globalRowId = globalRowId;
  }

  public String file() {
    return file;
  }

  /** ORC stripe ordinal or Parquet row-group ordinal. */
  public int blockIndex() {
    return blockIndex;
  }

  /** Row ordinal inside the stripe/row group. */
  public long rowInBlock() {
    return rowInBlock;
  }

  /** Row ordinal inside the file, used to order hits across blocks. */
  public long globalRowId() {
    return globalRowId;
  }

  @Override
  public String toString() {
    return file + "#block=" + blockIndex + ",row=" + rowInBlock + ",rid=" + globalRowId;
  }
}
