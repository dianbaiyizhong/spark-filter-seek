package com.saprk.spark.filterseek.limit;

/**
 * Batch-oriented global limit budget. One task claims a block of {@code batch} permits from the
 * shared counter with a single atomic round trip, then hands them out locally. The block is
 * clipped to the global cap so the total emitted across all tasks never exceeds {@code limit}.
 *
 * <p>With batch=1000 and limit=10000 there are at most 10 Redis round trips in total, independent
 * of the number of rows actually scanned.
 */
public final class LimitBudget {

  private final Counter counter;
  private final long limit;
  private final int batch;
  private long claimed;   // global value up to which this task has claimed permits
  private long cursor;    // next global sequence number this task will emit
  private long remaining; // un-emitted permits in the current block

  public LimitBudget(Counter counter, long limit, int batch) {
    if (limit < 0) {
      throw new IllegalArgumentException("limit must be >= 0");
    }
    if (batch <= 0) {
      throw new IllegalArgumentException("batch must be > 0");
    }
    this.counter = counter;
    this.limit = limit;
    this.batch = batch;
    this.claimed = 0;
    this.cursor = 0;
    this.remaining = 0;
  }

  /**
   * Try to reserve one global permit. Returns true while the global limit has not been reached.
   * A Redis call happens only when the local block is exhausted.
   */
  public boolean tryAcquire() {
    if (limit <= 0) {
      return false;
    }
    if (remaining <= 0 && !claimBlock()) {
      return false;
    }
    remaining--;
    cursor++;
    return true;
  }

  /**
   * Give back a permit that was acquired but turned out not to correspond to an emitted row
   * (e.g. the reader reached end-of-file). Only adjusts this task's local block; the Redis counter
   * is untouched, so no other task's sequence is affected.
   */
  public void refund() {
    if (remaining >= 0 && cursor > 0) {
      remaining++;
      cursor--;
    }
  }

  /** Number of rows this task has emitted; exposed for logging only. */
  public long emitted() {
    return cursor;
  }

  private boolean claimBlock() {
    long base = counter.getAndAdd(batch);
    long upper = Math.min(base + batch, limit);
    if (base >= upper) {
      return false;
    }
    claimed = upper;
    remaining = upper - base;
    return true;
  }
}
