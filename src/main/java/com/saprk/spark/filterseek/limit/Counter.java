package com.saprk.spark.filterseek.limit;

/**
 * Minimal atomic counter contract needed for global LIMIT. Kept as an interface so the budget
 * logic can be unit-tested without a live Redis server.
 */
public interface Counter {

  /** Atomically adds {@code delta} and returns the value before the add. */
  long getAndAdd(long delta);
}
