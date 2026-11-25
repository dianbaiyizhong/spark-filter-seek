package com.saprk.spark.filterseek;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.saprk.spark.filterseek.limit.Counter;
import com.saprk.spark.filterseek.limit.LimitBudget;

public class LimitBudgetTest {

  private static final class MemoryCounter implements Counter {
    private final AtomicLong value = new AtomicLong();
    private int calls;

    @Override
    public long getAndAdd(long delta) {
      calls++;
      return value.getAndAdd(delta);
    }
  }

  @Test
  public void neverExceedsGlobalLimit() {
    MemoryCounter shared = new MemoryCounter();
    LimitBudget a = new LimitBudget(shared, 25, 10);
    LimitBudget b = new LimitBudget(shared, 25, 10);
    LimitBudget c = new LimitBudget(shared, 25, 10);
    int total = 0;
    while (a.tryAcquire()) { total++; }
    while (b.tryAcquire()) { total++; }
    while (c.tryAcquire()) { total++; }
    assertEquals(25, total);
  }

  @Test
  public void batchesRedisCalls() {
    MemoryCounter shared = new MemoryCounter();
    LimitBudget budget = new LimitBudget(shared, 1000, 100);
    for (int i = 0; i < 1000; i++) {
      assertTrue(budget.tryAcquire());
    }
    assertEquals("1000 rows / batch 100 = 10 counter calls", 10, shared.calls);
    assertFalse(budget.tryAcquire());
  }

  @Test
  public void refundReturnedPermitIsReusedLocally() {
    MemoryCounter shared = new MemoryCounter();
    LimitBudget budget = new LimitBudget(shared, 5, 5);
    assertTrue(budget.tryAcquire());
    budget.refund();
    int total = 0;
    while (budget.tryAcquire()) { total++; }
    assertEquals(5, total);
    assertEquals("one initial claim plus one exhausted probe", 2, shared.calls);
  }

  @Test
  public void zeroLimitAcquiresNothing() {
    MemoryCounter shared = new MemoryCounter();
    LimitBudget budget = new LimitBudget(shared, 0, 10);
    assertFalse(budget.tryAcquire());
    assertEquals(0, shared.calls);
  }
}
