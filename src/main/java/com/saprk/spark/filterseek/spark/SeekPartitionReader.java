package com.saprk.spark.filterseek.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.saprk.spark.filterseek.limit.LimitBudget;
import com.saprk.spark.filterseek.limit.RedissonHolder;
import com.saprk.spark.filterseek.reader.OrcRowFetcher;
import com.saprk.spark.filterseek.reader.ParquetRowFetcher;
import com.saprk.spark.filterseek.seek.OrcSeekIndex;
import com.saprk.spark.filterseek.seek.ParquetSeekIndex;
import com.saprk.spark.filterseek.seek.RowFetcher;
import com.saprk.spark.filterseek.seek.SeekIndex;
import com.saprk.spark.filterseek.seek.SeekPlan;
import com.saprk.spark.filterseek.util.ConfUtils;

/** Drives phase 1 (positions) then phase 2 (column fetch) for one file. */
public final class SeekPartitionReader implements PartitionReader<InternalRow> {

  private static final Logger LOG = LoggerFactory.getLogger(SeekPartitionReader.class);

  private final SeekInputPartition partition;
  private final SeekIndex index;
  private RowFetcher fetcher;
  private InternalRow current;
  private boolean initialized;
  private boolean exhausted;
  private final LimitBudget budget;

  public SeekPartitionReader(SeekInputPartition partition) {
    this.partition = partition;
    this.index = "parquet".equals(partition.format())
        ? new ParquetSeekIndex() : new OrcSeekIndex();
    if (partition.limit() >= 0) {
      Map<String, String> conf = partition.conf();
      this.budget = new LimitBudget(
          RedissonHolder.counter(conf.get(ConfUtils.REDIS_ADDRESS_OPTION),
              conf.get(ConfUtils.REDIS_PASSWORD_OPTION),
              parseInt(conf.get(ConfUtils.REDIS_DB_OPTION), 0),
              parseInt(conf.get(ConfUtils.REDIS_POOL_OPTION), ConfUtils.DEFAULT_REDIS_POOL_SIZE),
              conf.get(ConfUtils.REDIS_KEY_OPTION)),
          partition.limit(),
          parseInt(conf.get(ConfUtils.REDIS_BATCH_OPTION), ConfUtils.DEFAULT_REDIS_BATCH));
    } else {
      this.budget = null;
    }
  }

  private static int parseInt(String raw, int fallback) {
    if (raw == null || raw.isEmpty()) {
      return fallback;
    }
    try {
      return Integer.parseInt(raw);
    } catch (NumberFormatException e) {
      return fallback;
    }
  }

  @Override
  public boolean next() throws IOException {
    if (exhausted) {
      return false;
    }
    // Global cap reached: stop before doing any more phase-1 scanning or column fetching.
    // Acquire a permit up front; if this call ends up with no row, refund it locally.
    boolean permitHeld = false;
    if (budget != null) {
      if (!budget.tryAcquire()) {
        exhausted = true;
        closeFetcher();
        LOG.info("Global limit reached, task emitted={}", budget.emitted());
        return false;
      }
      permitHeld = true;
    }
    if (!initialized) {
      initialized = true;
      List<com.saprk.spark.filterseek.seek.SeekHit> hits = null;
      if (partition.predicate() != null) {
        SeekPlan plan = index.build(partition.file(), partition.readSchema(),
            partition.predicate(), partition.conf());
        LOG.info("Phase-1 seek plan: {}", plan);
        if ("true".equalsIgnoreCase(partition.conf().get("seek.dumpPlan"))) {
          for (int block : plan.blocks()) {
            List<com.saprk.spark.filterseek.seek.SeekHit> blockHits = plan.hitsForBlock(block);
            LOG.info("  block={} hits={} sample={}", block, blockHits.size(),
                blockHits.subList(0, Math.min(5, blockHits.size())));
          }
        }
        if (!plan.isEmpty()) {
          hits = new ArrayList<>();
          for (int block : plan.blocks()) {
            hits.addAll(plan.hitsForBlock(block));
          }
          hits.sort((a, b) -> Long.compare(a.globalRowId(), b.globalRowId()));
        } else {
          exhausted = true;
          if (permitHeld) {
            budget.refund();
          }
          return false;
        }
      } else {
        LOG.info("No pushed predicate, doing a plain column-pruned scan of {}", partition.file());
      }
      fetcher = createFetcher(hits);
    }

    current = fetcher.next();
    if (current == null) {
      exhausted = true;
      if (permitHeld) {
        budget.refund();
      }
      return false;
    }
    return true;
  }

  @Override
  public InternalRow get() {
    return current;
  }

  private RowFetcher createFetcher(List<com.saprk.spark.filterseek.seek.SeekHit> hits)
      throws IOException {
    if ("parquet".equals(partition.format())) {
      return new ParquetRowFetcher(partition.file(), partition.readSchema(), hits, partition.conf());
    }
    return new OrcRowFetcher(partition.file(), partition.readSchema(), hits, partition.conf());
  }

  private void closeFetcher() throws IOException {
    if (fetcher != null) {
      fetcher.close();
      fetcher = null;
    }
  }

  @Override
  public void close() throws IOException {
    closeFetcher();
  }
}
