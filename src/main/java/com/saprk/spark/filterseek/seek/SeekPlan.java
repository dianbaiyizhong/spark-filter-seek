package com.saprk.spark.filterseek.seek;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Phase-1 output: matched row positions plus pruning counters for observability. */
public final class SeekPlan implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String file;
  private final String format;
  private final Map<Integer, List<SeekHit>> hitsByBlock;
  private final long blocksTotal;
  private final long blocksPruned;
  private final long rowsScanned;
  private final long hitCount;

  public SeekPlan(String file, String format, List<SeekHit> hits, long blocksTotal,
      long blocksPruned, long rowsScanned) {
    this.file = file;
    this.format = format;
    this.hitsByBlock = new TreeMap<>();
    for (SeekHit hit : hits) {
      hitsByBlock.computeIfAbsent(hit.blockIndex(), k -> new ArrayList<>()).add(hit);
    }
    for (List<SeekHit> blockHits : hitsByBlock.values()) {
      blockHits.sort((a, b) -> Long.compare(a.globalRowId(), b.globalRowId()));
    }
    this.blocksTotal = blocksTotal;
    this.blocksPruned = blocksPruned;
    this.rowsScanned = rowsScanned;
    this.hitCount = hits.size();
  }

  public String file() {
    return file;
  }

  public String format() {
    return format;
  }

  public List<Integer> blocks() {
    return Collections.unmodifiableList(new ArrayList<>(hitsByBlock.keySet()));
  }

  public List<SeekHit> hitsForBlock(int blockIndex) {
    List<SeekHit> hits = hitsByBlock.get(blockIndex);
    return hits == null ? Collections.emptyList() : hits;
  }

  public long hitCount() {
    return hitCount;
  }

  public long blocksTotal() {
    return blocksTotal;
  }

  public long blocksPruned() {
    return blocksPruned;
  }

  public long rowsScanned() {
    return rowsScanned;
  }

  public boolean isEmpty() {
    return hitCount == 0;
  }

  @Override
  public String toString() {
    return "SeekPlan{" + format + " " + file + ", blocks=" + blocksTotal
        + ", pruned=" + blocksPruned + ", rowsScanned=" + rowsScanned
        + ", hits=" + hitCount + '}';
  }
}
