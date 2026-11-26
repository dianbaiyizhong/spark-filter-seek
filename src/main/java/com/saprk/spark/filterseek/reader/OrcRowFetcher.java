package com.saprk.spark.filterseek.reader;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.saprk.spark.filterseek.seek.OrcSeekIndex;
import com.saprk.spark.filterseek.seek.RowFetcher;
import com.saprk.spark.filterseek.seek.SeekHit;
import com.saprk.spark.filterseek.util.ConfUtils;
import com.saprk.spark.filterseek.util.RowUtils;
import com.saprk.spark.filterseek.util.SchemaUtils;

/** ORC phase 2: read only the required columns, seeking directly to matched rows. */
public final class OrcRowFetcher implements RowFetcher {

  private final Reader reader;
  private final RecordReader rows;
  private final boolean[] include;
  private final List<StripeInformation> stripes;
  private final VectorizedRowBatch batch;
  private final List<String> names;
  private final StructType requiredSchema;
  private final int[] columnIndexes;

  private final List<SeekHit> hits;
  private final boolean fullScan;
  private int hitIndex;
  private int fullScanIndex;

  private RecordReader stripeRows;
  private int currentStripe = -1;
  private long batchStartInStripe;
  private boolean batchReady;

  public OrcRowFetcher(String file, StructType requiredSchema, List<SeekHit> hits,
      Map<String, String> confMap) throws IOException {
    Configuration conf = ConfUtils.hadoopConf(confMap);
    this.reader = OrcFile.createReader(new Path(file), OrcFile.readerOptions(conf));
    TypeDescription schema = reader.getSchema();
    this.names = schema.getFieldNames();
    this.requiredSchema = requiredSchema;
    this.columnIndexes = new int[requiredSchema.fields().length];

    boolean[] include = new boolean[schema.getMaximumId() + 1];
    include[0] = true;
    for (int i = 0; i < requiredSchema.fields().length; i++) {
      String field = requiredSchema.fields()[i].name();
      int index = indexOf(names, field);
      if (index < 0) {
        throw new IllegalArgumentException("Required column '" + field + "' not found in ORC file");
      }
      columnIndexes[i] = index;
      include[schema.getChildren().get(index).getId()] = true;
    }

    this.hits = hits;
    this.fullScan = hits == null || hits.isEmpty();
    this.include = include;
    this.stripes = reader.getStripes();
    this.rows = fullScan ? reader.rows(reader.options().include(include)) : null;
    this.batch = schema.createRowBatch(4096);
  }

  @Override
  public InternalRow next() throws IOException {
    if (fullScan) {
      if (batch == null || fullScanIndex >= batch.size) {
        if (!rows.nextBatch(batch)) {
          return null;
        }
        fullScanIndex = 0;
      }
      return convert(batch, fullScanIndex++);
    }

    if (hitIndex >= hits.size()) {
      return null;
    }
    SeekHit hit = hits.get(hitIndex);

    // One reader per stripe, scoped to the stripe's byte range. This avoids ORC's seekToRow(),
    // which throws an NPE on files with bloom-filter indexes (StripePlanner.readRowIndex).
    if (currentStripe != hit.blockIndex()) {
      closeStripeReader();
      StripeInformation stripe = stripes.get(hit.blockIndex());
      stripeRows = reader.rows(reader.options()
          .range(stripe.getOffset(), stripe.getLength())
          .include(include));
      currentStripe = hit.blockIndex();
      batchReady = false;
      batchStartInStripe = 0;
    }

    int offset;
    while (true) {
      if (!batchReady) {
        if (!stripeRows.nextBatch(batch)) {
          return null;
        }
        batchReady = true;
      }
      if (hit.rowInBlock() < batchStartInStripe + batch.size) {
        offset = (int) (hit.rowInBlock() - batchStartInStripe);
        break;
      }
      batchStartInStripe += batch.size;
      batchReady = false;
    }
    hitIndex++;
    return convert(batch, offset);
  }

  private InternalRow convert(VectorizedRowBatch batch, int row) {
    Object[] values = new Object[columnIndexes.length];
    StructField[] fields = requiredSchema.fields();
    for (int i = 0; i < columnIndexes.length; i++) {
      ColumnVector vector = batch.cols[columnIndexes[i]];
      int at = vector.isRepeating ? 0 : row;
      Object raw = vector.isNull[at] ? null
          : OrcSeekIndex.value(vector, at, fields[i].dataType());
      values[i] = RowUtils.internalValue(raw, fields[i].dataType());
    }
    return RowUtils.row(values);
  }

  private static int indexOf(List<String> names, String column) {
    for (int i = 0; i < names.size(); i++) {
      if (names.get(i).toLowerCase(Locale.ROOT).equals(column.toLowerCase(Locale.ROOT))) {
        return i;
      }
    }
    return -1;
  }

  private void closeStripeReader() throws IOException {
    if (stripeRows != null) {
      stripeRows.close();
      stripeRows = null;
    }
  }

  @Override
  public void close() throws IOException {
    closeStripeReader();
    if (rows != null) {
      rows.close();
    }
  }
}
