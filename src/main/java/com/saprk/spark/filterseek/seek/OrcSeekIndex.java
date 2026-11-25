package com.saprk.spark.filterseek.seek;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DateColumnStatistics;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;

import com.saprk.spark.filterseek.predicate.PredicateEvaluator;
import com.saprk.spark.filterseek.predicate.PruningEvaluator;
import com.saprk.spark.filterseek.predicate.SeekPredicate;
import com.saprk.spark.filterseek.util.ConfUtils;
import com.saprk.spark.filterseek.util.SchemaUtils;

/**
 * ORC phase 1. Only filter columns are included in the RecordReader. Stripe statistics prune
 * whole stripes before any column page is read; surviving rows are scanned with a vectorised
 * batch and converted into (stripe, row-in-stripe, row-in-file) positions.
 */
public final class OrcSeekIndex implements SeekIndex {

  private static final int BATCH_SIZE = 4096;

  @Override
  public SeekPlan build(String file, StructType tableSchema, SeekPredicate predicate,
      Map<String, String> confMap) throws IOException {
    Configuration conf = ConfUtils.hadoopConf(confMap);
    boolean ignoreCase = ConfUtils.ignoreCase(confMap);
    PredicateEvaluator evaluator = PredicateEvaluator.compile(predicate, ignoreCase);
    Reader reader = OrcFile.createReader(new Path(file), OrcFile.readerOptions(conf));
    TypeDescription schema = reader.getSchema();
    List<String> names = schema.getFieldNames();
    Set<String> filterColumns = predicate.referencedColumns();

    boolean[] include = new boolean[schema.getMaximumId() + 1];
    include[0] = true;
    for (String column : filterColumns) {
      int index = indexOf(names, column);
      if (index < 0) {
        throw new IllegalArgumentException("Filter column '" + column + "' not found in ORC schema");
      }
      include[schema.getChildren().get(index).getId()] = true;
    }

    List<StripeInformation> stripes = reader.getStripes();
    List<StripeStatistics> stripeStats = reader.getStripeStatistics();
    List<SeekHit> hits = new ArrayList<>();
    long blocksPruned = 0;
    long rowsScanned = 0;
    long stripeStart = 0;

    try (RecordReader rows = reader.rows(reader.options().include(include))) {
      VectorizedRowBatch batch = schema.createRowBatch(BATCH_SIZE);
      for (int stripeIndex = 0; stripeIndex < stripes.size(); stripeIndex++) {
        long stripeRows = stripes.get(stripeIndex).getNumberOfRows();
        PruningEvaluator.ColumnStatsProvider stats = new OrcColumnStatsProvider(
            stripeStats.get(stripeIndex), schema, names);
        if (!PruningEvaluator.mayMatch(predicate, stats, ignoreCase)) {
          blocksPruned++;
          stripeStart += stripeRows;
          continue;
        }

        rows.seekToRow(stripeStart);
        long rowInStripe = 0;
        long remaining = stripeRows;
        while (remaining > 0 && rows.nextBatch(batch)) {
          int count = (int) Math.min(remaining, batch.size);
          for (int r = 0; r < count; r++) {
            Map<String, Object> values = extract(batch, r, filterColumns, names, tableSchema);
            if (evaluator.matches(values)) {
              long globalRow = stripeStart + rowInStripe + r;
              hits.add(new SeekHit(file, stripeIndex, rowInStripe + r, globalRow));
            }
          }
          rowInStripe += count;
          remaining -= count;
          rowsScanned += count;
        }
        stripeStart += stripeRows;
      }
    }
    return new SeekPlan(file, "orc", hits, stripes.size(), blocksPruned, rowsScanned);
  }

  private static Map<String, Object> extract(VectorizedRowBatch batch, int row,
      Set<String> columns, List<String> names, StructType tableSchema) {
    Map<String, Object> values = new HashMap<>();
    for (String column : columns) {
      int index = indexOf(names, column);
      ColumnVector vector = batch.cols[index];
      int i = vector.isRepeating ? 0 : row;
      Object value = vector.isNull[i] ? null
          : value(vector, i, SchemaUtils.typeOf(tableSchema, column));
      values.put(column, value);
    }
    return values;
  }

  public static Object value(ColumnVector vector, int i, DataType type) {
    if (vector instanceof LongColumnVector) {
      long raw = ((LongColumnVector) vector).vector[i];
      if (type != null && type.equals(DataTypes.BooleanType)) {
        return raw != 0;
      }
      if (type != null && (type.equals(DataTypes.IntegerType) || type.equals(DataTypes.DateType)
          || type.equals(DataTypes.ShortType) || type.equals(DataTypes.ByteType))) {
        return (int) raw;
      }
      return raw;
    }
    if (vector instanceof DoubleColumnVector) {
      double raw = ((DoubleColumnVector) vector).vector[i];
      return type != null && type.equals(DataTypes.FloatType) ? (float) raw : raw;
    }
    if (vector instanceof BytesColumnVector) {
      return ((BytesColumnVector) vector).toString(i);
    }
    if (vector instanceof TimestampColumnVector) {
      TimestampColumnVector ts = (TimestampColumnVector) vector;
      return ts.getTime(i) * 1000L + ts.getNanos(i) / 1000L;
    }
    StringBuilder text = new StringBuilder();
    vector.stringifyValue(text, i);
    String rendered = text.toString();
    if (type instanceof DecimalType) {
      return new BigDecimal(rendered);
    }
    return rendered;
  }

  private static int indexOf(List<String> names, String column) {
    for (int i = 0; i < names.size(); i++) {
      if (names.get(i).toLowerCase(Locale.ROOT).equals(column)) {
        return i;
      }
    }
    return -1;
  }

  /** ORC stripe min/max statistics by column id. */
  private static final class OrcColumnStatsProvider implements PruningEvaluator.ColumnStatsProvider {
    private final StripeStatistics stripeStatistics;
    private final TypeDescription schema;
    private final List<String> names;

    OrcColumnStatsProvider(StripeStatistics stripeStatistics, TypeDescription schema,
        List<String> names) {
      this.stripeStatistics = stripeStatistics;
      this.schema = schema;
      this.names = names;
    }

    @Override
    public PruningEvaluator.ColumnStats stats(String column) {
      int index = indexOf(names, column);
      if (index < 0) {
        return null;
      }
      ColumnStatistics[] all = stripeStatistics.getColumnStatistics();
      int id = schema.getChildren().get(index).getId();
      if (id >= all.length) {
        return null;
      }
      ColumnStatistics cs = all[id];
      Object min = min(cs);
      Object max = max(cs);
      if (min == null || max == null) {
        return null;
      }
      return new PruningEvaluator.ColumnStats() {
        @Override
        public Object min() {
          return min;
        }

        @Override
        public Object max() {
          return max;
        }

        @Override
        public boolean hasNull() {
          return cs.hasNull();
        }
      };
    }

    private static Object min(ColumnStatistics cs) {
      if (cs instanceof IntegerColumnStatistics) {
        return ((IntegerColumnStatistics) cs).getMinimum();
      }
      if (cs instanceof DoubleColumnStatistics) {
        return ((DoubleColumnStatistics) cs).getMinimum();
      }
      if (cs instanceof StringColumnStatistics) {
        return ((StringColumnStatistics) cs).getMinimum();
      }
      if (cs instanceof DateColumnStatistics) {
        return ((DateColumnStatistics) cs).getMinimumDayOfEpoch();
      }
      if (cs instanceof TimestampColumnStatistics) {
        return ((TimestampColumnStatistics) cs).getMinimumUTC();
      }
      if (cs instanceof DecimalColumnStatistics) {
        return new BigDecimal(((DecimalColumnStatistics) cs).getMinimum().toString());
      }
      return null;
    }

    private static Object max(ColumnStatistics cs) {
      if (cs instanceof IntegerColumnStatistics) {
        return ((IntegerColumnStatistics) cs).getMaximum();
      }
      if (cs instanceof DoubleColumnStatistics) {
        return ((DoubleColumnStatistics) cs).getMaximum();
      }
      if (cs instanceof StringColumnStatistics) {
        return ((StringColumnStatistics) cs).getMaximum();
      }
      if (cs instanceof DateColumnStatistics) {
        return ((DateColumnStatistics) cs).getMaximumDayOfEpoch();
      }
      if (cs instanceof TimestampColumnStatistics) {
        return ((TimestampColumnStatistics) cs).getMaximumUTC();
      }
      if (cs instanceof DecimalColumnStatistics) {
        return new BigDecimal(((DecimalColumnStatistics) cs).getMaximum().toString());
      }
      return null;
    }
  }
}
