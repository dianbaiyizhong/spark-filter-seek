package com.saprk.spark.filterseek.seek;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.VersionParser;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
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
 * Parquet phase 1. Row-group statistics prune whole groups; for surviving groups only the filter
 * columns' pages are materialised, and each matched row becomes (row group, row-in-group, row-in-file).
 */
public final class ParquetSeekIndex implements SeekIndex {

  @Override
  public SeekPlan build(String file, StructType tableSchema, SeekPredicate predicate,
      Map<String, String> confMap) throws IOException {
    Configuration conf = ConfUtils.hadoopConf(confMap);
    boolean ignoreCase = ConfUtils.ignoreCase(confMap);
    PredicateEvaluator evaluator = PredicateEvaluator.compile(predicate, ignoreCase);
    try (ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(new Path(file), conf))) {
      MessageType schema = reader.getFileMetaData().getSchema();
      List<ColumnDescriptor> descriptors = schema.getColumns();
      List<String> fieldNames = new ArrayList<>();
      for (Type field : schema.getFields()) {
        fieldNames.add(field.getName());
      }
      ParsedVersion version = parseVersion(reader.getFileMetaData().getCreatedBy());

      Set<String> filterColumns = new HashSet<>(predicate.referencedColumns());
      Map<String, ColumnDescriptor> filterDescriptors = new HashMap<>();
      for (String column : filterColumns) {
        ColumnDescriptor descriptor = descriptor(descriptors, column);
        if (descriptor == null) {
          throw new IllegalArgumentException("Filter column '" + column + "' not found in Parquet schema");
        }
        filterDescriptors.put(column, descriptor);
      }

      List<BlockMetaData> blocks = reader.getFooter().getBlocks();
      List<SeekHit> hits = new ArrayList<>();
      long blocksPruned = 0;
      long rowsScanned = 0;
      long rowBase = 0;

      for (int blockIndex = 0; blockIndex < blocks.size(); blockIndex++) {
        BlockMetaData block = blocks.get(blockIndex);
        PruningEvaluator.ColumnStatsProvider stats =
            new ParquetStatsProvider(block, fieldNames, descriptors);
        if (!PruningEvaluator.mayMatch(predicate, stats, ignoreCase)) {
          blocksPruned++;
          rowBase += block.getRowCount();
          continue;
        }

        PageReadStore pages = reader.readRowGroup(blockIndex);
        Map<String, ColumnReaderImpl> readers = new HashMap<>();
        Map<String, ParquetValueConverter> converters = new HashMap<>();
        for (Map.Entry<String, ColumnDescriptor> entry : filterDescriptors.entrySet()) {
          PageReader pageReader = pages.getPageReader(entry.getValue());
          DataType type = SchemaUtils.typeOf(tableSchema, entry.getKey());
          ParquetValueConverter converter = new ParquetValueConverter(
              ParquetValueConverter.kindOf(type));
          converters.put(entry.getKey(), converter);
          readers.put(entry.getKey(), new ColumnReaderImpl(
              entry.getValue(), pageReader, converter, version));
        }

        int rowCount = (int) block.getRowCount();
        for (int row = 0; row < rowCount; row++) {
          Map<String, Object> values = new HashMap<>();
          for (Map.Entry<String, ColumnReaderImpl> entry : readers.entrySet()) {
            ColumnReaderImpl columnReader = entry.getValue();
            ColumnDescriptor descriptor = columnReader.getDescriptor();
            boolean defined = columnReader.getCurrentDefinitionLevel()
                == descriptor.getMaxDefinitionLevel();
            DataType type = SchemaUtils.typeOf(tableSchema, entry.getKey());
            columnReader.writeCurrentValueToConverter();
            values.put(entry.getKey(), defined
                ? value(converters.get(entry.getKey()).value(), type, descriptor) : null);
            columnReader.consume();
          }
          if (evaluator.matches(values)) {
            long globalRow = rowBase + row;
            hits.add(new SeekHit(file, blockIndex, row, globalRow));
          }
        }
        rowsScanned += rowCount;
        rowBase += rowCount;
      }
      return new SeekPlan(file, "parquet", hits, blocks.size(), blocksPruned, rowsScanned);
    }
  }

  public static Object value(Object raw, DataType type, ColumnDescriptor descriptor) {
    if (raw == null) {
      return null;
    }
    if (type == null) {
      return raw;
    }
    if (type.equals(DataTypes.TimestampType)) {
      long millis = raw instanceof Number ? ((Number) raw).longValue() : Long.parseLong(raw.toString());
      PrimitiveType primitive = descriptor.getPrimitiveType();
      return primitive.getOriginalType() == OriginalType.TIMESTAMP_MILLIS ? millis * 1000L : millis;
    }
    if (type instanceof DecimalType) {
      return new BigDecimal(raw.toString());
    }
    if (type.equals(DataTypes.StringType)) {
      return raw.toString();
    }
    return raw;
  }

  private static ColumnDescriptor descriptor(List<ColumnDescriptor> descriptors, String column) {
    for (ColumnDescriptor descriptor : descriptors) {
      if (descriptor.getPath()[0].toLowerCase(Locale.ROOT).equals(column)) {
        return descriptor;
      }
    }
    return null;
  }

  private static ParsedVersion parseVersion(String createdBy) {
    try {
      return VersionParser.parse(createdBy == null ? "parquet-mr version 1.13.1" : createdBy);
    } catch (Exception e) {
      return new ParsedVersion("parquet-mr", "1.13.1", null);
    }
  }

  /** Parquet row-group column statistics, normalised to the same comparable types as literals. */
  private static final class ParquetStatsProvider implements PruningEvaluator.ColumnStatsProvider {
    private final BlockMetaData block;
    private final List<String> fieldNames;
    private final List<ColumnDescriptor> descriptors;

    ParquetStatsProvider(BlockMetaData block, List<String> fieldNames,
        List<ColumnDescriptor> descriptors) {
      this.block = block;
      this.fieldNames = fieldNames;
      this.descriptors = descriptors;
    }

    @Override
    public PruningEvaluator.ColumnStats stats(String column) {
      ColumnDescriptor descriptor = descriptor(descriptors, column);
      if (descriptor == null) {
        return null;
      }
      ColumnChunkMetaData meta = null;
      for (ColumnChunkMetaData candidate : block.getColumns()) {
        if (java.util.Arrays.equals(candidate.getPath().toArray(), descriptor.getPath())) {
          meta = candidate;
          break;
        }
      }
      if (meta == null || meta.getStatistics() == null || !meta.getStatistics().hasNonNullValue()) {
        return null;
      }
      Object min = normalize(meta.getStatistics().genericGetMin(), meta.getPrimitiveType());
      Object max = normalize(meta.getStatistics().genericGetMax(), meta.getPrimitiveType());
      final boolean hasNull = meta.getStatistics().getNumNulls() > 0;
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
          return hasNull;
        }
      };
    }

    private static Object normalize(Object value, PrimitiveType type) {
      if (value == null) {
        return null;
      }
      if (value instanceof Binary) {
        return new String(((Binary) value).getBytes(), StandardCharsets.UTF_8);
      }
      if (value instanceof Number && type.getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
        return ((Number) value).longValue() * 1000L;
      }
      return value;
    }
  }
}
