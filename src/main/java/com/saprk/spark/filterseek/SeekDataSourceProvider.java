package com.saprk.spark.filterseek;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import com.saprk.spark.filterseek.spark.SeekTable;
import com.saprk.spark.filterseek.util.ConfUtils;
import com.saprk.spark.filterseek.util.SchemaUtils;

/**
 * Entry point: {@code spark.read.format("filter-seek").option("path", "...").load()}.
 * Reads ORC/Parquet row groups, returns exact positions for supported filters, then fetches
 * only the requested columns at those positions.
 */
public final class SeekDataSourceProvider implements TableProvider, DataSourceRegister {

  @Override
  public String shortName() {
    return "filter-seek";
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    Map<String, String> map = options.asCaseSensitiveMap();
    try {
      Configuration conf = ConfUtils.hadoopConf(map);
      List<String> files = ConfUtils.expandPaths(map, conf);
      if (files.isEmpty()) {
        throw new IllegalArgumentException("No files found for seek data source");
      }
      String format = ConfUtils.inferFormat(map, files);
      return readSchema(files.get(0), format, conf);
    } catch (IOException e) {
      throw new RuntimeException("Failed to infer seek table schema", e);
    }
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
    Map<String, String> options = new LinkedHashMap<>(properties);
    try {
      Configuration conf = ConfUtils.hadoopConf(options);
      List<String> files = ConfUtils.expandPaths(options, conf);
      String format = ConfUtils.inferFormat(options, files);
      options.put("format", format);
      return new SeekTable("filter-seek", schema, options, files);
    } catch (IOException e) {
      throw new RuntimeException("Failed to resolve seek table files", e);
    }
  }

  private static StructType readSchema(String file, String format, Configuration conf)
      throws IOException {
    Path path = new Path(file);
    if ("orc".equals(format)) {
      TypeDescription orcSchema = OrcFile.createReader(path, OrcFile.readerOptions(conf)).getSchema();
      return SchemaUtils.orcToSpark(orcSchema);
    }
    if ("parquet".equals(format)) {
      try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
        MessageType parquetSchema = reader.getFileMetaData().getSchema();
        return SchemaUtils.parquetToSpark(parquetSchema);
      }
    }
    throw new IllegalArgumentException("Unsupported seek format: " + format
        + ". Expected 'orc' or 'parquet'.");
  }
}
