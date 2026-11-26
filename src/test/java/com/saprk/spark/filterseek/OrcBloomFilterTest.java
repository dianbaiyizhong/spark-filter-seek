package com.saprk.spark.filterseek;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import com.saprk.spark.filterseek.predicate.SeekPredicate;
import com.saprk.spark.filterseek.reader.OrcRowFetcher;
import com.saprk.spark.filterseek.seek.OrcSeekIndex;
import com.saprk.spark.filterseek.seek.SeekHit;
import com.saprk.spark.filterseek.seek.SeekPlan;
import com.saprk.spark.filterseek.sql.DruidSqlParser;

/**
 * Regression: ORC files with bloom-filter indexes used to throw an NPE inside
 * StripePlanner when the seek path called seekToRow().
 */
public class OrcBloomFilterTest {

  @Test
  public void seekAndFetchOrCWithBloomFilter() throws Exception {
    File dir = new File("target/orc-bloom-test");
    dir.mkdirs();
    String file = new File(dir, "bloom.orc").getAbsolutePath();
    Configuration conf = new Configuration();
    Path path = new Path(file);
    path.getFileSystem(conf).delete(path, false);

    TypeDescription orcSchema = TypeDescription.fromString("struct<id:int,name:string>");
    try (Writer writer = OrcFile.createWriter(path,
        OrcFile.writerOptions(conf).setSchema(orcSchema).bloomFilterColumns("name"))) {
      VectorizedRowBatch batch = orcSchema.createRowBatch(1024);
      LongColumnVector ids = (LongColumnVector) batch.cols[0];
      BytesColumnVector names = (BytesColumnVector) batch.cols[1];
      for (int i = 0; i < 2000; i++) {
        int row = batch.size++;
        ids.vector[row] = i;
        names.setVal(row, ("row" + i).getBytes(StandardCharsets.UTF_8));
        if (batch.size == batch.getMaxSize()) {
          writer.addRowBatch(batch);
          batch.reset();
        }
      }
      if (batch.size > 0) {
        writer.addRowBatch(batch);
      }
    }

    StructType sparkSchema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[] {
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("name", DataTypes.StringType, true)
    });
    SeekPredicate predicate = new DruidSqlParser(sparkSchema).parseExpression("name = 'row1234'");
    SeekPlan plan = new OrcSeekIndex().build(file, sparkSchema, predicate,
        Collections.<String, String>emptyMap());
    assertEquals(1, plan.hitCount());

    List<SeekHit> hits = new ArrayList<>();
    for (int block : plan.blocks()) {
      hits.addAll(plan.hitsForBlock(block));
    }
    try (OrcRowFetcher fetcher = new OrcRowFetcher(file, sparkSchema, hits,
        Collections.<String, String>emptyMap())) {
      InternalRow row = fetcher.next();
      assertNotNull(row);
      assertEquals(1234, row.getInt(0));
      assertEquals("row1234", row.getString(1).toString());
      assertNull(fetcher.next());
    }
  }
}
