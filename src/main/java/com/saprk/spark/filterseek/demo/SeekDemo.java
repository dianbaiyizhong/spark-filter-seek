package com.saprk.spark.filterseek.demo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Runnable end-to-end example. Writes a small ORC and Parquet data set, then reads it through the
 * seek data source with Spark SQL filters (Spark Filter pushdown) and with a Druid-parsed SQL
 * passed through the seek.sql option.
 */
public final class SeekDemo {

  private static final String[] REGIONS = {"CN", "US", "CN", "EU", "CN", "US"};
  private static final String[] STATUSES = {"PAID", "NEW", "CLOSED", "PAID"};

  private SeekDemo() {
  }

  public static void main(String[] args) throws Exception {
    run();
  }

  /** 60 values: large enough to expose a per-row linear scan of the IN list. */
  private static String largeInList() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 60; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(i);
    }
    return sb.toString();
  }

  public static DemoResult run() throws Exception {
    SparkSession spark = SparkSession.builder()
        .master("local[2]")
        .appName("spark-filter-seek-demo")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.filterseek.ignoreCase", "true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    try {
      StructType schema = DataTypes.createStructType(new StructField[] {
          DataTypes.createStructField("id", DataTypes.IntegerType, false),
          DataTypes.createStructField("region", DataTypes.StringType, true),
          DataTypes.createStructField("amount", DataTypes.LongType, true),
          DataTypes.createStructField("status", DataTypes.StringType, true)
      });

      List<Row> data = new ArrayList<>();
      long expected = 0;
      for (int i = 0; i < 400; i++) {
        String region = REGIONS[i % REGIONS.length];
        long amount = (i * 7L) % 300L;
        String status = STATUSES[i % STATUSES.length];
        data.add(RowFactory.create(i, region, amount, status));
        if ("CN".equals(region) && amount >= 100 && ("PAID".equals(status) || "NEW".equals(status))) {
          expected++;
        }
      }
      Dataset<Row> source = spark.createDataFrame(data, schema);

      File base = new File("target/seek-demo-data");
      String orcPath = new File(base, "orc").getAbsolutePath();
      String parquetPath = new File(base, "parquet").getAbsolutePath();

      source.write().mode("overwrite").format("orc")
          .option("orc.stripe.size", "65536")
          .save(orcPath);
      source.write().mode("overwrite").format("parquet")
          .option("parquet.block.size", "4096")
          .save(parquetPath);

      Dataset<Row> orc = spark.read().format("filter-seek").option("path", orcPath)
          .option("seek.dumpPlan", "true").load();
      orc.createOrReplaceTempView("seek_orc");
      long orcCount = spark.sql("SELECT id, region, amount, status FROM seek_orc "
          + "WHERE region = 'CN' AND amount >= 100 AND status IN ('PAID', 'NEW')").count();

      Dataset<Row> parquet = spark.read().format("filter-seek").option("path", parquetPath).load();
      parquet.createOrReplaceTempView("seek_parquet");
      long parquetCount = spark.sql("SELECT id, region FROM seek_parquet "
          + "WHERE region LIKE 'C%' AND amount > 150").count();

      long expectedLike = data.stream()
          .filter(r -> r.getString(1).startsWith("C") && r.getLong(2) > 150)
          .count();

      Dataset<Row> druidRead = spark.read().format("filter-seek")
          .option("path", parquetPath)
          .option("seek.sql", "SELECT * FROM t WHERE region = 'US' AND amount >= 50")
          .load();
      long druidCount = druidRead.count();
      long expectedDruid = data.stream()
          .filter(r -> "US".equals(r.getString(1)) && r.getLong(2) >= 50)
          .count();

      String inList = largeInList();
      long expectedIn = data.stream()
          .filter(r -> "CN".equals(r.getString(1)) && r.getLong(2) < 60)
          .count();

      // SparkConf supplies the default: region = 'cn' matches 'CN'.
      String ciSql = "SELECT * FROM t WHERE region = 'cn' AND amount IN (" + inList + ")";
      long confCaseCount = spark.read().format("filter-seek")
          .option("path", parquetPath).option("seek.sql", ciSql).load().count();

      // Per-read option overrides SparkConf: plain comparison becomes case-sensitive.
      long exactCount = spark.read().format("filter-seek")
          .option("path", parquetPath).option("seek.sql", ciSql)
          .option("seek.ignoreCase", "false").load().count();

      // lower(...) forces case-insensitive evaluation for that leaf even when the option is false.
      String foldedSql = "SELECT * FROM t WHERE lower(region) = 'cn' AND amount IN (" + inList + ")";
      long foldedCount = spark.read().format("filter-seek")
          .option("path", parquetPath).option("seek.sql", foldedSql)
          .option("seek.ignoreCase", "false").load().count();

      System.out.println("ORC  Spark-pushdown filter hits : " + orcCount + " (expected " + expected + ")");
      System.out.println("Parquet LIKE filter hits        : " + parquetCount + " (expected " + expectedLike + ")");
      System.out.println("Druid seek.sql filter hits      : " + druidCount + " (expected " + expectedDruid + ")");
      System.out.println("SparkConf ignoreCase hits       : " + confCaseCount + " (expected " + expectedIn + ")");
      System.out.println("Option ignoreCase=false hits    : " + exactCount + " (expected 0)");
      System.out.println("lower() with ignoreCase=false   : " + foldedCount + " (expected " + expectedIn + ")");

      return new DemoResult(orcCount == expected, parquetCount == expectedLike,
          druidCount == expectedDruid, confCaseCount == expectedIn, exactCount == 0,
          foldedCount == expectedIn);
    } finally {
      spark.stop();
    }
  }

  public static final class DemoResult {
    public final boolean orcOk;
    public final boolean parquetOk;
    public final boolean druidOk;
    public final boolean sparkConfIgnoreCaseOk;
    public final boolean optionOverrideOk;
    public final boolean foldedFunctionOk;

    DemoResult(boolean orcOk, boolean parquetOk, boolean druidOk,
        boolean sparkConfIgnoreCaseOk, boolean optionOverrideOk, boolean foldedFunctionOk) {
      this.orcOk = orcOk;
      this.parquetOk = parquetOk;
      this.druidOk = druidOk;
      this.sparkConfIgnoreCaseOk = sparkConfIgnoreCaseOk;
      this.optionOverrideOk = optionOverrideOk;
      this.foldedFunctionOk = foldedFunctionOk;
    }

    public boolean allOk() {
      return orcOk && parquetOk && druidOk && sparkConfIgnoreCaseOk
          && optionOverrideOk && foldedFunctionOk;
    }
  }
}
