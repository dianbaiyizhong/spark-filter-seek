package com.saprk.spark.filterseek.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Options, path expansion and Hadoop configuration helpers shared by driver and executors. */
public final class ConfUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Per-read override. Wins over the SparkConf key below. */
  public static final String IGNORE_CASE_OPTION = "seek.ignoreCase";
  /** Session-level setting: spark.conf.set("spark.filterseek.ignoreCase", "true|false"). */
  public static final String IGNORE_CASE_SPARK_CONF = "spark.filterseek.ignoreCase";
  public static final boolean DEFAULT_IGNORE_CASE = true;

  /** Redis address for global LIMIT control, e.g. redis://127.0.0.1:6379. */
  public static final String REDIS_ADDRESS_OPTION = "seek.redis.address";
  public static final String REDIS_ADDRESS_SPARK_CONF = "spark.filterseek.redis.address";
  public static final String REDIS_PASSWORD_OPTION = "seek.redis.password";
  public static final String REDIS_PASSWORD_SPARK_CONF = "spark.filterseek.redis.password";
  public static final String REDIS_DB_OPTION = "seek.redis.database";
  public static final String REDIS_DB_SPARK_CONF = "spark.filterseek.redis.database";
  /** How many rows a task takes from Redis per round trip. */
  public static final String REDIS_BATCH_OPTION = "seek.redis.batch";
  public static final String REDIS_BATCH_SPARK_CONF = "spark.filterseek.redis.batch";
  public static final int DEFAULT_REDIS_BATCH = 1000;
  /** Shared Redis key name for the counter; auto-generated per query if absent. */
  public static final String REDIS_KEY_OPTION = "seek.redis.key";
  public static final String REDIS_KEY_SPARK_CONF = "spark.filterseek.redis.key";
  /** TTL of the counter key in seconds, to avoid leaks on cancelled queries. */
  public static final String REDIS_TTL_OPTION = "seek.redis.ttlSeconds";
  public static final String REDIS_TTL_SPARK_CONF = "spark.filterseek.redis.ttlSeconds";
  public static final int DEFAULT_REDIS_TTL_SECONDS = 3600;
  /** Max connections per Redisson client in one JVM; bounds total connections on executors. */
  public static final String REDIS_POOL_OPTION = "seek.redis.poolSize";
  public static final String REDIS_POOL_SPARK_CONF = "spark.filterseek.redis.poolSize";
  public static final int DEFAULT_REDIS_POOL_SIZE = 8;

  private ConfUtils() {
  }

  /**
   * Resolve case-sensitivity on the driver. Option first, then SparkConf, defaulting to true.
   * The resolved value is injected into the partition conf map so executors never need a
   * SparkSession.
   */
  public static boolean ignoreCase(Map<String, String> options) {
    String raw = options == null ? null : options.get(IGNORE_CASE_OPTION);
    if (isBlank(raw)) {
      raw = sparkConfValue(IGNORE_CASE_SPARK_CONF);
    }
    if (isBlank(raw)) {
      return DEFAULT_IGNORE_CASE;
    }
    return Boolean.parseBoolean(raw.trim());
  }

  /** Generic resolution: per-read option, then SparkConf, then default. */
  private static String resolve(Map<String, String> options, String optionKey,
      String sparkConfKey, String defaultValue) {
    String raw = options == null ? null : options.get(optionKey);
    if (isBlank(raw)) {
      raw = sparkConfValue(sparkConfKey);
    }
    if (isBlank(raw)) {
      return defaultValue;
    }
    return raw.trim();
  }

  private static int resolveInt(Map<String, String> options, String optionKey,
      String sparkConfKey, int defaultValue) {
    String raw = resolve(options, optionKey, sparkConfKey, null);
    if (raw == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid integer for " + optionKey + ": " + raw, e);
    }
  }

  public static String redisAddress(Map<String, String> options) {
    return resolve(options, REDIS_ADDRESS_OPTION, REDIS_ADDRESS_SPARK_CONF, null);
  }

  public static String redisPassword(Map<String, String> options) {
    return resolve(options, REDIS_PASSWORD_OPTION, REDIS_PASSWORD_SPARK_CONF, null);
  }

  public static int redisDatabase(Map<String, String> options) {
    return resolveInt(options, REDIS_DB_OPTION, REDIS_DB_SPARK_CONF, 0);
  }

  public static int redisBatch(Map<String, String> options) {
    int batch = resolveInt(options, REDIS_BATCH_OPTION, REDIS_BATCH_SPARK_CONF,
        DEFAULT_REDIS_BATCH);
    return batch <= 0 ? DEFAULT_REDIS_BATCH : batch;
  }

  public static String redisKey(Map<String, String> options) {
    return resolve(options, REDIS_KEY_OPTION, REDIS_KEY_SPARK_CONF, null);
  }

  public static int redisTtlSeconds(Map<String, String> options) {
    int ttl = resolveInt(options, REDIS_TTL_OPTION, REDIS_TTL_SPARK_CONF,
        DEFAULT_REDIS_TTL_SECONDS);
    return ttl <= 0 ? DEFAULT_REDIS_TTL_SECONDS : ttl;
  }

  public static int redisPoolSize(Map<String, String> options) {
    int size = resolveInt(options, REDIS_POOL_OPTION, REDIS_POOL_SPARK_CONF,
        DEFAULT_REDIS_POOL_SIZE);
    return size <= 0 ? DEFAULT_REDIS_POOL_SIZE : size;
  }

  private static String sparkConfValue(String key) {
    try {
      scala.Option<SparkSession> active = SparkSession.getActiveSession();
      if (active.isDefined()) {
        return active.get().sparkContext().getConf().get(key, null);
      }
    } catch (Throwable ignored) {
      // No active session (unit tests, non-Spark callers): fall back to the default.
    }
    return null;
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  public static Configuration hadoopConf(Map<String, String> options) {
    Configuration conf = new Configuration();
    if (options != null) {
      for (Map.Entry<String, String> entry : options.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (value == null) {
          continue;
        }
        if (key.toLowerCase(Locale.ROOT).startsWith("hadoop.")) {
          conf.set(key.substring("hadoop.".length()), value);
        } else {
          conf.set(key, value);
        }
      }
    }
    return conf;
  }

  /** Expand directories and globs into a stable, sorted list of concrete files. */
  public static List<String> expandPaths(Map<String, String> options, Configuration conf)
      throws IOException {
    List<String> raw = rawPaths(options);
    List<String> files = new ArrayList<>();
    for (String path : raw) {
      Path hadoopPath = new Path(path);
      FileSystem fs = hadoopPath.getFileSystem(conf);
      FileStatus[] statuses = fs.globStatus(hadoopPath);
      if (statuses == null) {
        throw new IOException("Path does not exist: " + path);
      }
      for (FileStatus status : statuses) {
        collect(fs, status, files);
      }
    }
    java.util.Collections.sort(files);
    return files;
  }

  private static void collect(FileSystem fs, FileStatus status, List<String> files)
      throws IOException {
    if (status.isDirectory()) {
      for (FileStatus child : fs.listStatus(status.getPath())) {
        collect(fs, child, files);
      }
    } else if (!status.getPath().getName().startsWith("_")) {
      files.add(status.getPath().toString());
    }
  }

  public static List<String> rawPaths(Map<String, String> options) {
    if (options == null) {
      throw new IllegalArgumentException("Missing path option");
    }
    String single = options.get("path");
    if (single != null && !single.trim().isEmpty()) {
      return Arrays.asList(single);
    }
    String multiple = options.get("paths");
    if (multiple != null && !multiple.trim().isEmpty()) {
      try {
        String[] paths = MAPPER.readValue(multiple, String[].class);
        return Arrays.asList(paths);
      } catch (Exception e) {
        throw new IllegalArgumentException("Cannot parse paths option as a JSON array: " + multiple, e);
      }
    }
    throw new IllegalArgumentException("Either 'path' or 'paths' option is required");
  }

  /** Resolve format from the explicit option or from the file extension. */
  public static String inferFormat(Map<String, String> options, List<String> paths) {
    if (options != null) {
      String format = options.get("format");
      if (format != null && !format.trim().isEmpty()) {
        return format.toLowerCase(Locale.ROOT);
      }
    }
    for (String path : paths) {
      String lower = path.toLowerCase(Locale.ROOT);
      if (lower.endsWith(".orc")) {
        return "orc";
      }
      if (lower.endsWith(".parquet")) {
        return "parquet";
      }
    }
    throw new IllegalArgumentException("Cannot infer format; set option 'format' to 'orc' or 'parquet'");
  }

  /** Stable string map used to rebuild Configuration inside an executor. */
  public static Map<String, String> confToMap(Configuration conf) {
    Map<String, String> result = new java.util.HashMap<>();
    for (Map.Entry<String, String> entry : conf) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
