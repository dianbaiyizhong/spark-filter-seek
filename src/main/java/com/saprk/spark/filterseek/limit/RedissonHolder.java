package com.saprk.spark.filterseek.limit;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * Process-wide Redisson clients. A Spark executor JVM runs many tasks; creating a client per task
 * would explode the connection count. Clients are cached by their connection parameters so all
 * tasks on one JVM share a single pool.
 */
public final class RedissonHolder {

  private static final ConcurrentMap<String, RedissonClient> CLIENTS = new ConcurrentHashMap<>();

  private RedissonHolder() {
  }

  /**
   * Reset a query's counter to 0 (the next sequence number to hand out) and set its TTL. Called
   * once on the driver before any task starts. A fresh key per query avoids cross-query clashes.
   */
  public static void init(String address, String password, int database, int poolSize,
      String key, int ttlSeconds) {
    RAtomicLong counter = client(address, password, database, poolSize).getAtomicLong(key);
    counter.set(0L);
    counter.expire(java.time.Instant.now().plusSeconds(ttlSeconds));
  }

  /** Get a shared counter; all callers with the same key see the same Redis atomic long. */
  public static Counter counter(String address, String password, int database, int poolSize,
      String key) {
    final RAtomicLong atomic = client(address, password, database, poolSize).getAtomicLong(key);
    return new Counter() {
      @Override
      public long getAndAdd(long delta) {
        return atomic.getAndAdd(delta);
      }
    };
  }

  /** Cached per JVM; the mapping function runs at most once per distinct parameter set. */
  private static RedissonClient client(String address, String password, int database,
      int poolSize) {
    final String cacheKey = address + "|" + String.valueOf(password) + "|" + database
        + "|" + poolSize;
    RedissonClient existing = CLIENTS.get(cacheKey);
    if (existing != null) {
      return existing;
    }
    synchronized (RedissonHolder.class) {
      existing = CLIENTS.get(cacheKey);
      if (existing != null) {
        return existing;
      }
      Config config = new Config();
      config.useSingleServer()
          .setAddress(address)
          .setDatabase(database)
          .setConnectionPoolSize(poolSize)
          .setConnectionMinimumIdleSize(Math.min(2, poolSize));
      if (password != null && !password.isEmpty()) {
        config.useSingleServer().setPassword(password);
      }
      RedissonClient created = Redisson.create(config);
      CLIENTS.put(cacheKey, created);
      return created;
    }
  }
}
