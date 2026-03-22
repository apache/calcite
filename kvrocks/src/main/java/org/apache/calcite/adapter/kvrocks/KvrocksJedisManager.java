/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.kvrocks;

import org.apache.calcite.util.trace.CalciteTrace;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import static java.util.Objects.requireNonNull;

/**
 * Manages pooled connections to a Kvrocks instance via Jedis.
 *
 * <p>Because Kvrocks speaks the Redis wire protocol, the standard Jedis
 * client works without modification. Connections are pooled per host using
 * a Guava {@link LoadingCache}.
 */
public class KvrocksJedisManager implements AutoCloseable {
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  private final LoadingCache<String, JedisPool> poolCache;
  private final JedisPoolConfig poolConfig;
  private final String host;
  private final int port;
  private final int database;
  private final @Nullable String password;

  KvrocksJedisManager(String host, int port, int database,
      @Nullable String password) {
    this.host = host;
    this.port = port;
    this.database = database;
    this.password = password;

    this.poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL);
    poolConfig.setMaxIdle(GenericObjectPoolConfig.DEFAULT_MAX_IDLE);
    poolConfig.setMinIdle(GenericObjectPoolConfig.DEFAULT_MIN_IDLE);

    this.poolCache = CacheBuilder.newBuilder()
        .removalListener(new PoolRemovalListener())
        .build(CacheLoader.from(this::createPool));
  }

  /** Returns a pooled {@link JedisPool} for the configured host. */
  public JedisPool getJedisPool() {
    return poolCache.getUnchecked(host);
  }

  /** Borrows a single {@link Jedis} connection from the pool. */
  public Jedis getResource() {
    return getJedisPool().getResource();
  }

  private JedisPool createPool() {
    String pwd = (password == null || password.isEmpty()) ? null : password;
    return new JedisPool(poolConfig, host, port,
        Protocol.DEFAULT_TIMEOUT, pwd, database);
  }

  @Override public void close() {
    poolCache.invalidateAll();
  }

  /** Destroys evicted pools so connections are released. */
  private static class PoolRemovalListener
      implements RemovalListener<String, JedisPool> {
    @Override public void onRemoval(
        RemovalNotification<String, JedisPool> notification) {
      try {
        requireNonNull(notification.getValue()).destroy();
      } catch (Exception e) {
        LOGGER.warn("Error destroying JedisPool for {}", notification.getKey(), e);
      }
    }
  }
}
