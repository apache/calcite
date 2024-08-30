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
package org.apache.calcite.adapter.redis;

import org.apache.calcite.util.trace.CalciteTrace;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.slf4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import static java.util.Objects.requireNonNull;

/**
 * Manages connections to the Redis nodes.
 */
public class RedisJedisManager implements AutoCloseable {
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();
  private final LoadingCache<String, JedisPool> jedisPoolCache;
  private final JedisPoolConfig jedisPoolConfig;

  private final String host;
  private final String password;
  private final int port;
  private final int database;

  public RedisJedisManager(String host, int port, int database, String password) {
    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL);
    jedisPoolConfig.setMaxIdle(GenericObjectPoolConfig.DEFAULT_MAX_IDLE);
    jedisPoolConfig.setMinIdle(GenericObjectPoolConfig.DEFAULT_MIN_IDLE);
    this.host = host;
    this.port = port;
    this.database = database;
    this.password = password;
    this.jedisPoolConfig = jedisPoolConfig;
    this.jedisPoolCache = CacheBuilder.newBuilder()
        .removalListener(new JedisPoolRemovalListener())
        .build(CacheLoader.from(this::createConsumer));
  }

  public JedisPool getJedisPool() {
    requireNonNull(host, "host is null");
    return jedisPoolCache.getUnchecked(host);
  }

  public Jedis getResource() {
    return getJedisPool().getResource();
  }

  private JedisPool createConsumer() {
    String pwd = password;
    if (StringUtils.isEmpty(pwd)) {
      pwd = null;
    }
    return new JedisPool(jedisPoolConfig, host, port, Protocol.DEFAULT_TIMEOUT,
        pwd, database);
  }

  /**
   * JedisPoolRemovalListener for remove elements from cache.
   */
  private static class JedisPoolRemovalListener
      implements RemovalListener<String, JedisPool> {
    @Override public void onRemoval(
        RemovalNotification<String, JedisPool> notification) {
      final JedisPool value = requireNonNull(notification.getValue());
      try {
        value.destroy();
      } catch (Exception e) {
        LOGGER.warn("While destroying JedisPool {}", notification.getKey());
      }
    }
  }

  @Override public void close() {
    jedisPoolCache.invalidateAll();
  }
}
