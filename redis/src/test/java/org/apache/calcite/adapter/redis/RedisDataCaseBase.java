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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * RedisDataTypeTest.
 */
public class RedisDataCaseBase extends RedisCaseBase {
  private JedisPool pool;

  final String[] tableNames = {
      "raw_01", "raw_02", "raw_03", "raw_04", "raw_05",
      "csv_01", "csv_02", "csv_03", "csv_04", "csv_05",
      "json_01", "json_02", "json_03", "json_04", "json_05"
  };

  @BeforeEach
  public void setUp() {
    try {
      JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
      jedisPoolConfig.setMaxTotal(10);
      pool = new JedisPool(jedisPoolConfig,  getRedisServerHost() , getRedisServerPort());

      // Flush all data
      try (Jedis jedis = pool.getResource()) {
        jedis.flushAll();
      }

      } catch (Exception e) {
      throw e;
    }
  }

  public void makeData() {
    try (Jedis jedis = pool.getResource()) {
      jedis.del(tableNames);
      //set string
      jedis.set("raw_01", "123");
      jedis.set("json_01", "{\"DEPTNO\":10,\"NAME\":\"Sales\"}");
      jedis.set("csv_01", "10:Sales");
      //set list
      jedis.lpush("raw_02", "book1", "book2");
      jedis.lpush("json_02", "{\"DEPTNO\":10,\"NAME\":\"Sales1\"}", "{\"DEPTNO\":20,"
          + "\"NAME\":\"Sales2\"}");
      jedis.lpush("csv_02", "10:Sales", "20:Sales");
      //set Set
      jedis.sadd("raw_03", "user1", "user2");
      jedis.sadd("json_03", "{\"DEPTNO\":10,\"NAME\":\"Sales1\"}", "{\"DEPTNO\":20,"
          + "\"NAME\":\"Sales1\"}");
      jedis.sadd("csv_03", "10:Sales", "20:Sales");
      // set sortSet
      jedis.zadd("raw_04", 22, "user3");
      jedis.zadd("raw_04", 24, "user4");
      jedis.zadd("json_04", 1, "{\"DEPTNO\":10,\"NAME\":\"Sales1\"}");
      jedis.zadd("json_04", 2, "{\"DEPTNO\":11,\"NAME\":\"Sales2\"}");
      jedis.zadd("csv_04", 1, "10:Sales");
      jedis.zadd("csv_04", 2, "20:Sales");
      //set map
      Map<String, String> raw_05 = new HashMap<>();
      raw_05.put("stuA", "a1");
      raw_05.put("stuB", "b2");
      jedis.hmset("raw_05", raw_05);

      Map<String, String> json_05 = new HashMap<>();
      json_05.put("stuA", "{\"DEPTNO\":10,\"NAME\":\"stuA\"}");
      json_05.put("stuB", "{\"DEPTNO\":10,\"NAME\":\"stuB\"}");
      jedis.hmset("json_05", json_05);

      Map<String, String> csv_05 = new HashMap<>();
      csv_05.put("stuA", "10:Sales");
      csv_05.put("stuB", "20:Sales");
      jedis.hmset("csv_05", csv_05);
    }
  }

  @AfterEach
  public void shutDown() {
    if (null != pool) {
      pool.destroy();
    }
  }
}
