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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.embedded.RedisServer;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * RedisServerMini for redis dataType's test.
 */
public class RedisMiniServer {
  private static JedisPool pool;
  private static final int PORT = 6379;
  private static final String HOST = "127.0.0.1";

  @BeforeEach
  public void setUp() {
    try {
      RedisServer redisServer = new RedisServer(PORT);
      redisServer.start();
      JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
      jedisPoolConfig.setMaxTotal(10);
      pool = new JedisPool(jedisPoolConfig, HOST, PORT);
      makeData();
      System.out.println("The redis server is started at host: " + HOST + " port: " + PORT);
    } catch (Exception e) {
      assertNotNull(e.getMessage());
    }
  }

  private void makeData() {
    try (Jedis jedis = pool.getResource()) {
      jedis.del("raw_01");
      jedis.del("raw_02");
      jedis.del("raw_03");
      jedis.del("raw_04");
      jedis.del("raw5");
      jedis.del("json_01");
      jedis.del("json_02");
      jedis.del("json_03");
      jedis.del("json_04");
      jedis.del("json_05");
      jedis.del("csv_01");
      jedis.del("csv_02");
      jedis.del("csv_03");
      jedis.del("csv_04");
      jedis.del("csv_05");
      //set string
      jedis.set("raw_01", "123");
      jedis.set("json_01", "{\"DEPTNO\":10,\"NAME\":\"Sales\"}");
      jedis.set("csv_01", "10:Sales");
      //set list
      jedis.lpush("raw_02", "book1");
      jedis.lpush("raw_02", "book2");
      jedis.lpush("json_02", "{\"DEPTNO\":10,\"NAME\":\"Sales1\"}");
      jedis.lpush("json_02", "{\"DEPTNO\":20,\"NAME\":\"Sales2\"}");
      jedis.lpush("csv_02", "10:Sales");
      jedis.lpush("csv_02", "20:Sales");
      //set Set
      jedis.sadd("raw_03", "user1");
      jedis.sadd("raw_03", "user2");
      jedis.sadd("json_03", "{\"DEPTNO\":10,\"NAME\":\"Sales1\"}");
      jedis.sadd("json_03", "{\"DEPTNO\":20,\"NAME\":\"Sales1\"}");
      jedis.sadd("csv_03", "10:Sales");
      jedis.sadd("csv_03", "20:Sales");
      // set sortSet
      jedis.zadd("raw_04", 22, "user3");
      jedis.zadd("raw_04", 24, "user4");
      jedis.zadd("json_04", 1, "{\"DEPTNO\":10,\"NAME\":\"Sales1\"}");
      jedis.zadd("json_04", 2, "{\"DEPTNO\":11,\"NAME\":\"Sales2\"}");
      jedis.zadd("csv_04", 1, "10:Sales");
      jedis.zadd("csv_04", 2, "20:Sales");
      //set map
      Map<String, String> raw5 = new HashMap<>();
      raw5.put("stuA", "a1");
      raw5.put("stuB", "b2");
      jedis.hmset("raw_05", raw5);

      Map<String, String> json5 = new HashMap<>();
      json5.put("stuA", "{\"DEPTNO\":10,\"NAME\":\"stuA\"}");
      json5.put("stuB", "{\"DEPTNO\":10,\"NAME\":\"stuB\"}");
      jedis.hmset("json_05", json5);

      Map<String, String> csv5 = new HashMap<>();
      csv5.put("stuA", "10:Sales");
      csv5.put("stuB", "20:Sales");
      jedis.hmset("csv_05", csv5);
    }
  }

  @EnabledIfEnvironmentVariable(named = "RedisMiniServerEnabled", matches = "true")
  @Test public void redisServerMiniTest() {
  }
}
