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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Base class that populates test data into Kvrocks before each test.
 *
 * <p>Creates 15 tables across 3 data formats (raw, csv, json) x 5 data
 * types (string, list, set, sorted-set, hash).
 */
public class KvrocksDataCaseBase extends KvrocksCaseBase {
  private JedisPool pool;

  final String[] tableNames = {
      "raw_01", "raw_02", "raw_03", "raw_04", "raw_05",
      "csv_01", "csv_02", "csv_03", "csv_04", "csv_05",
      "json_01", "json_02", "json_03", "json_04", "json_05"
  };

  @BeforeEach
  public void setUp() {
    assumeTrue(isKvrocksRunning(), "Kvrocks container is not running");

    JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxTotal(10);
    pool = new JedisPool(config, getKvrocksServerHost(), getKvrocksServerPort());

    try (Jedis jedis = pool.getResource()) {
      jedis.flushAll();
    }
  }

  /** Populates all 15 test tables into Kvrocks. */
  public void makeData() {
    try (Jedis jedis = pool.getResource()) {
      jedis.del(tableNames);

      // STRING type
      jedis.set("raw_01", "123");
      jedis.set("json_01", "{\"DEPTNO\":10,\"NAME\":\"Sales\"}");
      jedis.set("csv_01", "10:Sales");

      // LIST type
      jedis.lpush("raw_02", "book1", "book2");
      jedis.lpush("json_02",
          "{\"DEPTNO\":10,\"NAME\":\"Sales1\"}",
          "{\"DEPTNO\":20,\"NAME\":\"Sales2\"}");
      jedis.lpush("csv_02", "10:Sales", "20:Sales");

      // SET type
      jedis.sadd("raw_03", "user1", "user2");
      jedis.sadd("json_03",
          "{\"DEPTNO\":10,\"NAME\":\"Sales1\"}",
          "{\"DEPTNO\":20,\"NAME\":\"Sales1\"}");
      jedis.sadd("csv_03", "10:Sales", "20:Sales");

      // SORTED SET type
      jedis.zadd("raw_04", 22, "user3");
      jedis.zadd("raw_04", 24, "user4");
      jedis.zadd("json_04", 1, "{\"DEPTNO\":10,\"NAME\":\"Sales1\"}");
      jedis.zadd("json_04", 2, "{\"DEPTNO\":11,\"NAME\":\"Sales2\"}");
      jedis.zadd("csv_04", 1, "10:Sales");
      jedis.zadd("csv_04", 2, "20:Sales");

      // HASH type
      Map<String, String> rawHash = new HashMap<>();
      rawHash.put("stuA", "a1");
      rawHash.put("stuB", "b2");
      jedis.hmset("raw_05", rawHash);

      Map<String, String> jsonHash = new HashMap<>();
      jsonHash.put("stuA", "{\"DEPTNO\":10,\"NAME\":\"stuA\"}");
      jsonHash.put("stuB", "{\"DEPTNO\":10,\"NAME\":\"stuB\"}");
      jedis.hmset("json_05", jsonHash);

      Map<String, String> csvHash = new HashMap<>();
      csvHash.put("stuA", "10:Sales");
      csvHash.put("stuB", "20:Sales");
      jedis.hmset("csv_05", csvHash);
    }
  }

  @AfterEach
  public void tearDown() {
    if (pool != null) {
      pool.destroy();
    }
  }
}
