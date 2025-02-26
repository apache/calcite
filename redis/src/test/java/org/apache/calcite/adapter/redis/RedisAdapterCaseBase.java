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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Sources;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Protocol;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for the {@code org.apache.calcite.adapter.redis} package.
 */
public class RedisAdapterCaseBase extends RedisDataCaseBase {
  /**
   * URL of the "redis-zips" model.
   */
  private final String filePath =
      Sources.of(RedisAdapterCaseBase.class.getResource("/redis-mix-model.json"))
          .file().getAbsolutePath();

  private String model;

  @SuppressWarnings("unchecked")
  private static final Map<String, Integer> TABLE_MAPS = new HashMap(15);

  static {
    TABLE_MAPS.put("raw_01", 1);
    TABLE_MAPS.put("raw_02", 2);
    TABLE_MAPS.put("raw_03", 2);
    TABLE_MAPS.put("raw_04", 2);
    TABLE_MAPS.put("raw_05", 2);
    TABLE_MAPS.put("csv_01", 1);
    TABLE_MAPS.put("csv_02", 2);
    TABLE_MAPS.put("csv_03", 2);
    TABLE_MAPS.put("csv_04", 2);
    TABLE_MAPS.put("csv_05", 2);
    TABLE_MAPS.put("json_01", 1);
    TABLE_MAPS.put("json_02", 2);
    TABLE_MAPS.put("json_03", 2);
    TABLE_MAPS.put("json_04", 2);
    TABLE_MAPS.put("json_05", 2);
  }

  @BeforeEach
  @Override public void makeData() {
    super.makeData();
    readModelByJson();
  }

  /**
   * Whether to run this test.
   */
  private boolean enabled() {
    return CalciteSystemProperty.TEST_REDIS.value();
  }

  /**
   * Creates a query against a data set given by a map.
   */
  private CalciteAssert.AssertQuery sql(String sql) {
    assertNotNull(model, "model cannot be null!");
    return CalciteAssert.model(model)
        .enable(enabled())
        .query(sql);
  }

  private void readModelByJson() {
    String strResult = null;
    try {
      ObjectMapper objMapper = new ObjectMapper();
      objMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
          .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
          .configure(JsonParser.Feature.ALLOW_COMMENTS, true);
      File file = new File(filePath);
      if (file.exists()) {
        JsonNode rootNode = objMapper.readTree(file);
        strResult =
            rootNode.toString().replace(Integer.toString(Protocol.DEFAULT_PORT),
                Integer.toString(getRedisServerPort()));
      }
    } catch (Exception ignored) {
    }
    model = strResult;
  }

  @Test void testRedisBySql() {
    TABLE_MAPS.forEach((table, count) -> {
      String sql = "Select count(*) as c from \"" + table + "\" where true";
      sql(sql).returnsUnordered("C=" + count);
    });
  }

  @Test void testSqlWithJoin() {
    String sql = "Select a.DEPTNO, b.NAME "
        + "from \"csv_01\" a left join \"json_02\" b "
        + "on a.DEPTNO=b.DEPTNO where true";
    sql(sql).returnsUnordered("DEPTNO=10; NAME=\"Sales1\"");
  }
}
