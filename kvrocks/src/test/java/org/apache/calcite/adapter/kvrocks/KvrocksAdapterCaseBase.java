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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for the Kvrocks adapter.
 *
 * <p>Validates SQL queries against data populated into a live Kvrocks
 * instance via Docker.
 */
public class KvrocksAdapterCaseBase extends KvrocksDataCaseBase {

  private final String filePath =
      Sources.of(KvrocksAdapterCaseBase.class.getResource("/kvrocks-mix-model.json"))
          .file().getAbsolutePath();

  private String model;

  @SuppressWarnings("unchecked")
  private static final Map<String, Integer> TABLE_ROW_COUNTS = new HashMap<>(15);

  static {
    TABLE_ROW_COUNTS.put("raw_01", 1);
    TABLE_ROW_COUNTS.put("raw_02", 2);
    TABLE_ROW_COUNTS.put("raw_03", 2);
    TABLE_ROW_COUNTS.put("raw_04", 2);
    TABLE_ROW_COUNTS.put("raw_05", 2);
    TABLE_ROW_COUNTS.put("csv_01", 1);
    TABLE_ROW_COUNTS.put("csv_02", 2);
    TABLE_ROW_COUNTS.put("csv_03", 2);
    TABLE_ROW_COUNTS.put("csv_04", 2);
    TABLE_ROW_COUNTS.put("csv_05", 2);
    TABLE_ROW_COUNTS.put("json_01", 1);
    TABLE_ROW_COUNTS.put("json_02", 2);
    TABLE_ROW_COUNTS.put("json_03", 2);
    TABLE_ROW_COUNTS.put("json_04", 2);
    TABLE_ROW_COUNTS.put("json_05", 2);
  }

  @BeforeEach
  @Override public void makeData() {
    super.makeData();
    readModelJson();
  }

  private CalciteAssert.AssertQuery sql(String sql) {
    assertNotNull(model, "model cannot be null");
    return CalciteAssert.model(model)
        .enable(isKvrocksRunning())
        .query(sql);
  }

  private void readModelJson() {
    try {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
      mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
      mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);

      File file = new File(filePath);
      if (file.exists()) {
        JsonNode root = mapper.readTree(file);
        model = root.toString()
            .replace("6666",
                Integer.toString(getKvrocksServerPort()));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read model JSON", e);
    }
  }

  @Test void testKvrocksBySql() {
    assumeTrue(isKvrocksRunning());
    TABLE_ROW_COUNTS.forEach((table, count) -> {
      String query = "SELECT count(*) AS c FROM \"" + table + "\" WHERE true";
      sql(query).returnsUnordered("C=" + count);
    });
  }

  @Test void testSqlWithJoin() {
    assumeTrue(isKvrocksRunning());
    String query = "SELECT a.DEPTNO, b.NAME "
        + "FROM \"csv_01\" a LEFT JOIN \"json_02\" b "
        + "ON a.DEPTNO = b.DEPTNO WHERE true";
    sql(query).returnsUnordered("DEPTNO=10; NAME=\"Sales1\"");
  }
}
