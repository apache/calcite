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
package org.apache.calcite.adapter.elasticsearch;

import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Map;

/**
 * Test with model.json for ES adapter
 */
@ResourceLock(value = "elasticsearch-scrolls", mode = ResourceAccessMode.READ)
public class ElasticSearchAdapterFromModelFileTest {
  public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

  public static final ImmutableMap<String, String> MODEL =
      ImmutableMap.of("model",
          Sources.of(ElasticSearchAdapterFromModelFileTest.class.getResource("/model.json"))
              .file().getAbsolutePath());

  private static final String INDEX = "ElasticSearchAdapterFromModelFileTest".toLowerCase();

  @BeforeAll
  public static void setupInstance() throws Exception {
    final Map<String, String> mapping =
        ImmutableMap.of("city", "keyword", "state", "keyword", "pop", "long");
    NODE.createIndex(INDEX, mapping);
  }

  @Test void testScan() {
    CalciteAssert.that()
        .with(MODEL)
        .query(String.format("select * from \"%s\"", INDEX))
        .returnsCount(0);
  }

  @Test void testScanWithNonExistingTable() {
    String tableName = INDEX + "01";
    try {
      CalciteAssert.that()
          .with(MODEL)
          .query(String.format("select * from \"%s\"", tableName))
          .returnsCount(0);
    } catch (Exception e) {
      String message = e.getMessage();
      Assertions.assertTrue(message.contains(String.format("Object '%s' not found", tableName)));
    }
  }
}
