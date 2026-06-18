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

import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.commands.ProtocolCommand;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

/**
 * Documents Kvrocks features that are deliberately not exposed by the
 * adapter.
 *
 * <p>These tests define the current boundary of the adapter. When a feature
 * is implemented, its corresponding test should be replaced by a positive
 * behavior test.
 */
class KvrocksUnsupportedFeaturesTest {

  @Test void hashScanDoesNotExposeFieldNames() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("departments")).thenReturn("hash");
    Mockito.when(jedis.hvals("departments"))
        .thenReturn(Collections.singletonList("Sales"));

    List<Object[]> rows =
        new KvrocksDataProcess(jedis,
            fieldInfo("departments", "raw", field("value", "value"))).read();

    assertEquals(1, rows.size());
    assertEquals("Sales", rows.get(0)[0]);
    Mockito.verify(jedis).hvals("departments");
  }

  @Test void sortedSetScanDoesNotExposeScores() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("ranking")).thenReturn("zset");
    Mockito.when(jedis.zrange("ranking", 0, -1))
        .thenReturn(Collections.singleton("Alice"));

    List<Object[]> rows =
        new KvrocksDataProcess(jedis,
            fieldInfo("ranking", "raw", field("member", "member"))).read();

    assertEquals(1, rows.size());
    assertEquals("Alice", rows.get(0)[0]);
    Mockito.verify(jedis).zrange("ranking", 0, -1);
  }

  @Test void streamScanDoesNotExposeEntryIds() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("events")).thenReturn("stream");
    Map<String, String> values = Collections.singletonMap("NAME", "Sales");
    Mockito.when(
        jedis.xrange(eq("events"), eq((StreamEntryID) null),
            eq((StreamEntryID) null), eq(Integer.MAX_VALUE)))
        .thenReturn(
            Collections.singletonList(
                new StreamEntry(new StreamEntryID("123-0"), values)));

    List<Object[]> rows =
        new KvrocksDataProcess(jedis,
            fieldInfo("events", "json",
                field("ENTRY_ID", "ENTRY_ID"),
                field("NAME", "NAME"))).read();

    assertEquals(1, rows.size());
    assertEquals("", rows.get(0)[0]);
    assertEquals("\"Sales\"", rows.get(0)[1].toString());
  }

  @Test void jsonMappingsDoNotSupportJsonPath() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("department")).thenReturn("ReJSON-RL");
    Mockito.when(jedis.sendCommand(any(ProtocolCommand.class), eq("department")))
        .thenReturn("{\"DEPTNO\":10}");

    List<Object[]> rows =
        new KvrocksDataProcess(jedis,
            fieldInfo("department", "json",
                field("DEPTNO", "$.DEPTNO"))).read();

    assertEquals(1, rows.size());
    assertEquals("", rows.get(0)[0]);
  }

  @Test void tableSupportsOnlyReadOnlyFullScans() {
    KvrocksTable table =
        new KvrocksTable(Mockito.mock(KvrocksSchema.class), "values", null,
            ImmutableMap.of("key", "key"), "raw",
            new KvrocksConfig("localhost", 6666, 0, ""));

    assertTrue(table instanceof ScannableTable);
    assertFalse(table instanceof FilterableTable);
    assertFalse(table instanceof ProjectableFilterableTable);
    assertFalse(table instanceof ModifiableTable);
  }

  @SafeVarargs
  private static KvrocksTableFieldInfo fieldInfo(String tableName,
      String format, LinkedHashMap<String, Object>... fields) {
    KvrocksTableFieldInfo info = new KvrocksTableFieldInfo();
    info.setTableName(tableName);
    info.setDataFormat(format);
    info.setFields(Arrays.asList(fields));
    return info;
  }

  private static LinkedHashMap<String, Object> field(String name,
      String mapping) {
    LinkedHashMap<String, Object> field = new LinkedHashMap<>();
    field.put("name", name);
    field.put("type", "varchar");
    field.put("mapping", mapping);
    return field;
  }
}
