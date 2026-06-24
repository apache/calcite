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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.commands.ProtocolCommand;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

/** Unit tests for {@link KvrocksDataProcess}. */
class KvrocksDataProcessTest {

  @Test void readsStringValuesAsRawRows() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("raw_string")).thenReturn("string");
    Mockito.when(jedis.keys("raw_string"))
        .thenReturn(Collections.singleton("raw_string"));
    Mockito.when(jedis.get("raw_string")).thenReturn("hello");

    List<Object[]> rows =
        new KvrocksDataProcess(jedis, fieldInfo("raw_string", "raw")).read();

    assertEquals(1, rows.size());
    assertArrayEquals(new Object[]{"hello"}, rows.get(0));
  }

  @Test void skipsMissingStringValues() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("raw_string")).thenReturn("string");
    Mockito.when(jedis.keys("raw_string"))
        .thenReturn(Collections.singleton("raw_string"));
    Mockito.when(jedis.get("raw_string")).thenReturn(null);

    List<Object[]> rows =
        new KvrocksDataProcess(jedis, fieldInfo("raw_string", "raw")).read();

    assertEquals(0, rows.size());
  }

  @Test void readsListValuesAsCsvRows() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("csv_list")).thenReturn("list");
    Mockito.when(jedis.lrange("csv_list", 0, -1))
        .thenReturn(Arrays.asList("10:Sales", "20:Marketing"));

    List<Object[]> rows =
        new KvrocksDataProcess(jedis, fieldInfo("csv_list", "csv")).read();

    assertEquals(2, rows.size());
    assertArrayEquals(new Object[]{"10", "Sales"}, rows.get(0));
    assertArrayEquals(new Object[]{"20", "Marketing"}, rows.get(1));
  }

  @Test void padsMissingCsvColumnsWithEmptyString() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("csv_list")).thenReturn("list");
    Mockito.when(jedis.lrange("csv_list", 0, -1))
        .thenReturn(Collections.singletonList("10"));

    List<Object[]> rows =
        new KvrocksDataProcess(jedis, fieldInfo("csv_list", "csv")).read();

    assertArrayEquals(new Object[]{"10", ""}, rows.get(0));
  }

  @Test void readsSetValues() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("raw_set")).thenReturn("set");
    Set<String> members = new HashSet<>(Arrays.asList("user1", "user2"));
    Mockito.when(jedis.smembers("raw_set")).thenReturn(members);

    List<Object[]> rows =
        new KvrocksDataProcess(jedis, fieldInfo("raw_set", "raw")).read();

    assertEquals(2, rows.size());
    assertEquals(members,
        new HashSet<>(
            Arrays.asList(rows.get(0)[0].toString(),
            rows.get(1)[0].toString())));
  }

  @Test void readsSortedSetValues() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("csv_zset")).thenReturn("zset");
    Mockito.when(jedis.zrange("csv_zset", 0, -1))
        .thenReturn(new HashSet<>(Arrays.asList("10:Sales", "20:Marketing")));

    List<Object[]> rows =
        new KvrocksDataProcess(jedis, fieldInfo("csv_zset", "csv")).read();

    assertEquals(2, rows.size());
    Set<String> departments =
        new HashSet<>(Arrays.asList(rows.get(0)[0].toString(), rows.get(1)[0].toString()));
    assertEquals(new HashSet<>(Arrays.asList("10", "20")), departments);
  }

  @Test void readsHashValues() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("json_hash")).thenReturn("hash");
    Mockito.when(jedis.hvals("json_hash"))
        .thenReturn(
            Arrays.asList("{\"DEPTNO\":10,\"NAME\":\"Sales\"}",
            "{\"DEPTNO\":20,\"NAME\":\"Marketing\"}"));

    List<Object[]> rows =
        new KvrocksDataProcess(jedis, fieldInfo("json_hash", "json")).read();

    assertEquals(2, rows.size());
    assertEquals("10", rows.get(0)[0].toString());
    assertEquals("\"Sales\"", rows.get(0)[1].toString());
  }

  @Test void readsStreamEntriesAsJsonRows() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("stream")).thenReturn("stream");
    Map<String, String> fields = new HashMap<>();
    fields.put("DEPTNO", "10");
    fields.put("NAME", "Sales");
    Mockito.when(
        jedis.xrange(eq("stream"), eq((StreamEntryID) null),
        eq((StreamEntryID) null), eq(Integer.MAX_VALUE)))
        .thenReturn(
            Collections.singletonList(
            new StreamEntry(new StreamEntryID("1-0"), fields)));

    List<Object[]> rows =
        new KvrocksDataProcess(jedis, fieldInfo("stream", "json")).read();

    assertEquals(1, rows.size());
    assertEquals("\"10\"", rows.get(0)[0].toString());
    assertEquals("\"Sales\"", rows.get(0)[1].toString());
  }

  @Test void nativeJsonUsesJsonGetCommand() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("native_json")).thenReturn("ReJSON-RL");
    Mockito.when(jedis.sendCommand(any(ProtocolCommand.class), eq("native_json")))
        .thenReturn("{\"DEPTNO\":10,\"NAME\":\"Sales\"}");

    List<Object[]> rows =
        new KvrocksDataProcess(jedis, fieldInfo("native_json", "json")).read();

    assertEquals(1, rows.size());
    assertEquals("10", rows.get(0)[0].toString());
    assertEquals("\"Sales\"", rows.get(0)[1].toString());
  }

  @Test void nativeJsonAcceptsByteArrayResponse() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("native_json")).thenReturn("ReJSON-RL");
    Mockito.when(jedis.sendCommand(any(ProtocolCommand.class), eq("native_json")))
        .thenReturn("{\"DEPTNO\":20,\"NAME\":\"Marketing\"}"
            .getBytes(StandardCharsets.UTF_8));

    List<Object[]> rows =
        new KvrocksDataProcess(jedis, fieldInfo("native_json", "json")).read();

    assertEquals("20", rows.get(0)[0].toString());
    assertEquals("\"Marketing\"", rows.get(0)[1].toString());
  }

  @Test void nativeJsonReturnsNoRowsForNullResponse() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("native_json")).thenReturn("ReJSON-RL");
    Mockito.when(jedis.sendCommand(any(ProtocolCommand.class), eq("native_json")))
        .thenReturn(null);

    List<Object[]> rows =
        new KvrocksDataProcess(jedis, fieldInfo("native_json", "json")).read();

    assertEquals(0, rows.size());
  }

  @Test void jsonMissingMappingReturnsEmptyString() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("json_list")).thenReturn("list");
    Mockito.when(jedis.lrange("json_list", 0, -1))
        .thenReturn(Collections.singletonList("{\"DEPTNO\":10}"));

    List<Object[]> rows =
        new KvrocksDataProcess(jedis, fieldInfo("json_list", "json")).read();

    assertEquals("10", rows.get(0)[0].toString());
    assertEquals("", rows.get(0)[1]);
  }

  @Test void jsonFieldWithoutMappingReturnsEmptyString() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("json_list")).thenReturn("list");
    Mockito.when(jedis.lrange("json_list", 0, -1))
        .thenReturn(Collections.singletonList("{\"DEPTNO\":10}"));

    KvrocksTableFieldInfo info = fieldInfo("json_list", "json");
    info.getFields().get(1).remove("mapping");

    List<Object[]> rows = new KvrocksDataProcess(jedis, info).read();

    assertEquals("10", rows.get(0)[0].toString());
    assertEquals("", rows.get(0)[1]);
  }

  @Test void invalidJsonThrowsClearException() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("json_list")).thenReturn("list");
    Mockito.when(jedis.lrange("json_list", 0, -1))
        .thenReturn(Collections.singletonList("{"));

    KvrocksDataProcess process =
        new KvrocksDataProcess(jedis, fieldInfo("json_list", "json"));

    RuntimeException e = assertThrows(RuntimeException.class, process::read);
    assertEquals("Failed to parse JSON value: {", e.getMessage());
  }

  @Test void bloomThrowsClearUnsupportedException() {
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(jedis.type("members")).thenReturn("MBbloom--");

    KvrocksDataProcess process =
        new KvrocksDataProcess(jedis, fieldInfo("members", "raw"));

    UnsupportedOperationException e =
        assertThrows(UnsupportedOperationException.class, process::read);
    assertEquals("Kvrocks Bloom filters cannot be scanned as relational rows",
        e.getMessage());
  }

  private static KvrocksTableFieldInfo fieldInfo(String tableName,
      String dataFormat) {
    KvrocksTableFieldInfo info = new KvrocksTableFieldInfo();
    info.setTableName(tableName);
    info.setDataFormat(dataFormat);
    info.setFields(fields());
    return info;
  }

  private static List<LinkedHashMap<String, Object>> fields() {
    LinkedHashMap<String, Object> deptno = new LinkedHashMap<>();
    deptno.put("name", "DEPTNO");
    deptno.put("type", "varchar");
    deptno.put("mapping", "DEPTNO");

    LinkedHashMap<String, Object> name = new LinkedHashMap<>();
    name.put("name", "NAME");
    name.put("type", "varchar");
    name.put("mapping", "NAME");

    return Collections.unmodifiableList(java.util.Arrays.asList(deptno, name));
  }
}
