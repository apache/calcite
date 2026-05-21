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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.ProtocolCommand;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

/** Unit tests for {@link KvrocksDataProcess}. */
class KvrocksDataProcessTest {

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
