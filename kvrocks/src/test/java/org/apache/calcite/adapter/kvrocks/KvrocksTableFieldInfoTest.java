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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link KvrocksTableFieldInfo}.
 *
 * <p>These tests do not require a running Kvrocks instance.
 */
class KvrocksTableFieldInfoTest {

  @Test void defaultKeyDelimiterIsColon() {
    assertEquals(":", new KvrocksTableFieldInfo().getKeyDelimiter());
  }

  @Test void gettersReflectSetters() {
    KvrocksTableFieldInfo info = new KvrocksTableFieldInfo();
    LinkedHashMap<String, Object> field = new LinkedHashMap<>();
    field.put("name", "DEPTNO");
    field.put("type", "varchar");
    field.put("mapping", 0);
    List<LinkedHashMap<String, Object>> fields =
        Collections.singletonList(field);

    info.setTableName("emp");
    info.setDataFormat("csv");
    info.setFields(fields);
    info.setKeyDelimiter("|");

    assertEquals("emp", info.getTableName());
    assertEquals("csv", info.getDataFormat());
    assertEquals(fields, info.getFields());
    assertEquals("|", info.getKeyDelimiter());
  }
}
