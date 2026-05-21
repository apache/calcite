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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link KvrocksDataType}.
 *
 * <p>These tests do not require a running Kvrocks instance.
 */
class KvrocksDataTypeTest {

  @Test void fromTypeNameReturnsMatchingEnum() {
    assertEquals(KvrocksDataType.STRING,
        KvrocksDataType.fromTypeName("string"));
    assertEquals(KvrocksDataType.HASH,
        KvrocksDataType.fromTypeName("hash"));
    assertEquals(KvrocksDataType.LIST,
        KvrocksDataType.fromTypeName("list"));
    assertEquals(KvrocksDataType.SET,
        KvrocksDataType.fromTypeName("set"));
    assertEquals(KvrocksDataType.SORTED_SET,
        KvrocksDataType.fromTypeName("zset"));
    assertEquals(KvrocksDataType.STREAM,
        KvrocksDataType.fromTypeName("stream"));
    assertEquals(KvrocksDataType.JSON,
        KvrocksDataType.fromTypeName("ReJSON-RL"));
    assertEquals(KvrocksDataType.BLOOM,
        KvrocksDataType.fromTypeName("MBbloom--"));
  }

  @Test void fromTypeNameReturnsNullForUnknown() {
    assertNull(KvrocksDataType.fromTypeName("unknown"));
    assertNull(KvrocksDataType.fromTypeName(""));
    assertNull(KvrocksDataType.fromTypeName("STRING"));
  }

  @Test void getTypeNameReturnsWireValue() {
    assertEquals("string", KvrocksDataType.STRING.getTypeName());
    assertEquals("zset", KvrocksDataType.SORTED_SET.getTypeName());
    assertEquals("ReJSON-RL", KvrocksDataType.JSON.getTypeName());
    assertEquals("MBbloom--", KvrocksDataType.BLOOM.getTypeName());
  }
}
