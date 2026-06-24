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
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link KvrocksSchemaFactory} argument validation.
 *
 * <p>These tests do not require a running Kvrocks instance.
 */
class KvrocksSchemaFactoryTest {

  private static Map<String, Object> baseOperand() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("host", "localhost");
    operand.put("port", 6666);
    operand.put("database", 0);
    operand.put("tables", Collections.emptyList());
    return operand;
  }

  @Test void missingHostThrows() {
    Map<String, Object> operand = baseOperand();
    operand.remove("host");
    KvrocksSchemaFactory factory = new KvrocksSchemaFactory();
    assertThrows(IllegalArgumentException.class,
        () -> factory.create(null, "kv", operand));
  }

  @Test void missingPortThrows() {
    Map<String, Object> operand = baseOperand();
    operand.remove("port");
    KvrocksSchemaFactory factory = new KvrocksSchemaFactory();
    assertThrows(IllegalArgumentException.class,
        () -> factory.create(null, "kv", operand));
  }

  @Test void missingDatabaseThrows() {
    Map<String, Object> operand = baseOperand();
    operand.remove("database");
    KvrocksSchemaFactory factory = new KvrocksSchemaFactory();
    assertThrows(IllegalArgumentException.class,
        () -> factory.create(null, "kv", operand));
  }

  @Test void missingTablesThrows() {
    Map<String, Object> operand = baseOperand();
    operand.remove("tables");
    KvrocksSchemaFactory factory = new KvrocksSchemaFactory();
    assertThrows(IllegalArgumentException.class,
        () -> factory.create(null, "kv", operand));
  }

  @Test void readsNamespaceToken() {
    Map<String, Object> operand = baseOperand();
    operand.put("password", "administrator-token");
    operand.put("namespace", "namespace-token");

    KvrocksSchema schema =
        (KvrocksSchema) new KvrocksSchemaFactory()
            .create(null, "kv", operand);

    assertEquals("administrator-token", schema.password);
    assertEquals("namespace-token", schema.namespace);
  }
}
