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
package org.apache.calcite.adapter.kafka;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KafkaTableFactory}.
 */
public class KafkaTableFactoryTest {
  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7770">
   * Improve checks on KafkaTableFactory operands</a>. The test verifies
   * if specified converter class does not implement the appropriate interface
   * the method throws the appropriate exception. */
  @Test void testKafkaTableFactoryCreateInvalidRowConverterThrows() {
    KafkaTableFactory factory = new KafkaTableFactory();
    SchemaPlus schema = CalciteSchema.createRootSchema(false).plus();
    String invalidConverter = "org.apache.calcite.adapter.kafka.InvalidKafkaOperand";
    Map<String, Object> operand =
        Collections.singletonMap(KafkaTableConstants.SCHEMA_ROW_CONVERTER, invalidConverter);
    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> factory.create(schema, "t", operand, null));
    assertTrue(ex.getMessage()
        .contains("not valid for plugin type org.apache.calcite.adapter.kafka.KafkaRowConverter"));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7770">
   * Improve checks on KafkaTableFactory operands</a>. The test verifies
   * if specified consumer class does not implement the appropriate interface
   * the method throws the appropriate exception. */
  @Test void testKafkaTableFactoryCreateInvalidConsumerThrows() {
    KafkaTableFactory factory = new KafkaTableFactory();
    SchemaPlus schema = CalciteSchema.createRootSchema(false).plus();
    String invalidConsumer = "org.apache.calcite.adapter.kafka.InvalidKafkaConsumer";
    Map<String, Object> operand =
        Collections.singletonMap(KafkaTableConstants.SCHEMA_CUST_CONSUMER, invalidConsumer);
    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> factory.create(schema, "t", operand, null));
    assertInstanceOf(ClassCastException.class, ex.getCause());
  }
}
