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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for GovDataSchemaFactory.
 */
@Tag("unit")
public class GovDataSchemaFactoryTest {

  @Test
  void testCreateSecSchema() {
    GovDataSchemaFactory factory = new GovDataSchemaFactory();
    
    Map<String, Object> operand = new HashMap<>();
    operand.put("dataSource", "sec");
    operand.put("testMode", true);
    operand.put("ephemeralCache", true);
    operand.put("ciks", "AAPL");
    
    // Should not throw and should return non-null schema
    assertNotNull(factory.create(null, "test", operand));
  }

  @Test
  void testDefaultsToSec() {
    GovDataSchemaFactory factory = new GovDataSchemaFactory();
    
    Map<String, Object> operand = new HashMap<>();
    // No dataSource specified - should default to SEC
    operand.put("testMode", true);
    operand.put("ephemeralCache", true);
    operand.put("ciks", "AAPL");
    
    // Should not throw and should return non-null schema (defaulting to SEC)
    assertNotNull(factory.create(null, "test", operand));
  }

  @Test
  void testUnsupportedDataSource() {
    GovDataSchemaFactory factory = new GovDataSchemaFactory();
    
    Map<String, Object> operand = new HashMap<>();
    operand.put("dataSource", "unsupported");
    
    // Should throw IllegalArgumentException for unsupported data source
    assertThrows(IllegalArgumentException.class, () -> 
        factory.create(null, "test", operand));
  }

  @Test
  void testCensusNotImplemented() {
    GovDataSchemaFactory factory = new GovDataSchemaFactory();
    
    Map<String, Object> operand = new HashMap<>();
    operand.put("dataSource", "census");
    
    // Should throw UnsupportedOperationException for future data sources
    assertThrows(UnsupportedOperationException.class, () -> 
        factory.create(null, "test", operand));
  }
}