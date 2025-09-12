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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for SecSchemaFactory instantiation.
 */
@Tag("unit")
public class SecSchemaFactoryTest {

  @Test public void testFactoryIsFound() throws Exception {
    // Try to load the factory using the same mechanism Calcite uses
    String factoryClassName = "org.apache.calcite.adapter.govdata.GovDataSchemaFactory";

    // First verify class can be loaded
    Class<?> factoryClass = Class.forName(factoryClassName);
    assertNotNull(factoryClass);
    System.err.println("Factory class loaded: " + factoryClass);

    // Verify it implements SchemaFactory
    assertTrue(SchemaFactory.class.isAssignableFrom(factoryClass));
    System.err.println("Factory implements SchemaFactory: true");

    // Try to instantiate it
    SchemaFactory factory = (SchemaFactory) factoryClass.getDeclaredConstructor().newInstance();
    assertNotNull(factory);
    System.err.println("Factory instance created: " + factory);

    // Try to create a schema with it
    SchemaPlus rootSchema = org.apache.calcite.jdbc.CalciteSchema.createRootSchema(false).plus();
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "/tmp/test-sec");
    operand.put("ciks", new String[]{"_DJIA_CONSTITUENTS"});
    operand.put("autoDownload", false); // Don't actually download for this test

    Schema schema = factory.create(rootSchema, "test", operand);
    assertNotNull(schema);
    System.err.println("Schema created: " + schema);
  }

  @Test public void testFactoryViaModelHandler() throws Exception {
    // This tests if ModelHandler can find the factory
    String factoryClassName = "org.apache.calcite.adapter.govdata.GovDataSchemaFactory";

    // ModelHandler uses this pattern to load factories
    Class<?> clazz = Class.forName(factoryClassName);
    SchemaFactory factory = (SchemaFactory) clazz.getDeclaredConstructor().newInstance();

    assertNotNull(factory);
    System.err.println("Factory loaded via ModelHandler pattern: " + factory);
  }
}
