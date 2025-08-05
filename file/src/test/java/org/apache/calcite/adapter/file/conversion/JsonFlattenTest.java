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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Test;

import static org.apache.calcite.adapter.file.FileAdapterTests.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for JSON flattening functionality.
 */
public class JsonFlattenTest {

  @Test void testJsonFlattening() {
    // Test flattened JSON table access
    // Note: Using flattenSeparator="_" in config
    sql("sales-json-flatten", "select * from NESTED_FLAT")
        .returns("id=1; name=John Doe; address_street=123 Main St; "
            + "address_city=Anytown; address_zip=12345; tags=customer,vip,active",
            "id=2; name=Jane Smith; address_street=456 Oak Ave; "
            + "address_city=Other City; address_zip=67890; tags=customer,new")
        .ok();
  }

  @Test void testJsonFlatteningSpecificColumns() {
    // Test accessing specific flattened columns
    // Note: Using flattenSeparator="_" in config
    sql("sales-json-flatten",
        "select \"id\", \"name\", \"address_city\" from NESTED_FLAT where \"id\" = 1")
        .returns("id=1; name=John Doe; address_city=Anytown")
        .ok();
  }

  @Test void testJsonFlatteningUnit() {
    // Test default flattener now uses "__" separator
    JsonFlattener flattener = new JsonFlattener();

    java.util.Map<String, Object> input = new java.util.LinkedHashMap<>();
    input.put("name", "John");

    java.util.Map<String, Object> address = new java.util.LinkedHashMap<>();
    address.put("street", "123 Main");
    address.put("city", "Anytown");
    input.put("address", address);

    java.util.List<String> tags = java.util.Arrays.asList("a", "b", "c");
    input.put("tags", tags);

    java.util.Map<String, Object> result = flattener.flatten(input);

    assertEquals("John", result.get("name"));
    assertEquals("123 Main", result.get("address__street"));
    assertEquals("Anytown", result.get("address__city"));
    assertEquals("a,b,c", result.get("tags"));
  }

  @Test void testJsonFlatteningCustomSeparator() {
    // Test custom separator (underscore) that works well with Parquet
    JsonFlattener flattener = new JsonFlattener(",", 3, "", "_");

    java.util.Map<String, Object> input = new java.util.LinkedHashMap<>();
    input.put("name", "John");

    java.util.Map<String, Object> address = new java.util.LinkedHashMap<>();
    address.put("street", "123 Main");
    address.put("city", "Anytown");
    input.put("address", address);

    java.util.List<String> tags = java.util.Arrays.asList("a", "b", "c");
    input.put("tags", tags);

    java.util.Map<String, Object> result = flattener.flatten(input);

    assertEquals("John", result.get("name"));
    assertEquals("123 Main", result.get("address_street"));
    assertEquals("Anytown", result.get("address_city"));
    assertEquals("a,b,c", result.get("tags"));
  }
}
