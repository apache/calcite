/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.test;

import org.apache.optiq.model.*;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Unit test for data models.
 */
public class ModelTest {
  private ObjectMapper mapper() {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    return mapper;
  }

  /** Reads a simple schema from a string into objects. */
  @Test public void testRead() throws IOException {
    final ObjectMapper mapper = mapper();
    JsonRoot root = mapper.readValue(
        "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'FoodMart',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'time_by_day',\n"
        + "           columns: [\n"
        + "             {\n"
        + "               name: 'time_id'\n"
        + "             }\n"
        + "           ]\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'sales_fact_1997',\n"
        + "           columns: [\n"
        + "             {\n"
        + "               name: 'time_id'\n"
        + "             }\n"
        + "           ]\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}",
        JsonRoot.class);
    assertEquals("1.0", root.version);
    assertEquals(1, root.schemas.size());
    final JsonMapSchema schema = (JsonMapSchema) root.schemas.get(0);
    assertEquals("FoodMart", schema.name);
    assertEquals(2, schema.tables.size());
    final JsonTable table0 = schema.tables.get(0);
    assertEquals("time_by_day", table0.name);
    final JsonTable table1 = schema.tables.get(1);
    assertEquals("sales_fact_1997", table1.name);
    assertEquals(1, table0.columns.size());
    final JsonColumn column = table0.columns.get(0);
    assertEquals("time_id", column.name);
  }

  /** Reads a simple schema containing JdbcSchema, a sub-type of Schema. */
  @Test public void testSubtype() throws IOException {
    final ObjectMapper mapper = mapper();
    JsonRoot root = mapper.readValue(
        "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       type: 'jdbc',\n"
        + "       name: 'FoodMart',\n"
        + "       jdbcUser: 'u_baz',\n"
        + "       jdbcPassword: 'p_baz',\n"
        + "       jdbcUrl: 'jdbc:baz',\n"
        + "       jdbcCatalog: 'cat_baz',\n"
        + "       jdbcSchema: ''\n"
        + "     }\n"
        + "   ]\n"
        + "}",
        JsonRoot.class);
    assertEquals("1.0", root.version);
    assertEquals(1, root.schemas.size());
    final JsonJdbcSchema schema = (JsonJdbcSchema) root.schemas.get(0);
    assertEquals("FoodMart", schema.name);
  }

  /** Reads a custom schema. */
  @Test public void testCustomSchema() throws IOException {
    final ObjectMapper mapper = mapper();
    JsonRoot root = mapper.readValue(
        "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       type: 'custom',\n"
        + "       name: 'My Custom Schema',\n"
        + "       factory: 'com.acme.MySchemaFactory',\n"
        + "       operand: {a: 'foo', b: [1, 3.5] }\n"
        + "     }\n"
        + "   ]\n"
        + "}",
        JsonRoot.class);
    assertEquals("1.0", root.version);
    assertEquals(1, root.schemas.size());
    final JsonCustomSchema schema = (JsonCustomSchema) root.schemas.get(0);
    assertEquals("My Custom Schema", schema.name);
    assertEquals("com.acme.MySchemaFactory", schema.factory);
    assertEquals("foo", schema.operand.get("a"));
    assertNull(schema.operand.get("c"));
    assertTrue(schema.operand.get("b") instanceof List);
    final List list = (List) schema.operand.get("b");
    assertEquals(2, list.size());
    assertEquals(1, list.get(0));
    assertEquals(3.5, list.get(1));
  }

  /** Tests that an immutable schema in a model cannot contain a
   * materialization. */
  @Test public void testModelImmutableSchemaCannotContainMaterialization()
    throws Exception {
    final OptiqAssert.AssertThat that =
        OptiqAssert.that().withModel(
            "{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'adhoc',\n"
            + "  schemas: [\n"
            + "    {\n"
            + "      name: 'empty'\n"
            + "    },\n"
            + "    {\n"
            + "      name: 'adhoc',\n"
            + "      type: 'custom',\n"
            + "      factory: '"
            + JdbcTest.MySchemaFactory.class.getName()
            + "',\n"
            + "      operand: {\n"
            + "           'tableName': 'ELVIS',\n"
            + "           'mutable': false\n"
            + "      },\n"
            + "      materializations: [\n"
            + "        {\n"
            + "          table: 'v',\n"
            + "          sql: 'values (1)'\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
            + "}");
    that.connectThrows(
        "Cannot define materialization; parent schema 'adhoc' is not a "
        + "SemiMutableSchema");
  }
}

// End ModelTest.java
