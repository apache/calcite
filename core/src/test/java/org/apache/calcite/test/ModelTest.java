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
package org.apache.calcite.test;

import org.apache.calcite.model.JsonColumn;
import org.apache.calcite.model.JsonCustomSchema;
import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.model.JsonJdbcSchema;
import org.apache.calcite.model.JsonLattice;
import org.apache.calcite.model.JsonMapSchema;
import org.apache.calcite.model.JsonRoot;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.model.JsonTypeAttribute;
import org.apache.calcite.model.JsonView;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
        + "       types: [\n"
        + "         {\n"
        + "           name: 'mytype1',\n"
        + "           attributes: [\n"
        + "             {\n"
        + "               name: 'f1',\n"
        + "               type: 'BIGINT'\n"
        + "             }\n"
        + "           ]\n"
        + "         }\n"
        + "       ],\n"
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
    assertEquals(1, schema.types.size());
    final List<JsonTypeAttribute> attributes = schema.types.get(0).attributes;
    assertEquals("f1", attributes.get(0).name);
    assertEquals("BIGINT", attributes.get(0).type);
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
    JsonRoot root = mapper.readValue("{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       name: 'My Custom Schema',\n"
            + "       factory: 'com.acme.MySchemaFactory',\n"
            + "       operand: {a: 'foo', b: [1, 3.5] },\n"
            + "       tables: [\n"
            + "         { type: 'custom', name: 'T1' },\n"
            + "         { type: 'custom', name: 'T2', operand: {} },\n"
            + "         { type: 'custom', name: 'T3', operand: {a: 'foo'} }\n"
            + "       ]\n"
            + "     },\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       name: 'has-no-operand'\n"
            + "     }\n"
            + "   ]\n"
            + "}",
        JsonRoot.class);
    assertEquals("1.0", root.version);
    assertEquals(2, root.schemas.size());
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

    assertEquals(3, schema.tables.size());
    assertNull(((JsonCustomTable) schema.tables.get(0)).operand);
    assertTrue(((JsonCustomTable) schema.tables.get(1)).operand.isEmpty());
  }

  /** Tests that an immutable schema in a model cannot contain a
   * materialization. */
  @Test public void testModelImmutableSchemaCannotContainMaterialization()
      throws Exception {
    CalciteAssert.model("{\n"
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
        + "}")
        .connectThrows("Cannot define materialization; parent schema 'adhoc' "
            + "is not a SemiMutableSchema");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1899">[CALCITE-1899]
   * When reading model, give error if mandatory JSON attributes are
   * missing</a>.
   *
   * <p>Schema without name should give useful error, not
   * NullPointerException. */
  @Test public void testSchemaWithoutName() throws Exception {
    final String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'adhoc',\n"
        + "  schemas: [ {\n"
        + "  } ]\n"
        + "}";
    CalciteAssert.model(model)
        .connectThrows("Field 'name' is required in JsonMapSchema");
  }

  @Test public void testCustomSchemaWithoutFactory() throws Exception {
    final String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'adhoc',\n"
        + "  schemas: [ {\n"
        + "    type: 'custom',\n"
        + "    name: 'my_custom_schema'\n"
        + "  } ]\n"
        + "}";
    CalciteAssert.model(model)
        .connectThrows("Field 'factory' is required in JsonCustomSchema");
  }

  /** Tests a model containing a lattice and some views. */
  @Test public void testReadLattice() throws IOException {
    final ObjectMapper mapper = mapper();
    JsonRoot root = mapper.readValue("{\n"
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
            + "         },\n"
            + "         {\n"
            + "           name: 'V',\n"
            + "           type: 'view',\n"
            + "           sql: 'values (1)'\n"
            + "         },\n"
            + "         {\n"
            + "           name: 'V2',\n"
            + "           type: 'view',\n"
            + "           sql: [ 'values (1)', '(2)' ]\n"
            + "         }\n"
            + "       ],\n"
            + "       lattices: [\n"
            + "         {\n"
            + "           name: 'SalesStar',\n"
            + "           sql: 'select * from sales_fact_1997'\n"
            + "         },\n"
            + "         {\n"
            + "           name: 'SalesStar2',\n"
            + "           sql: [ 'select *', 'from sales_fact_1997' ]\n"
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
    assertEquals(2, schema.lattices.size());
    final JsonLattice lattice0 = schema.lattices.get(0);
    assertEquals("SalesStar", lattice0.name);
    assertEquals("select * from sales_fact_1997", lattice0.getSql());
    final JsonLattice lattice1 = schema.lattices.get(1);
    assertEquals("SalesStar2", lattice1.name);
    assertEquals("select *\nfrom sales_fact_1997\n", lattice1.getSql());
    assertEquals(4, schema.tables.size());
    final JsonTable table1 = schema.tables.get(1);
    assertTrue(!(table1 instanceof JsonView));
    final JsonTable table2 = schema.tables.get(2);
    assertTrue(table2 instanceof JsonView);
    assertThat(((JsonView) table2).getSql(), equalTo("values (1)"));
    final JsonTable table3 = schema.tables.get(3);
    assertTrue(table3 instanceof JsonView);
    assertThat(((JsonView) table3).getSql(), equalTo("values (1)\n(2)\n"));
  }

  /** Tests a model with bad multi-line SQL. */
  @Test public void testReadBadMultiLineSql() throws IOException {
    final ObjectMapper mapper = mapper();
    JsonRoot root = mapper.readValue("{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       name: 'FoodMart',\n"
            + "       tables: [\n"
            + "         {\n"
            + "           name: 'V',\n"
            + "           type: 'view',\n"
            + "           sql: [ 'values (1)', 2 ]\n"
            + "         }\n"
            + "       ]\n"
            + "     }\n"
            + "   ]\n"
            + "}",
        JsonRoot.class);
    assertEquals(1, root.schemas.size());
    final JsonMapSchema schema = (JsonMapSchema) root.schemas.get(0);
    assertEquals(1, schema.tables.size());
    final JsonView table1 = (JsonView) schema.tables.get(0);
    try {
      String s = table1.getSql();
      fail("expected error, got " + s);
    } catch (RuntimeException e) {
      assertThat(e.getMessage(),
          equalTo("each element of a string list must be a string; found: 2"));
    }
  }

  @Test public void testYamlInlineDetection() throws Exception {
    // yaml model with different line endings
    final String yamlModel = "version: 1.0\r\n"
        + "schemas: \n"
        + "- type: custom\r\n"
        + "  name: 'MyCustomSchema'\n"
        + "  factory: " + JdbcTest.MySchemaFactory.class.getName() + "\r\n";
    CalciteAssert.model(yamlModel).doWithConnection(calciteConnection -> null);
    // with a comment
    CalciteAssert.model("\n  \r\n# comment\n " + yamlModel)
        .doWithConnection(calciteConnection -> null);
    // if starts with { => treated as json
    CalciteAssert.model("  { " + yamlModel + " }")
        .connectThrows("Unexpected character ('s' (code 115)): "
            + "was expecting comma to separate Object entries");
    // if starts with /* => treated as json
    CalciteAssert.model("  /* " + yamlModel)
        .connectThrows("Unexpected end-of-input in a comment");
  }

  @Test public void testYamlFileDetection() throws Exception {
    final URL inUrl = ModelTest.class.getResource("/empty-model.yaml");
    CalciteAssert.that()
        .withModel(inUrl)
        .doWithConnection(calciteConnection -> null);
  }
}

// End ModelTest.java
