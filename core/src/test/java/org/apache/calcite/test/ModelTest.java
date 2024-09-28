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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import static java.util.Objects.requireNonNull;

/**
 * Unit test for data models.
 */
class ModelTest {
  private ObjectMapper mapper() {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    return mapper;
  }

  /** Reads a simple schema from a string into objects. */
  @Test void testRead() throws IOException {
    final ObjectMapper mapper = mapper();
    final String json = "{\n"
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
        + "           factory: 'com.test',\n"
        + "           columns: [\n"
        + "             {\n"
        + "               name: 'time_id'\n"
        + "             }\n"
        + "           ]\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'sales_fact_1997',\n"
        + "           factory: 'com.test',\n"
        + "           columns: [\n"
        + "             {\n"
        + "               name: 'time_id'\n"
        + "             }\n"
        + "           ]\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}";
    JsonRoot root = mapper.readValue(json, JsonRoot.class);
    assertThat(root.version, is("1.0"));
    assertThat(root.schemas, hasSize(1));
    final JsonMapSchema schema = (JsonMapSchema) root.schemas.get(0);
    assertThat(schema.name, is("FoodMart"));
    assertThat(schema.types, hasSize(1));
    final List<JsonTypeAttribute> attributes = schema.types.get(0).attributes;
    assertThat(attributes.get(0).name, is("f1"));
    assertThat(attributes.get(0).type, is("BIGINT"));
    assertThat(schema.tables, hasSize(2));
    final JsonTable table0 = schema.tables.get(0);
    assertThat(table0.name, is("time_by_day"));
    final JsonTable table1 = schema.tables.get(1);
    assertThat(table1.name, is("sales_fact_1997"));
    assertThat(table0.columns, hasSize(1));
    final JsonColumn column = table0.columns.get(0);
    assertThat(column.name, is("time_id"));
  }

  /** Reads a simple schema containing JdbcSchema, a sub-type of Schema. */
  @Test void testSubtype() throws IOException {
    final ObjectMapper mapper = mapper();
    final String json = "{\n"
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
        + "}";
    JsonRoot root = mapper.readValue(json, JsonRoot.class);
    assertThat(root.version, is("1.0"));
    assertThat(root.schemas, hasSize(1));
    final JsonJdbcSchema schema = (JsonJdbcSchema) root.schemas.get(0);
    assertThat(schema.name, is("FoodMart"));
  }

  /** Reads a custom schema. */
  @Test void testCustomSchema() throws IOException {
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
            + "         { type: 'custom', name: 'T1', factory: 'com.test' },\n"
            + "         { type: 'custom', name: 'T2', factory: 'com.test', operand: {} },\n"
            + "         { type: 'custom', name: 'T3', factory: 'com.test', operand: {a: 'foo'} }\n"
            + "       ]\n"
            + "     },\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       factory: 'com.acme.MySchemaFactory',\n"
            + "       name: 'has-no-operand'\n"
            + "     }\n"
            + "   ]\n"
            + "}",
        JsonRoot.class);
    assertThat(root.version, is("1.0"));
    assertThat(root.schemas, hasSize(2));
    final JsonCustomSchema schema = (JsonCustomSchema) root.schemas.get(0);
    assertThat(schema.name, is("My Custom Schema"));
    assertThat(schema.factory, is("com.acme.MySchemaFactory"));
    assertThat(schema.operand, notNullValue());
    assertThat(schema.operand.get("a"), is("foo"));
    assertNull(schema.operand.get("c"));
    assertThat(schema.operand.get("b"), instanceOf(List.class));
    final List<Object> list = (List<Object>) schema.operand.get("b");
    assertThat(list, hasSize(2));
    assertThat(list.get(0), is(1));
    assertThat(list.get(1), is(3.5));

    assertThat(schema.tables, hasSize(3));
    final JsonCustomTable table0 = (JsonCustomTable) schema.tables.get(0);
    assertThat(table0.operand, nullValue());
    final JsonCustomTable table1 = (JsonCustomTable) schema.tables.get(1);
    assertThat(table1.operand, notNullValue());
    assertThat(table1.operand, anEmptyMap());
  }

  /** Tests that an immutable schema in a model cannot contain a
   * materialization. */
  @Test void testModelImmutableSchemaCannotContainMaterialization() {
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
  @Test void testSchemaWithoutName() {
    final String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'adhoc',\n"
        + "  schemas: [ {\n"
        + "  } ]\n"
        + "}";
    CalciteAssert.model(model)
        .connectThrows("Missing required creator property 'name'");
  }

  @Test void testCustomSchemaWithoutFactory() {
    final String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'adhoc',\n"
        + "  schemas: [ {\n"
        + "    type: 'custom',\n"
        + "    name: 'my_custom_schema'\n"
        + "  } ]\n"
        + "}";
    CalciteAssert.model(model)
        .connectThrows("Missing required creator property 'factory'");
  }

  /** Tests a model containing a lattice and some views. */
  @Test void testReadLattice() throws IOException {
    final ObjectMapper mapper = mapper();
    JsonRoot root = mapper.readValue("{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       name: 'FoodMart',\n"
            + "       tables: [\n"
            + "         {\n"
            + "           name: 'time_by_day',\n"
            + "           factory: 'com.test',\n"
            + "           columns: [\n"
            + "             {\n"
            + "               name: 'time_id'\n"
            + "             }\n"
            + "           ]\n"
            + "         },\n"
            + "         {\n"
            + "           name: 'sales_fact_1997',\n"
            + "           factory: 'com.test',\n"
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
    assertThat(root.version, is("1.0"));
    assertThat(root.schemas, hasSize(1));
    final JsonMapSchema schema = (JsonMapSchema) root.schemas.get(0);
    assertThat(schema.name, is("FoodMart"));
    assertThat(schema.lattices, hasSize(2));
    final JsonLattice lattice0 = schema.lattices.get(0);
    assertThat(lattice0.name, is("SalesStar"));
    assertThat(lattice0.getSql(), is("select * from sales_fact_1997"));
    final JsonLattice lattice1 = schema.lattices.get(1);
    assertThat(lattice1.name, is("SalesStar2"));
    assertThat(lattice1.getSql(), is("select *\nfrom sales_fact_1997\n"));
    assertThat(schema.tables, hasSize(4));
    final JsonTable table1 = schema.tables.get(1);
    assertTrue(!(table1 instanceof JsonView));
    final JsonTable table2 = schema.tables.get(2);
    assertThat(table2, instanceOf(JsonView.class));
    assertThat(((JsonView) table2).getSql(), equalTo("values (1)"));
    final JsonTable table3 = schema.tables.get(3);
    assertThat(table3, instanceOf(JsonView.class));
    assertThat(((JsonView) table3).getSql(), equalTo("values (1)\n(2)\n"));
  }

  /** Tests a model with bad multi-line SQL. */
  @Test void testReadBadMultiLineSql() throws IOException {
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
    assertThat(root.schemas, hasSize(1));
    final JsonMapSchema schema = (JsonMapSchema) root.schemas.get(0);
    assertThat(schema.tables, hasSize(1));
    final JsonView table1 = (JsonView) schema.tables.get(0);
    try {
      String s = table1.getSql();
      fail("expected error, got " + s);
    } catch (RuntimeException e) {
      assertThat(e.getMessage(),
          equalTo("each element of a string list must be a string; found: 2"));
    }
  }

  @Test void testYamlInlineDetection() throws Exception {
    // yaml model with different line endings
    final String yamlModel = "version: 1.0\r\n"
        + "schemas:\n"
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

  @Test void testYamlFileDetection() throws Exception {
    final URL inUrl =
        requireNonNull(ModelTest.class.getResource("/empty-model.yaml"), "url");
    CalciteAssert.that()
        .withModel(inUrl)
        .doWithConnection(calciteConnection -> null);
  }
}
