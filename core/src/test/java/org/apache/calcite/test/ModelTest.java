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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.io.IOException;
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
public abstract class ModelTest {

  protected abstract ObjectMapper getMapper();

  protected abstract String getSimpleSchema();

  protected abstract String getSimpleSchemaWithSubtype();

  protected abstract String getCustomSchemaWithSubtype();

  protected abstract String getImmutableSchemaWithMaterialization();

  protected abstract String getSchemaWithoutName();

  protected abstract String getCustomSchemaWithoutFactory();

  protected abstract String getLattice();

  protected abstract String getBadMultilineSql();

  /** Reads a simple schema from a string into objects. */
  @Test public void testRead() throws IOException {
    final ObjectMapper mapper = getMapper();
    JsonRoot root = mapper.readValue(getSimpleSchema(), JsonRoot.class);
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
    final ObjectMapper mapper = getMapper();
    JsonRoot root = mapper.readValue(getSimpleSchemaWithSubtype(),
        JsonRoot.class);
    assertEquals("1.0", root.version);
    assertEquals(1, root.schemas.size());
    final JsonJdbcSchema schema = (JsonJdbcSchema) root.schemas.get(0);
    assertEquals("FoodMart", schema.name);
  }

  /** Reads a custom schema. */
  @Test public void testCustomSchema() throws IOException {
    final ObjectMapper mapper = getMapper();
    JsonRoot root = mapper.readValue(getCustomSchemaWithSubtype(),
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
    CalciteAssert.model(getImmutableSchemaWithMaterialization())
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
    final String model = getSchemaWithoutName();
    CalciteAssert.model(model)
        .connectThrows("Field 'name' is required in JsonMapSchema");
  }

  @Test public void testCustomSchemaWithoutFactory() throws Exception {
    final String model = getCustomSchemaWithoutFactory();
    CalciteAssert.model(model)
        .connectThrows("Field 'factory' is required in JsonCustomSchema");
  }

  /** Tests a model containing a lattice and some views. */
  @Test public void testReadLattice() throws IOException {
    final ObjectMapper mapper = getMapper();
    JsonRoot root = mapper.readValue(getLattice(), JsonRoot.class);
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
    final ObjectMapper mapper = getMapper();
    JsonRoot root = mapper.readValue(getBadMultilineSql(), JsonRoot.class);
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
}

// End ModelTest.java
