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

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.test.catalog.MockCatalogReader;
import org.apache.calcite.util.ImmutableBitSet;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link org.apache.calcite.test.catalog.MockCatalogReader.MockTable}.
 */
public class MockTableTest {
  private static final SqlTypeFactoryImpl TYPE_FACTORY =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  @Test void testAddColumnCreatesIndividualKeys() {
    MockCatalogReader.MockTable t = newTable();
    t.addColumn("k1", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), true);
    t.addColumn("k2", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), true);
    assertThat(t.getKeys(), hasToString("[{0}, {1}]"));
  }

  @Test void testAddKeyWithOneEntryCreatesSimpleKey() {
    MockCatalogReader.MockTable t = newTable();
    t.addColumn("k1", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    t.addColumn("k2", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    t.addKey("k1");
    assertThat(t.getKeys(), hasToString("[{0}]"));
  }

  @Test void testAddKeyWithMultipleEntriesCreatesCompositeKey() {
    MockCatalogReader.MockTable t = newTable();
    t.addColumn("k1", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    t.addColumn("k2", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    t.addKey("k1", "k2");
    assertThat(t.getKeys(), hasToString("[{0, 1}]"));
  }

  @Test void testAddKeyWithMissingColumnNameThrowsException() {
    MockCatalogReader.MockTable t = newTable();
    t.addColumn("k1", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    t.addColumn("k2", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    assertThrows(IllegalArgumentException.class, () -> t.addKey("k1", "k3"));
  }

  @Test void testAddKeyUsingColumnIndex() {
    MockCatalogReader.MockTable t = newTable();
    t.addColumn("k1", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    t.addColumn("k2", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    t.addKey(ImmutableBitSet.of(0, 1));
    assertThat(t.getKeys(), hasToString("[{0, 1}]"));
  }

  @Test void testAddKeyUsingWrongIndexThrowsException() {
    MockCatalogReader.MockTable t = newTable();
    t.addColumn("k1", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    t.addColumn("k2", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    assertThrows(IllegalArgumentException.class, () -> t.addKey(ImmutableBitSet.of(0, 2)));
  }

  @Test void testAddKeyMultipleTimes() {
    MockCatalogReader.MockTable t = newTable();
    t.addColumn("k1", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    t.addColumn("k2", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    t.addColumn("k3", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    t.addKey("k1");
    t.addKey("k2", "k3");
    assertThat(t.getKeys(), hasToString("[{0}, {1, 2}]"));
  }

  private static MockCatalogReader.MockTable newTable() {

    MockCatalogReader catalogReader = new MockCatalogReader(TYPE_FACTORY, false) {
      @Override public MockCatalogReader init() {
        return this;
      }
    };
    return new MockCatalogReader.MockTable(catalogReader, "catalog", "schema", "table", false,
        false, 0.0, null, NullInitializerExpressionFactory.INSTANCE);
  }
}
