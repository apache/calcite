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
package org.apache.calcite.test.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Registers dynamic tables.
 *
 * <p>Not thread-safe.
 */
public class MockCatalogReaderDynamic extends MockCatalogReader {
  /**
   * Creates a MockCatalogReader.
   *
   * <p>Caller must then call {@link #init} to populate with data.</p>
   *
   * @param typeFactory   Type factory
   * @param caseSensitive case sensitivity
   */
  public MockCatalogReaderDynamic(RelDataTypeFactory typeFactory,
      boolean caseSensitive) {
    super(typeFactory, caseSensitive);
  }

  @Override public MockCatalogReader init() {
    // Register "DYNAMIC" schema.
    MockSchema schema = new MockSchema("SALES");
    registerSchema(schema);

    MockDynamicTable nationTable =
        new MockDynamicTable(schema.getCatalogName(),
            schema.getName(), "NATION");
    registerTable(nationTable);

    Supplier<MockDynamicTable> customerTableSupplier = () ->
        new MockDynamicTable(schema.getCatalogName(), schema.getName(), "CUSTOMER");

    MockDynamicTable customerTable = customerTableSupplier.get();
    registerTable(customerTable);

    // CREATE TABLE "REGION" - static table with known schema.
    final RelDataType intType =
        typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType varcharType =
        typeFactory.createSqlType(SqlTypeName.VARCHAR);

    MockTable regionTable =
        MockTable.create(this, schema, "REGION", false, 100);
    regionTable.addColumn("R_REGIONKEY", intType);
    regionTable.addColumn("R_NAME", varcharType);
    regionTable.addColumn("R_COMMENT", varcharType);
    registerTable(regionTable);

    List<String> custModifiableViewNames = Arrays.asList(
        schema.getCatalogName(), schema.getName(), "CUSTOMER_MODIFIABLEVIEW");
    TableMacro custModifiableViewMacro = MockModifiableViewRelOptTable.viewMacro(rootSchema,
        "select n_name from SALES.CUSTOMER", custModifiableViewNames.subList(0, 2),
        Collections.singletonList(custModifiableViewNames.get(2)), true);
    TranslatableTable empModifiableView = custModifiableViewMacro.apply(Collections.emptyList());
    MockTable mockCustViewTable = MockRelViewTable.create(
        (ViewTable) empModifiableView, this,
        custModifiableViewNames.get(0), custModifiableViewNames.get(1),
        custModifiableViewNames.get(2), false, 20, null);
    registerTable(mockCustViewTable);

    // re-registers customer table to clear its row type after view registration
    reregisterTable(customerTableSupplier.get());

    return this;
  }
}

// End MockCatalogReaderDynamic.java
