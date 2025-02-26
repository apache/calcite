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

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.test.catalog.MockCatalogReader;

import org.checkerframework.checker.nullness.qual.NonNull;

/** A catalog reader with tables "T1" and "T2" whose schema contains all
 * test data types. */
public class TCatalogReader extends MockCatalogReader {
  private final boolean caseSensitive;

  TCatalogReader(RelDataTypeFactory typeFactory, boolean caseSensitive) {
    super(typeFactory, false);
    this.caseSensitive = caseSensitive;
  }

  /** Creates and initializes a TCatalogReader. */
  public static @NonNull TCatalogReader create(RelDataTypeFactory typeFactory,
      boolean caseSensitive) {
    return new TCatalogReader(typeFactory, caseSensitive).init();
  }

  @Override public TCatalogReader init() {
    final TypeCoercionTest.Fixture f =
        TypeCoercionTest.DEFAULT_FIXTURE.withTypeFactory(typeFactory);
    MockSchema tSchema = new MockSchema("SALES");
    registerSchema(tSchema);
    // Register "T1" table.
    final MockTable t1 =
        MockTable.create(this, tSchema, "T1", false, 7.0, null);
    t1.addColumn("t1_varchar20", f.varchar20Type, true);
    t1.addColumn("t1_smallint", f.smallintType);
    t1.addColumn("t1_int", f.intType);
    t1.addColumn("t1_bigint", f.bigintType);
    t1.addColumn("t1_real", f.realType);
    t1.addColumn("t1_double", f.doubleType);
    t1.addColumn("t1_decimal", f.decimalType);
    t1.addColumn("t1_timestamp", f.timestampType);
    t1.addColumn("t1_date", f.dateType);
    t1.addColumn("t1_binary", f.binaryType);
    t1.addColumn("t1_boolean", f.booleanType);
    registerTable(t1);

    final MockTable t2 =
        MockTable.create(this, tSchema, "T2", false, 7.0, null);
    t2.addColumn("t2_varchar20", f.varchar20Type, true);
    t2.addColumn("t2_smallint", f.smallintType);
    t2.addColumn("t2_int", f.intType);
    t2.addColumn("t2_bigint", f.bigintType);
    t2.addColumn("t2_real", f.realType);
    t2.addColumn("t2_double", f.doubleType);
    t2.addColumn("t2_decimal", f.decimalType);
    t2.addColumn("t2_timestamp", f.timestampType);
    t2.addColumn("t2_date", f.dateType);
    t2.addColumn("t2_binary", f.binaryType);
    t2.addColumn("t2_boolean", f.booleanType);
    registerTable(t2);
    return this;
  }

  @Override public boolean isCaseSensitive() {
    return caseSensitive;
  }
}
