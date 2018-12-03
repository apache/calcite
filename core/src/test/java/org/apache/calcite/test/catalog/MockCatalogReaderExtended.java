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

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Adds some extra tables to the mock catalog. These increase the time and
 * complexity of initializing the catalog (because they contain views whose
 * SQL needs to be parsed) and so are not used for all tests. */
public class MockCatalogReaderExtended extends MockCatalogReaderSimple {
  /**
   * Creates a MockCatalogReader.
   *
   * <p>Caller must then call {@link #init} to populate with data.</p>
   *
   * @param typeFactory   Type factory
   * @param caseSensitive case sensitivity
   */
  public MockCatalogReaderExtended(RelDataTypeFactory typeFactory,
      boolean caseSensitive) {
    super(typeFactory, caseSensitive);
  }

  @Override public MockCatalogReader init() {
    super.init();

    MockSchema salesSchema = new MockSchema("SALES");
    // Same as "EMP_20" except it uses ModifiableViewTable which populates
    // constrained columns with default values on INSERT and has a single constraint on DEPTNO.
    List<String> empModifiableViewNames = ImmutableList.of(
        salesSchema.getCatalogName(), salesSchema.getName(), "EMP_MODIFIABLEVIEW");
    TableMacro empModifiableViewMacro = MockModifiableViewRelOptTable.viewMacro(rootSchema,
        "select EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, SLACKER from EMPDEFAULTS"
            + " where DEPTNO = 20", empModifiableViewNames.subList(0, 2),
        ImmutableList.of(empModifiableViewNames.get(2)), true);
    TranslatableTable empModifiableView = empModifiableViewMacro.apply(ImmutableList.of());
    MockModifiableViewRelOptTable mockEmpViewTable = MockModifiableViewRelOptTable.create(
        (MockModifiableViewRelOptTable.MockModifiableViewTable) empModifiableView, this,
        empModifiableViewNames.get(0), empModifiableViewNames.get(1),
        empModifiableViewNames.get(2), false, 20, null);
    registerTable(mockEmpViewTable);

    // Same as "EMP_MODIFIABLEVIEW" except that all columns are in the view, columns are reordered,
    // and there is an `extra` extended column.
    List<String> empModifiableViewNames2 = ImmutableList.of(
        salesSchema.getCatalogName(), salesSchema.getName(), "EMP_MODIFIABLEVIEW2");
    TableMacro empModifiableViewMacro2 = MockModifiableViewRelOptTable.viewMacro(rootSchema,
        "select ENAME, EMPNO, JOB, DEPTNO, SLACKER, SAL, EXTRA, HIREDATE, MGR, COMM"
            + " from EMPDEFAULTS extend (EXTRA boolean)"
            + " where DEPTNO = 20", empModifiableViewNames2.subList(0, 2),
        ImmutableList.of(empModifiableViewNames.get(2)), true);
    TranslatableTable empModifiableView2 = empModifiableViewMacro2.apply(ImmutableList.of());
    MockModifiableViewRelOptTable mockEmpViewTable2 = MockModifiableViewRelOptTable.create(
        (MockModifiableViewRelOptTable.MockModifiableViewTable) empModifiableView2, this,
        empModifiableViewNames2.get(0), empModifiableViewNames2.get(1),
        empModifiableViewNames2.get(2), false, 20, null);
    registerTable(mockEmpViewTable2);

    // Same as "EMP_MODIFIABLEVIEW" except that comm is not in the view.
    List<String> empModifiableViewNames3 = ImmutableList.of(
        salesSchema.getCatalogName(), salesSchema.getName(), "EMP_MODIFIABLEVIEW3");
    TableMacro empModifiableViewMacro3 = MockModifiableViewRelOptTable.viewMacro(rootSchema,
        "select EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, SLACKER from EMPDEFAULTS"
            + " where DEPTNO = 20", empModifiableViewNames3.subList(0, 2),
        ImmutableList.of(empModifiableViewNames3.get(2)), true);
    TranslatableTable empModifiableView3 = empModifiableViewMacro3.apply(ImmutableList.of());
    MockModifiableViewRelOptTable mockEmpViewTable3 = MockModifiableViewRelOptTable.create(
        (MockModifiableViewRelOptTable.MockModifiableViewTable) empModifiableView3, this,
        empModifiableViewNames3.get(0), empModifiableViewNames3.get(1),
        empModifiableViewNames3.get(2), false, 20, null);
    registerTable(mockEmpViewTable3);

    MockSchema structTypeSchema = new MockSchema("STRUCT");
    registerSchema(structTypeSchema);
    final Fixture f = new Fixture(typeFactory);
    final List<CompoundNameColumn> columnsExtended = Arrays.asList(
        new CompoundNameColumn("", "K0", f.varchar20TypeNull),
        new CompoundNameColumn("", "C1", f.varchar20TypeNull),
        new CompoundNameColumn("F0", "C0", f.intType),
        new CompoundNameColumn("F1", "C1", f.intTypeNull));
    final List<CompoundNameColumn> extendedColumns =
        new ArrayList<CompoundNameColumn>(columnsExtended);
    extendedColumns.add(new CompoundNameColumn("F2", "C2", f.varchar20Type));
    final CompoundNameColumnResolver structExtendedTableResolver =
        new CompoundNameColumnResolver(extendedColumns, "F0");
    final MockTable structExtendedTypeTable =
        MockTable.create(this, structTypeSchema, "T_EXTEND", false, 100,
            structExtendedTableResolver);
    for (CompoundNameColumn column : columnsExtended) {
      structExtendedTypeTable.addColumn(column.getName(), column.type);
    }
    registerTable(structExtendedTypeTable);

    return this;
  }
}

// End MockCatalogReaderExtended.java
