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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlOperatorTables;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.NonNull;

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
   * <p>Caller must then call {@link #init} to populate with data;
   * constructor is protected to encourage you to call {@link #create}.
   *
   * @param typeFactory   Type factory
   * @param caseSensitive case sensitivity
   */
  protected MockCatalogReaderExtended(RelDataTypeFactory typeFactory,
      boolean caseSensitive) {
    super(typeFactory, caseSensitive);
  }

  /** Creates and initializes a MockCatalogReaderExtended. */
  public static @NonNull MockCatalogReaderExtended create(
      RelDataTypeFactory typeFactory, boolean caseSensitive) {
    return new MockCatalogReaderExtended(typeFactory, caseSensitive).init();
  }

  @Override public MockCatalogReaderExtended init() {
    super.init();

    final Fixture f = new Fixture(typeFactory);
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

    // Register "EMPM" table.
    // Same as "EMP" but with a "COUNT_PLUS_100" measure column.
    final MockTable empmTable =
        MockTable.create(this, salesSchema, "EMPM", false, 14);
    empmTable.addColumn("EMPNO", f.intType, true);
    empmTable.addColumn("ENAME", f.varchar20Type);
    empmTable.addColumn("JOB", f.varchar10Type);
    empmTable.addColumn("MGR", f.intTypeNull);
    empmTable.addColumn("HIREDATE", f.timestampType);
    empmTable.addColumn("SAL", f.intType);
    empmTable.addColumn("COMM", f.intType);
    empmTable.addColumn("DEPTNO", f.intType);
    empmTable.addColumn("SLACKER", f.booleanType);
    empmTable.addColumn("COUNT_PLUS_100",
        f.typeFactory.createMeasureType(f.intType));
    registerTable(empmTable);

    MockSchema structTypeSchema = new MockSchema("STRUCT");
    registerSchema(structTypeSchema);
    final List<CompoundNameColumn> columnsExtended = Arrays.asList(
        new CompoundNameColumn("", "K0", f.varchar20TypeNull),
        new CompoundNameColumn("", "C1", f.varchar20TypeNull),
        new CompoundNameColumn("F0", "C0", f.intType),
        new CompoundNameColumn("F1", "C1", f.intTypeNull));
    final List<CompoundNameColumn> extendedColumns =
        new ArrayList<>(columnsExtended);
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

    // Defines a table with
    // schema(A int, B bigint, C varchar(10), D as a + 1 stored, E as b * 3 virtual).
    MockSchema virtualColumnsSchema = new MockSchema("VIRTUALCOLUMNS");
    registerSchema(virtualColumnsSchema);
    final MockTable virtualColumnsTable1 =
        MockTable.create(this, virtualColumnsSchema, "VC_T1", false, 100,
            null, new VirtualColumnsExpressionFactory(), true);
    virtualColumnsTable1.addColumn("A", f.intTypeNull);
    virtualColumnsTable1.addColumn("B", f.bigintType);
    virtualColumnsTable1.addColumn("C", f.varchar10Type);
    virtualColumnsTable1.addColumn("D", f.intTypeNull);
    // Column E has the same type as column A because it's a virtual column
    // with expression that references column A.
    virtualColumnsTable1.addColumn("E", f.intTypeNull);
    // Same schema with VC_T1 but with different table name.
    final MockTable virtualColumnsTable2 =
        MockTable.create(this, virtualColumnsSchema, "VC_T2", false, 100,
            null, new VirtualColumnsExpressionFactory(), false);
    virtualColumnsTable2.addColumn("A", f.intTypeNull);
    virtualColumnsTable2.addColumn("B", f.bigintType);
    virtualColumnsTable2.addColumn("C", f.varchar10Type);
    virtualColumnsTable2.addColumn("D", f.intTypeNull);
    virtualColumnsTable2.addColumn("E", f.bigintType);
    registerTable(virtualColumnsTable1);
    registerTable(virtualColumnsTable2);

    // Register table with complex data type rows.
    MockSchema complexTypeColumnsSchema = new MockSchema("COMPLEXTYPES");
    registerSchema(complexTypeColumnsSchema);
    final MockTable complexTypeColumnsTable =
        MockTable.create(this, complexTypeColumnsSchema, "CTC_T1",
            false, 100);
    complexTypeColumnsTable.addColumn("A", f.recordType1);
    complexTypeColumnsTable.addColumn("B", f.recordType2);
    complexTypeColumnsTable.addColumn("C", f.recordType3);
    complexTypeColumnsTable.addColumn("D", f.recordType4);
    complexTypeColumnsTable.addColumn("E", f.recordType5);
    complexTypeColumnsTable.addColumn("intArrayType", f.intArrayType);
    complexTypeColumnsTable.addColumn("varchar5ArrayType", f.varchar5ArrayType);
    complexTypeColumnsTable.addColumn("intArrayArrayType", f.intArrayArrayType);
    complexTypeColumnsTable.addColumn("varchar5ArrayArrayType", f.varchar5ArrayArrayType);
    complexTypeColumnsTable.addColumn("intMultisetType", f.intMultisetType);
    complexTypeColumnsTable.addColumn("varchar5MultisetType", f.varchar5MultisetType);
    complexTypeColumnsTable.addColumn("intMultisetArrayType", f.intMultisetArrayType);
    complexTypeColumnsTable.addColumn("varchar5MultisetArrayType",
        f.varchar5MultisetArrayType);
    complexTypeColumnsTable.addColumn("intArrayMultisetType", f.intArrayMultisetType);
    complexTypeColumnsTable.addColumn("rowArrayMultisetType", f.rowArrayMultisetType);
    registerTable(complexTypeColumnsTable);

    MockSchema nullableRowsSchema = new MockSchema("NULLABLEROWS");
    registerSchema(nullableRowsSchema);
    final MockTable nullableRowsTable =
        MockTable.create(this, nullableRowsSchema, "NR_T1", false, 100);
    RelDataType bigIntNotNull = typeFactory.createSqlType(SqlTypeName.BIGINT);
    RelDataType nullableRecordType =
        typeFactory.builder()
            .nullableRecord(true)
            .add("NOT_NULL_FIELD", bigIntNotNull)
            .add("NULLABLE_FIELD", bigIntNotNull).nullable(true)
            .build();

    nullableRowsTable.addColumn("ROW_COLUMN", nullableRecordType, false);
    nullableRowsTable.addColumn(
        "ROW_COLUMN_ARRAY",
        typeFactory.createArrayType(nullableRecordType, -1),
        true);
    registerTable(nullableRowsTable);

    MockSchema geoSchema = new MockSchema("GEO");
    registerSchema(geoSchema);
    final MockTable restaurantTable =
        MockTable.create(this, geoSchema, "RESTAURANTS", false, 100);
    restaurantTable.addColumn("NAME", f.varchar20Type, true);
    restaurantTable.addColumn("LATITUDE", f.intType);
    restaurantTable.addColumn("LONGITUDE", f.intType);
    restaurantTable.addColumn("CUISINE", f.varchar10Type);
    restaurantTable.addColumn("HILBERT", f.bigintType);
    restaurantTable.addMonotonic("HILBERT");
    restaurantTable.addWrap(
        new BuiltInMetadata.AllPredicates.Handler() {
          @Override public RelOptPredicateList getAllPredicates(RelNode r,
              RelMetadataQuery mq) {
            // Return the predicate:
            //  r.hilbert = hilbert(r.longitude, r.latitude)
            //
            // (Yes, x = longitude, y = latitude. Same as ST_MakePoint.)
            final RexBuilder rexBuilder = r.getCluster().getRexBuilder();
            final RexInputRef refLatitude = rexBuilder.makeInputRef(r, 1);
            final RexInputRef refLongitude = rexBuilder.makeInputRef(r, 2);
            final RexInputRef refHilbert = rexBuilder.makeInputRef(r, 4);
            return RelOptPredicateList.of(rexBuilder,
                ImmutableList.of(
                    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                        refHilbert,
                        rexBuilder.makeCall(hilbertOp(),
                            refLongitude, refLatitude))));
          }

          SqlOperator hilbertOp() {
            for (SqlOperator op
                : SqlOperatorTables.spatialInstance().getOperatorList()) {
              if (op.getKind() == SqlKind.HILBERT
                  && op.getOperandCountRange().isValidCount(2)) {
                return op;
              }
            }
            throw new AssertionError();
          }

          @Override public MetadataDef<BuiltInMetadata.AllPredicates> getDef() {
            return BuiltInMetadata.AllPredicates.DEF;
          }
        });
    registerTable(restaurantTable);

    return this;
  }
}
