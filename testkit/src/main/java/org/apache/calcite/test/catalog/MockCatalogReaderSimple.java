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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Simple catalog reader for testing.
 */
public class MockCatalogReaderSimple extends MockCatalogReader {
  private final ObjectSqlType addressType;

  /**
   * Creates a MockCatalogReader.
   *
   * <p>Caller must then call {@link #init} to populate with data;
   * constructor is protected to encourage you to call {@link #create}.
   *
   * @param typeFactory   Type factory
   * @param caseSensitive case sensitivity
   */
  protected MockCatalogReaderSimple(RelDataTypeFactory typeFactory,
      boolean caseSensitive) {
    super(typeFactory, caseSensitive);

    addressType = new Fixture(typeFactory).addressType;
  }

  /** Creates and initializes a MockCatalogReaderSimple. */
  public static @NonNull MockCatalogReaderSimple create(
      RelDataTypeFactory typeFactory, boolean caseSensitive) {
    return new MockCatalogReaderSimple(typeFactory, caseSensitive).init();
  }

  @Override public RelDataType getNamedType(SqlIdentifier typeName) {
    if (typeName.equalsDeep(addressType.getSqlIdentifier(), Litmus.IGNORE)) {
      return addressType;
    } else {
      return super.getNamedType(typeName);
    }
  }

  private void registerTableEmp(MockTable empTable, Fixture fixture) {
    empTable.addColumn("EMPNO", fixture.intType, true);
    empTable.addColumn("ENAME", fixture.varchar20Type);
    empTable.addColumn("JOB", fixture.varchar10Type);
    empTable.addColumn("MGR", fixture.intTypeNull);
    empTable.addColumn("HIREDATE", fixture.timestampType);
    empTable.addColumn("SAL", fixture.intType);
    empTable.addColumn("COMM", fixture.intType);
    empTable.addColumn("DEPTNO", fixture.intType);
    empTable.addColumn("SLACKER", fixture.booleanType);
    registerTable(empTable);
  }

  private void registerTableEmpNullables(MockTable empNullablesTable, Fixture fixture) {
    empNullablesTable.addColumn("EMPNO", fixture.intType, true);
    empNullablesTable.addColumn("ENAME", fixture.varchar20TypeNull);
    empNullablesTable.addColumn("JOB", fixture.varchar10TypeNull);
    empNullablesTable.addColumn("MGR", fixture.intTypeNull);
    empNullablesTable.addColumn("HIREDATE", fixture.timestampTypeNull);
    empNullablesTable.addColumn("SAL", fixture.intTypeNull);
    empNullablesTable.addColumn("COMM", fixture.intTypeNull);
    empNullablesTable.addColumn("DEPTNO", fixture.intTypeNull);
    empNullablesTable.addColumn("SLACKER", fixture.booleanTypeNull);
    registerTable(empNullablesTable);
  }

  private void registerTableEmpDefaults(MockSchema salesSchema, Fixture fixture) {
    final MockTable empDefaultsTable =
        MockTable.create(this, salesSchema, "EMPDEFAULTS", false, 14, null,
            new EmpInitializerExpressionFactory(), false);
    empDefaultsTable.addColumn("EMPNO", fixture.intType, true);
    empDefaultsTable.addColumn("ENAME", fixture.varchar20Type);
    empDefaultsTable.addColumn("JOB", fixture.varchar10TypeNull);
    empDefaultsTable.addColumn("MGR", fixture.intTypeNull);
    empDefaultsTable.addColumn("HIREDATE", fixture.timestampTypeNull);
    empDefaultsTable.addColumn("SAL", fixture.intTypeNull);
    empDefaultsTable.addColumn("COMM", fixture.intTypeNull);
    empDefaultsTable.addColumn("DEPTNO", fixture.intTypeNull);
    empDefaultsTable.addColumn("SLACKER", fixture.booleanTypeNull);
    registerTable(empDefaultsTable);
  }

  private void registerTableEmpB(MockSchema salesSchema, Fixture fixture) {
    final MockTable empBTable =
        MockTable.create(this, salesSchema, "EMP_B", false, 14);
    empBTable.addColumn("EMPNO", fixture.intType, true);
    empBTable.addColumn("ENAME", fixture.varchar20Type);
    empBTable.addColumn("JOB", fixture.varchar10Type);
    empBTable.addColumn("MGR", fixture.intTypeNull);
    empBTable.addColumn("HIREDATE", fixture.timestampType);
    empBTable.addColumn("SAL", fixture.intType);
    empBTable.addColumn("COMM", fixture.intType);
    empBTable.addColumn("DEPTNO", fixture.intType);
    empBTable.addColumn("SLACKER", fixture.booleanType);
    empBTable.addColumn("BIRTHDATE", fixture.dateType);
    registerTable(empBTable);
  }

  private void registerTableDept(MockSchema salesSchema, Fixture fixture) {
    MockTable deptTable = MockTable.create(this, salesSchema, "DEPT", false, 4);
    deptTable.addColumn("DEPTNO", fixture.intType, true);
    deptTable.addColumn("NAME", fixture.varchar10Type);
    registerTable(deptTable);
  }

  private void registerTableDeptNullables(MockSchema salesSchema, Fixture fixture) {
    MockTable deptNullablesTable = MockTable.create(this, salesSchema, "DEPTNULLABLES", false, 4);
    deptNullablesTable.addColumn("DEPTNO", fixture.intTypeNull, true);
    deptNullablesTable.addColumn("NAME", fixture.varchar10TypeNull);
    registerTable(deptNullablesTable);
  }

  private void registerTableDeptSingle(MockSchema salesSchema, Fixture fixture) {
    MockTable deptSingleTable =
        MockTable.create(this, salesSchema, "DEPT_SINGLE", false, 4);
    deptSingleTable.addColumn("SKILL", fixture.singleRecordType);
    registerTable(deptSingleTable);
  }

  private void registerTableDeptNested(MockSchema salesSchema, Fixture fixture) {
    MockTable deptNestedTable =
        MockTable.create(this, salesSchema, "DEPT_NESTED", false, 4);
    deptNestedTable.addColumn("DEPTNO", fixture.intType, true);
    deptNestedTable.addColumn("NAME", fixture.varchar10Type);
    deptNestedTable.addColumn("SKILL", fixture.skillRecordType);
    deptNestedTable.addColumn("EMPLOYEES", fixture.empListType);
    registerTable(deptNestedTable);
  }

  private void registerTableDeptNestedExpanded(MockSchema salesSchema, Fixture fixture) {
    MockTable deptNestedExpandedTable =
        MockTable.create(this, salesSchema, "DEPT_NESTED_EXPANDED", false, 4);
    deptNestedExpandedTable.addColumn("DEPTNO", fixture.intType, true);
    deptNestedExpandedTable.addColumn("NAME", fixture.varchar10Type);
    deptNestedExpandedTable.addColumn("EMPLOYEES", fixture.empListType);
    deptNestedExpandedTable.addColumn("ADMINS", fixture.varchar5ArrayType);
    deptNestedExpandedTable.addColumn("OFFICES", fixture.rectilinearPeekCoordMultisetType);
    registerTable(deptNestedExpandedTable);
  }

  private void registerTableBonus(MockSchema salesSchema, Fixture fixture) {
    MockTable bonusTable =
        MockTable.create(this, salesSchema, "BONUS", false, 0);
    bonusTable.addColumn("ENAME", fixture.varchar20Type);
    bonusTable.addColumn("JOB", fixture.varchar10Type);
    bonusTable.addColumn("SAL", fixture.intType);
    bonusTable.addColumn("COMM", fixture.intType);
    registerTable(bonusTable);
  }

  private void registerTableSalgrade(MockSchema salesSchema, Fixture fixture) {
    MockTable salgradeTable =
        MockTable.create(this, salesSchema, "SALGRADE", false, 5);
    salgradeTable.addColumn("GRADE", fixture.intType, true);
    salgradeTable.addColumn("LOSAL", fixture.intType);
    salgradeTable.addColumn("HISAL", fixture.intType);
    registerTable(salgradeTable);
  }

  private void registerTableEmpAddress(MockSchema salesSchema, Fixture fixture) {
    MockTable contactAddressTable =
        MockTable.create(this, salesSchema, "EMP_ADDRESS", false, 26);
    contactAddressTable.addColumn("EMPNO", fixture.intType, true);
    contactAddressTable.addColumn("HOME_ADDRESS", addressType);
    contactAddressTable.addColumn("MAILING_ADDRESS", addressType);
    registerTable(contactAddressTable);

  }

  private void registerTableContact(MockSchema customerSchema, Fixture fixture) {
    MockTable contactTable =
        MockTable.create(this, customerSchema, "CONTACT", false, 1000);
    contactTable.addColumn("CONTACTNO", fixture.intType);
    contactTable.addColumn("FNAME", fixture.varchar10Type);
    contactTable.addColumn("LNAME", fixture.varchar10Type);
    contactTable.addColumn("EMAIL", fixture.varchar20Type);
    contactTable.addColumn("COORD", fixture.rectilinearCoordType);
    registerTable(contactTable);
  }

  private void registerTableContactPeek(MockSchema customerSchema, Fixture fixture) {
    MockTable contactPeekTable =
        MockTable.create(this, customerSchema, "CONTACT_PEEK", false, 1000);
    contactPeekTable.addColumn("CONTACTNO", fixture.intType);
    contactPeekTable.addColumn("FNAME", fixture.varchar10Type);
    contactPeekTable.addColumn("LNAME", fixture.varchar10Type);
    contactPeekTable.addColumn("EMAIL", fixture.varchar20Type);
    contactPeekTable.addColumn("COORD", fixture.rectilinearPeekCoordType);
    contactPeekTable.addColumn("COORD_NE", fixture.rectilinearPeekNoExpandCoordType);
    registerTable(contactPeekTable);
  }

  private void registerTableAccount(MockSchema customerSchema, Fixture fixture) {
    MockTable accountTable =
        MockTable.create(this, customerSchema, "ACCOUNT", false, 457);
    accountTable.addColumn("ACCTNO", fixture.intType);
    accountTable.addColumn("TYPE", fixture.varchar20Type);
    accountTable.addColumn("BALANCE", fixture.intType);
    registerTable(accountTable);
  }

  private void registerTableOrders(MockSchema salesSchema, Fixture fixture) {
    MockTable ordersStream =
        MockTable.create(this, salesSchema, "ORDERS", true,
            Double.POSITIVE_INFINITY);
    ordersStream.addColumn("ROWTIME", fixture.timestampType);
    ordersStream.addMonotonic("ROWTIME");
    ordersStream.addColumn("PRODUCTID", fixture.intType);
    ordersStream.addColumn("ORDERID", fixture.intType);
    registerTable(ordersStream);
  }

  private void registerTableShipments(MockSchema salesSchema, Fixture fixture) {
    // "ROWTIME" is not column 0, just to mix things up.
    MockTable shipmentsStream =
        MockTable.create(this, salesSchema, "SHIPMENTS", true,
            Double.POSITIVE_INFINITY);
    shipmentsStream.addColumn("ORDERID", fixture.intType);
    shipmentsStream.addColumn("ROWTIME", fixture.timestampType);
    shipmentsStream.addMonotonic("ROWTIME");
    registerTable(shipmentsStream);
  }

  private void registerTableProducts(MockSchema salesSchema, Fixture fixture) {
    MockTable productsTable =
        MockTable.create(this, salesSchema, "PRODUCTS", false, 200D);
    productsTable.addColumn("PRODUCTID", fixture.intType);
    productsTable.addColumn("NAME", fixture.varchar20Type);
    productsTable.addColumn("SUPPLIERID", fixture.intType);
    registerTable(productsTable);
  }

  private void registerTableEmptyProducts(MockSchema salesSchema, Fixture fixture) {
    MockTable emptyProductsTable =
        MockTable.create(this, salesSchema, "EMPTY_PRODUCTS", false, 0D, 0D);
    emptyProductsTable.addColumn("PRODUCTID", fixture.intType);
    emptyProductsTable.addColumn("NAME", fixture.varchar20Type);
    emptyProductsTable.addColumn("SUPPLIERID", fixture.intType);
    registerTable(emptyProductsTable);
  }

  private void registerTableProductsTemporal(MockSchema salesSchema, Fixture fixture) {
    MockTable productsTemporalTable =
        MockTable.create(this, salesSchema, "PRODUCTS_TEMPORAL", false, 200D,
            null, NullInitializerExpressionFactory.INSTANCE, true);
    productsTemporalTable.addColumn("PRODUCTID", fixture.intType);
    productsTemporalTable.addColumn("NAME", fixture.varchar20Type);
    productsTemporalTable.addColumn("SUPPLIERID", fixture.intType);
    productsTemporalTable.addColumn("SYS_START", fixture.timestampType);
    productsTemporalTable.addColumn("SYS_END", fixture.timestampType);
    productsTemporalTable.addColumn(
        "SYS_START_LOCAL_TIMESTAMP", fixture.timestampTypeWithLocalTimeZone);
    productsTemporalTable.addColumn(
        "SYS_END_LOCAL_TIMESTAMP", fixture.timestampTypeWithLocalTimeZone);
    registerTable(productsTemporalTable);
  }

  private void registerTableSuppliers(MockSchema salesSchema, Fixture fixture) {
    MockTable suppliersTable =
        MockTable.create(this, salesSchema, "SUPPLIERS", false, 10D);
    suppliersTable.addColumn("SUPPLIERID", fixture.intType);
    suppliersTable.addColumn("NAME", fixture.varchar20Type);
    suppliersTable.addColumn("CITY", fixture.intType);
    registerTable(suppliersTable);
  }

  private void registerViewEmp20(MockSchema salesSchema, MockTable empTable, Fixture fixture) {
    final ImmutableIntList m0 = ImmutableIntList.of(0, 1, 2, 3, 4, 5, 6, 8);
    MockTable emp20View =
        new MockViewTable(this, salesSchema.getCatalogName(), salesSchema.getName(),
            "EMP_20", false, 600, empTable, m0, null,
            NullInitializerExpressionFactory.INSTANCE) {
          @Override public RexNode getConstraint(RexBuilder rexBuilder,
              RelDataType tableRowType) {
            final RelDataTypeField deptnoField =
                tableRowType.getFieldList().get(7);
            final RelDataTypeField salField =
                tableRowType.getFieldList().get(5);
            final List<RexNode> nodes =
                Arrays.asList(
                    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                        rexBuilder.makeInputRef(deptnoField.getType(),
                            deptnoField.getIndex()),
                        rexBuilder.makeExactLiteral(BigDecimal.valueOf(20L),
                            deptnoField.getType())),
                    rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                        rexBuilder.makeInputRef(salField.getType(),
                            salField.getIndex()),
                        rexBuilder.makeExactLiteral(BigDecimal.valueOf(1000L),
                            salField.getType())));
            return RexUtil.composeConjunction(rexBuilder, nodes);
          }
        };
    salesSchema.addTable(Util.last(emp20View.getQualifiedName()));
    emp20View.addColumn("EMPNO", fixture.intType);
    emp20View.addColumn("ENAME", fixture.varchar20Type);
    emp20View.addColumn("JOB", fixture.varchar10Type);
    emp20View.addColumn("MGR", fixture.intTypeNull);
    emp20View.addColumn("HIREDATE", fixture.timestampType);
    emp20View.addColumn("SAL", fixture.intType);
    emp20View.addColumn("COMM", fixture.intType);
    emp20View.addColumn("SLACKER", fixture.booleanType);
    registerTable(emp20View);
  }

  private void registerViewEmpNullables20(
      MockSchema salesSchema, MockTable empNullablesTable, Fixture fixture) {
    final ImmutableIntList m0 = ImmutableIntList.of(0, 1, 2, 3, 4, 5, 6, 8);
    MockTable empNullables20View =
        new MockViewTable(this, salesSchema.getCatalogName(), salesSchema.getName(),
            "EMPNULLABLES_20", false, 600, empNullablesTable, m0, null,
            NullInitializerExpressionFactory.INSTANCE) {
          @Override public RexNode getConstraint(RexBuilder rexBuilder,
              RelDataType tableRowType) {
            final RelDataTypeField deptnoField =
                tableRowType.getFieldList().get(7);
            final RelDataTypeField salField =
                tableRowType.getFieldList().get(5);
            final List<RexNode> nodes =
                Arrays.asList(
                    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                        rexBuilder.makeInputRef(deptnoField.getType(),
                            deptnoField.getIndex()),
                        rexBuilder.makeExactLiteral(BigDecimal.valueOf(20L),
                            deptnoField.getType())),
                    rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                        rexBuilder.makeInputRef(salField.getType(),
                            salField.getIndex()),
                        rexBuilder.makeExactLiteral(BigDecimal.valueOf(1000L),
                            salField.getType())));
            return RexUtil.composeConjunction(rexBuilder, nodes);
          }
        };
    salesSchema.addTable(Util.last(empNullables20View.getQualifiedName()));
    empNullables20View.addColumn("EMPNO", fixture.intType);
    empNullables20View.addColumn("ENAME", fixture.varchar20TypeNull);
    empNullables20View.addColumn("JOB", fixture.varchar10TypeNull);
    empNullables20View.addColumn("MGR", fixture.intTypeNull);
    empNullables20View.addColumn("HIREDATE", fixture.timestampTypeNull);
    empNullables20View.addColumn("SAL", fixture.intTypeNull);
    empNullables20View.addColumn("COMM", fixture.intTypeNull);
    empNullables20View.addColumn("SLACKER", fixture.booleanTypeNull);
    registerTable(empNullables20View);
  }

  private void registerStructTypeTables(Fixture fixture) {
    MockSchema structTypeSchema = new MockSchema("STRUCT");
    registerSchema(structTypeSchema);
    final List<CompoundNameColumn> columns =
        Arrays.asList(new CompoundNameColumn("", "K0", fixture.varchar20Type),
            new CompoundNameColumn("", "C1", fixture.varchar20Type),
            new CompoundNameColumn("F1", "A0", fixture.intType),
            new CompoundNameColumn("F2", "A0", fixture.booleanType),
            new CompoundNameColumn("F0", "C0", fixture.intType),
            new CompoundNameColumn("F1", "C0", fixture.intTypeNull),
            new CompoundNameColumn("F0", "C1", fixture.intType),
            new CompoundNameColumn("F1", "C2", fixture.intType),
            new CompoundNameColumn("F2", "C3", fixture.intType));
    final CompoundNameColumnResolver structTypeTableResolver =
        new CompoundNameColumnResolver(columns, "F0");
    final MockTable structTypeTable =
        MockTable.create(this, structTypeSchema, "T", false, 100,
            structTypeTableResolver);
    for (CompoundNameColumn column : columns) {
      structTypeTable.addColumn(column.getName(), column.type);
    }
    registerTable(structTypeTable);

    final List<CompoundNameColumn> columnsNullable =
        Arrays.asList(new CompoundNameColumn("", "K0", fixture.varchar20TypeNull),
            new CompoundNameColumn("", "C1", fixture.varchar20TypeNull),
            new CompoundNameColumn("F1", "A0", fixture.intTypeNull),
            new CompoundNameColumn("F2", "A0", fixture.booleanTypeNull),
            new CompoundNameColumn("F0", "C0", fixture.intTypeNull),
            new CompoundNameColumn("F1", "C0", fixture.intTypeNull),
            new CompoundNameColumn("F0", "C1", fixture.intTypeNull),
            new CompoundNameColumn("F1", "C2", fixture.intType),
            new CompoundNameColumn("F2", "C3", fixture.intTypeNull));
    final MockTable structNullableTypeTable =
        MockTable.create(this, structTypeSchema, "T_NULLABLES", false, 100,
            structTypeTableResolver);
    for (CompoundNameColumn column : columnsNullable) {
      structNullableTypeTable.addColumn(column.getName(), column.type);
    }
    registerTable(structNullableTypeTable);

    // Register "STRUCT.T_10" view.
    // Same columns as "STRUCT.T", but "F0.C0" is set to 10 by default, which is the equivalent of:
    //   SELECT * FROM T WHERE F0.C0 = 10
    // This table uses MockViewTable which does not populate the constrained columns with default
    // values on INSERT.
    final ImmutableIntList m1 = ImmutableIntList.of(0, 1, 2, 3, 4, 5, 6, 7, 8);
    MockTable struct10View =
        new MockViewTable(this, structTypeSchema.getCatalogName(),
            structTypeSchema.getName(), "T_10", false, 20, structTypeTable,
            m1, structTypeTableResolver,
            NullInitializerExpressionFactory.INSTANCE) {
          @Override public RexNode getConstraint(RexBuilder rexBuilder,
              RelDataType tableRowType) {
            final RelDataTypeField c0Field =
                tableRowType.getFieldList().get(4);
            return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(c0Field.getType(),
                    c0Field.getIndex()),
                rexBuilder.makeExactLiteral(BigDecimal.valueOf(10L),
                    c0Field.getType()));
          }
        };
    structTypeSchema.addTable(Util.last(struct10View.getQualifiedName()));
    for (CompoundNameColumn column : columns) {
      struct10View.addColumn(column.getName(), column.type);
    }
    registerTable(struct10View);
  }

  private void registerTableDoublePK(MockSchema salesSchema, Fixture fixture) {
    final MockTable doublePK =
        MockTable.create(this, salesSchema, "DOUBLE_PK", false, 14);
    doublePK.addColumn("ID1", fixture.intType, true);
    doublePK.addColumn("ID2", fixture.varchar20Type);
    doublePK.addColumn("NAME", fixture.varchar20Type);
    doublePK.addColumn("AGE", fixture.intType);
    doublePK.keyList.add(ImmutableBitSet.of(0, 1));
    registerTable(doublePK);
  }

  @Override public MockCatalogReaderSimple init() {
    final Fixture fixture = new Fixture(typeFactory);

    // Register "SALES" schema.
    MockSchema salesSchema = new MockSchema("SALES");
    registerSchema(salesSchema);

    // Register "EMP" table with customer InitializerExpressionFactory
    // to check whether newDefaultValue method called or not.
    final InitializerExpressionFactory countingInitializerExpressionFactory =
        new CountingFactory(ImmutableList.of("DEPTNO"));

    registerType(
        ImmutableList.of(salesSchema.getCatalogName(), salesSchema.getName(),
            "customBigInt"),
        typeFactory -> typeFactory.createSqlType(SqlTypeName.BIGINT));

    // Register "EMP" table.
    final MockTable empTable =
        MockTable.create(this, salesSchema, "EMP", false, 14, null,
            countingInitializerExpressionFactory, false);
    registerTableEmp(empTable, fixture);

    // Register "EMPNULLABLES" table with nullable columns.
    final MockTable empNullablesTable =
        MockTable.create(this, salesSchema, "EMPNULLABLES", false, 14);
    registerTableEmpNullables(empNullablesTable, fixture);

    // Register "EMPDEFAULTS" table with default values for some columns.
    registerTableEmpDefaults(salesSchema, fixture);

    // Register "EMP_B" table. As "EMP", birth with a "BIRTHDATE" column.
    registerTableEmpB(salesSchema, fixture);

    // Register "DEPT" table.
    registerTableDept(salesSchema, fixture);

    // Register "DEPTNULLABLES" table.
    registerTableDeptNullables(salesSchema, fixture);

    // Register "DEPT_SINGLE" table.
    registerTableDeptSingle(salesSchema, fixture);

    // Register "DEPT_NESTED" table.
    registerTableDeptNested(salesSchema, fixture);

    // Register "DEPT_NESTED_EXPANDED" table.
    registerTableDeptNestedExpanded(salesSchema, fixture);

    // Register "BONUS" table.
    registerTableBonus(salesSchema, fixture);

    // Register "SALGRADE" table.
    registerTableSalgrade(salesSchema, fixture);

    // Register "EMP_ADDRESS" table
    registerTableEmpAddress(salesSchema, fixture);

    // Register "CUSTOMER" schema.
    MockSchema customerSchema = new MockSchema("CUSTOMER");
    registerSchema(customerSchema);

    // Register "CONTACT" table.
    registerTableContact(customerSchema, fixture);

    // Register "CONTACT_PEEK" table. The
    registerTableContactPeek(customerSchema, fixture);

    // Register "ACCOUNT" table.
    registerTableAccount(customerSchema, fixture);

    // Register "ORDERS" stream.
    registerTableOrders(salesSchema, fixture);

    // Register "SHIPMENTS" stream.
    registerTableShipments(salesSchema, fixture);

    // Register "PRODUCTS" table.
    registerTableProducts(salesSchema, fixture);

    // Register "EMPTY_PRODUCTS" table.
    registerTableEmptyProducts(salesSchema, fixture);

    // Register "PRODUCTS_TEMPORAL" table.
    registerTableProductsTemporal(salesSchema, fixture);

    // Register "SUPPLIERS" table.
    registerTableSuppliers(salesSchema, fixture);

    // Register "EMP_20" and "EMPNULLABLES_20 views.
    // Same columns as "EMP" amd "EMPNULLABLES", but "DEPTNO" not visible and set to 20 by default
    // and "SAL" is visible but must be greater than 1000, which is the equivalent of:
    //   SELECT EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, SLACKER
    //   FROM EMP WHERE DEPTNO = 20 AND SAL > 1000
    registerViewEmp20(salesSchema, empTable, fixture);

    registerViewEmpNullables20(salesSchema, empNullablesTable, fixture);

    registerStructTypeTables(fixture);

    registerTablesWithRollUp(salesSchema, fixture);

    registerTableDoublePK(salesSchema, fixture);

    return this;
  }
}
