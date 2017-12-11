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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.DynamicRecordTypeImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.CustomColumnResolvingTable;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.Path;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ModifiableViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock implementation of {@link SqlValidatorCatalogReader} which returns tables
 * "EMP", "DEPT", "BONUS", "SALGRADE" (same as Oracle's SCOTT schema).
 * Also two streams "ORDERS", "SHIPMENTS";
 * and a view "EMP_20".
 */
public class MockCatalogReader extends CalciteCatalogReader {
  //~ Static fields/initializers ---------------------------------------------

  static final String DEFAULT_CATALOG = "CATALOG";
  static final String DEFAULT_SCHEMA = "SALES";
  static final List<String> PREFIX = ImmutableList.of(DEFAULT_SCHEMA);

  //~ Instance fields --------------------------------------------------------

  private RelDataType addressType;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a MockCatalogReader.
   *
   * <p>Caller must then call {@link #init} to populate with data.</p>
   *
   * @param typeFactory Type factory
   */
  public MockCatalogReader(RelDataTypeFactory typeFactory,
      boolean caseSensitive) {
    super(CalciteSchema.createRootSchema(false, true, DEFAULT_CATALOG),
        SqlNameMatchers.withCaseSensitive(caseSensitive),
        ImmutableList.of(PREFIX, ImmutableList.<String>of()),
        typeFactory, null);
  }

  @Override public boolean isCaseSensitive() {
    return nameMatcher.isCaseSensitive();
  }

  public SqlNameMatcher nameMatcher() {
    return nameMatcher;
  }

  /**
   * Initializes this catalog reader.
   */
  public MockCatalogReader init() {
    final Fixture f = new Fixture();
    addressType = f.addressType;

    // Register "SALES" schema.
    MockSchema salesSchema = new MockSchema("SALES");
    registerSchema(salesSchema);

    // Register "EMP" table with customer InitializerExpressionFactory
    // to check whether newDefaultValue method called or not.
    final InitializerExpressionFactory countingInitializerExpressionFactory =
        new CountingFactory(ImmutableList.of("DEPTNO"));

    // Register "EMP" table.
    final MockTable empTable =
        MockTable.create(this, salesSchema, "EMP", false, 14, null,
            countingInitializerExpressionFactory);
    empTable.addColumn("EMPNO", f.intType, true);
    empTable.addColumn("ENAME", f.varchar20Type);
    empTable.addColumn("JOB", f.varchar10Type);
    empTable.addColumn("MGR", f.intTypeNull);
    empTable.addColumn("HIREDATE", f.timestampType);
    empTable.addColumn("SAL", f.intType);
    empTable.addColumn("COMM", f.intType);
    empTable.addColumn("DEPTNO", f.intType);
    empTable.addColumn("SLACKER", f.booleanType);
    registerTable(empTable);

    // Register "EMPNULLABLES" table with nullable columns.
    final MockTable empNullablesTable =
        MockTable.create(this, salesSchema, "EMPNULLABLES", false, 14);
    empNullablesTable.addColumn("EMPNO", f.intType, true);
    empNullablesTable.addColumn("ENAME", f.varchar20Type);
    empNullablesTable.addColumn("JOB", f.varchar10TypeNull);
    empNullablesTable.addColumn("MGR", f.intTypeNull);
    empNullablesTable.addColumn("HIREDATE", f.timestampTypeNull);
    empNullablesTable.addColumn("SAL", f.intTypeNull);
    empNullablesTable.addColumn("COMM", f.intTypeNull);
    empNullablesTable.addColumn("DEPTNO", f.intTypeNull);
    empNullablesTable.addColumn("SLACKER", f.booleanTypeNull);
    registerTable(empNullablesTable);

    // Register "EMPDEFAULTS" table with default values for some columns.
    final MockTable empDefaultsTable =
        MockTable.create(this, salesSchema, "EMPDEFAULTS", false, 14, null,
            new EmpInitializerExpressionFactory());
    empDefaultsTable.addColumn("EMPNO", f.intType, true);
    empDefaultsTable.addColumn("ENAME", f.varchar20Type);
    empDefaultsTable.addColumn("JOB", f.varchar10TypeNull);
    empDefaultsTable.addColumn("MGR", f.intTypeNull);
    empDefaultsTable.addColumn("HIREDATE", f.timestampTypeNull);
    empDefaultsTable.addColumn("SAL", f.intTypeNull);
    empDefaultsTable.addColumn("COMM", f.intTypeNull);
    empDefaultsTable.addColumn("DEPTNO", f.intTypeNull);
    empDefaultsTable.addColumn("SLACKER", f.booleanTypeNull);
    registerTable(empDefaultsTable);

    // Register "EMP_B" table. As "EMP", birth with a "BIRTHDATE" column.
    final MockTable empBTable =
        MockTable.create(this, salesSchema, "EMP_B", false, 14);
    empBTable.addColumn("EMPNO", f.intType, true);
    empBTable.addColumn("ENAME", f.varchar20Type);
    empBTable.addColumn("JOB", f.varchar10Type);
    empBTable.addColumn("MGR", f.intTypeNull);
    empBTable.addColumn("HIREDATE", f.timestampType);
    empBTable.addColumn("SAL", f.intType);
    empBTable.addColumn("COMM", f.intType);
    empBTable.addColumn("DEPTNO", f.intType);
    empBTable.addColumn("SLACKER", f.booleanType);
    empBTable.addColumn("BIRTHDATE", f.dateType);
    registerTable(empBTable);

    // Register "DEPT" table.
    MockTable deptTable = MockTable.create(this, salesSchema, "DEPT", false, 4);
    deptTable.addColumn("DEPTNO", f.intType, true);
    deptTable.addColumn("NAME", f.varchar10Type);
    registerTable(deptTable);

    // Register "DEPT_NESTED" table.
    MockTable deptNestedTable =
        MockTable.create(this, salesSchema, "DEPT_NESTED", false, 4);
    deptNestedTable.addColumn("DEPTNO", f.intType, true);
    deptNestedTable.addColumn("NAME", f.varchar10Type);
    deptNestedTable.addColumn("EMPLOYEES", f.empListType);
    registerTable(deptNestedTable);

    // Register "BONUS" table.
    MockTable bonusTable =
        MockTable.create(this, salesSchema, "BONUS", false, 0);
    bonusTable.addColumn("ENAME", f.varchar20Type);
    bonusTable.addColumn("JOB", f.varchar10Type);
    bonusTable.addColumn("SAL", f.intType);
    bonusTable.addColumn("COMM", f.intType);
    registerTable(bonusTable);

    // Register "SALGRADE" table.
    MockTable salgradeTable =
        MockTable.create(this, salesSchema, "SALGRADE", false, 5);
    salgradeTable.addColumn("GRADE", f.intType, true);
    salgradeTable.addColumn("LOSAL", f.intType);
    salgradeTable.addColumn("HISAL", f.intType);
    registerTable(salgradeTable);

    // Register "EMP_ADDRESS" table
    MockTable contactAddressTable =
        MockTable.create(this, salesSchema, "EMP_ADDRESS", false, 26);
    contactAddressTable.addColumn("EMPNO", f.intType, true);
    contactAddressTable.addColumn("HOME_ADDRESS", addressType);
    contactAddressTable.addColumn("MAILING_ADDRESS", addressType);
    registerTable(contactAddressTable);

    // Register "DYNAMIC" schema.
    MockSchema dynamicSchema = new MockSchema("DYNAMIC");
    registerSchema(dynamicSchema);

    MockTable nationTable =
        new MockDynamicTable(this, dynamicSchema.getCatalogName(),
            dynamicSchema.getName(), "NATION", false, 100);
    registerTable(nationTable);

    MockTable customerTable =
        new MockDynamicTable(this, dynamicSchema.getCatalogName(),
            dynamicSchema.getName(), "CUSTOMER", false, 100);
    registerTable(customerTable);

    // Register "CUSTOMER" schema.
    MockSchema customerSchema = new MockSchema("CUSTOMER");
    registerSchema(customerSchema);

    // Register "CONTACT" table.
    MockTable contactTable = MockTable.create(this, customerSchema, "CONTACT",
        false, 1000);
    contactTable.addColumn("CONTACTNO", f.intType);
    contactTable.addColumn("FNAME", f.varchar10Type);
    contactTable.addColumn("LNAME", f.varchar10Type);
    contactTable.addColumn("EMAIL", f.varchar20Type);
    contactTable.addColumn("COORD", f.rectilinearCoordType);
    registerTable(contactTable);

    // Register "CONTACT_PEEK" table. The
    MockTable contactPeekTable =
        MockTable.create(this, customerSchema, "CONTACT_PEEK", false, 1000);
    contactPeekTable.addColumn("CONTACTNO", f.intType);
    contactPeekTable.addColumn("FNAME", f.varchar10Type);
    contactPeekTable.addColumn("LNAME", f.varchar10Type);
    contactPeekTable.addColumn("EMAIL", f.varchar20Type);
    contactPeekTable.addColumn("COORD", f.rectilinearPeekCoordType);
    contactPeekTable.addColumn("COORD_NE", f.rectilinearPeekNoExpandCoordType);
    registerTable(contactPeekTable);

    // Register "ACCOUNT" table.
    MockTable accountTable = MockTable.create(this, customerSchema, "ACCOUNT",
        false, 457);
    accountTable.addColumn("ACCTNO", f.intType);
    accountTable.addColumn("TYPE", f.varchar20Type);
    accountTable.addColumn("BALANCE", f.intType);
    registerTable(accountTable);

    // Register "ORDERS" stream.
    MockTable ordersStream = MockTable.create(this, salesSchema, "ORDERS",
        true, Double.POSITIVE_INFINITY);
    ordersStream.addColumn("ROWTIME", f.timestampType);
    ordersStream.addMonotonic("ROWTIME");
    ordersStream.addColumn("PRODUCTID", f.intType);
    ordersStream.addColumn("ORDERID", f.intType);
    registerTable(ordersStream);

    // Register "SHIPMENTS" stream.
    // "ROWTIME" is not column 0, just to mix things up.
    MockTable shipmentsStream = MockTable.create(this, salesSchema, "SHIPMENTS",
        true, Double.POSITIVE_INFINITY);
    shipmentsStream.addColumn("ORDERID", f.intType);
    shipmentsStream.addColumn("ROWTIME", f.timestampType);
    shipmentsStream.addMonotonic("ROWTIME");
    registerTable(shipmentsStream);

    // Register "PRODUCTS" table.
    MockTable productsTable = MockTable.create(this, salesSchema, "PRODUCTS",
        false, 200D);
    productsTable.addColumn("PRODUCTID", f.intType);
    productsTable.addColumn("NAME", f.varchar20Type);
    productsTable.addColumn("SUPPLIERID", f.intType);
    registerTable(productsTable);

    // Register "SUPPLIERS" table.
    MockTable suppliersTable = MockTable.create(this, salesSchema, "SUPPLIERS",
        false, 10D);
    suppliersTable.addColumn("SUPPLIERID", f.intType);
    suppliersTable.addColumn("NAME", f.varchar20Type);
    suppliersTable.addColumn("CITY", f.intType);
    registerTable(suppliersTable);

    // Register "EMP_20" and "EMPNULLABLES_20 views.
    // Same columns as "EMP" amd "EMPNULLABLES",
    // but "DEPTNO" not visible and set to 20 by default
    // and "SAL" is visible but must be greater than 1000,
    // which is the equivalent of:
    //   SELECT EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, SLACKER
    //   FROM EMP
    //   WHERE DEPTNO = 20 AND SAL > 1000
    final ImmutableIntList m0 = ImmutableIntList.of(0, 1, 2, 3, 4, 5, 6, 8);
    MockTable emp20View =
        new MockViewTable(this, salesSchema.getCatalogName(), salesSchema.name,
            "EMP_20", false, 600, empTable, m0, null,
            NullInitializerExpressionFactory.INSTANCE) {
          public RexNode getConstraint(RexBuilder rexBuilder,
              RelDataType tableRowType) {
            final RelDataTypeField deptnoField =
                tableRowType.getFieldList().get(7);
            final RelDataTypeField salField =
                tableRowType.getFieldList().get(5);
            final List<RexNode> nodes = Arrays.asList(
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
            return RexUtil.composeConjunction(rexBuilder, nodes, false);
          }
        };
    salesSchema.addTable(Util.last(emp20View.getQualifiedName()));
    emp20View.addColumn("EMPNO", f.intType);
    emp20View.addColumn("ENAME", f.varchar20Type);
    emp20View.addColumn("JOB", f.varchar10Type);
    emp20View.addColumn("MGR", f.intTypeNull);
    emp20View.addColumn("HIREDATE", f.timestampType);
    emp20View.addColumn("SAL", f.intType);
    emp20View.addColumn("COMM", f.intType);
    emp20View.addColumn("SLACKER", f.booleanType);
    registerTable(emp20View);

    MockTable empNullables20View =
        new MockViewTable(this, salesSchema.getCatalogName(), salesSchema.name,
            "EMPNULLABLES_20", false, 600, empNullablesTable, m0, null,
            NullInitializerExpressionFactory.INSTANCE) {
          public RexNode getConstraint(RexBuilder rexBuilder,
              RelDataType tableRowType) {
            final RelDataTypeField deptnoField =
                tableRowType.getFieldList().get(7);
            final RelDataTypeField salField =
                tableRowType.getFieldList().get(5);
            final List<RexNode> nodes = Arrays.asList(
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
            return RexUtil.composeConjunction(rexBuilder, nodes, false);
          }
        };
    salesSchema.addTable(Util.last(empNullables20View.getQualifiedName()));
    empNullables20View.addColumn("EMPNO", f.intType);
    empNullables20View.addColumn("ENAME", f.varchar20Type);
    empNullables20View.addColumn("JOB", f.varchar10TypeNull);
    empNullables20View.addColumn("MGR", f.intTypeNull);
    empNullables20View.addColumn("HIREDATE", f.timestampTypeNull);
    empNullables20View.addColumn("SAL", f.intTypeNull);
    empNullables20View.addColumn("COMM", f.intTypeNull);
    empNullables20View.addColumn("SLACKER", f.booleanTypeNull);
    registerTable(empNullables20View);

    MockSchema structTypeSchema = new MockSchema("STRUCT");
    registerSchema(structTypeSchema);
    final List<CompoundNameColumn> columns = Arrays.asList(
        new CompoundNameColumn("", "K0", f.varchar20Type),
        new CompoundNameColumn("", "C1", f.varchar20Type),
        new CompoundNameColumn("F1", "A0", f.intType),
        new CompoundNameColumn("F2", "A0", f.booleanType),
        new CompoundNameColumn("F0", "C0", f.intType),
        new CompoundNameColumn("F1", "C0", f.intTypeNull),
        new CompoundNameColumn("F0", "C1", f.intType),
        new CompoundNameColumn("F1", "C2", f.intType),
        new CompoundNameColumn("F2", "C3", f.intType));
    final CompoundNameColumnResolver structTypeTableResolver =
        new CompoundNameColumnResolver(columns, "F0");
    final MockTable structTypeTable =
        MockTable.create(this, structTypeSchema, "T", false, 100,
            structTypeTableResolver);
    for (CompoundNameColumn column : columns) {
      structTypeTable.addColumn(column.getName(), column.type);
    }
    registerTable(structTypeTable);

    final List<CompoundNameColumn> columnsNullable = Arrays.asList(
        new CompoundNameColumn("", "K0", f.varchar20TypeNull),
        new CompoundNameColumn("", "C1", f.varchar20TypeNull),
        new CompoundNameColumn("F1", "A0", f.intTypeNull),
        new CompoundNameColumn("F2", "A0", f.booleanTypeNull),
        new CompoundNameColumn("F0", "C0", f.intTypeNull),
        new CompoundNameColumn("F1", "C0", f.intTypeNull),
        new CompoundNameColumn("F0", "C1", f.intTypeNull),
        new CompoundNameColumn("F1", "C2", f.intType),
        new CompoundNameColumn("F2", "C3", f.intTypeNull));
    final MockTable structNullableTypeTable =
        MockTable.create(this, structTypeSchema, "T_NULLABLES", false, 100,
            structTypeTableResolver);
    for (CompoundNameColumn column : columnsNullable) {
      structNullableTypeTable.addColumn(column.getName(), column.type);
    }
    registerTable(structNullableTypeTable);

    // Register "STRUCT.T_10" view.
    // Same columns as "STRUCT.T",
    // but "F0.C0" is set to 10 by default,
    // which is the equivalent of:
    //   SELECT *
    //   FROM T
    //   WHERE F0.C0 = 10
    // This table uses MockViewTable which does not populate the constrained columns with default
    // values on INSERT.
    final ImmutableIntList m1 = ImmutableIntList.of(0, 1, 2, 3, 4, 5, 6, 7, 8);
    MockTable struct10View =
        new MockViewTable(this, structTypeSchema.getCatalogName(),
            structTypeSchema.name, "T_10", false, 20, structTypeTable,
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
    registerTablesWithRollUp(salesSchema, f);
    return this;
  }

  private void registerTablesWithRollUp(MockSchema schema, Fixture f) {
    // Register "EMP_R" table. Contains a rolled up column.
    final MockTable empRolledTable =
            MockTable.create(this, schema, "EMP_R", false, 14);
    empRolledTable.addColumn("EMPNO", f.intType, true);
    empRolledTable.addColumn("DEPTNO", f.intType);
    empRolledTable.addColumn("SLACKER", f.booleanType);
    empRolledTable.addColumn("SLACKINGMIN", f.intType);
    empRolledTable.registerRolledUpColumn("SLACKINGMIN");
    registerTable(empRolledTable);

    // Register the "DEPT_R" table. Doesn't contain a rolled up column,
    // but is useful for testing join
    MockTable deptSlackingTable = MockTable.create(this, schema, "DEPT_R", false, 4);
    deptSlackingTable.addColumn("DEPTNO", f.intType, true);
    deptSlackingTable.addColumn("SLACKINGMIN", f.intType);
    registerTable(deptSlackingTable);

    // Register nested schema NEST that contains table with a rolled up column.
    MockSchema nestedSchema = new MockSchema("NEST");
    registerNestedSchema(schema, nestedSchema);

    // Register "EMP_R" table which contains a rolled up column in NEST schema.
    ImmutableList<String> tablePath =
        ImmutableList.of(schema.getCatalogName(), schema.name, nestedSchema.name, "EMP_R");
    final MockTable nestedEmpRolledTable = MockTable.create(this, tablePath, false, 14);
    nestedEmpRolledTable.addColumn("EMPNO", f.intType, true);
    nestedEmpRolledTable.addColumn("DEPTNO", f.intType);
    nestedEmpRolledTable.addColumn("SLACKER", f.booleanType);
    nestedEmpRolledTable.addColumn("SLACKINGMIN", f.intType);
    nestedEmpRolledTable.registerRolledUpColumn("SLACKINGMIN");
    registerTable(nestedEmpRolledTable);
  }

  /** Adds some extra tables to the mock catalog. These increase the time and
   * complexity of initializing the catalog (because they contain views whose
   * SQL needs to be parsed) and so are not used for all tests. */
  public MockCatalogReader init2() {
    MockSchema salesSchema = new MockSchema("SALES");
    // Same as "EMP_20" except it uses ModifiableViewTable which populates
    // constrained columns with default values on INSERT and has a single constraint on DEPTNO.
    List<String> empModifiableViewNames = ImmutableList.of(
        salesSchema.getCatalogName(), salesSchema.name, "EMP_MODIFIABLEVIEW");
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
        salesSchema.getCatalogName(), salesSchema.name, "EMP_MODIFIABLEVIEW2");
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
        salesSchema.getCatalogName(), salesSchema.name, "EMP_MODIFIABLEVIEW3");
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
    final Fixture f = new Fixture();
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

  //~ Methods ----------------------------------------------------------------

  protected void registerTable(final MockTable table) {
    table.onRegister(typeFactory);
    final WrapperTable wrapperTable = new WrapperTable(table);
    if (table.stream) {
      registerTable(table.names,
          new StreamableWrapperTable(table) {
            public Table stream() {
              return wrapperTable;
            }
          });
    } else {
      registerTable(table.names, wrapperTable);
    }
  }

  private void registerTable(final List<String> names, final Table table) {
    assert names.get(0).equals(DEFAULT_CATALOG);
    final List<String> schemaPath = Util.skipLast(names);
    final String tableName = Util.last(names);
    final CalciteSchema schema = SqlValidatorUtil.getSchema(rootSchema,
        schemaPath, SqlNameMatchers.withCaseSensitive(true));
    schema.add(tableName, table);
  }

  protected void registerSchema(MockSchema schema) {
    rootSchema.add(schema.name, new AbstractSchema());
  }

  private void registerNestedSchema(MockSchema parentSchema, MockSchema schema) {
    rootSchema.getSubSchema(parentSchema.getName(), true)
        .add(schema.name, new AbstractSchema());
  }

  public RelDataType getNamedType(SqlIdentifier typeName) {
    if (typeName.equalsDeep(addressType.getSqlIdentifier(), Litmus.IGNORE)) {
      return addressType;
    } else {
      return null;
    }
  }

  private static List<RelCollation> deduceMonotonicity(
      Prepare.PreparingTable table) {
    final List<RelCollation> collationList = Lists.newArrayList();

    // Deduce which fields the table is sorted on.
    int i = -1;
    for (RelDataTypeField field : table.getRowType().getFieldList()) {
      ++i;
      final SqlMonotonicity monotonicity =
          table.getMonotonicity(field.getName());
      if (monotonicity != SqlMonotonicity.NOT_MONOTONIC) {
        final RelFieldCollation.Direction direction =
            monotonicity.isDecreasing()
                ? RelFieldCollation.Direction.DESCENDING
                : RelFieldCollation.Direction.ASCENDING;
        collationList.add(
            RelCollations.of(
                new RelFieldCollation(i, direction)));
      }
    }
    return collationList;
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Column resolver*/
  public interface ColumnResolver {
    List<Pair<RelDataTypeField, List<String>>> resolveColumn(
        RelDataType rowType, RelDataTypeFactory typeFactory, List<String> names);
  }

  /** Mock schema. */
  public static class MockSchema {
    private final List<String> tableNames = Lists.newArrayList();
    private String name;

    public MockSchema(String name) {
      this.name = name;
    }

    public void addTable(String name) {
      tableNames.add(name);
    }

    public String getCatalogName() {
      return DEFAULT_CATALOG;
    }

    public String getName() {
      return name;
    }
  }

  /**
   * Mock implementation of
   * {@link org.apache.calcite.prepare.Prepare.PreparingTable}.
   */
  public static class MockTable extends Prepare.AbstractPreparingTable {
    protected final MockCatalogReader catalogReader;
    protected final boolean stream;
    protected final double rowCount;
    protected final List<Map.Entry<String, RelDataType>> columnList =
        new ArrayList<>();
    protected final List<Integer> keyList = new ArrayList<>();
    protected final List<RelReferentialConstraint> referentialConstraints =
        new ArrayList<>();
    protected RelDataType rowType;
    protected List<RelCollation> collationList;
    protected final List<String> names;
    protected final Set<String> monotonicColumnSet = Sets.newHashSet();
    protected StructKind kind = StructKind.FULLY_QUALIFIED;
    protected final ColumnResolver resolver;
    protected final InitializerExpressionFactory initializerFactory;
    protected final Set<String> rolledUpColumns = new HashSet<>();

    public MockTable(MockCatalogReader catalogReader, String catalogName,
        String schemaName, String name, boolean stream, double rowCount,
        ColumnResolver resolver,
        InitializerExpressionFactory initializerFactory) {
      this(catalogReader, ImmutableList.of(catalogName, schemaName, name), stream, rowCount,
          resolver, initializerFactory);
    }

    public void registerRolledUpColumn(String columnName) {
      rolledUpColumns.add(columnName);
    }

    private MockTable(MockCatalogReader catalogReader, List<String> names, boolean stream,
        double rowCount, ColumnResolver resolver, InitializerExpressionFactory initializerFactory) {
      this.catalogReader = catalogReader;
      this.stream = stream;
      this.rowCount = rowCount;
      this.names = names;
      this.resolver = resolver;
      this.initializerFactory = initializerFactory;
    }

    /**
     * Copy constructor.
     */
    protected MockTable(MockCatalogReader catalogReader, boolean stream, double rowCount,
        List<Map.Entry<String, RelDataType>> columnList, List<Integer> keyList,
        RelDataType rowType, List<RelCollation> collationList, List<String> names,
        Set<String> monotonicColumnSet, StructKind kind, ColumnResolver resolver,
        InitializerExpressionFactory initializerFactory) {
      this.catalogReader = catalogReader;
      this.stream = stream;
      this.rowCount = rowCount;
      this.rowType = rowType;
      this.collationList = collationList;
      this.names = names;
      this.kind = kind;
      this.resolver = resolver;
      this.initializerFactory = initializerFactory;
      for (String name : monotonicColumnSet) {
        addMonotonic(name);
      }
    }

    /** Implementation of AbstractModifiableTable. */
    private class ModifiableTable extends JdbcTest.AbstractModifiableTable
        implements ExtensibleTable, Wrapper {
      protected ModifiableTable(String tableName) {
        super(tableName);
      }

      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(MockTable.this.getRowType().getFieldList());
      }

      @Override public Collection getModifiableCollection() {
        return null;
      }

      @Override public <E> Queryable<E>
      asQueryable(QueryProvider queryProvider, SchemaPlus schema,
          String tableName) {
        return null;
      }

      @Override public Type getElementType() {
        return null;
      }

      @Override public Expression getExpression(SchemaPlus schema,
          String tableName, Class clazz) {
        return null;
      }

      @Override public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(initializerFactory)) {
          return aClass.cast(initializerFactory);
        } else if (aClass.isInstance(MockTable.this)) {
          return aClass.cast(MockTable.this);
        }
        return super.unwrap(aClass);
      }

      @Override public Table extend(final List<RelDataTypeField> fields) {
        return new ModifiableTable(Util.last(names)) {
          @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            ImmutableList<RelDataTypeField> allFields = ImmutableList.copyOf(
                Iterables.concat(
                    ModifiableTable.this.getRowType(typeFactory).getFieldList(),
                    fields));
            return typeFactory.createStructType(allFields);
          }
        };
      }

      @Override public int getExtendedColumnOffset() {
        return rowType.getFieldCount();
      }
    }

    @Override protected RelOptTable extend(final Table extendedTable) {
      return new MockTable(catalogReader, names, stream, rowCount, resolver, initializerFactory) {
        @Override public RelDataType getRowType() {
          return extendedTable.getRowType(catalogReader.typeFactory);
        }
      };
    }

    public static MockTable create(MockCatalogReader catalogReader,
        MockSchema schema, String name, boolean stream, double rowCount) {
      return create(catalogReader, schema, name, stream, rowCount, null);
    }

    public static MockTable create(MockCatalogReader catalogReader,
        List<String> names, boolean stream, double rowCount) {
      return new MockTable(catalogReader, names, stream, rowCount, null,
          NullInitializerExpressionFactory.INSTANCE);
    }

    public static MockTable create(MockCatalogReader catalogReader,
        MockSchema schema, String name, boolean stream, double rowCount,
        ColumnResolver resolver) {
      return create(catalogReader, schema, name, stream, rowCount, resolver,
          NullInitializerExpressionFactory.INSTANCE);
    }

    public static MockTable create(MockCatalogReader catalogReader,
        MockSchema schema, String name, boolean stream, double rowCount,
        ColumnResolver resolver,
        InitializerExpressionFactory initializerExpressionFactory) {
      MockTable table =
          new MockTable(catalogReader, schema.getCatalogName(), schema.name,
              name, stream, rowCount, resolver, initializerExpressionFactory);
      schema.addTable(name);
      return table;
    }

    public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      if (clazz.isInstance(initializerFactory)) {
        return clazz.cast(initializerFactory);
      }
      if (clazz.isAssignableFrom(Table.class)) {
        final Table table = resolver == null
            ? new ModifiableTable(Util.last(names))
                : new ModifiableTableWithCustomColumnResolving(Util.last(names));
        return clazz.cast(table);
      }
      return null;
    }

    public double getRowCount() {
      return rowCount;
    }

    public RelOptSchema getRelOptSchema() {
      return catalogReader;
    }

    public RelNode toRel(ToRelContext context) {
      return LogicalTableScan.create(context.getCluster(), this);
    }

    public List<RelCollation> getCollationList() {
      return collationList;
    }

    public RelDistribution getDistribution() {
      return RelDistributions.BROADCAST_DISTRIBUTED;
    }

    public boolean isKey(ImmutableBitSet columns) {
      return !keyList.isEmpty()
          && columns.contains(ImmutableBitSet.of(keyList));
    }

    public List<RelReferentialConstraint> getReferentialConstraints() {
      return referentialConstraints;
    }

    public RelDataType getRowType() {
      return rowType;
    }

    public boolean supportsModality(SqlModality modality) {
      return modality == (stream ? SqlModality.STREAM : SqlModality.RELATION);
    }

    public void onRegister(RelDataTypeFactory typeFactory) {
      rowType = typeFactory.createStructType(kind, Pair.right(columnList),
          Pair.left(columnList));
      collationList = deduceMonotonicity(this);
    }

    public List<String> getQualifiedName() {
      return names;
    }

    public SqlMonotonicity getMonotonicity(String columnName) {
      return monotonicColumnSet.contains(columnName)
          ? SqlMonotonicity.INCREASING
          : SqlMonotonicity.NOT_MONOTONIC;
    }

    public SqlAccessType getAllowedAccess() {
      return SqlAccessType.ALL;
    }

    public Expression getExpression(Class clazz) {
      throw new UnsupportedOperationException();
    }

    public void addColumn(String name, RelDataType type) {
      addColumn(name, type, false);
    }

    public void addColumn(String name, RelDataType type, boolean isKey) {
      if (isKey) {
        keyList.add(columnList.size());
      }
      columnList.add(Pair.of(name, type));
    }

    public void addMonotonic(String name) {
      monotonicColumnSet.add(name);
      assert Pair.left(columnList).contains(name);
    }

    public void setKind(StructKind kind) {
      this.kind = kind;
    }

    public StructKind getKind() {
      return kind;
    }

    /**
     * Subclass of {@link ModifiableTable} that also implements
     * {@link CustomColumnResolvingTable}.
     */
    private class ModifiableTableWithCustomColumnResolving
        extends ModifiableTable implements CustomColumnResolvingTable, Wrapper {

      ModifiableTableWithCustomColumnResolving(String tableName) {
        super(tableName);
      }

      @Override public List<Pair<RelDataTypeField, List<String>>> resolveColumn(
          RelDataType rowType, RelDataTypeFactory typeFactory,
          List<String> names) {
        return resolver.resolveColumn(rowType, typeFactory, names);
      }
    }
  }

  /**
   * Alternative to MockViewTable that exercises code paths in ModifiableViewTable
   * and ModifiableViewTableInitializerExpressionFactory.
   */
  public static class MockModifiableViewRelOptTable extends MockTable {
    private final MockModifiableViewTable modifiableViewTable;

    private MockModifiableViewRelOptTable(MockModifiableViewTable modifiableViewTable,
        MockCatalogReader catalogReader, String catalogName, String schemaName, String name,
        boolean stream, double rowCount, ColumnResolver resolver,
        InitializerExpressionFactory initializerExpressionFactory) {
      super(catalogReader, ImmutableList.of(catalogName, schemaName, name), stream, rowCount,
          resolver, initializerExpressionFactory);
      this.modifiableViewTable = modifiableViewTable;
    }

    /**
     * Copy constructor.
     */
    private MockModifiableViewRelOptTable(MockModifiableViewTable modifiableViewTable,
        MockCatalogReader catalogReader, boolean stream, double rowCount,
        List<Map.Entry<String, RelDataType>> columnList, List<Integer> keyList,
        RelDataType rowType, List<RelCollation> collationList, List<String> names,
        Set<String> monotonicColumnSet, StructKind kind, ColumnResolver resolver,
        InitializerExpressionFactory initializerFactory) {
      super(catalogReader, stream, rowCount, columnList, keyList, rowType, collationList, names,
          monotonicColumnSet, kind, resolver, initializerFactory);
      this.modifiableViewTable = modifiableViewTable;
    }

    public static MockModifiableViewRelOptTable create(MockModifiableViewTable modifiableViewTable,
        MockCatalogReader catalogReader, String catalogName, String schemaName, String name,
        boolean stream, double rowCount, ColumnResolver resolver) {
      final Table underlying = modifiableViewTable.unwrap(Table.class);
      final InitializerExpressionFactory initializerExpressionFactory =
          underlying != null && underlying instanceof Wrapper
              ? ((Wrapper) underlying).unwrap(InitializerExpressionFactory.class)
              : NullInitializerExpressionFactory.INSTANCE;
      return new MockModifiableViewRelOptTable(modifiableViewTable,
          catalogReader, catalogName, schemaName, name, stream, rowCount,
          resolver, Util.first(initializerExpressionFactory,
          NullInitializerExpressionFactory.INSTANCE));
    }

    public static MockViewTableMacro viewMacro(CalciteSchema schema, String viewSql,
        List<String> schemaPath, List<String> viewPath, Boolean modifiable) {
      return new MockViewTableMacro(schema, viewSql, schemaPath, viewPath, modifiable);
    }

    @Override public RelDataType getRowType() {
      return modifiableViewTable.getRowType(catalogReader.typeFactory);
    }

    @Override protected RelOptTable extend(Table extendedTable) {
      return new MockModifiableViewRelOptTable((MockModifiableViewTable) extendedTable,
          catalogReader, stream, rowCount, columnList, keyList, rowType, collationList, names,
          monotonicColumnSet, kind, resolver, initializerFactory);
    }

    @Override public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(modifiableViewTable)) {
        return clazz.cast(modifiableViewTable);
      }
      return super.unwrap(clazz);
    }

    /**
     * A TableMacro that creates mock ModifiableViewTable.
     */
    public static class MockViewTableMacro extends ViewTableMacro {
      MockViewTableMacro(CalciteSchema schema, String viewSql, List<String> schemaPath,
          List<String> viewPath, Boolean modifiable) {
        super(schema, viewSql, schemaPath, viewPath, modifiable);
      }

      @Override protected ModifiableViewTable modifiableViewTable(
          CalcitePrepare.AnalyzeViewResult parsed, String viewSql,
          List<String> schemaPath, List<String> viewPath, CalciteSchema schema) {
        final JavaTypeFactory typeFactory = (JavaTypeFactory) parsed.typeFactory;
        final Type elementType = typeFactory.getJavaClass(parsed.rowType);
        return new MockModifiableViewTable(elementType,
            RelDataTypeImpl.proto(parsed.rowType), viewSql, schemaPath, viewPath,
            parsed.table, Schemas.path(schema.root(), parsed.tablePath),
            parsed.constraint, parsed.columnMapping);
      }
    }

    /**
     * A mock of ModifiableViewTable that can unwrap a mock RelOptTable.
     */
    private static class MockModifiableViewTable extends ModifiableViewTable {
      private final RexNode constraint;

      MockModifiableViewTable(Type elementType, RelProtoDataType rowType,
          String viewSql, List<String> schemaPath, List<String> viewPath,
          Table table, Path tablePath, RexNode constraint,
          ImmutableIntList columnMapping) {
        super(elementType, rowType, viewSql, schemaPath, viewPath, table,
            tablePath, constraint, columnMapping);
        this.constraint = constraint;
      }

      @Override public ModifiableViewTable extend(Table extendedTable,
          RelProtoDataType protoRowType, ImmutableIntList newColumnMapping) {
        return new MockModifiableViewTable(getElementType(), protoRowType,
            getViewSql(), getSchemaPath(), getViewPath(), extendedTable,
            getTablePath(), constraint, newColumnMapping);
      }
    }
  }

  /**
   * Mock implementation of
   * {@link org.apache.calcite.prepare.Prepare.PreparingTable} for views.
   */
  public abstract static class MockViewTable extends MockTable {
    private final MockTable fromTable;
    private final Table table;
    private final ImmutableIntList mapping;

    MockViewTable(MockCatalogReader catalogReader, String catalogName,
        String schemaName, String name, boolean stream, double rowCount,
        MockTable fromTable, ImmutableIntList mapping, ColumnResolver resolver,
        InitializerExpressionFactory initializerFactory) {
      super(catalogReader, catalogName, schemaName, name, stream, rowCount,
          resolver, initializerFactory);
      this.fromTable = fromTable;
      this.table = fromTable.unwrap(Table.class);
      this.mapping = mapping;
    }

    /** Implementation of AbstractModifiableView. */
    private class ModifiableView extends JdbcTest.AbstractModifiableView
        implements Wrapper {
      @Override public Table getTable() {
        return fromTable.unwrap(Table.class);
      }

      @Override public Path getTablePath() {
        final ImmutableList.Builder<Pair<String, Schema>> builder =
            ImmutableList.builder();
        for (String name : fromTable.names) {
          builder.add(Pair.<String, Schema>of(name, null));
        }
        return Schemas.path(builder.build());
      }

      @Override public ImmutableIntList getColumnMapping() {
        return mapping;
      }

      @Override public RexNode getConstraint(RexBuilder rexBuilder,
          RelDataType tableRowType) {
        return MockViewTable.this.getConstraint(rexBuilder, tableRowType);
      }

      @Override public RelDataType
      getRowType(final RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(
            new AbstractList<Map.Entry<String, RelDataType>>() {
              @Override public Map.Entry<String, RelDataType>
              get(int index) {
                return table.getRowType(typeFactory).getFieldList()
                    .get(mapping.get(index));
              }

              @Override public int size() {
                return mapping.size();
              }
            });
      }

      @Override public <C> C unwrap(Class<C> aClass) {
        if (table instanceof Wrapper) {
          final C c = ((Wrapper) table).unwrap(aClass);
          if (c != null) {
            return c;
          }
        }
        return super.unwrap(aClass);
      }
    }

    /**
     * Subclass of ModifiableView that also implements
     * CustomColumnResolvingTable.
     */
    private class ModifiableViewWithCustomColumnResolving
        extends ModifiableView implements CustomColumnResolvingTable, Wrapper {

      @Override public List<Pair<RelDataTypeField, List<String>>> resolveColumn(
          RelDataType rowType, RelDataTypeFactory typeFactory, List<String> names) {
        return resolver.resolveColumn(rowType, typeFactory, names);
      }

      @Override public <C> C unwrap(Class<C> aClass) {
        if (table instanceof Wrapper) {
          final C c = ((Wrapper) table).unwrap(aClass);
          if (c != null) {
            return c;
          }
        }
        return super.unwrap(aClass);
      }
    }

    protected abstract RexNode getConstraint(RexBuilder rexBuilder,
        RelDataType tableRowType);

    @Override public void onRegister(RelDataTypeFactory typeFactory) {
      super.onRegister(typeFactory);
      // To simulate getRowType() behavior in ViewTable.
      final RelProtoDataType protoRowType = RelDataTypeImpl.proto(rowType);
      rowType = protoRowType.apply(typeFactory);
    }

    @Override public RelNode toRel(ToRelContext context) {
      RelNode rel = LogicalTableScan.create(context.getCluster(), fromTable);
      final RexBuilder rexBuilder = context.getCluster().getRexBuilder();
      rel = LogicalFilter.create(
          rel, getConstraint(rexBuilder, rel.getRowType()));
      final List<RelDataTypeField> fieldList =
          rel.getRowType().getFieldList();
      final List<Pair<RexNode, String>> projects =
          new AbstractList<Pair<RexNode, String>>() {
            @Override public Pair<RexNode, String> get(int index) {
              return RexInputRef.of2(mapping.get(index), fieldList);
            }

            @Override public int size() {
              return mapping.size();
            }
          };
      return LogicalProject.create(rel, Pair.left(projects),
          Pair.right(projects));
    }

    @Override public <T> T unwrap(Class<T> clazz) {
      if (clazz.isAssignableFrom(ModifiableView.class)) {
        ModifiableView view = resolver == null
            ? new ModifiableView()
                : new ModifiableViewWithCustomColumnResolving();
        return clazz.cast(view);
      }
      return super.unwrap(clazz);
    }
  }

  /**
   * Mock implementation of
   * {@link org.apache.calcite.prepare.Prepare.PreparingTable} with dynamic record type.
   */
  public static class MockDynamicTable extends MockTable {
    MockDynamicTable(MockCatalogReader catalogReader, String catalogName,
        String schemaName, String name, boolean stream, double rowCount) {
      super(catalogReader, catalogName, schemaName, name, stream, rowCount,
          null, NullInitializerExpressionFactory.INSTANCE);
    }

    public void onRegister(RelDataTypeFactory typeFactory) {
      rowType = new DynamicRecordTypeImpl(typeFactory);
    }

    /**
     * Recreates an immutable rowType, if the table has Dynamic Record Type,
     * when converts table to Rel.
     */
    public RelNode toRel(ToRelContext context) {
      if (rowType.isDynamicStruct()) {
        rowType = new RelRecordType(rowType.getFieldList());
      }
      return super.toRel(context);
    }
  }

  /** Struct type based on another struct type. */
  private static class DelegateStructType implements RelDataType {
    private RelDataType delegate;
    private StructKind structKind;

    DelegateStructType(RelDataType delegate, StructKind structKind) {
      assert delegate.isStruct();
      this.delegate = delegate;
      this.structKind = structKind;
    }

    public boolean isStruct() {
      return delegate.isStruct();
    }

    public boolean isDynamicStruct() {
      return delegate.isDynamicStruct();
    }

    public List<RelDataTypeField> getFieldList() {
      return delegate.getFieldList();
    }

    public List<String> getFieldNames() {
      return delegate.getFieldNames();
    }

    public int getFieldCount() {
      return delegate.getFieldCount();
    }

    public StructKind getStructKind() {
      return structKind;
    }

    public RelDataTypeField getField(String fieldName, boolean caseSensitive,
        boolean elideRecord) {
      return delegate.getField(fieldName, caseSensitive, elideRecord);
    }

    public boolean isNullable() {
      return delegate.isNullable();
    }

    public RelDataType getComponentType() {
      return delegate.getComponentType();
    }

    public RelDataType getKeyType() {
      return delegate.getKeyType();
    }

    public RelDataType getValueType() {
      return delegate.getValueType();
    }

    public Charset getCharset() {
      return delegate.getCharset();
    }

    public SqlCollation getCollation() {
      return delegate.getCollation();
    }

    public SqlIntervalQualifier getIntervalQualifier() {
      return delegate.getIntervalQualifier();
    }

    public int getPrecision() {
      return delegate.getPrecision();
    }

    public int getScale() {
      return delegate.getScale();
    }

    public SqlTypeName getSqlTypeName() {
      return delegate.getSqlTypeName();
    }

    public SqlIdentifier getSqlIdentifier() {
      return delegate.getSqlIdentifier();
    }

    public String getFullTypeString() {
      return delegate.getFullTypeString();
    }

    public RelDataTypeFamily getFamily() {
      return delegate.getFamily();
    }

    public RelDataTypePrecedenceList getPrecedenceList() {
      return delegate.getPrecedenceList();
    }

    public RelDataTypeComparability getComparability() {
      return delegate.getComparability();
    }
  }

  /** Column having names with multiple parts. */
  private static final class CompoundNameColumn {
    final String first;
    final String second;
    final RelDataType type;

    CompoundNameColumn(String first, String second, RelDataType type) {
      this.first = first;
      this.second = second;
      this.type = type;
    }

    String getName() {
      return (first.isEmpty() ? "" : ("\"" + first + "\"."))
          + ("\"" + second + "\"");
    }
  }

  /** ColumnResolver implementation that resolves CompoundNameColumn by simulating
   *  Phoenix behaviors. */
  private static final class CompoundNameColumnResolver implements ColumnResolver {
    private final Map<String, Integer> nameMap = Maps.newHashMap();
    private final Map<String, Map<String, Integer>> groupMap = Maps.newHashMap();
    private final String defaultColumnGroup;

    CompoundNameColumnResolver(
        List<CompoundNameColumn> columns, String defaultColumnGroup) {
      this.defaultColumnGroup = defaultColumnGroup;
      for (Ord<CompoundNameColumn> column : Ord.zip(columns)) {
        nameMap.put(column.e.getName(), column.i);
        Map<String, Integer> subMap = groupMap.get(column.e.first);
        if (subMap == null) {
          subMap = Maps.newHashMap();
          groupMap.put(column.e.first, subMap);
        }
        subMap.put(column.e.second, column.i);
      }
    }

    @Override public List<Pair<RelDataTypeField, List<String>>> resolveColumn(
        RelDataType rowType, RelDataTypeFactory typeFactory, List<String> names) {
      List<Pair<RelDataTypeField, List<String>>> ret = new ArrayList<>();
      if (names.size() >= 2) {
        Map<String, Integer> subMap = groupMap.get(names.get(0));
        if (subMap != null) {
          Integer index = subMap.get(names.get(1));
          if (index != null) {
            ret.add(
                new Pair<RelDataTypeField, List<String>>(
                    rowType.getFieldList().get(index),
                    names.subList(2, names.size())));
          }
        }
      }

      final String columnName = names.get(0);
      final List<String> remainder = names.subList(1, names.size());
      Integer index = nameMap.get(columnName);
      if (index != null) {
        ret.add(
            new Pair<RelDataTypeField, List<String>>(
                rowType.getFieldList().get(index), remainder));
        return ret;
      }

      final List<String> priorityGroups = Arrays.asList("", defaultColumnGroup);
      for (String group : priorityGroups) {
        Map<String, Integer> subMap = groupMap.get(group);
        if (subMap != null) {
          index = subMap.get(columnName);
          if (index != null) {
            ret.add(
                new Pair<RelDataTypeField, List<String>>(
                    rowType.getFieldList().get(index), remainder));
            return ret;
          }
        }
      }
      for (Map.Entry<String, Map<String, Integer>> entry : groupMap.entrySet()) {
        if (priorityGroups.contains(entry.getKey())) {
          continue;
        }
        index = entry.getValue().get(columnName);
        if (index != null) {
          ret.add(
              new Pair<RelDataTypeField, List<String>>(
                  rowType.getFieldList().get(index), remainder));
        }
      }

      if (ret.isEmpty() && names.size() == 1) {
        Map<String, Integer> subMap = groupMap.get(columnName);
        if (subMap != null) {
          List<Map.Entry<String, Integer>> entries =
              new ArrayList<>(subMap.entrySet());
          Collections.sort(
              entries,
              new Comparator<Map.Entry<String, Integer>>() {
                @Override public int compare(
                    Entry<String, Integer> o1, Entry<String, Integer> o2) {
                  return o1.getValue() - o2.getValue();
                }
              });
          ret.add(
              new Pair<RelDataTypeField, List<String>>(
                  new RelDataTypeFieldImpl(
                      columnName, -1,
                      createStructType(
                          rowType,
                          typeFactory,
                          entries)),
                  remainder));
        }
      }

      return ret;
    }

    private static RelDataType createStructType(
        final RelDataType rowType,
        RelDataTypeFactory typeFactory,
        final List<Map.Entry<String, Integer>> entries) {
      return typeFactory.createStructType(
          StructKind.PEEK_FIELDS,
          new AbstractList<RelDataType>() {
            @Override public RelDataType get(int index) {
              final int i = entries.get(index).getValue();
              return rowType.getFieldList().get(i).getType();
            }
            @Override public int size() {
              return entries.size();
            }
          },
          new AbstractList<String>() {
            @Override public String get(int index) {
              return entries.get(index).getKey();
            }
            @Override public int size() {
              return entries.size();
            }
          });
    }
  }

  /** Wrapper around a {@link MockTable}, giving it a {@link Table} interface.
   * You can get the {@code MockTable} by calling {@link #unwrap(Class)}. */
  private static class WrapperTable implements Table, Wrapper {
    private final MockTable table;

    WrapperTable(MockTable table) {
      this.table = table;
    }

    public <C> C unwrap(Class<C> aClass) {
      return aClass.isInstance(this) ? aClass.cast(this)
          : aClass.isInstance(table) ? aClass.cast(table)
          : null;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return table.getRowType();
    }

    public Statistic getStatistic() {
      return new Statistic() {
        public Double getRowCount() {
          return table.rowCount;
        }

        public boolean isKey(ImmutableBitSet columns) {
          return table.isKey(columns);
        }

        public List<RelReferentialConstraint> getReferentialConstraints() {
          return table.getReferentialConstraints();
        }

        public List<RelCollation> getCollations() {
          return table.collationList;
        }

        public RelDistribution getDistribution() {
          return table.getDistribution();
        }
      };
    }

    @Override public boolean isRolledUp(String column) {
      return table.rolledUpColumns.contains(column);
    }

    @Override public boolean rolledUpColumnValidInsideAgg(String column,
                                                          SqlCall call, SqlNode parent,
                                                          CalciteConnectionConfig config) {
      // For testing
      return call.getKind() != SqlKind.MAX
              && (parent.getKind() == SqlKind.SELECT || parent.getKind() == SqlKind.FILTER);
    }

    public Schema.TableType getJdbcTableType() {
      return table.stream ? Schema.TableType.STREAM : Schema.TableType.TABLE;
    }
  }

  /** Wrapper around a {@link MockTable}, giving it a {@link StreamableTable}
   * interface. */
  private static class StreamableWrapperTable extends WrapperTable
      implements StreamableTable {
    StreamableWrapperTable(MockTable table) {
      super(table);
    }

    public Table stream() {
      return this;
    }
  }

  /** Default values for the "EMPDEFAULTS" table. */
  private static class EmpInitializerExpressionFactory
      extends NullInitializerExpressionFactory {
    @Override public ColumnStrategy generationStrategy(RelOptTable table,
        int iColumn) {
      switch (iColumn) {
      case 0:
      case 1:
      case 5:
        return ColumnStrategy.DEFAULT;
      default:
        return super.generationStrategy(table, iColumn);
      }
    }

    @Override public RexNode newColumnDefaultValue(RelOptTable table,
        int iColumn, InitializerContext context) {
      final RexBuilder rexBuilder = context.getRexBuilder();
      final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
      switch (iColumn) {
      case 0:
        return rexBuilder.makeExactLiteral(new BigDecimal(123),
            typeFactory.createSqlType(SqlTypeName.INTEGER));
      case 1:
        return rexBuilder.makeLiteral("Bob");
      case 5:
        return rexBuilder.makeExactLiteral(new BigDecimal(555),
            typeFactory.createSqlType(SqlTypeName.INTEGER));
      default:
        return rexBuilder.constantNull();
      }
    }
  }

  /** Types used during initialization. */
  private class Fixture {
    final RelDataType intType =
        typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType intTypeNull =
        typeFactory.createTypeWithNullability(intType, true);
    final RelDataType varchar10Type =
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
    final RelDataType varchar10TypeNull =
        typeFactory.createTypeWithNullability(varchar10Type, true);
    final RelDataType varchar20Type =
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 20);
    final RelDataType varchar20TypeNull =
        typeFactory.createTypeWithNullability(varchar20Type, true);
    final RelDataType timestampType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    final RelDataType timestampTypeNull =
        typeFactory.createTypeWithNullability(timestampType, true);
    final RelDataType dateType =
        typeFactory.createSqlType(SqlTypeName.DATE);
    final RelDataType booleanType =
        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataType booleanTypeNull =
        typeFactory.createTypeWithNullability(booleanType, true);
    final RelDataType rectilinearCoordType =
        typeFactory.builder()
            .add("X", intType)
            .add("Y", intType)
            .build();
    final RelDataType rectilinearPeekCoordType =
        typeFactory.builder()
            .add("X", intType)
            .add("Y", intType)
            .kind(StructKind.PEEK_FIELDS)
            .build();
    final RelDataType rectilinearPeekNoExpandCoordType =
        typeFactory.builder()
            .add("M", intType)
            .add("SUB",
                typeFactory.builder()
                    .add("A", intType)
                    .add("B", intType)
                    .kind(StructKind.PEEK_FIELDS_NO_EXPAND)
                    .build())
            .kind(StructKind.PEEK_FIELDS_NO_EXPAND)
            .build();
    final RelDataType skillRecordType =
        typeFactory.builder()
            .add("TYPE", varchar10Type)
            .add("DESC", varchar20Type)
            .build();
    final RelDataType empRecordType =
        typeFactory.builder()
            .add("EMPNO", intType)
            .add("ENAME", varchar10Type)
            .add("SKILLS", typeFactory.createArrayType(skillRecordType, -1))
            .build();
    final RelDataType empListType =
        typeFactory.createArrayType(empRecordType, -1);

    // TODO jvs 12-Feb-2005: register this canonical instance with type
    // factory
    final ObjectSqlType addressType =
        new ObjectSqlType(SqlTypeName.STRUCTURED,
            new SqlIdentifier("ADDRESS", SqlParserPos.ZERO),
            false,
            Arrays.asList(
                new RelDataTypeFieldImpl("STREET", 0, varchar20Type),
                new RelDataTypeFieldImpl("CITY", 1, varchar20Type),
                new RelDataTypeFieldImpl("ZIP", 2, intType),
                new RelDataTypeFieldImpl("STATE", 3, varchar20Type)),
            RelDataTypeComparability.NONE);
  }

  /** To check whether
   * {@link InitializerExpressionFactory#newColumnDefaultValue} is called.
   *
   * <p>If a column is in {@code defaultColumns}, returns 1 as the default
   * value. */
  public static class CountingFactory extends NullInitializerExpressionFactory {
    static final ThreadLocal<AtomicInteger> THREAD_CALL_COUNT =
        new ThreadLocal<AtomicInteger>() {
          protected AtomicInteger initialValue() {
            return new AtomicInteger();
          }
        };

    private final List<String> defaultColumns;

    CountingFactory(List<String> defaultColumns) {
      this.defaultColumns = ImmutableList.copyOf(defaultColumns);
    }

    @Override public ColumnStrategy generationStrategy(RelOptTable table,
        int iColumn) {
      final RelDataTypeField field =
          table.getRowType().getFieldList().get(iColumn);
      if (defaultColumns.contains(field.getName())) {
        return ColumnStrategy.DEFAULT;
      }
      return super.generationStrategy(table, iColumn);
    }

    @Override public RexNode newColumnDefaultValue(RelOptTable table,
        int iColumn, InitializerContext context) {
      THREAD_CALL_COUNT.get().incrementAndGet();
      final RelDataTypeField field =
          table.getRowType().getFieldList().get(iColumn);
      if (defaultColumns.contains(field.getName())) {
        final RexBuilder rexBuilder = context.getRexBuilder();
        return rexBuilder.makeExactLiteral(BigDecimal.ONE);
      }
      return super.newColumnDefaultValue(table, iColumn, context);
    }

    @Override public RexNode newAttributeInitializer(RelDataType type,
        SqlFunction constructor, int iAttribute,
        List<RexNode> constructorArgs, InitializerContext context) {
      THREAD_CALL_COUNT.get().incrementAndGet();
      return super.newAttributeInitializer(type, constructor, iAttribute,
         constructorArgs, context);
    }
  }
}

// End MockCatalogReader.java
