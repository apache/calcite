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

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerImpl;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Mock implementation of {@link SqlValidatorCatalogReader} which returns tables
 * "EMP", "DEPT", "BONUS", "SALGRADE" (same as Oracle's SCOTT schema).
 */
public class MockCatalogReader implements Prepare.CatalogReader {
  //~ Static fields/initializers ---------------------------------------------

  protected static final String DEFAULT_CATALOG = "CATALOG";
  protected static final String DEFAULT_SCHEMA = "SALES";

  public static final Ordering<Iterable<String>>
  CASE_INSENSITIVE_LIST_COMPARATOR =
      Ordering.from(String.CASE_INSENSITIVE_ORDER).lexicographical();

  //~ Instance fields --------------------------------------------------------

  protected final RelDataTypeFactory typeFactory;
  private final boolean caseSensitive;
  private final boolean elideRecord = true;
  private final Map<List<String>, MockTable> tables;
  protected final Map<String, MockSchema> schemas;
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
    this.typeFactory = typeFactory;
    this.caseSensitive = caseSensitive;
    if (caseSensitive) {
      tables = new HashMap<List<String>, MockTable>();
      schemas = new HashMap<String, MockSchema>();
    } else {
      tables = new TreeMap<List<String>, MockTable>(
          CASE_INSENSITIVE_LIST_COMPARATOR);
      schemas = new TreeMap<String, MockSchema>(String.CASE_INSENSITIVE_ORDER);
    }
  }

  /**
   * Initializes this catalog reader.
   */
  public MockCatalogReader init() {
    final RelDataType intType =
        typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType intTypeNull =
        typeFactory.createTypeWithNullability(intType, true);
    final RelDataType varchar10Type =
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
    final RelDataType varchar20Type =
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 20);
    final RelDataType timestampType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    final RelDataType booleanType =
        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataType rectilinearCoordType =
        typeFactory.builder().add("X", intType).add("Y", intType).build();

    // TODO jvs 12-Feb-2005: register this canonical instance with type
    // factory
    addressType =
        new ObjectSqlType(
            SqlTypeName.STRUCTURED,
            new SqlIdentifier("ADDRESS", SqlParserPos.ZERO),
            false,
            Arrays.asList(
                new RelDataTypeFieldImpl("STREET", 0, varchar20Type),
                new RelDataTypeFieldImpl("CITY", 1, varchar20Type),
                new RelDataTypeFieldImpl("ZIP", 1, intType),
                new RelDataTypeFieldImpl("STATE", 1, varchar20Type)),
            RelDataTypeComparability.NONE);

    // Register "SALES" schema.
    MockSchema salesSchema = new MockSchema("SALES");
    registerSchema(salesSchema);

    // Register "EMP" table.
    MockTable empTable = MockTable.create(this, salesSchema, "EMP", false);
    empTable.addColumn("EMPNO", intType);
    empTable.addColumn("ENAME", varchar20Type);
    empTable.addColumn("JOB", varchar10Type);
    empTable.addColumn("MGR", intTypeNull);
    empTable.addColumn("HIREDATE", timestampType);
    empTable.addColumn("SAL", intType);
    empTable.addColumn("COMM", intType);
    empTable.addColumn("DEPTNO", intType);
    empTable.addColumn("SLACKER", booleanType);
    registerTable(empTable);

    // Register "DEPT" table.
    MockTable deptTable = MockTable.create(this, salesSchema, "DEPT", false);
    deptTable.addColumn("DEPTNO", intType);
    deptTable.addColumn("NAME", varchar10Type);
    registerTable(deptTable);

    // Register "BONUS" table.
    MockTable bonusTable = MockTable.create(this, salesSchema, "BONUS", false);
    bonusTable.addColumn("ENAME", varchar20Type);
    bonusTable.addColumn("JOB", varchar10Type);
    bonusTable.addColumn("SAL", intType);
    bonusTable.addColumn("COMM", intType);
    registerTable(bonusTable);

    // Register "SALGRADE" table.
    MockTable salgradeTable = MockTable.create(this, salesSchema, "SALGRADE",
        false);
    salgradeTable.addColumn("GRADE", intType);
    salgradeTable.addColumn("LOSAL", intType);
    salgradeTable.addColumn("HISAL", intType);
    registerTable(salgradeTable);

    // Register "EMP_ADDRESS" table
    MockTable contactAddressTable =
        MockTable.create(this, salesSchema, "EMP_ADDRESS", false);
    contactAddressTable.addColumn("EMPNO", intType);
    contactAddressTable.addColumn("HOME_ADDRESS", addressType);
    contactAddressTable.addColumn("MAILING_ADDRESS", addressType);
    registerTable(contactAddressTable);

    // Register "CUSTOMER" schema.
    MockSchema customerSchema = new MockSchema("CUSTOMER");
    registerSchema(customerSchema);

    // Register "CONTACT" table.
    MockTable contactTable = MockTable.create(this, customerSchema, "CONTACT",
        false);
    contactTable.addColumn("CONTACTNO", intType);
    contactTable.addColumn("FNAME", varchar10Type);
    contactTable.addColumn("LNAME", varchar10Type);
    contactTable.addColumn("EMAIL", varchar20Type);
    contactTable.addColumn("COORD", rectilinearCoordType);
    registerTable(contactTable);

    // Register "ACCOUNT" table.
    MockTable accountTable = MockTable.create(this, customerSchema, "ACCOUNT",
        false);
    accountTable.addColumn("ACCTNO", intType);
    accountTable.addColumn("TYPE", varchar20Type);
    accountTable.addColumn("BALANCE", intType);
    registerTable(accountTable);

    // Register "ORDERS" stream.
    MockTable ordersStream = MockTable.create(this, salesSchema, "ORDERS",
        true);
    ordersStream.addColumn("ROWTIME", timestampType);
    ordersStream.addMonotonic("ROWTIME");
    ordersStream.addColumn("PRODUCTID", intType);
    ordersStream.addColumn("ORDERID", intType);
    registerTable(ordersStream);

    // Register "SHIPMENTS" stream.
    MockTable shipmentsStream = MockTable.create(this, salesSchema, "SHIPMENTS",
        true);
    shipmentsStream.addColumn("ROWTIME", timestampType);
    shipmentsStream.addMonotonic("ROWTIME");
    shipmentsStream.addColumn("ORDERID", intType);
    registerTable(shipmentsStream);

    return this;
  }

  //~ Methods ----------------------------------------------------------------

  public Prepare.CatalogReader withSchemaPath(List<String> schemaPath) {
    return this;
  }

  public Prepare.PreparingTable getTableForMember(List<String> names) {
    return getTable(names);
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public void registerRules(RelOptPlanner planner) {
  }

  protected void registerTable(MockTable table) {
    table.onRegister(typeFactory);
    tables.put(table.getQualifiedName(), table);
  }

  protected void registerSchema(MockSchema schema) {
    schemas.put(schema.name, schema);
  }

  public Prepare.PreparingTable getTable(final List<String> names) {
    switch (names.size()) {
    case 1:
      // assume table in SALES schema (the original default)
      // if it's not supplied, because SqlValidatorTest is effectively
      // using SALES as its default schema.
      return tables.get(
          ImmutableList.of(DEFAULT_CATALOG, DEFAULT_SCHEMA, names.get(0)));
    case 2:
      return tables.get(
          ImmutableList.of(DEFAULT_CATALOG, names.get(0), names.get(1)));
    case 3:
      return tables.get(names);
    default:
      return null;
    }
  }

  public RelDataType getNamedType(SqlIdentifier typeName) {
    if (typeName.equalsDeep(
        addressType.getSqlIdentifier(),
        false)) {
      return addressType;
    } else {
      return null;
    }
  }

  public List<SqlMoniker> getAllSchemaObjectNames(List<String> names) {
    List<SqlMoniker> result;
    switch (names.size()) {
    case 0:
      // looking for catalog and schema names
      return ImmutableList.<SqlMoniker>builder()
          .add(new SqlMonikerImpl(DEFAULT_CATALOG, SqlMonikerType.CATALOG))
          .addAll(getAllSchemaObjectNames(ImmutableList.of(DEFAULT_CATALOG)))
          .build();
    case 1:
      // looking for schema names
      result = new ArrayList<SqlMoniker>();
      for (MockSchema schema : schemas.values()) {
        final String catalogName = names.get(0);
        if (schema.getCatalogName().equals(catalogName)) {
          final ImmutableList<String> names1 =
              ImmutableList.of(catalogName, schema.name);
          result.add(new SqlMonikerImpl(names1, SqlMonikerType.SCHEMA));
        }
      }
      return result;
    case 2:
      // looking for table names in the given schema
      MockSchema schema = schemas.get(names.get(1));
      if (schema == null) {
        return Collections.emptyList();
      }
      result = new ArrayList<SqlMoniker>();
      for (String tableName : schema.tableNames) {
        result.add(
            new SqlMonikerImpl(
                ImmutableList.of(schema.getCatalogName(), schema.name,
                    tableName),
                SqlMonikerType.TABLE));
      }
      return result;
    default:
      return Collections.emptyList();
    }
  }

  public List<String> getSchemaName() {
    return ImmutableList.of(DEFAULT_CATALOG, DEFAULT_SCHEMA);
  }

  private MockSchema getMockSchema(List<String> names) {
    return schemas.get(names.get(0));
  }

  public List<SqlMoniker> getAllSchemaObjectNames2(List<String> names) {
    List<SqlMoniker> result = new ArrayList<SqlMoniker>();
    if (names.isEmpty()) {
      for (MockSchema schema : schemas.values()) {
        result.add(
            new SqlMonikerImpl(schema.name, SqlMonikerType.SCHEMA));
      }
    }
    // looking for table names in the given schema
    MockSchema schema = getMockSchema(names);
    if (schema != null) {
      for (String tableName : schema.tableNames) {
        result.add(
            new SqlMonikerImpl(
                tableName, SqlMonikerType.TABLE));
      }
    }
    return result;
  }

  public RelDataTypeField field(RelDataType rowType, String alias) {
    return SqlValidatorUtil.lookupField(caseSensitive, elideRecord, rowType,
        alias);
  }

  public int fieldOrdinal(RelDataType rowType, String alias) {
    final RelDataTypeField field = field(rowType, alias);
    return field != null ? field.getIndex() : -1;
  }

  public boolean matches(String string, String name) {
    return Util.matches(caseSensitive, string, name);
  }

  public int match(List<String> strings, String name) {
    return Util.findMatch(strings, name, caseSensitive);
  }

  public RelDataType createTypeFromProjection(final RelDataType type,
      final List<String> columnNameList) {
    return SqlValidatorUtil.createTypeFromProjection(type, columnNameList,
        typeFactory, caseSensitive, elideRecord);
  }

  private static List<RelCollation> deduceMonotonicity(
      Prepare.PreparingTable table) {
    final List<RelCollation> collationList =
        new ArrayList<RelCollation>();

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
                new RelFieldCollation(i, direction,
                    RelFieldCollation.NullDirection.UNSPECIFIED)));
      }
    }
    return collationList;
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Mock schema. */
  public static class MockSchema {
    private final List<String> tableNames = new ArrayList<String>();
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
  }

  /**
   * Mock implementation of
   * {@link org.apache.calcite.prepare.Prepare.PreparingTable}.
   */
  public static class MockTable implements Prepare.PreparingTable {
    private final MockCatalogReader catalogReader;
    private final boolean stream;
    private final List<Map.Entry<String, RelDataType>> columnList =
        Lists.newArrayList();
    private RelDataType rowType;
    private List<RelCollation> collationList;
    private final List<String> names;
    private final Set<String> monotonicColumnSet = Sets.newHashSet();

    public MockTable(MockCatalogReader catalogReader, String catalogName,
        String schemaName, String name, boolean stream) {
      this.catalogReader = catalogReader;
      this.stream = stream;
      this.names = ImmutableList.of(catalogName, schemaName, name);
    }

    public static MockTable create(MockCatalogReader catalogReader,
        MockSchema schema, String name, boolean stream) {
      MockTable table =
          new MockTable(catalogReader, schema.getCatalogName(), schema.name,
              name, stream);
      schema.addTable(name);
      return table;
    }

    public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      return null;
    }

    public double getRowCount() {
      return 0;
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
      return false;
    }

    public RelDataType getRowType() {
      return rowType;
    }

    public boolean supportsModality(SqlModality modality) {
      return modality == (stream ? SqlModality.STREAM : SqlModality.RELATION);
    }

    public void onRegister(RelDataTypeFactory typeFactory) {
      rowType = typeFactory.createStructType(columnList);
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

    public void addColumn(int index, String name, RelDataType type) {
      columnList.add(index, Pair.of(name, type));
    }

    public void addColumn(String name, RelDataType type) {
      columnList.add(Pair.of(name, type));
    }

    public void addMonotonic(String name) {
      monotonicColumnSet.add(name);
      assert Pair.left(columnList).contains(name);
    }

    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
      final MockTable table = new MockTable(catalogReader, names.get(0),
          names.get(1), names.get(2), stream);
      table.columnList.addAll(columnList);
      table.columnList.addAll(extendedFields);
      table.onRegister(catalogReader.typeFactory);
      return table;
    }
  }
}

// End MockCatalogReader.java
