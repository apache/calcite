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
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
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
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerImpl;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
      Ordering.<String>from(String.CASE_INSENSITIVE_ORDER).lexicographical();

  //~ Instance fields --------------------------------------------------------

  protected final RelDataTypeFactory typeFactory;
  private final boolean caseSensitive;
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
    MockTable empTable = new MockTable(this, salesSchema, "EMP");
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
    MockTable deptTable = new MockTable(this, salesSchema, "DEPT");
    deptTable.addColumn("DEPTNO", intType);
    deptTable.addColumn("NAME", varchar10Type);
    registerTable(deptTable);

    // Register "BONUS" table.
    MockTable bonusTable = new MockTable(this, salesSchema, "BONUS");
    bonusTable.addColumn("ENAME", varchar20Type);
    bonusTable.addColumn("JOB", varchar10Type);
    bonusTable.addColumn("SAL", intType);
    bonusTable.addColumn("COMM", intType);
    registerTable(bonusTable);

    // Register "SALGRADE" table.
    MockTable salgradeTable = new MockTable(this, salesSchema, "SALGRADE");
    salgradeTable.addColumn("GRADE", intType);
    salgradeTable.addColumn("LOSAL", intType);
    salgradeTable.addColumn("HISAL", intType);
    registerTable(salgradeTable);

    // Register "EMP_ADDRESS" table
    MockTable contactAddressTable =
        new MockTable(this, salesSchema, "EMP_ADDRESS");
    contactAddressTable.addColumn("EMPNO", intType);
    contactAddressTable.addColumn("HOME_ADDRESS", addressType);
    contactAddressTable.addColumn("MAILING_ADDRESS", addressType);
    registerTable(contactAddressTable);

    // Register "CUSTOMER" schema.
    MockSchema customerSchema = new MockSchema("CUSTOMER");
    registerSchema(customerSchema);

    // Register "CONTACT" table.
    MockTable contactTable = new MockTable(this, customerSchema, "CONTACT");
    contactTable.addColumn("CONTACTNO", intType);
    contactTable.addColumn("FNAME", varchar10Type);
    contactTable.addColumn("LNAME", varchar10Type);
    contactTable.addColumn("EMAIL", varchar20Type);
    contactTable.addColumn("COORD", rectilinearCoordType);
    registerTable(contactTable);

    // Register "ACCOUNT" table.
    MockTable accountTable = new MockTable(this, customerSchema, "ACCOUNT");
    accountTable.addColumn("ACCTNO", intType);
    accountTable.addColumn("TYPE", varchar20Type);
    accountTable.addColumn("BALANCE", intType);
    registerTable(accountTable);
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
      // looking for schema names
      result = new ArrayList<SqlMoniker>();
      for (MockSchema schema : schemas.values()) {
        result.add(
            new SqlMonikerImpl(schema.name, SqlMonikerType.SCHEMA));
      }
      return result;
    case 1:
      // looking for table names in the given schema
      MockSchema schema = schemas.get(names.get(0));
      if (schema == null) {
        return Collections.emptyList();
      }
      result = new ArrayList<SqlMoniker>();
      for (String tableName : schema.tableNames) {
        result.add(
            new SqlMonikerImpl(
                tableName,
                SqlMonikerType.TABLE));
      }
      return result;
    default:
      return Collections.emptyList();
    }
  }

  public List<String> getSchemaName() {
    return Collections.singletonList(DEFAULT_SCHEMA);
  }

  public RelDataTypeField field(RelDataType rowType, String alias) {
    return SqlValidatorUtil.lookupField(caseSensitive, rowType, alias);
  }

  public int fieldOrdinal(RelDataType rowType, String alias) {
    final RelDataTypeField field = field(rowType, alias);
    return field != null ? field.getIndex() : -1;
  }

  public int match(List<String> strings, String name) {
    return Util.match2(strings, name, caseSensitive);
  }

  public RelDataType createTypeFromProjection(final RelDataType type,
      final List<String> columnNameList) {
    return SqlValidatorUtil.createTypeFromProjection(type, columnNameList,
        typeFactory, caseSensitive);
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
            RelCollationImpl.of(
                new RelFieldCollation(
                    i,
                    direction,
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
    private final List<Pair<String, RelDataType>> columnList =
        new ArrayList<Pair<String, RelDataType>>();
    private RelDataType rowType;
    private List<RelCollation> collationList;
    private final List<String> names;

    public MockTable(
        MockCatalogReader catalogReader,
        MockSchema schema,
        String name) {
      this.catalogReader = catalogReader;
      this.names =
          ImmutableList.of(schema.getCatalogName(), schema.name, name);
      schema.addTable(name);
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
      return new LogicalTableScan(context.getCluster(), this);
    }

    public List<RelCollation> getCollationList() {
      return collationList;
    }

    public boolean isKey(BitSet columns) {
      return false;
    }

    public RelDataType getRowType() {
      return rowType;
    }

    public void onRegister(RelDataTypeFactory typeFactory) {
      rowType = typeFactory.createStructType(columnList);
      collationList = deduceMonotonicity(this);
    }

    public List<String> getQualifiedName() {
      return names;
    }

    public SqlMonotonicity getMonotonicity(String columnName) {
      return SqlMonotonicity.NOT_MONOTONIC;
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
  }
}

// End MockCatalogReader.java
