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

import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
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
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.schema.Path;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
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
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Mock implementation of {@link SqlValidatorCatalogReader} which returns tables
 * "EMP", "DEPT", "BONUS", "SALGRADE" (same as Oracle's SCOTT schema).
 * Also two streams "ORDERS", "SHIPMENTS";
 * and a view "EMP_20".
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
      tables = Maps.newHashMap();
      schemas = Maps.newHashMap();
    } else {
      tables = Maps.newTreeMap(CASE_INSENSITIVE_LIST_COMPARATOR);
      schemas = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    }
  }

  @Override public boolean isCaseSensitive() {
    return caseSensitive;
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
    final RelDataType dateType =
        typeFactory.createSqlType(SqlTypeName.DATE);
    final RelDataType booleanType =
        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
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
    final RelDataType empRecordType =
        typeFactory.builder()
            .add("EMPNO", intType)
            .add("ENAME", varchar10Type).build();
    final RelDataType empListType =
        typeFactory.createArrayType(empRecordType, -1);

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
                new RelDataTypeFieldImpl("ZIP", 2, intType),
                new RelDataTypeFieldImpl("STATE", 3, varchar20Type)),
            RelDataTypeComparability.NONE);

    // Register "SALES" schema.
    MockSchema salesSchema = new MockSchema("SALES");
    registerSchema(salesSchema);

    // Register "EMP" table.
    final MockTable empTable =
        MockTable.create(this, salesSchema, "EMP", false, 14);
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

    // Register "EMP_B" table. As "EMP", birth with a "BIRTHDATE" column.
    final MockTable empBTable =
        MockTable.create(this, salesSchema, "EMP_B", false, 14);
    empBTable.addColumn("EMPNO", intType);
    empBTable.addColumn("ENAME", varchar20Type);
    empBTable.addColumn("JOB", varchar10Type);
    empBTable.addColumn("MGR", intTypeNull);
    empBTable.addColumn("HIREDATE", timestampType);
    empBTable.addColumn("SAL", intType);
    empBTable.addColumn("COMM", intType);
    empBTable.addColumn("DEPTNO", intType);
    empBTable.addColumn("SLACKER", booleanType);
    empBTable.addColumn("BIRTHDATE", dateType);
    registerTable(empBTable);

    // Register "DEPT" table.
    MockTable deptTable = MockTable.create(this, salesSchema, "DEPT", false, 4);
    deptTable.addColumn("DEPTNO", intType);
    deptTable.addColumn("NAME", varchar10Type);
    registerTable(deptTable);

    // Register "DEPT_NESTED" table.
    MockTable deptNestedTable =
        MockTable.create(this, salesSchema, "DEPT_NESTED", false, 4);
    deptNestedTable.addColumn("DEPTNO", intType);
    deptNestedTable.addColumn("NAME", varchar10Type);
    deptNestedTable.addColumn("EMPLOYEES", empListType);
    registerTable(deptNestedTable);

    // Register "BONUS" table.
    MockTable bonusTable =
        MockTable.create(this, salesSchema, "BONUS", false, 0);
    bonusTable.addColumn("ENAME", varchar20Type);
    bonusTable.addColumn("JOB", varchar10Type);
    bonusTable.addColumn("SAL", intType);
    bonusTable.addColumn("COMM", intType);
    registerTable(bonusTable);

    // Register "SALGRADE" table.
    MockTable salgradeTable =
        MockTable.create(this, salesSchema, "SALGRADE", false, 5);
    salgradeTable.addColumn("GRADE", intType);
    salgradeTable.addColumn("LOSAL", intType);
    salgradeTable.addColumn("HISAL", intType);
    registerTable(salgradeTable);

    // Register "EMP_ADDRESS" table
    MockTable contactAddressTable =
        MockTable.create(this, salesSchema, "EMP_ADDRESS", false, 26);
    contactAddressTable.addColumn("EMPNO", intType);
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
    contactTable.addColumn("CONTACTNO", intType);
    contactTable.addColumn("FNAME", varchar10Type);
    contactTable.addColumn("LNAME", varchar10Type);
    contactTable.addColumn("EMAIL", varchar20Type);
    contactTable.addColumn("COORD", rectilinearCoordType);
    registerTable(contactTable);

    // Register "CONTACT_PEEK" table. The
    MockTable contactPeekTable =
        MockTable.create(this, customerSchema, "CONTACT_PEEK", false, 1000);
    contactPeekTable.addColumn("CONTACTNO", intType);
    contactPeekTable.addColumn("FNAME", varchar10Type);
    contactPeekTable.addColumn("LNAME", varchar10Type);
    contactPeekTable.addColumn("EMAIL", varchar20Type);
    contactPeekTable.addColumn("COORD", rectilinearPeekCoordType);
    registerTable(contactPeekTable);

    // Register "ACCOUNT" table.
    MockTable accountTable = MockTable.create(this, customerSchema, "ACCOUNT",
        false, 457);
    accountTable.addColumn("ACCTNO", intType);
    accountTable.addColumn("TYPE", varchar20Type);
    accountTable.addColumn("BALANCE", intType);
    registerTable(accountTable);

    // Register "ORDERS" stream.
    MockTable ordersStream = MockTable.create(this, salesSchema, "ORDERS",
        true, Double.POSITIVE_INFINITY);
    ordersStream.addColumn("ROWTIME", timestampType);
    ordersStream.addMonotonic("ROWTIME");
    ordersStream.addColumn("PRODUCTID", intType);
    ordersStream.addColumn("ORDERID", intType);
    registerTable(ordersStream);

    // Register "SHIPMENTS" stream.
    MockTable shipmentsStream = MockTable.create(this, salesSchema, "SHIPMENTS",
        true, Double.POSITIVE_INFINITY);
    shipmentsStream.addColumn("ROWTIME", timestampType);
    shipmentsStream.addMonotonic("ROWTIME");
    shipmentsStream.addColumn("ORDERID", intType);
    registerTable(shipmentsStream);

    // Register "PRODUCTS" table.
    MockTable productsTable = MockTable.create(this, salesSchema, "PRODUCTS",
        false, 200D);
    productsTable.addColumn("PRODUCTID", intType);
    productsTable.addColumn("NAME", varchar20Type);
    productsTable.addColumn("SUPPLIERID", intType);
    registerTable(productsTable);

    // Register "SUPPLIERS" table.
    MockTable suppliersTable = MockTable.create(this, salesSchema, "SUPPLIERS",
        false, 10D);
    suppliersTable.addColumn("SUPPLIERID", intType);
    suppliersTable.addColumn("NAME", varchar20Type);
    suppliersTable.addColumn("CITY", intType);
    registerTable(suppliersTable);

    // Register "EMP_20" view.
    // Same columns as "EMP",
    // but "DEPTNO" not visible and set to 20 by default
    // and "SAL" is visible but must be greater than 1000
    MockTable emp20View = new MockTable(this, salesSchema.getCatalogName(),
        salesSchema.name, "EMP_20", false, 600) {
      private final Table table = empTable.unwrap(Table.class);
      private final ImmutableIntList mapping =
          ImmutableIntList.of(0, 1, 2, 3, 4, 5, 6, 8);

      @Override public RelNode toRel(ToRelContext context) {
        // Expand to the equivalent of:
        //   SELECT EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, SLACKER
        //   FROM EMP
        //   WHERE DEPTNO = 20 AND SAL > 1000
        RelNode rel = LogicalTableScan.create(context.getCluster(), empTable);
        final RexBuilder rexBuilder = context.getCluster().getRexBuilder();
        rel = LogicalFilter.create(rel,
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                    rexBuilder.makeInputRef(rel, 7),
                    rexBuilder.makeExactLiteral(BigDecimal.valueOf(20))),
                rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                    rexBuilder.makeInputRef(rel, 5),
                    rexBuilder.makeExactLiteral(BigDecimal.valueOf(1000)))));
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
          return clazz.cast(
              new JdbcTest.AbstractModifiableView() {
                @Override public Table getTable() {
                  return empTable.unwrap(Table.class);
                }

                @Override public Path getTablePath() {
                  final ImmutableList.Builder<Pair<String, Schema>> builder =
                      ImmutableList.builder();
                  builder.add(Pair.<String, Schema>of(empTable.names.get(0), null));
                  builder.add(Pair.<String, Schema>of(empTable.names.get(1), null));
                  builder.add(Pair.<String, Schema>of(empTable.names.get(2), null));
                  return Schemas.path(builder.build());
                }

                @Override public ImmutableIntList getColumnMapping() {
                  return mapping;
                }

                @Override public RexNode getConstraint(RexBuilder rexBuilder,
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
              });
        }
        return super.unwrap(clazz);
      }
    };
    salesSchema.addTable(Util.last(emp20View.getQualifiedName()));
    emp20View.addColumn("EMPNO", intType);
    emp20View.addColumn("ENAME", varchar20Type);
    emp20View.addColumn("JOB", varchar10Type);
    emp20View.addColumn("MGR", intTypeNull);
    emp20View.addColumn("HIREDATE", timestampType);
    emp20View.addColumn("SAL", intType);
    emp20View.addColumn("COMM", intType);
    emp20View.addColumn("SLACKER", booleanType);
    registerTable(emp20View);

    MockSchema structTypeSchema = new MockSchema("STRUCT");
    registerSchema(structTypeSchema);
    MockTable structTypeTable = MockTable.create(this, structTypeSchema, "T",
        false, 100);
    structTypeTable.addColumn("K0", varchar20Type);
    structTypeTable.addColumn("C1", varchar20Type);
    final RelDataType f0Type = typeFactory.builder()
        .add("C0", intType)
        .add("C1", intType)
        .kind(StructKind.PEEK_FIELDS_DEFAULT)
        .build();
    structTypeTable.addColumn("F0", f0Type);
    final RelDataType f1Type = typeFactory.builder()
        .add("C0", intTypeNull)
        .add("C2", intType)
        .add("A0", intType)
        .kind(StructKind.PEEK_FIELDS)
        .build();
    structTypeTable.addColumn("F1", f1Type);
    final RelDataType f2Type = typeFactory.builder()
        .add("C3", intType)
        .add("A0", booleanType)
        .kind(StructKind.PEEK_FIELDS)
        .build();
    structTypeTable.addColumn("F2", f2Type);
    registerTable(structTypeTable);
    return this;
  }

  //~ Methods ----------------------------------------------------------------

  public void lookupOperatorOverloads(SqlIdentifier opName,
      SqlFunctionCategory category, SqlSyntax syntax,
      List<SqlOperator> operatorList) {
  }

  public List<SqlOperator> getOperatorList() {
    return ImmutableList.of();
  }

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
    if (typeName.equalsDeep(addressType.getSqlIdentifier(), Litmus.IGNORE)) {
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
      result = Lists.newArrayList();
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
      result = Lists.newArrayList();
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

  public RelDataTypeField field(RelDataType rowType, String alias) {
    return SqlValidatorUtil.lookupField(caseSensitive, rowType, alias);
  }

  public boolean matches(String string, String name) {
    return Util.matches(caseSensitive, string, name);
  }

  public RelDataType createTypeFromProjection(final RelDataType type,
      final List<String> columnNameList) {
    return SqlValidatorUtil.createTypeFromProjection(type, columnNameList,
        typeFactory, caseSensitive);
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
  public static class MockTable implements Prepare.PreparingTable {
    protected final MockCatalogReader catalogReader;
    private final boolean stream;
    private final double rowCount;
    protected final List<Map.Entry<String, RelDataType>> columnList =
        new ArrayList<>();
    protected RelDataType rowType;
    private List<RelCollation> collationList;
    protected final List<String> names;
    private final Set<String> monotonicColumnSet = Sets.newHashSet();
    private StructKind kind = StructKind.FULLY_QUALIFIED;

    public MockTable(MockCatalogReader catalogReader, String catalogName,
        String schemaName, String name, boolean stream, double rowCount) {
      this.catalogReader = catalogReader;
      this.stream = stream;
      this.rowCount = rowCount;
      this.names = ImmutableList.of(catalogName, schemaName, name);
    }

    public static MockTable create(MockCatalogReader catalogReader,
        MockSchema schema, String name, boolean stream, double rowCount) {
      MockTable table =
          new MockTable(catalogReader, schema.getCatalogName(), schema.name,
              name, stream, rowCount);
      schema.addTable(name);
      return table;
    }

    public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      if (clazz.isAssignableFrom(Table.class)) {
        return clazz.cast(
            new JdbcTest.AbstractModifiableTable(Util.last(names)) {
              @Override public RelDataType
              getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.createStructType(rowType.getFieldList());
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
            });
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
      return false;
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
      columnList.add(Pair.of(name, type));
    }

    public void addMonotonic(String name) {
      monotonicColumnSet.add(name);
      assert Pair.left(columnList).contains(name);
    }

    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
      final MockTable table = new MockTable(catalogReader, names.get(0),
          names.get(1), names.get(2), stream, rowCount);
      table.columnList.addAll(columnList);
      table.columnList.addAll(extendedFields);
      table.onRegister(catalogReader.typeFactory);
      return table;
    }

    public void setKind(StructKind kind) {
      this.kind = kind;
    }

    public StructKind getKind() {
      return kind;
    }
  }

  /**
   * Mock implementation of
   * {@link org.apache.calcite.prepare.Prepare.PreparingTable} with dynamic record type.
   */
  public static class MockDynamicTable extends MockTable {
    MockDynamicTable(MockCatalogReader catalogReader, String catalogName,
        String schemaName, String name, boolean stream, double rowCount) {
      super(catalogReader, catalogName, schemaName, name, stream, rowCount);
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
}

// End MockCatalogReader.java
