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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
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
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.CustomColumnResolvingTable;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.Path;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ModifiableViewTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.test.JdbcTest;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Mock implementation of {@link SqlValidatorCatalogReader} which returns tables
 * "EMP", "DEPT", "BONUS", "SALGRADE" (same as Oracle's SCOTT schema).
 * Also two streams "ORDERS", "SHIPMENTS";
 * and a view "EMP_20".
 */
public abstract class MockCatalogReader extends CalciteCatalogReader {
  //~ Static fields/initializers ---------------------------------------------

  static final String DEFAULT_CATALOG = "CATALOG";
  static final String DEFAULT_SCHEMA = "SALES";
  static final List<String> PREFIX = ImmutableList.of(DEFAULT_SCHEMA);

  //~ Instance fields --------------------------------------------------------

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
    super(CalciteSchema.createRootSchema(false, false, DEFAULT_CATALOG),
        SqlNameMatchers.withCaseSensitive(caseSensitive),
        ImmutableList.of(PREFIX, ImmutableList.of()),
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
  public abstract MockCatalogReader init();

  protected void registerTablesWithRollUp(MockSchema schema, Fixture f) {
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

  //~ Methods ----------------------------------------------------------------

  protected void registerType(final List<String> names, final RelProtoDataType relProtoDataType) {
    assert names.get(0).equals(DEFAULT_CATALOG);
    final List<String> schemaPath = Util.skipLast(names);
    final CalciteSchema schema = SqlValidatorUtil.getSchema(rootSchema,
        schemaPath, SqlNameMatchers.withCaseSensitive(true));
    schema.add(Util.last(names), relProtoDataType);
  }

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

  void registerTable(MockDynamicTable table) {
    registerTable(table.names, table);
  }

  void reregisterTable(MockDynamicTable table) {
    List<String> names = table.names;
    assert names.get(0).equals(DEFAULT_CATALOG);
    List<String> schemaPath = Util.skipLast(names);
    String tableName = Util.last(names);
    CalciteSchema schema = SqlValidatorUtil.getSchema(rootSchema,
        schemaPath, SqlNameMatchers.withCaseSensitive(true));
    schema.removeTable(tableName);
    schema.add(tableName, table);
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

  private static List<RelCollation> deduceMonotonicity(
      Prepare.PreparingTable table) {
    final List<RelCollation> collationList = new ArrayList<>();

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
    private final List<String> tableNames = new ArrayList<>();
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
    protected final Set<String> monotonicColumnSet = new HashSet<>();
    protected StructKind kind = StructKind.FULLY_QUALIFIED;
    protected final ColumnResolver resolver;
    private final boolean temporal;
    protected final InitializerExpressionFactory initializerFactory;
    protected final Set<String> rolledUpColumns = new HashSet<>();

    public MockTable(MockCatalogReader catalogReader, String catalogName,
        String schemaName, String name, boolean stream, boolean temporal,
        double rowCount, ColumnResolver resolver,
        InitializerExpressionFactory initializerFactory) {
      this(catalogReader, ImmutableList.of(catalogName, schemaName, name),
          stream, temporal, rowCount, resolver, initializerFactory);
    }

    public void registerRolledUpColumn(String columnName) {
      rolledUpColumns.add(columnName);
    }

    private MockTable(MockCatalogReader catalogReader, List<String> names,
        boolean stream, boolean temporal, double rowCount,
        ColumnResolver resolver,
        InitializerExpressionFactory initializerFactory) {
      this.catalogReader = catalogReader;
      this.stream = stream;
      this.temporal = temporal;
      this.rowCount = rowCount;
      this.names = names;
      this.resolver = resolver;
      this.initializerFactory = initializerFactory;
    }

    /**
     * Copy constructor.
     */
    protected MockTable(MockCatalogReader catalogReader, boolean stream,
        boolean temporal, double rowCount,
        List<Map.Entry<String, RelDataType>> columnList, List<Integer> keyList,
        RelDataType rowType, List<RelCollation> collationList, List<String> names,
        Set<String> monotonicColumnSet, StructKind kind, ColumnResolver resolver,
        InitializerExpressionFactory initializerFactory) {
      this.catalogReader = catalogReader;
      this.stream = stream;
      this.temporal = temporal;
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

      @Override public boolean isRolledUp(String column) {
        return rolledUpColumns.contains(column);
      }

      @Override public boolean rolledUpColumnValidInsideAgg(String column,
          SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
        // For testing
        return call.getKind() != SqlKind.MAX
            && (parent.getKind() == SqlKind.SELECT || parent.getKind() == SqlKind.FILTER);
      }
    }

    @Override protected RelOptTable extend(final Table extendedTable) {
      return new MockTable(catalogReader, names, stream, temporal, rowCount,
          resolver, initializerFactory) {
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
      return new MockTable(catalogReader, names, stream, false, rowCount, null,
          NullInitializerExpressionFactory.INSTANCE);
    }

    public static MockTable create(MockCatalogReader catalogReader,
        MockSchema schema, String name, boolean stream, double rowCount,
        ColumnResolver resolver) {
      return create(catalogReader, schema, name, stream, rowCount, resolver,
          NullInitializerExpressionFactory.INSTANCE, false);
    }

    public static MockTable create(MockCatalogReader catalogReader,
        MockSchema schema, String name, boolean stream, double rowCount,
        ColumnResolver resolver,
        InitializerExpressionFactory initializerExpressionFactory,
        boolean temporal) {
      MockTable table =
          new MockTable(catalogReader, schema.getCatalogName(), schema.name,
              name, stream, temporal, rowCount, resolver,
              initializerExpressionFactory);
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

    @Override public boolean isTemporal() {
      return temporal;
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
      super(catalogReader, ImmutableList.of(catalogName, schemaName, name),
          stream, false, rowCount, resolver, initializerExpressionFactory);
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
      super(catalogReader, stream, false, rowCount, columnList, keyList,
          rowType, collationList, names,
          monotonicColumnSet, kind, resolver, initializerFactory);
      this.modifiableViewTable = modifiableViewTable;
    }

    public static MockModifiableViewRelOptTable create(MockModifiableViewTable modifiableViewTable,
        MockCatalogReader catalogReader, String catalogName, String schemaName, String name,
        boolean stream, double rowCount, ColumnResolver resolver) {
      final Table underlying = modifiableViewTable.unwrap(Table.class);
      final InitializerExpressionFactory initializerExpressionFactory =
          underlying instanceof Wrapper
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
    public static class MockModifiableViewTable extends ModifiableViewTable {
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
   * Mock implementation of {@link Prepare.AbstractPreparingTable} which holds {@link ViewTable}
   * and delegates {@link MockTable#toRel} call to the view.
   */
  public static class MockRelViewTable extends MockTable {
    private final ViewTable viewTable;

    private MockRelViewTable(ViewTable viewTable,
        MockCatalogReader catalogReader, String catalogName, String schemaName, String name,
        boolean stream, double rowCount, ColumnResolver resolver,
        InitializerExpressionFactory initializerExpressionFactory) {
      super(catalogReader, ImmutableList.of(catalogName, schemaName, name),
          stream, false, rowCount, resolver, initializerExpressionFactory);
      this.viewTable = viewTable;
    }

    public static MockRelViewTable create(ViewTable viewTable,
        MockCatalogReader catalogReader, String catalogName, String schemaName, String name,
        boolean stream, double rowCount, ColumnResolver resolver) {
      Table underlying = viewTable.unwrap(Table.class);
      InitializerExpressionFactory initializerExpressionFactory =
          underlying instanceof Wrapper
              ? ((Wrapper) underlying).unwrap(InitializerExpressionFactory.class)
              : NullInitializerExpressionFactory.INSTANCE;
      return new MockRelViewTable(viewTable,
          catalogReader, catalogName, schemaName, name, stream, rowCount,
          resolver, Util.first(initializerExpressionFactory,
          NullInitializerExpressionFactory.INSTANCE));
    }

    @Override public RelDataType getRowType() {
      return viewTable.getRowType(catalogReader.typeFactory);
    }

    @Override public RelNode toRel(RelOptTable.ToRelContext context) {
      return viewTable.toRel(context, this);
    }

    @Override public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(viewTable)) {
        return clazz.cast(viewTable);
      }
      return super.unwrap(clazz);
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
      super(catalogReader, catalogName, schemaName, name, stream, false,
          rowCount, resolver, initializerFactory);
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
          builder.add(Pair.of(name, null));
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
   * Mock implementation of {@link AbstractQueryableTable} with dynamic record type.
   */
  public static class MockDynamicTable
      extends AbstractQueryableTable implements TranslatableTable {
    private final DynamicRecordTypeImpl rowType;
    protected final List<String> names;

    MockDynamicTable(String catalogName, String schemaName, String name) {
      super(Object.class);
      this.names = Arrays.asList(catalogName, schemaName, name);
      this.rowType = new DynamicRecordTypeImpl(new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return rowType;
    }

    @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
        SchemaPlus schema, String tableName) {
      throw new UnsupportedOperationException();
    }

    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
      return LogicalTableScan.create(context.getCluster(), relOptTable);
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
        SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
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

}

// End MockCatalogReader.java
