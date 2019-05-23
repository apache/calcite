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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptSchemaWithSampling;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.test.catalog.MockCatalogReader;
import org.apache.calcite.test.catalog.MockCatalogReaderDynamic;
import org.apache.calcite.test.catalog.MockCatalogReaderSimple;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * SqlToRelTestBase is an abstract base for tests which involve conversion from
 * SQL to relational algebra.
 *
 * <p>SQL statements to be translated can use the schema defined in
 * {@link MockCatalogReader}; note that this is slightly different from
 * Farrago's SALES schema. If you get a parser or validator error from your test
 * SQL, look down in the stack until you see "Caused by", which will usually
 * tell you the real error.
 */
public abstract class SqlToRelTestBase {
  //~ Static fields/initializers ---------------------------------------------

  protected static final String NL = System.getProperty("line.separator");

  //~ Instance fields --------------------------------------------------------

  protected final Tester tester = createTester();

  //~ Methods ----------------------------------------------------------------

  public SqlToRelTestBase() {
    super();
  }

  protected Tester createTester() {
    return new TesterImpl(getDiffRepos(), false, false, true, false,
        null, null, SqlToRelConverter.Config.DEFAULT,
        SqlConformanceEnum.DEFAULT, Contexts.empty());
  }

  protected Tester createTester(SqlConformance conformance) {
    return new TesterImpl(getDiffRepos(), false, false, true, false,
        null, null, SqlToRelConverter.Config.DEFAULT, conformance, Contexts.empty());
  }

  protected Tester getTesterWithDynamicTable() {
    return tester.withCatalogReaderFactory(MockCatalogReaderDynamic::new);
  }

  /**
   * Returns the default diff repository for this test, or null if there is
   * no repository.
   *
   * <p>The default implementation returns null.
   *
   * <p>Sub-classes that want to use a diff repository can override.
   * Sub-sub-classes can override again, inheriting test cases and overriding
   * selected test results.
   *
   * <p>And individual test cases can override by providing a different
   * tester object.
   *
   * @return Diff repository
   */
  protected DiffRepository getDiffRepos() {
    return null;
  }

  /**
   * Checks that every node of a relational expression is valid.
   *
   * @param rel Relational expression
   */
  public static void assertValid(RelNode rel) {
    SqlToRelConverterTest.RelValidityChecker checker =
        new SqlToRelConverterTest.RelValidityChecker();
    checker.go(rel);
    assertEquals(0, checker.invalidCount);
  }

  //~ Inner Interfaces -------------------------------------------------------

  /**
   * Helper class which contains default implementations of methods used for
   * running sql-to-rel conversion tests.
   */
  public interface Tester {
    /**
     * Converts a SQL string to a {@link RelNode} tree.
     *
     * @param sql SQL statement
     * @return Relational expression, never null
     */
    RelRoot convertSqlToRel(String sql);

    SqlNode parseQuery(String sql) throws Exception;

    /**
     * Factory method to create a {@link SqlValidator}.
     */
    SqlValidator createValidator(
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory);

    /**
     * Factory method for a
     * {@link org.apache.calcite.prepare.Prepare.CatalogReader}.
     */
    Prepare.CatalogReader createCatalogReader(
        RelDataTypeFactory typeFactory);

    RelOptPlanner createPlanner();

    /**
     * Returns the {@link SqlOperatorTable} to use.
     */
    SqlOperatorTable getOperatorTable();

    /**
     * Returns the SQL dialect to test.
     */
    SqlConformance getConformance();

    /**
     * Checks that a SQL statement converts to a given plan.
     *
     * @param sql  SQL query
     * @param plan Expected plan
     */
    void assertConvertsTo(
        String sql,
        String plan);

    /**
     * Checks that a SQL statement converts to a given plan, optionally
     * trimming columns that are not needed.
     *
     * @param sql  SQL query
     * @param plan Expected plan
     * @param trim Whether to trim columns that are not needed
     */
    void assertConvertsTo(
        String sql,
        String plan,
        boolean trim);

    /**
     * Returns the diff repository.
     *
     * @return Diff repository
     */
    DiffRepository getDiffRepos();

    /**
     * Returns the validator.
     *
     * @return Validator
     */
    SqlValidator getValidator();

    /** Returns a tester that optionally decorrelates queries. */
    Tester withDecorrelation(boolean enable);

    /** Returns a tester that optionally decorrelates queries after planner
     * rules have fired. */
    Tester withLateDecorrelation(boolean enable);

    /** Returns a tester that optionally expands sub-queries.
     * If {@code expand} is false, the plan contains a
     * {@link org.apache.calcite.rex.RexSubQuery} for each sub-query.
     *
     * @see Prepare#THREAD_EXPAND */
    Tester withExpand(boolean expand);

    /** Returns a tester that optionally uses a
     * {@code SqlToRelConverter.Config}. */
    Tester withConfig(SqlToRelConverter.Config config);

    /** Returns a tester with a {@link SqlConformance}. */
    Tester withConformance(SqlConformance conformance);

    Tester withCatalogReaderFactory(
        SqlTestFactory.MockCatalogReaderFactory factory);

    /** Returns a tester that optionally trims unused fields. */
    Tester withTrim(boolean enable);

    Tester withClusterFactory(Function<RelOptCluster, RelOptCluster> function);

    boolean isLateDecorrelate();

    /** Returns a tester that uses a given context. */
    Tester withContext(Context context);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Mock implementation of {@link RelOptSchema}.
   */
  protected static class MockRelOptSchema implements RelOptSchemaWithSampling {
    private final SqlValidatorCatalogReader catalogReader;
    private final RelDataTypeFactory typeFactory;

    public MockRelOptSchema(
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory) {
      this.catalogReader = catalogReader;
      this.typeFactory = typeFactory;
    }

    public RelOptTable getTableForMember(List<String> names) {
      final SqlValidatorTable table =
          catalogReader.getTable(names);
      final RelDataType rowType = table.getRowType();
      final List<RelCollation> collationList = deduceMonotonicity(table);
      if (names.size() < 3) {
        String[] newNames2 = {"CATALOG", "SALES", ""};
        List<String> newNames = new ArrayList<>();
        int i = 0;
        while (newNames.size() < newNames2.length) {
          newNames.add(i, newNames2[i]);
          ++i;
        }
        names = newNames;
      }
      return createColumnSet(table, names, rowType, collationList);
    }

    private List<RelCollation> deduceMonotonicity(SqlValidatorTable table) {
      final RelDataType rowType = table.getRowType();
      final List<RelCollation> collationList = new ArrayList<>();

      // Deduce which fields the table is sorted on.
      int i = -1;
      for (RelDataTypeField field : rowType.getFieldList()) {
        ++i;
        final SqlMonotonicity monotonicity =
            table.getMonotonicity(field.getName());
        if (monotonicity != SqlMonotonicity.NOT_MONOTONIC) {
          final RelFieldCollation.Direction direction =
              monotonicity.isDecreasing()
                  ? RelFieldCollation.Direction.DESCENDING
                  : RelFieldCollation.Direction.ASCENDING;
          collationList.add(
              RelCollations.of(new RelFieldCollation(i, direction)));
        }
      }
      return collationList;
    }

    public RelOptTable getTableForMember(
        List<String> names,
        final String datasetName,
        boolean[] usedDataset) {
      final RelOptTable table = getTableForMember(names);

      // If they're asking for a sample, just for test purposes,
      // assume there's a table called "<table>:<sample>".
      RelOptTable datasetTable =
          new DelegatingRelOptTable(table) {
            public List<String> getQualifiedName() {
              final List<String> list =
                  new ArrayList<>(super.getQualifiedName());
              list.set(
                  list.size() - 1,
                  list.get(list.size() - 1) + ":" + datasetName);
              return ImmutableList.copyOf(list);
            }
          };
      if (usedDataset != null) {
        assert usedDataset.length == 1;
        usedDataset[0] = true;
      }
      return datasetTable;
    }

    protected MockColumnSet createColumnSet(
        SqlValidatorTable table,
        List<String> names,
        final RelDataType rowType,
        final List<RelCollation> collationList) {
      return new MockColumnSet(names, rowType, collationList);
    }

    public RelDataTypeFactory getTypeFactory() {
      return typeFactory;
    }

    public void registerRules(RelOptPlanner planner) throws Exception {
    }

    /** Mock column set. */
    protected class MockColumnSet implements RelOptTable {
      private final List<String> names;
      private final RelDataType rowType;
      private final List<RelCollation> collationList;

      protected MockColumnSet(
          List<String> names,
          RelDataType rowType,
          final List<RelCollation> collationList) {
        this.names = ImmutableList.copyOf(names);
        this.rowType = rowType;
        this.collationList = collationList;
      }

      public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
          return clazz.cast(this);
        }
        return null;
      }

      public List<String> getQualifiedName() {
        return names;
      }

      public double getRowCount() {
        // use something other than 0 to give costing tests
        // some room, and make emps bigger than depts for
        // join asymmetry
        if (Iterables.getLast(names).equals("EMP")) {
          return 1000;
        } else {
          return 100;
        }
      }

      public RelDataType getRowType() {
        return rowType;
      }

      public RelOptSchema getRelOptSchema() {
        return MockRelOptSchema.this;
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

      public List<RelReferentialConstraint> getReferentialConstraints() {
        return ImmutableList.of();
      }

      public List<ColumnStrategy> getColumnStrategies() {
        throw new UnsupportedOperationException();
      }

      public Expression getExpression(Class clazz) {
        return null;
      }

      public RelOptTable extend(List<RelDataTypeField> extendedFields) {
        final RelDataType extendedRowType =
            getRelOptSchema().getTypeFactory().builder()
                .addAll(rowType.getFieldList())
                .addAll(extendedFields)
                .build();
        return new MockColumnSet(names, extendedRowType, collationList);
      }
    }
  }

  /** Table that delegates to a given table. */
  private static class DelegatingRelOptTable implements RelOptTable {
    private final RelOptTable parent;

    DelegatingRelOptTable(RelOptTable parent) {
      this.parent = parent;
    }

    public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      return parent.unwrap(clazz);
    }

    public Expression getExpression(Class clazz) {
      return parent.getExpression(clazz);
    }

    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
      return parent.extend(extendedFields);
    }

    public List<String> getQualifiedName() {
      return parent.getQualifiedName();
    }

    public double getRowCount() {
      return parent.getRowCount();
    }

    public RelDataType getRowType() {
      return parent.getRowType();
    }

    public RelOptSchema getRelOptSchema() {
      return parent.getRelOptSchema();
    }

    public RelNode toRel(ToRelContext context) {
      return LogicalTableScan.create(context.getCluster(), this);
    }

    public List<RelCollation> getCollationList() {
      return parent.getCollationList();
    }

    public RelDistribution getDistribution() {
      return parent.getDistribution();
    }

    public boolean isKey(ImmutableBitSet columns) {
      return parent.isKey(columns);
    }

    public List<RelReferentialConstraint> getReferentialConstraints() {
      return parent.getReferentialConstraints();
    }

    public List<ColumnStrategy> getColumnStrategies() {
      return parent.getColumnStrategies();
    }
  }

  /**
   * Default implementation of {@link Tester}, using mock classes
   * {@link MockRelOptSchema} and {@link MockRelOptPlanner}.
   */
  public static class TesterImpl implements Tester {
    private RelOptPlanner planner;
    private SqlOperatorTable opTab;
    private final DiffRepository diffRepos;
    private final boolean enableDecorrelate;
    private final boolean enableLateDecorrelate;
    private final boolean enableTrim;
    private final boolean enableExpand;
    private final SqlConformance conformance;
    private final SqlTestFactory.MockCatalogReaderFactory catalogReaderFactory;
    private final Function<RelOptCluster, RelOptCluster> clusterFactory;
    private RelDataTypeFactory typeFactory;
    public final SqlToRelConverter.Config config;
    private final Context context;

    /**
     * Creates a TesterImpl.
     *
     * @param diffRepos Diff repository
     * @param enableDecorrelate Whether to decorrelate
     * @param enableTrim Whether to trim unused fields
     * @param enableExpand Whether to expand sub-queries
     * @param catalogReaderFactory Function to create catalog reader, or null
     * @param clusterFactory Called after a cluster has been created
     */
    protected TesterImpl(DiffRepository diffRepos, boolean enableDecorrelate,
        boolean enableTrim, boolean enableExpand,
        boolean enableLateDecorrelate,
        SqlTestFactory.MockCatalogReaderFactory
            catalogReaderFactory,
        Function<RelOptCluster, RelOptCluster> clusterFactory) {
      this(diffRepos, enableDecorrelate, enableTrim, enableExpand,
          enableLateDecorrelate,
          catalogReaderFactory,
          clusterFactory,
          SqlToRelConverter.Config.DEFAULT,
          SqlConformanceEnum.DEFAULT,
          Contexts.empty());
    }

    protected TesterImpl(DiffRepository diffRepos, boolean enableDecorrelate,
        boolean enableTrim, boolean enableExpand, boolean enableLateDecorrelate,
        SqlTestFactory.MockCatalogReaderFactory catalogReaderFactory,
        Function<RelOptCluster, RelOptCluster> clusterFactory,
        SqlToRelConverter.Config config, SqlConformance conformance,
        Context context) {
      this.diffRepos = diffRepos;
      this.enableDecorrelate = enableDecorrelate;
      this.enableTrim = enableTrim;
      this.enableExpand = enableExpand;
      this.enableLateDecorrelate = enableLateDecorrelate;
      this.catalogReaderFactory = catalogReaderFactory;
      this.clusterFactory = clusterFactory;
      this.config = config;
      this.conformance = conformance;
      this.context = context;
    }

    public RelRoot convertSqlToRel(String sql) {
      Objects.requireNonNull(sql);
      final SqlNode sqlQuery;
      final SqlToRelConverter.Config localConfig;
      try {
        sqlQuery = parseQuery(sql);
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Exception e) {
        throw TestUtil.rethrow(e);
      }
      final RelDataTypeFactory typeFactory = getTypeFactory();
      final Prepare.CatalogReader catalogReader =
          createCatalogReader(typeFactory);
      final SqlValidator validator =
          createValidator(
              catalogReader, typeFactory);
      final CalciteConnectionConfig calciteConfig = context.unwrap(CalciteConnectionConfig.class);
      if (calciteConfig != null) {
        validator.setDefaultNullCollation(calciteConfig.defaultNullCollation());
      }
      if (config == SqlToRelConverter.Config.DEFAULT) {
        localConfig = SqlToRelConverter.configBuilder()
            .withTrimUnusedFields(true).withExpand(enableExpand).build();
      } else {
        localConfig = config;
      }

      final SqlToRelConverter converter =
          createSqlToRelConverter(
              validator,
              catalogReader,
              typeFactory,
              localConfig);

      final SqlNode validatedQuery = validator.validate(sqlQuery);
      RelRoot root =
          converter.convertQuery(validatedQuery, false, true);
      assert root != null;
      if (enableDecorrelate || enableTrim) {
        root = root.withRel(converter.flattenTypes(root.rel, true));
      }
      if (enableDecorrelate) {
        root = root.withRel(converter.decorrelate(sqlQuery, root.rel));
      }
      if (enableTrim) {
        root = root.withRel(converter.trimUnusedFields(true, root.rel));
      }
      return root;
    }

    protected SqlToRelConverter createSqlToRelConverter(
        final SqlValidator validator,
        final Prepare.CatalogReader catalogReader,
        final RelDataTypeFactory typeFactory,
        final SqlToRelConverter.Config config) {
      final RexBuilder rexBuilder = new RexBuilder(typeFactory);
      RelOptCluster cluster =
          RelOptCluster.create(getPlanner(), rexBuilder);
      if (clusterFactory != null) {
        cluster = clusterFactory.apply(cluster);
      }
      RelOptTable.ViewExpander viewExpander =
          new MockViewExpander(validator, catalogReader, cluster, config);
      return new SqlToRelConverter(viewExpander, validator, catalogReader, cluster,
          StandardConvertletTable.INSTANCE, config);
    }

    protected final RelDataTypeFactory getTypeFactory() {
      if (typeFactory == null) {
        typeFactory = createTypeFactory();
      }
      return typeFactory;
    }

    protected RelDataTypeFactory createTypeFactory() {
      return new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    }

    protected final RelOptPlanner getPlanner() {
      if (planner == null) {
        planner = createPlanner();
      }
      return planner;
    }

    public SqlNode parseQuery(String sql) throws Exception {
      final SqlParser.Config config =
          SqlParser.configBuilder().setConformance(getConformance()).build();
      SqlParser parser = SqlParser.create(sql, config);
      return parser.parseQuery();
    }

    public SqlConformance getConformance() {
      return conformance;
    }

    public SqlValidator createValidator(
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory) {
      return new FarragoTestValidator(
          getOperatorTable(),
          catalogReader,
          typeFactory,
          getConformance());
    }

    public final SqlOperatorTable getOperatorTable() {
      if (opTab == null) {
        opTab = createOperatorTable();
      }
      return opTab;
    }

    /**
     * Creates an operator table.
     *
     * @return New operator table
     */
    protected SqlOperatorTable createOperatorTable() {
      final MockSqlOperatorTable opTab =
          new MockSqlOperatorTable(SqlStdOperatorTable.instance());
      MockSqlOperatorTable.addRamp(opTab);
      return opTab;
    }

    public Prepare.CatalogReader createCatalogReader(
        RelDataTypeFactory typeFactory) {
      MockCatalogReader catalogReader;
      if (this.catalogReaderFactory != null) {
        catalogReader = catalogReaderFactory.create(typeFactory, true);
      } else {
        catalogReader = new MockCatalogReaderSimple(typeFactory, true);
      }
      return catalogReader.init();
    }

    public RelOptPlanner createPlanner() {
      return new MockRelOptPlanner(context);
    }

    public void assertConvertsTo(
        String sql,
        String plan) {
      assertConvertsTo(sql, plan, false);
    }

    public void assertConvertsTo(
        String sql,
        String plan,
        boolean trim) {
      String sql2 = getDiffRepos().expand("sql", sql);
      RelNode rel = convertSqlToRel(sql2).project();

      assertTrue(rel != null);
      assertValid(rel);

      if (trim) {
        final RelBuilder relBuilder =
            RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
        final RelFieldTrimmer trimmer = createFieldTrimmer(relBuilder);
        rel = trimmer.trim(rel);
        assertTrue(rel != null);
        assertValid(rel);
      }

      // NOTE jvs 28-Mar-2006:  insert leading newline so
      // that plans come out nicely stacked instead of first
      // line immediately after CDATA start
      String actual = NL + RelOptUtil.toString(rel);
      diffRepos.assertEquals("plan", plan, actual);
    }

    /**
     * Creates a RelFieldTrimmer.
     *
     * @param relBuilder Builder
     * @return Field trimmer
     */
    public RelFieldTrimmer createFieldTrimmer(RelBuilder relBuilder) {
      return new RelFieldTrimmer(getValidator(), relBuilder);
    }

    public DiffRepository getDiffRepos() {
      return diffRepos;
    }

    public SqlValidator getValidator() {
      final RelDataTypeFactory typeFactory = getTypeFactory();
      final SqlValidatorCatalogReader catalogReader =
          createCatalogReader(typeFactory);
      return createValidator(catalogReader, typeFactory);
    }

    public TesterImpl withDecorrelation(boolean enableDecorrelate) {
      return this.enableDecorrelate == enableDecorrelate
          ? this
          : new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
              enableExpand, enableLateDecorrelate, catalogReaderFactory,
              clusterFactory, config, conformance, context);
    }

    public Tester withLateDecorrelation(boolean enableLateDecorrelate) {
      return this.enableLateDecorrelate == enableLateDecorrelate
          ? this
          : new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
              enableExpand, enableLateDecorrelate, catalogReaderFactory,
              clusterFactory, config, conformance, context);
    }

    public TesterImpl withConfig(SqlToRelConverter.Config config) {
      return this.config == config
          ? this
          : new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
              enableExpand, enableLateDecorrelate, catalogReaderFactory,
              clusterFactory, config, conformance, context);
    }

    public Tester withTrim(boolean enableTrim) {
      return this.enableTrim == enableTrim
          ? this
          : new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
              enableExpand, enableLateDecorrelate, catalogReaderFactory,
              clusterFactory, config, conformance, context);
    }

    public Tester withExpand(boolean enableExpand) {
      return this.enableExpand == enableExpand
          ? this
          : new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
              enableExpand, enableLateDecorrelate, catalogReaderFactory,
              clusterFactory, config, conformance, context);
    }

    public Tester withConformance(SqlConformance conformance) {
      return new TesterImpl(diffRepos, enableDecorrelate, false,
          enableExpand, enableLateDecorrelate, catalogReaderFactory,
          clusterFactory, config, conformance, context);
    }

    public Tester withCatalogReaderFactory(
        SqlTestFactory.MockCatalogReaderFactory factory) {
      return new TesterImpl(diffRepos, enableDecorrelate, false,
          enableExpand, enableLateDecorrelate, factory,
          clusterFactory, config, conformance, context);
    }

    public Tester withClusterFactory(
        Function<RelOptCluster, RelOptCluster> clusterFactory) {
      return new TesterImpl(diffRepos, enableDecorrelate, false,
          enableExpand, enableLateDecorrelate, catalogReaderFactory,
          clusterFactory, config, conformance, context);
    }

    public Tester withContext(Context context) {
      return new TesterImpl(diffRepos, enableDecorrelate, false,
          enableExpand, enableLateDecorrelate, catalogReaderFactory,
          clusterFactory, config, conformance, context);
    }

    public boolean isLateDecorrelate() {
      return enableLateDecorrelate;
    }
  }

    /** Validator for testing. */
  private static class FarragoTestValidator extends SqlValidatorImpl {
    FarragoTestValidator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        SqlConformance conformance) {
      super(opTab, catalogReader, typeFactory, conformance);
    }

    // override SqlValidator
    public boolean shouldExpandIdentifiers() {
      return true;
    }
  }

  /**
   * {@link RelOptTable.ViewExpander} implementation for testing usage.
   */
  private static class MockViewExpander implements RelOptTable.ViewExpander {
    private final SqlValidator validator;
    private final Prepare.CatalogReader catalogReader;
    private final RelOptCluster cluster;
    private final SqlToRelConverter.Config config;

    MockViewExpander(SqlValidator validator, Prepare.CatalogReader catalogReader,
        RelOptCluster cluster, SqlToRelConverter.Config config) {
      this.validator = validator;
      this.catalogReader = catalogReader;
      this.cluster = cluster;
      this.config = config;
    }

    @Override public RelRoot expandView(RelDataType rowType, String queryString,
        List<String> schemaPath, List<String> viewPath) {
      try {
        SqlNode parsedNode = SqlParser.create(queryString).parseStmt();
        SqlNode validatedNode = validator.validate(parsedNode);
        SqlToRelConverter converter = new SqlToRelConverter(
            this, validator, catalogReader, cluster,
            StandardConvertletTable.INSTANCE, config);
        return converter.convertQuery(validatedNode, false, true);
      } catch (SqlParseException e) {
        throw new RuntimeException("Error happened while expanding view.", e);
      }
    }
  }
}

// End SqlToRelTestBase.java
