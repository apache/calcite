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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
  protected static final Supplier<RelDataTypeFactory> DFLT_TYPE_FACTORY_SUPPLIER =
      Suppliers.memoize(() -> new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));

  //~ Instance fields --------------------------------------------------------

  protected final Tester tester = createTester();
  // Same as tester but without implicit type coercion.
  protected final Tester strictTester = tester.enableTypeCoercion(false);

  protected Tester createTester() {
    final TesterImpl tester =
        new TesterImpl(getDiffRepos(), false, false, false, true, null, null,
            MockRelOptPlanner::new, UnaryOperator.identity(),
            SqlConformanceEnum.DEFAULT, UnaryOperator.identity(), DFLT_TYPE_FACTORY_SUPPLIER);
    return tester.withConfig(c ->
        c.withTrimUnusedFields(true)
            .withExpand(true)
            .addRelBuilderConfigTransform(b ->
                b.withAggregateUnique(true)
                    .withPruneInputOfAggregate(false)));
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

    /**
     * Converts an expression string to  {@link RexNode}.
     *
     * @param expr The expression
     */
    RexNode convertExprToRex(String expr);

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
     * Checks that a SQL statement converts to a given plan, optionally
     * trimming columns that are not needed.
     *
     * @param sql  SQL query or expression
     * @param plan Expected plan
     * @param trim Whether to trim columns that are not needed
     * @param query True if {@code sql} is a query, false if it is an expression
     */
    void assertConvertsTo(
        String sql,
        String plan,
        boolean trim,
        boolean query);

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

    /** Returns a tester that applies a transform to its
     * {@code SqlToRelConverter.Config} before it uses it. */
    Tester withConfig(UnaryOperator<SqlToRelConverter.Config> transform);

    /** Returns a tester with a {@link SqlConformance}. */
    Tester withConformance(SqlConformance conformance);

    /** Returns a tester with a specified if allows type coercion. */
    Tester enableTypeCoercion(boolean typeCoercion);

    Tester withCatalogReaderFactory(
        SqlTestFactory.MockCatalogReaderFactory factory);

    /** Returns a tester that optionally trims unused fields. */
    Tester withTrim(boolean enable);

    Tester withClusterFactory(Function<RelOptCluster, RelOptCluster> function);

    boolean isLateDecorrelate();

    /** Returns a tester that uses a given context. */
    Tester withContext(UnaryOperator<Context> transform);

    /** Returns a tester that uses a type factory. */
    Tester withTypeFactorySupplier(Supplier<RelDataTypeFactory> typeFactorySupplier);

    /** Trims a RelNode. */
    RelNode trimRelNode(RelNode relNode);

    SqlNode parseExpression(String expr) throws Exception;
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

    public void registerRules(RelOptPlanner planner) {
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
        return LogicalTableScan.create(context.getCluster(), this,
            context.getTableHints());
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

      public List<ImmutableBitSet> getKeys() {
        return ImmutableList.of();
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
      return LogicalTableScan.create(context.getCluster(), this,
          context.getTableHints());
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

    public List<ImmutableBitSet> getKeys() {
      return parent.getKeys();
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
    private final boolean enableTypeCoercion;
    private final Function<Context, RelOptPlanner> plannerFactory;
    private final SqlConformance conformance;
    private final SqlTestFactory.MockCatalogReaderFactory catalogReaderFactory;
    private final Function<RelOptCluster, RelOptCluster> clusterFactory;
    private final Supplier<RelDataTypeFactory> typeFactorySupplier;
    private final UnaryOperator<SqlToRelConverter.Config> configTransform;
    private final UnaryOperator<Context> contextTransform;

    /** Creates a TesterImpl with default options. */
    protected TesterImpl(DiffRepository diffRepos) {
      this(diffRepos, true, true, false, true, null, null,
          MockRelOptPlanner::new, UnaryOperator.identity(),
          SqlConformanceEnum.DEFAULT, c -> Contexts.empty(), DFLT_TYPE_FACTORY_SUPPLIER);
    }

    /**
     * Creates a TesterImpl.
     *
     * @param diffRepos Diff repository
     * @param enableDecorrelate Whether to decorrelate
     * @param enableTrim Whether to trim unused fields
     * @param catalogReaderFactory Function to create catalog reader, or null
     * @param clusterFactory Called after a cluster has been created
     */
    protected TesterImpl(DiffRepository diffRepos, boolean enableDecorrelate,
        boolean enableTrim, boolean enableLateDecorrelate,
        boolean enableTypeCoercion,
        SqlTestFactory.MockCatalogReaderFactory catalogReaderFactory,
        Function<RelOptCluster, RelOptCluster> clusterFactory,
        Function<Context, RelOptPlanner> plannerFactory,
        UnaryOperator<SqlToRelConverter.Config> configTransform,
        SqlConformance conformance, UnaryOperator<Context> contextTransform,
        Supplier<RelDataTypeFactory> typeFactorySupplier) {
      this.diffRepos = diffRepos;
      this.enableDecorrelate = enableDecorrelate;
      this.enableTrim = enableTrim;
      this.enableLateDecorrelate = enableLateDecorrelate;
      this.enableTypeCoercion = enableTypeCoercion;
      this.catalogReaderFactory = catalogReaderFactory;
      this.clusterFactory = clusterFactory;
      this.configTransform = Objects.requireNonNull(configTransform, "configTransform");
      this.plannerFactory = Objects.requireNonNull(plannerFactory, "plannerFactory");
      this.conformance = Objects.requireNonNull(conformance, "conformance");
      this.contextTransform = Objects.requireNonNull(contextTransform, "contextTransform");
      this.typeFactorySupplier = Objects.requireNonNull(typeFactorySupplier, "typeFactorySupplier");
    }

    public RelRoot convertSqlToRel(String sql) {
      Objects.requireNonNull(sql, "sql");
      final SqlNode sqlQuery;
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
          createValidator(catalogReader, typeFactory);
      SqlToRelConverter converter =
          createSqlToRelConverter(validator, catalogReader);

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

    public RelNode trimRelNode(RelNode relNode) {
      final RelDataTypeFactory typeFactory = getTypeFactory();
      final Prepare.CatalogReader catalogReader =
          createCatalogReader(typeFactory);
      final SqlValidator validator =
          createValidator(catalogReader, typeFactory);

      final SqlToRelConverter converter =
          createSqlToRelConverter(validator, catalogReader);
      relNode = converter.flattenTypes(relNode, true);
      relNode = converter.trimUnusedFields(true, relNode);
      return relNode;
    }

    private SqlToRelConverter createSqlToRelConverter(SqlValidator validator,
                                                      Prepare.CatalogReader catalogReader) {
      final Context context = getContext();
      context.maybeUnwrap(CalciteConnectionConfig.class)
          .ifPresent(calciteConfig -> {
            validator.transform(config ->
                config.withDefaultNullCollation(calciteConfig.defaultNullCollation()));
          });
      final SqlToRelConverter.Config config =
          configTransform.apply(SqlToRelConverter.config());

      return createSqlToRelConverter(
          validator,
          catalogReader,
          getTypeFactory(),
          config);
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
      return typeFactorySupplier.get();
    }

    protected final RelOptPlanner getPlanner() {
      if (planner == null) {
        planner = createPlanner();
      }
      return planner;
    }

    public SqlNode parseQuery(String sql) throws Exception {
      final SqlParser.Config config =
          SqlParser.config().withConformance(getConformance());
      SqlParser parser = SqlParser.create(sql, config);
      return parser.parseQuery();
    }

    @Override public SqlNode parseExpression(String expr) throws Exception {
      final SqlParser.Config config =
              SqlParser.config().withConformance(getConformance());
      SqlParser parser = SqlParser.create(expr, config);
      return parser.parseExpression();
    }

    public SqlConformance getConformance() {
      return conformance;
    }

    public SqlValidator createValidator(
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory) {
      final SqlOperatorTable operatorTable = getOperatorTable();
      final SqlConformance conformance = getConformance();
      final List<SqlOperatorTable> list = new ArrayList<>();
      list.add(operatorTable);
      if (conformance.allowGeometry()) {
        list.add(SqlOperatorTables.spatialInstance());
      }
      return new FarragoTestValidator(
          SqlOperatorTables.chain(list),
          catalogReader,
          typeFactory,
          SqlValidator.Config.DEFAULT
              .withSqlConformance(conformance)
              .withTypeCoercionEnabled(enableTypeCoercion)
              .withIdentifierExpansion(true));
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
      return getContext().maybeUnwrap(SqlOperatorTable.class)
          .orElseGet(() -> {
            final MockSqlOperatorTable opTab =
                new MockSqlOperatorTable(SqlStdOperatorTable.instance());
            MockSqlOperatorTable.addRamp(opTab);
            return opTab;
          });
    }

    private Context getContext() {
      return contextTransform.apply(Contexts.empty());
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
      return plannerFactory.apply(getContext());
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
      assertConvertsTo(sql, plan, false, true);
    }

    public void assertConvertsTo(
        String sql,
        String plan,
        boolean trim,
        boolean query) {
      if (query) {
        assertSqlConvertsTo(sql, plan, trim);
      } else {
        assertExprConvertsTo(sql, plan);
      }
    }

    private void assertExprConvertsTo(
        String expr,
        String plan) {
      String expr2 = getDiffRepos().expand("sql", expr);
      RexNode rex = convertExprToRex(expr2);
      assertNotNull(rex);
      // NOTE jvs 28-Mar-2006:  insert leading newline so
      // that plans come out nicely stacked instead of first
      // line immediately after CDATA start
      String actual = NL + rex.toString() + NL;
      diffRepos.assertEquals("plan", plan, actual);
    }

    private void assertSqlConvertsTo(
        String sql,
        String plan,
        boolean trim) {
      String sql2 = getDiffRepos().expand("sql", sql);
      RelNode rel = convertSqlToRel(sql2).project();

      assertNotNull(rel);
      assertValid(rel);

      if (trim) {
        final RelBuilder relBuilder =
            RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
        final RelFieldTrimmer trimmer = createFieldTrimmer(relBuilder);
        rel = trimmer.trim(rel);
        assertNotNull(rel);
        assertValid(rel);
      }

      // NOTE jvs 28-Mar-2006:  insert leading newline so
      // that plans come out nicely stacked instead of first
      // line immediately after CDATA start
      String actual = NL + RelOptUtil.toString(rel);
      diffRepos.assertEquals("plan", plan, actual);
    }

    public RexNode convertExprToRex(String expr) {
      Objects.requireNonNull(expr, "expr");
      final SqlNode sqlQuery;
      try {
        sqlQuery = parseExpression(expr);
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
      SqlToRelConverter converter = createSqlToRelConverter(validator, catalogReader);

      final SqlNode validatedQuery = validator.validate(sqlQuery);
      return converter.convertExpression(validatedQuery);
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
              enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
              clusterFactory, plannerFactory, configTransform, conformance,
              contextTransform, typeFactorySupplier);
    }

    public TesterImpl withLateDecorrelation(boolean enableLateDecorrelate) {
      return this.enableLateDecorrelate == enableLateDecorrelate
          ? this
          : new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
              enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
              clusterFactory, plannerFactory, configTransform, conformance,
              contextTransform, typeFactorySupplier);
    }

    public Tester withConfig(UnaryOperator<SqlToRelConverter.Config> transform) {
      final UnaryOperator<SqlToRelConverter.Config> configTransform =
          this.configTransform.andThen(transform)::apply;
      return new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
          enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
          clusterFactory, plannerFactory, configTransform, conformance,
          contextTransform, typeFactorySupplier);
    }

    public TesterImpl withTrim(boolean enableTrim) {
      return this.enableTrim == enableTrim
          ? this
          : new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
              enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
              clusterFactory, plannerFactory, configTransform, conformance,
              contextTransform, typeFactorySupplier);
    }

    public TesterImpl withConformance(SqlConformance conformance) {
      return new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
          enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
          clusterFactory, plannerFactory, configTransform, conformance,
          contextTransform, typeFactorySupplier);
    }

    public Tester enableTypeCoercion(boolean enableTypeCoercion) {
      return new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
          enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
          clusterFactory, plannerFactory, configTransform, conformance,
          contextTransform, typeFactorySupplier);
    }

    public Tester withCatalogReaderFactory(
        SqlTestFactory.MockCatalogReaderFactory catalogReaderFactory) {
      return new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
          enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
          clusterFactory, plannerFactory, configTransform, conformance,
          contextTransform, typeFactorySupplier);
    }

    public Tester withClusterFactory(
        Function<RelOptCluster, RelOptCluster> clusterFactory) {
      return new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
          enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
          clusterFactory, plannerFactory, configTransform, conformance,
          contextTransform, typeFactorySupplier);
    }

    public Tester withPlannerFactory(
        Function<Context, RelOptPlanner> plannerFactory) {
      return this.plannerFactory == plannerFactory
          ? this
          : new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
              enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
              clusterFactory, plannerFactory, configTransform, conformance,
              contextTransform, typeFactorySupplier);
    }

    public Tester withTypeFactorySupplier(
        Supplier<RelDataTypeFactory> typeFactorySupplier) {
      return this.typeFactorySupplier == typeFactorySupplier
          ? this
          : new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
              enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
              clusterFactory, plannerFactory, configTransform, conformance,
              contextTransform, typeFactorySupplier);
    }

    public TesterImpl withContext(UnaryOperator<Context> context) {
      return new TesterImpl(diffRepos, enableDecorrelate, enableTrim,
          enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
          clusterFactory, plannerFactory, configTransform, conformance,
          context, typeFactorySupplier);
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
        Config config) {
      super(opTab, catalogReader, typeFactory, config);
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

    MockViewExpander(
        SqlValidator validator,
        Prepare.CatalogReader catalogReader,
        RelOptCluster cluster,
        SqlToRelConverter.Config config) {
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

  /**
   * Custom implementation of Correlate for testing.
   */
  public static class CustomCorrelate extends Correlate {
    public CustomCorrelate(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode left,
        RelNode right,
        CorrelationId correlationId,
        ImmutableBitSet requiredColumns,
        JoinRelType joinType) {
      super(cluster, traits, left, right, correlationId, requiredColumns, joinType);
    }

    @Override public Correlate copy(RelTraitSet traitSet,
        RelNode left, RelNode right, CorrelationId correlationId,
        ImmutableBitSet requiredColumns, JoinRelType joinType) {
      return new CustomCorrelate(getCluster(), traitSet, left, right,
          correlationId, requiredColumns, joinType);
    }

    @Override public RelNode accept(RelShuttle shuttle) {
      return shuttle.visit(this);
    }
  }
}
