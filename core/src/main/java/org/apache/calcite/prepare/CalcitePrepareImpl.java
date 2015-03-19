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
package org.apache.calcite.prepare;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableBindable;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreterRule;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.interpreter.Interpreters;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BinaryExpression;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateStarTableRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.Typed;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Shit just got real.
 *
 * <p>This class is public so that projects that create their own JDBC driver
 * and server can fine-tune preferences. However, this class and its methods are
 * subject to change without notice.</p>
 */
public class CalcitePrepareImpl implements CalcitePrepare {

  public static final boolean DEBUG =
      "true".equals(System.getProperties().getProperty("calcite.debug"));

  public static final boolean COMMUTE =
      "true".equals(
          System.getProperties().getProperty("calcite.enable.join.commute"));

  /** Whether to enable the collation trait. Some extra optimizations are
   * possible if enabled, but queries should work either way. At some point
   * this will become a preference, or we will run multiple phases: first
   * disabled, then enabled. */
  private static final boolean ENABLE_COLLATION_TRAIT = true;

  /** Whether the bindable convention should be the root convention of any
   * plan. If not, enumerable convention is the default. */
  public static final boolean ENABLE_BINDABLE = false;

  /** Whether the enumerable convention is enabled. */
  public static final boolean ENABLE_ENUMERABLE = true;

  /** Whether the streaming is enabled. */
  public static final boolean ENABLE_STREAM = true;

  private static final Set<String> SIMPLE_SQLS =
      ImmutableSet.of(
          "SELECT 1",
          "select 1",
          "SELECT 1 FROM DUAL",
          "select 1 from dual",
          "values 1",
          "VALUES 1");

  private static final List<RelOptRule> ENUMERABLE_RULES =
      ImmutableList.of(
          EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
          EnumerableRules.ENUMERABLE_CORRELATE_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE,
          EnumerableRules.ENUMERABLE_FILTER_RULE,
          EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_LIMIT_RULE,
          EnumerableRules.ENUMERABLE_COLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNION_RULE,
          EnumerableRules.ENUMERABLE_INTERSECT_RULE,
          EnumerableRules.ENUMERABLE_MINUS_RULE,
          EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
          EnumerableRules.ENUMERABLE_VALUES_RULE,
          EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
          EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE);

  private static final List<RelOptRule> DEFAULT_RULES =
      ImmutableList.of(
          AggregateStarTableRule.INSTANCE,
          AggregateStarTableRule.INSTANCE2,
          TableScanRule.INSTANCE,
          COMMUTE
              ? JoinAssociateRule.INSTANCE
              : ProjectMergeRule.INSTANCE,
          FilterTableScanRule.INSTANCE,
          ProjectFilterTransposeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          FilterJoinRule.FILTER_ON_JOIN,
          AggregateExpandDistinctAggregatesRule.INSTANCE,
          AggregateReduceFunctionsRule.INSTANCE,
          FilterAggregateTransposeRule.INSTANCE,
          JoinCommuteRule.INSTANCE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT,
          SortProjectTransposeRule.INSTANCE);

  private static final List<RelOptRule> CONSTANT_REDUCTION_RULES =
      ImmutableList.of(
          ReduceExpressionsRule.PROJECT_INSTANCE,
          ReduceExpressionsRule.FILTER_INSTANCE,
          ReduceExpressionsRule.CALC_INSTANCE,
          ReduceExpressionsRule.JOIN_INSTANCE,
          ValuesReduceRule.FILTER_INSTANCE,
          ValuesReduceRule.PROJECT_FILTER_INSTANCE,
          ValuesReduceRule.PROJECT_INSTANCE);

  public CalcitePrepareImpl() {
  }

  public ParseResult parse(
      Context context, String sql) {
    return parse_(context, sql, false, false, false);
  }

  public ConvertResult convert(Context context, String sql) {
    return (ConvertResult) parse_(context, sql, true, false, false);
  }

  public AnalyzeViewResult analyzeView(Context context, String sql, boolean fail) {
    return (AnalyzeViewResult) parse_(context, sql, true, true, fail);
  }

  /** Shared implementation for {@link #parse}, {@link #convert} and
   * {@link #analyzeView}. */
  private ParseResult parse_(Context context, String sql, boolean convert,
      boolean analyze, boolean fail) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            context.getRootSchema(),
            context.config().caseSensitive(),
            context.getDefaultSchemaPath(),
            typeFactory);
    SqlParser parser = createParser(sql);
    SqlNode sqlNode;
    try {
      sqlNode = parser.parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException("parse failed", e);
    }
    final SqlValidator validator =
        new CalciteSqlValidator(
            SqlStdOperatorTable.instance(), catalogReader, typeFactory);
    SqlNode sqlNode1 = validator.validate(sqlNode);
    if (convert) {
      return convert_(
          context, sql, analyze, fail, catalogReader, validator, sqlNode1);
    }
    return new ParseResult(this, validator, sql, sqlNode1,
        validator.getValidatedNodeType(sqlNode1));
  }

  private ParseResult convert_(Context context, String sql, boolean analyze,
      boolean fail, CalciteCatalogReader catalogReader, SqlValidator validator,
      SqlNode sqlNode1) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final Convention resultConvention =
        ENABLE_BINDABLE ? BindableConvention.INSTANCE
            : EnumerableConvention.INSTANCE;
    final HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    final CalcitePreparingStmt preparingStmt =
        new CalcitePreparingStmt(this, context, catalogReader, typeFactory,
            context.getRootSchema(), null, planner, resultConvention);
    final SqlToRelConverter converter =
        preparingStmt.getSqlToRelConverter(validator, catalogReader);
    if (analyze) {
      converter.enableTableAccessConversion(false);
    }
    final RelNode relNode = converter.convertQuery(sqlNode1, false, true);
    if (analyze) {
      return analyze_(validator, sql, sqlNode1, relNode, fail);
    }
    return new ConvertResult(this, validator, sql, sqlNode1,
        validator.getValidatedNodeType(sqlNode1), relNode);
  }

  private AnalyzeViewResult analyze_(SqlValidator validator, String sql,
      SqlNode sqlNode, RelNode rel, boolean fail) {
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    final RelNode viewRel = rel;
    Project project;
    if (rel instanceof Project) {
      project = (Project) rel;
      rel = project.getInput();
    } else {
      project = null;
    }
    Filter filter;
    if (rel instanceof Filter) {
      filter = (Filter) rel;
      rel = filter.getInput();
    } else {
      filter = null;
    }
    TableScan scan;
    if (rel instanceof TableScan) {
      scan = (TableScan) rel;
    } else {
      scan = null;
    }
    if (scan == null) {
      if (fail) {
        throw validator.newValidationError(sqlNode,
            RESOURCE.modifiableViewMustBeBasedOnSingleTable());
      }
      return new AnalyzeViewResult(this, validator, sql, sqlNode,
          validator.getValidatedNodeType(sqlNode), rel, null, null, null,
          null);
    }
    final RelOptTable targetRelTable = scan.getTable();
    final RelDataType targetRowType = targetRelTable.getRowType();
    final Table table = targetRelTable.unwrap(Table.class);
    final List<String> tablePath = targetRelTable.getQualifiedName();
    assert table != null;
    List<Integer> columnMapping;
    final Map<Integer, RexNode> projectMap = new HashMap<>();
    if (project == null) {
      columnMapping = ImmutableIntList.range(0, targetRowType.getFieldCount());
    } else {
      columnMapping = new ArrayList<>();
      for (Ord<RexNode> node : Ord.zip(project.getProjects())) {
        if (node.e instanceof RexInputRef) {
          RexInputRef rexInputRef = (RexInputRef) node.e;
          int index = rexInputRef.getIndex();
          if (projectMap.get(index) != null) {
            if (fail) {
              throw validator.newValidationError(sqlNode,
                  RESOURCE.moreThanOneMappedColumn(
                      targetRowType.getFieldList().get(index).getName(),
                      Util.last(tablePath)));
            }
            return new AnalyzeViewResult(this, validator, sql, sqlNode,
                validator.getValidatedNodeType(sqlNode), rel, null, null, null,
                null);
          }
          projectMap.put(index, rexBuilder.makeInputRef(viewRel, node.i));
          columnMapping.add(index);
        } else {
          columnMapping.add(-1);
        }
      }
    }
    final RexNode constraint;
    if (filter != null) {
      constraint = filter.getCondition();
    } else {
      constraint = rexBuilder.makeLiteral(true);
    }
    final List<RexNode> filters = new ArrayList<>();
    RelOptUtil.inferViewPredicates(projectMap, filters, constraint);

    // Check that all columns that are not projected have a constant value
    for (RelDataTypeField field : targetRowType.getFieldList()) {
      final int x = columnMapping.indexOf(field.getIndex());
      if (x >= 0) {
        assert Util.skip(columnMapping, x + 1).indexOf(field.getIndex()) < 0
            : "column projected more than once; should have checked above";
        continue; // target column is projected
      }
      if (projectMap.get(field.getIndex()) != null) {
        continue; // constant expression
      }
      if (field.getType().isNullable()) {
        continue; // don't need expression for nullable columns; NULL suffices
      }
      if (fail) {
        throw validator.newValidationError(sqlNode,
            RESOURCE.noValueSuppliedForViewColumn(field.getName(),
                Util.last(tablePath)));
      }
      return new AnalyzeViewResult(this, validator, sql, sqlNode,
          validator.getValidatedNodeType(sqlNode), rel, null, null, null,
          null);
    }

    return new AnalyzeViewResult(this, validator, sql, sqlNode,
        validator.getValidatedNodeType(sqlNode), rel, table,
        ImmutableList.copyOf(tablePath),
        constraint, ImmutableIntList.copyOf(columnMapping));
  }

  /** Factory method for default SQL parser. */
  protected SqlParser createParser(String sql) {
    return createParser(sql, createParserConfig());
  }

  /** Factory method for SQL parser with a given configuration. */
  protected SqlParser createParser(String sql,
      SqlParser.ConfigBuilder parserConfig) {
    return SqlParser.create(sql, parserConfig.build());
  }

  /** Factory method for SQL parser configuration. */
  protected SqlParser.ConfigBuilder createParserConfig() {
    return SqlParser.configBuilder();
  }

  /** Factory method for cluster. */
  protected RelOptCluster createCluster(RelOptPlanner planner,
      RexBuilder rexBuilder) {
    return RelOptCluster.create(planner, rexBuilder);
  }

  /** Creates a collection of planner factories.
   *
   * <p>The collection must have at least one factory, and each factory must
   * create a planner. If the collection has more than one planner, Calcite will
   * try each planner in turn.</p>
   *
   * <p>One of the things you can do with this mechanism is to try a simpler,
   * faster, planner with a smaller rule set first, then fall back to a more
   * complex planner for complex and costly queries.</p>
   *
   * <p>The default implementation returns a factory that calls
   * {@link #createPlanner(org.apache.calcite.jdbc.CalcitePrepare.Context)}.</p>
   */
  protected List<Function1<Context, RelOptPlanner>> createPlannerFactories() {
    return Collections.<Function1<Context, RelOptPlanner>>singletonList(
        new Function1<Context, RelOptPlanner>() {
          public RelOptPlanner apply(Context context) {
            return createPlanner(context, null, null);
          }
        });
  }

  /** Creates a query planner and initializes it with a default set of
   * rules. */
  protected RelOptPlanner createPlanner(CalcitePrepare.Context prepareContext) {
    return createPlanner(prepareContext, null, null);
  }

  /** Creates a query planner and initializes it with a default set of
   * rules. */
  protected RelOptPlanner createPlanner(
      final CalcitePrepare.Context prepareContext,
      org.apache.calcite.plan.Context externalContext,
      RelOptCostFactory costFactory) {
    if (externalContext == null) {
      externalContext = Contexts.of(prepareContext.config());
    }
    final VolcanoPlanner planner =
        new VolcanoPlanner(costFactory, externalContext);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    if (ENABLE_COLLATION_TRAIT) {
      planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
      planner.registerAbstractRelationalRules();
    }
    RelOptUtil.registerAbstractRels(planner);
    for (RelOptRule rule : DEFAULT_RULES) {
      planner.addRule(rule);
    }

    if (ENABLE_BINDABLE) {
      for (RelOptRule rule : Bindables.RULES) {
        planner.addRule(rule);
      }
    }
    planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
    planner.addRule(ProjectTableScanRule.INSTANCE);
    planner.addRule(ProjectTableScanRule.INTERPRETER);

    if (ENABLE_ENUMERABLE) {
      for (RelOptRule rule : ENUMERABLE_RULES) {
        planner.addRule(rule);
      }
      planner.addRule(EnumerableInterpreterRule.INSTANCE);
    }

    if (ENABLE_BINDABLE && ENABLE_ENUMERABLE) {
      planner.addRule(
          EnumerableBindable.EnumerableToBindableConverterRule.INSTANCE);
    }

    if (ENABLE_STREAM) {
      for (RelOptRule rule : StreamRules.RULES) {
        planner.addRule(rule);
      }
    }

    // Change the below to enable constant-reduction.
    if (false) {
      for (RelOptRule rule : CONSTANT_REDUCTION_RULES) {
        planner.addRule(rule);
      }
    }

    final SparkHandler spark = prepareContext.spark();
    if (spark.enabled()) {
      spark.registerRules(
          new SparkHandler.RuleSetBuilder() {
          public void addRule(RelOptRule rule) {
            // TODO:
          }

          public void removeRule(RelOptRule rule) {
            // TODO:
          }
        });
    }
    return planner;
  }

  public <T> CalciteSignature<T> prepareQueryable(
      Context context,
      Queryable<T> queryable) {
    return prepare_(context, null, queryable, queryable.getElementType(), -1);
  }

  public <T> CalciteSignature<T> prepareSql(
      Context context,
      String sql,
      Queryable<T> expression,
      Type elementType,
      int maxRowCount) {
    return prepare_(context, sql, expression, elementType, maxRowCount);
  }

  <T> CalciteSignature<T> prepare_(
      Context context,
      String sql,
      Queryable<T> queryable,
      Type elementType,
      int maxRowCount) {
    if (SIMPLE_SQLS.contains(sql)) {
      return simplePrepare(context, sql);
    }
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            context.getRootSchema(),
            context.config().caseSensitive(),
            context.getDefaultSchemaPath(),
            typeFactory);
    final List<Function1<Context, RelOptPlanner>> plannerFactories =
        createPlannerFactories();
    if (plannerFactories.isEmpty()) {
      throw new AssertionError("no planner factories");
    }
    RuntimeException exception = new RuntimeException();
    for (Function1<Context, RelOptPlanner> plannerFactory : plannerFactories) {
      final RelOptPlanner planner = plannerFactory.apply(context);
      if (planner == null) {
        throw new AssertionError("factory returned null planner");
      }
      try {
        return prepare2_(context, sql, queryable, elementType, maxRowCount,
            catalogReader, planner);
      } catch (RelOptPlanner.CannotPlanException e) {
        exception = e;
      }
    }
    throw exception;
  }

  /** Quickly prepares a simple SQL statement, circumventing the usual
   * preparation process. */
  private <T> CalciteSignature<T> simplePrepare(Context context, String sql) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final RelDataType x =
        typeFactory.builder().add("EXPR$0", SqlTypeName.INTEGER).build();
    @SuppressWarnings("unchecked")
    final List<T> list = (List) ImmutableList.of(1);
    final List<String> origin = null;
    final List<List<String>> origins =
        Collections.nCopies(x.getFieldCount(), origin);
    final List<ColumnMetaData> columns =
        getColumnMetaDataList(typeFactory, x, x, origins);
    final Meta.CursorFactory cursorFactory =
        Meta.CursorFactory.deduce(columns, null);
    return new CalciteSignature<T>(
        sql,
        ImmutableList.<AvaticaParameter>of(),
        ImmutableMap.<String, Object>of(),
        x,
        columns,
        cursorFactory,
        -1,
        new Bindable<T>() {
          public Enumerable<T> bind(DataContext dataContext) {
            return Linq4j.asEnumerable(list);
          }
        }
    );
  }

  <T> CalciteSignature<T> prepare2_(
      Context context,
      String sql,
      Queryable<T> queryable,
      Type elementType,
      int maxRowCount,
      CalciteCatalogReader catalogReader,
      RelOptPlanner planner) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final EnumerableRel.Prefer prefer;
    if (elementType == Object[].class) {
      prefer = EnumerableRel.Prefer.ARRAY;
    } else {
      prefer = EnumerableRel.Prefer.CUSTOM;
    }
    final Convention resultConvention =
        ENABLE_BINDABLE ? BindableConvention.INSTANCE
            : EnumerableConvention.INSTANCE;
    final CalcitePreparingStmt preparingStmt =
        new CalcitePreparingStmt(this, context, catalogReader, typeFactory,
            context.getRootSchema(), prefer, planner, resultConvention);

    final RelDataType x;
    final Prepare.PreparedResult preparedResult;
    if (sql != null) {
      assert queryable == null;
      final CalciteConnectionConfig config = context.config();
      SqlParser parser = createParser(sql,
          createParserConfig()
              .setQuotedCasing(config.quotedCasing())
              .setUnquotedCasing(config.unquotedCasing())
              .setQuoting(config.quoting()));
      SqlNode sqlNode;
      try {
        sqlNode = parser.parseStmt();
      } catch (SqlParseException e) {
        throw new RuntimeException(
            "parse failed: " + e.getMessage(), e);
      }

      Hook.PARSE_TREE.run(new Object[] {sql, sqlNode});

      final CalciteSchema rootSchema = context.getRootSchema();
      final ChainedSqlOperatorTable opTab =
          new ChainedSqlOperatorTable(
              ImmutableList.of(SqlStdOperatorTable.instance(), catalogReader));
      final SqlValidator validator =
          new CalciteSqlValidator(opTab, catalogReader, typeFactory);
      validator.setIdentifierExpansion(true);

      final List<Prepare.Materialization> materializations =
          config.materializationsEnabled()
              ? MaterializationService.instance().query(rootSchema)
              : ImmutableList.<Prepare.Materialization>of();
      for (Prepare.Materialization materialization : materializations) {
        populateMaterializations(context, planner, materialization);
      }
      final List<CalciteSchema.LatticeEntry> lattices =
          Schemas.getLatticeEntries(rootSchema);
      preparedResult = preparingStmt.prepareSql(
          sqlNode, Object.class, validator, true, materializations, lattices);
      switch (sqlNode.getKind()) {
      case INSERT:
      case EXPLAIN:
        // FIXME: getValidatedNodeType is wrong for DML
        x = RelOptUtil.createDmlRowType(sqlNode.getKind(), typeFactory);
        break;
      default:
        x = validator.getValidatedNodeType(sqlNode);
      }
    } else {
      assert queryable != null;
      x = context.getTypeFactory().createType(elementType);
      preparedResult =
          preparingStmt.prepareQueryable(queryable, x);
    }

    final List<AvaticaParameter> parameters = new ArrayList<AvaticaParameter>();
    final RelDataType parameterRowType = preparedResult.getParameterRowType();
    for (RelDataTypeField field : parameterRowType.getFieldList()) {
      RelDataType type = field.getType();
      parameters.add(
          new AvaticaParameter(
              false,
              getPrecision(type),
              getScale(type),
              getTypeOrdinal(type),
              getTypeName(type),
              getClassName(type),
              field.getName()));
    }

    RelDataType jdbcType = makeStruct(typeFactory, x);
    final List<List<String>> originList = preparedResult.getFieldOrigins();
    final List<ColumnMetaData> columns =
        getColumnMetaDataList(typeFactory, x, jdbcType, originList);
    Class resultClazz = null;
    if (preparedResult instanceof Typed) {
      resultClazz = (Class) ((Typed) preparedResult).getElementType();
    }
    //noinspection unchecked
    final Bindable<T> bindable = preparedResult.getBindable();
    return new CalciteSignature<T>(
        sql,
        parameters,
        preparingStmt.internalParameters,
        jdbcType,
        columns,
        preparingStmt.resultConvention == BindableConvention.INSTANCE
            ? Meta.CursorFactory.ARRAY
            : Meta.CursorFactory.deduce(columns, resultClazz),
        maxRowCount,
        bindable);
  }

  private List<ColumnMetaData> getColumnMetaDataList(
      JavaTypeFactory typeFactory, RelDataType x, RelDataType jdbcType,
      List<List<String>> originList) {
    final List<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
    for (Ord<RelDataTypeField> pair : Ord.zip(jdbcType.getFieldList())) {
      final RelDataTypeField field = pair.e;
      final RelDataType type = field.getType();
      final RelDataType fieldType =
          x.isStruct() ? x.getFieldList().get(pair.i).getType() : type;
      columns.add(
          metaData(typeFactory, columns.size(), field.getName(), type,
              fieldType, originList.get(pair.i)));
    }
    return columns;
  }

  private ColumnMetaData metaData(JavaTypeFactory typeFactory, int ordinal,
      String fieldName, RelDataType type, RelDataType fieldType,
      List<String> origins) {
    final ColumnMetaData.AvaticaType avaticaType =
        avaticaType(typeFactory, type, fieldType);
    return new ColumnMetaData(
        ordinal,
        false,
        true,
        false,
        false,
        type.isNullable()
            ? DatabaseMetaData.columnNullable
            : DatabaseMetaData.columnNoNulls,
        true,
        type.getPrecision(),
        fieldName,
        origin(origins, 0),
        origin(origins, 2),
        getPrecision(type),
        getScale(type),
        origin(origins, 1),
        null,
        avaticaType,
        true,
        false,
        false,
        avaticaType.columnClassName());
  }

  private ColumnMetaData.AvaticaType avaticaType(JavaTypeFactory typeFactory,
      RelDataType type, RelDataType fieldType) {
    final Type clazz = typeFactory.getJavaClass(Util.first(fieldType, type));
    final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(clazz);
    assert rep != null;
    final String typeName = getTypeName(type);
    if (type.getComponentType() != null) {
      final ColumnMetaData.AvaticaType componentType =
          avaticaType(typeFactory, type.getComponentType(), null);
      return ColumnMetaData.array(componentType, typeName, rep);
    } else {
      return ColumnMetaData.scalar(getTypeOrdinal(type), typeName, rep);
    }
  }

  private static String origin(List<String> origins, int offsetFromEnd) {
    return origins == null || offsetFromEnd >= origins.size()
        ? null
        : origins.get(origins.size() - 1 - offsetFromEnd);
  }

  private int getTypeOrdinal(RelDataType type) {
    return type.getSqlTypeName().getJdbcOrdinal();
  }

  private static String getClassName(RelDataType type) {
    return null;
  }

  private static int getScale(RelDataType type) {
    return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
        ? 0
        : type.getScale();
  }

  private static int getPrecision(RelDataType type) {
    return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
        ? 0
        : type.getPrecision();
  }

  private static String getTypeName(RelDataType type) {
    SqlTypeName sqlTypeName = type.getSqlTypeName();
    if (type instanceof RelDataTypeFactoryImpl.JavaType) {
      // We'd rather print "INTEGER" than "JavaType(int)".
      return sqlTypeName.getName();
    }
    switch (sqlTypeName) {
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY_TIME:
      // e.g. "INTERVAL_MONTH" or "INTERVAL_YEAR_MONTH"
      return "INTERVAL_"
          + type.getIntervalQualifier().toString().replace(' ', '_');
    default:
      return type.toString(); // e.g. "VARCHAR(10)", "INTEGER ARRAY"
    }
  }

  protected void populateMaterializations(Context context,
      RelOptPlanner planner, Prepare.Materialization materialization) {
    // REVIEW: initialize queryRel and tableRel inside MaterializationService,
    // not here?
    try {
      final CalciteSchema schema = materialization.materializedTable.schema;
      CalciteCatalogReader catalogReader =
          new CalciteCatalogReader(
              schema.root(),
              context.config().caseSensitive(),
              Util.skipLast(materialization.materializedTable.path()),
              context.getTypeFactory());
      final CalciteMaterializer materializer =
          new CalciteMaterializer(this, context, catalogReader, schema, planner);
      materializer.populate(materialization);
    } catch (Exception e) {
      throw new RuntimeException("While populating materialization "
          + materialization.materializedTable.path(), e);
    }
  }

  private static RelDataType makeStruct(
      RelDataTypeFactory typeFactory,
      RelDataType type) {
    if (type.isStruct()) {
      return type;
    }
    return typeFactory.builder().add("$0", type).build();
  }

  /** Executes a prepare action. */
  public <R> R perform(CalciteServerStatement statement,
      Frameworks.PrepareAction<R> action) {
    final CalcitePrepare.Context prepareContext =
        statement.createPrepareContext();
    final JavaTypeFactory typeFactory = prepareContext.getTypeFactory();
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(prepareContext.getRootSchema(),
            prepareContext.config().caseSensitive(),
            prepareContext.getDefaultSchemaPath(),
            typeFactory);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelOptPlanner planner =
        createPlanner(prepareContext,
            action.getConfig().getContext(),
            action.getConfig().getCostFactory());
    final RelOptCluster cluster = createCluster(planner, rexBuilder);
    return action.apply(cluster, catalogReader,
        prepareContext.getRootSchema().plus(), statement);
  }

  /** Holds state for the process of preparing a SQL statement. */
  static class CalcitePreparingStmt extends Prepare
      implements RelOptTable.ViewExpander {
    protected final RelOptPlanner planner;
    protected final RexBuilder rexBuilder;
    protected final CalcitePrepareImpl prepare;
    protected final CalciteSchema schema;
    protected final RelDataTypeFactory typeFactory;
    private final EnumerableRel.Prefer prefer;
    private final Map<String, Object> internalParameters =
        Maps.newLinkedHashMap();
    private int expansionDepth;
    private SqlValidator sqlValidator;

    public CalcitePreparingStmt(CalcitePrepareImpl prepare,
        Context context,
        CatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        CalciteSchema schema,
        EnumerableRel.Prefer prefer,
        RelOptPlanner planner,
        Convention resultConvention) {
      super(context, catalogReader, resultConvention);
      this.prepare = prepare;
      this.schema = schema;
      this.prefer = prefer;
      this.planner = planner;
      this.typeFactory = typeFactory;
      this.rexBuilder = new RexBuilder(typeFactory);
    }

    @Override protected void init(Class runtimeContextClass) {
    }

    public PreparedResult prepareQueryable(
        Queryable queryable,
        RelDataType resultType) {
      queryString = null;
      Class runtimeContextClass = Object.class;
      init(runtimeContextClass);

      final RelOptCluster cluster = prepare.createCluster(planner, rexBuilder);

      RelNode rootRel =
          new LixToRelTranslator(cluster, CalcitePreparingStmt.this)
              .translate(queryable);

      if (timingTracer != null) {
        timingTracer.traceTime("end sql2rel");
      }

      final RelDataType jdbcType =
          makeStruct(rexBuilder.getTypeFactory(), resultType);
      fieldOrigins = Collections.nCopies(jdbcType.getFieldCount(), null);
      parameterRowType = rexBuilder.getTypeFactory().builder().build();

      // Structured type flattening, view expansion, and plugging in
      // physical storage.
      rootRel = flattenTypes(rootRel, true);

      // Trim unused fields.
      rootRel = trimUnusedFields(rootRel);

      final List<Materialization> materializations = ImmutableList.of();
      final List<CalciteSchema.LatticeEntry> lattices = ImmutableList.of();
      rootRel = optimize(rootRel, materializations, lattices);

      if (timingTracer != null) {
        timingTracer.traceTime("end optimization");
      }

      return implement(
          resultType,
          rootRel,
          SqlKind.SELECT);
    }

    @Override protected SqlToRelConverter getSqlToRelConverter(
        SqlValidator validator,
        CatalogReader catalogReader) {
      final RelOptCluster cluster = prepare.createCluster(planner, rexBuilder);
      SqlToRelConverter sqlToRelConverter =
          new SqlToRelConverter(this, validator, catalogReader, cluster,
              StandardConvertletTable.INSTANCE);
      sqlToRelConverter.setTrimUnusedFields(true);
      return sqlToRelConverter;
    }

    @Override public RelNode flattenTypes(
        RelNode rootRel,
        boolean restructure) {
      final SparkHandler spark = context.spark();
      if (spark.enabled()) {
        return spark.flattenTypes(planner, rootRel, restructure);
      }
      return rootRel;
    }

    @Override protected RelNode decorrelate(SqlToRelConverter sqlToRelConverter,
        SqlNode query, RelNode rootRel) {
      return sqlToRelConverter.decorrelate(query, rootRel);
    }

    @Override public RelNode expandView(
        RelDataType rowType,
        String queryString,
        List<String> schemaPath) {
      expansionDepth++;

      SqlParser parser = prepare.createParser(queryString);
      SqlNode sqlNode;
      try {
        sqlNode = parser.parseQuery();
      } catch (SqlParseException e) {
        throw new RuntimeException("parse failed", e);
      }
      // View may have different schema path than current connection.
      final CatalogReader catalogReader =
          this.catalogReader.withSchemaPath(schemaPath);
      SqlValidator validator = createSqlValidator(catalogReader);
      SqlNode sqlNode1 = validator.validate(sqlNode);

      SqlToRelConverter sqlToRelConverter =
          getSqlToRelConverter(validator, catalogReader);
      RelNode relNode =
          sqlToRelConverter.convertQuery(sqlNode1, true, false);

      --expansionDepth;
      return relNode;
    }

    private SqlValidatorImpl createSqlValidator(CatalogReader catalogReader) {
      return new SqlValidatorImpl(
          SqlStdOperatorTable.instance(), catalogReader,
          rexBuilder.getTypeFactory(), SqlConformance.DEFAULT) { };
    }

    @Override protected SqlValidator getSqlValidator() {
      if (sqlValidator == null) {
        sqlValidator = createSqlValidator(catalogReader);
      }
      return sqlValidator;
    }

    @Override protected PreparedResult createPreparedExplanation(
        RelDataType resultType,
        RelDataType parameterRowType,
        RelNode rootRel,
        boolean explainAsXml,
        SqlExplainLevel detailLevel) {
      return new CalcitePreparedExplain(
          resultType, parameterRowType, rootRel, explainAsXml, detailLevel);
    }

    @Override protected PreparedResult implement(
        RelDataType rowType,
        RelNode rootRel,
        SqlKind sqlKind) {
      RelDataType resultType = rootRel.getRowType();
      boolean isDml = sqlKind.belongsTo(SqlKind.DML);
      final Bindable bindable;
      if (resultConvention == BindableConvention.INSTANCE) {
        bindable = Interpreters.bindable(rootRel);
      } else {
        bindable = EnumerableInterpretable.toBindable(internalParameters,
            context.spark(), (EnumerableRel) rootRel, prefer);
      }

      if (timingTracer != null) {
        timingTracer.traceTime("end codegen");
      }

      if (timingTracer != null) {
        timingTracer.traceTime("end compilation");
      }

      return new PreparedResultImpl(
          resultType,
          parameterRowType,
          fieldOrigins,
          rootRel,
          mapTableModOp(isDml, sqlKind),
          isDml) {
        public String getCode() {
          throw new UnsupportedOperationException();
        }

        public Bindable getBindable() {
          return bindable;
        }

        public Type getElementType() {
          return ((Typed) bindable).getElementType();
        }
      };
    }
  }

  /** An {@code EXPLAIN} statement, prepared and ready to execute. */
  private static class CalcitePreparedExplain extends Prepare.PreparedExplain {
    public CalcitePreparedExplain(
        RelDataType resultType,
        RelDataType parameterRowType,
        RelNode rootRel,
        boolean explainAsXml,
        SqlExplainLevel detailLevel) {
      super(resultType, parameterRowType, rootRel, explainAsXml, detailLevel);
    }

    public Bindable getBindable() {
      final String explanation = getCode();
      return new Bindable() {
        public Enumerable bind(DataContext dataContext) {
          return Linq4j.singletonEnumerable(explanation);
        }
      };
    }
  }

  /** Translator from Java AST to {@link RexNode}. */
  interface ScalarTranslator {
    RexNode toRex(BlockStatement statement);
    List<RexNode> toRexList(BlockStatement statement);
    RexNode toRex(Expression expression);
    ScalarTranslator bind(List<ParameterExpression> parameterList,
        List<RexNode> values);
  }

  /** Basic translator. */
  static class EmptyScalarTranslator implements ScalarTranslator {
    private final RexBuilder rexBuilder;

    public EmptyScalarTranslator(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    public static ScalarTranslator empty(RexBuilder builder) {
      return new EmptyScalarTranslator(builder);
    }

    public List<RexNode> toRexList(BlockStatement statement) {
      final List<Expression> simpleList = simpleList(statement);
      final List<RexNode> list = new ArrayList<RexNode>();
      for (Expression expression1 : simpleList) {
        list.add(toRex(expression1));
      }
      return list;
    }

    public RexNode toRex(BlockStatement statement) {
      return toRex(Blocks.simple(statement));
    }

    private static List<Expression> simpleList(BlockStatement statement) {
      Expression simple = Blocks.simple(statement);
      if (simple instanceof NewExpression) {
        NewExpression newExpression = (NewExpression) simple;
        return newExpression.arguments;
      } else {
        return Collections.singletonList(simple);
      }
    }

    public RexNode toRex(Expression expression) {
      switch (expression.getNodeType()) {
      case MemberAccess:
        // Case-sensitive name match because name was previously resolved.
        return rexBuilder.makeFieldAccess(
            toRex(
                ((MemberExpression) expression).expression),
            ((MemberExpression) expression).field.getName(),
            true);
      case GreaterThan:
        return binary(expression, SqlStdOperatorTable.GREATER_THAN);
      case LessThan:
        return binary(expression, SqlStdOperatorTable.LESS_THAN);
      case Parameter:
        return parameter((ParameterExpression) expression);
      case Call:
        MethodCallExpression call = (MethodCallExpression) expression;
        SqlOperator operator =
            RexToLixTranslator.JAVA_TO_SQL_METHOD_MAP.get(call.method);
        if (operator != null) {
          return rexBuilder.makeCall(
              type(call),
              operator,
              toRex(
                  Expressions.<Expression>list()
                      .appendIfNotNull(call.targetExpression)
                      .appendAll(call.expressions)));
        }
        throw new RuntimeException(
            "Could translate call to method " + call.method);
      case Constant:
        final ConstantExpression constant =
            (ConstantExpression) expression;
        Object value = constant.value;
        if (value instanceof Number) {
          Number number = (Number) value;
          if (value instanceof Double || value instanceof Float) {
            return rexBuilder.makeApproxLiteral(
                BigDecimal.valueOf(number.doubleValue()));
          } else if (value instanceof BigDecimal) {
            return rexBuilder.makeExactLiteral((BigDecimal) value);
          } else {
            return rexBuilder.makeExactLiteral(
                BigDecimal.valueOf(number.longValue()));
          }
        } else if (value instanceof Boolean) {
          return rexBuilder.makeLiteral((Boolean) value);
        } else {
          return rexBuilder.makeLiteral(constant.toString());
        }
      default:
        throw new UnsupportedOperationException(
            "unknown expression type " + expression.getNodeType() + " "
            + expression);
      }
    }

    private RexNode binary(Expression expression, SqlBinaryOperator op) {
      BinaryExpression call = (BinaryExpression) expression;
      return rexBuilder.makeCall(type(call), op,
          toRex(ImmutableList.of(call.expression0, call.expression1)));
    }

    private List<RexNode> toRex(List<Expression> expressions) {
      ArrayList<RexNode> list = new ArrayList<RexNode>();
      for (Expression expression : expressions) {
        list.add(toRex(expression));
      }
      return list;
    }

    protected RelDataType type(Expression expression) {
      final Type type = expression.getType();
      return ((JavaTypeFactory) rexBuilder.getTypeFactory()).createType(type);
    }

    public ScalarTranslator bind(
        List<ParameterExpression> parameterList, List<RexNode> values) {
      return new LambdaScalarTranslator(
          rexBuilder, parameterList, values);
    }

    public RexNode parameter(ParameterExpression param) {
      throw new RuntimeException("unknown parameter " + param);
    }
  }

  /** Translator that looks for parameters. */
  private static class LambdaScalarTranslator extends EmptyScalarTranslator {
    private final List<ParameterExpression> parameterList;
    private final List<RexNode> values;

    public LambdaScalarTranslator(
        RexBuilder rexBuilder,
        List<ParameterExpression> parameterList,
        List<RexNode> values) {
      super(rexBuilder);
      this.parameterList = parameterList;
      this.values = values;
    }

    public RexNode parameter(ParameterExpression param) {
      int i = parameterList.indexOf(param);
      if (i >= 0) {
        return values.get(i);
      }
      throw new RuntimeException("unknown parameter " + param);
    }
  }
}

// End CalcitePrepareImpl.java
