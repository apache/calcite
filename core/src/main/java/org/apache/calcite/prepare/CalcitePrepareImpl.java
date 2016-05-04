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
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
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
import org.apache.calcite.jdbc.CalciteSchema.LatticeEntry;
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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
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
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.MaterializedViewFilterScanRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
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
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.Types;
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

  public static final boolean DEBUG = Util.getBooleanProperty("calcite.debug");

  public static final boolean COMMUTE =
      Util.getBooleanProperty("calcite.enable.join.commute");

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
          JoinPushExpressionsRule.INSTANCE,
          AggregateExpandDistinctAggregatesRule.INSTANCE,
          AggregateReduceFunctionsRule.INSTANCE,
          FilterAggregateTransposeRule.INSTANCE,
          ProjectWindowTransposeRule.INSTANCE,
          JoinCommuteRule.INSTANCE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT,
          SortProjectTransposeRule.INSTANCE,
          SortJoinTransposeRule.INSTANCE,
          SortUnionTransposeRule.INSTANCE);

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
    final SqlValidator validator = createSqlValidator(context, catalogReader);
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
    final RelRoot root = converter.convertQuery(sqlNode1, false, true);
    if (analyze) {
      return analyze_(validator, sql, sqlNode1, root, fail);
    }
    return new ConvertResult(this, validator, sql, sqlNode1,
        validator.getValidatedNodeType(sqlNode1), root);
  }

  private AnalyzeViewResult analyze_(SqlValidator validator, String sql,
      SqlNode sqlNode, RelRoot root, boolean fail) {
    final RexBuilder rexBuilder = root.rel.getCluster().getRexBuilder();
    RelNode rel = root.rel;
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
          validator.getValidatedNodeType(sqlNode), root, null, null, null,
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
                validator.getValidatedNodeType(sqlNode), root, null, null, null,
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
          validator.getValidatedNodeType(sqlNode), root, null, null, null,
          null);
    }

    return new AnalyzeViewResult(this, validator, sql, sqlNode,
        validator.getValidatedNodeType(sqlNode), root, table,
        ImmutableList.copyOf(tablePath),
        constraint, ImmutableIntList.copyOf(columnMapping));
  }

  @Override public void executeDdl(Context context, SqlNode node) {
    throw new UnsupportedOperationException();
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
    if (prepareContext.config().materializationsEnabled()) {
      planner.addRule(MaterializedViewFilterScanRule.INSTANCE);
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
    return prepare_(context, Query.of(queryable), queryable.getElementType(),
        -1);
  }

  public <T> CalciteSignature<T> prepareSql(
      Context context,
      Query<T> query,
      Type elementType,
      long maxRowCount) {
    return prepare_(context, query, elementType, maxRowCount);
  }

  <T> CalciteSignature<T> prepare_(
      Context context,
      Query<T> query,
      Type elementType,
      long maxRowCount) {
    if (SIMPLE_SQLS.contains(query.sql)) {
      return simplePrepare(context, query.sql);
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
    RuntimeException exception = Util.FoundOne.NULL;
    for (Function1<Context, RelOptPlanner> plannerFactory : plannerFactories) {
      final RelOptPlanner planner = plannerFactory.apply(context);
      if (planner == null) {
        throw new AssertionError("factory returned null planner");
      }
      try {
        return prepare2_(context, query, elementType, maxRowCount,
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
        typeFactory.builder()
            .add(SqlUtil.deriveAliasFromOrdinal(0), SqlTypeName.INTEGER)
            .build();
    @SuppressWarnings("unchecked")
    final List<T> list = (List) ImmutableList.of(1);
    final List<String> origin = null;
    final List<List<String>> origins =
        Collections.nCopies(x.getFieldCount(), origin);
    final List<ColumnMetaData> columns =
        getColumnMetaDataList(typeFactory, x, x, origins);
    final Meta.CursorFactory cursorFactory =
        Meta.CursorFactory.deduce(columns, null);
    return new CalciteSignature<>(
        sql,
        ImmutableList.<AvaticaParameter>of(),
        ImmutableMap.<String, Object>of(),
        x,
        columns,
        cursorFactory,
        ImmutableList.<RelCollation>of(),
        -1,
        new Bindable<T>() {
          public Enumerable<T> bind(DataContext dataContext) {
            return Linq4j.asEnumerable(list);
          }
        },
        Meta.StatementType.SELECT);
  }

  /**
   * Routine to figure out the StatementType and defaults to SELECT
   * As CASE increases the default may change
   * @param kind a SqlKind
   * @return Meta.StatementType*/
  private Meta.StatementType getStatementType(SqlKind kind) {
    switch (kind) {
    case INSERT:
      return Meta.StatementType.INSERT;
    default:
      return Meta.StatementType.SELECT;
    }
  }

  /**
   * Routine to figure out the StatementType if call does not have sql
   * defaults to SELECT
   * @param preparedResult An objecet returned from prepareQueryable or prepareRel
   * @return Meta.StatementType*/
  private Meta.StatementType getStatementType(Prepare.PreparedResult preparedResult) {
    if (preparedResult.isDml()) {
      return Meta.StatementType.IS_DML;
    } else {
      return Meta.StatementType.SELECT;
    }
  }

  <T> CalciteSignature<T> prepare2_(
      Context context,
      Query<T> query,
      Type elementType,
      long maxRowCount,
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
    final Meta.StatementType statementType;
    if (query.sql != null) {
      final CalciteConnectionConfig config = context.config();
      SqlParser parser = createParser(query.sql,
          createParserConfig()
              .setQuotedCasing(config.quotedCasing())
              .setUnquotedCasing(config.unquotedCasing())
              .setQuoting(config.quoting()));
      SqlNode sqlNode;
      try {
        sqlNode = parser.parseStmt();
        statementType = getStatementType(sqlNode.getKind());
      } catch (SqlParseException e) {
        throw new RuntimeException(
            "parse failed: " + e.getMessage(), e);
      }

      Hook.PARSE_TREE.run(new Object[] {query.sql, sqlNode});

      if (sqlNode.getKind().belongsTo(SqlKind.DDL)) {
        executeDdl(context, sqlNode);

        // Return a dummy signature that contains no rows
        final Bindable<T> bindable = new Bindable<T>() {
          public Enumerable<T> bind(DataContext dataContext) {
            return Linq4j.emptyEnumerable();
          }
        };
        return new CalciteSignature<>(query.sql,
            ImmutableList.<AvaticaParameter>of(),
            ImmutableMap.<String, Object>of(), null,
            ImmutableList.<ColumnMetaData>of(), Meta.CursorFactory.OBJECT,
            ImmutableList.<RelCollation>of(), -1, bindable);
      }

      final SqlValidator validator =
          createSqlValidator(context, catalogReader);
      validator.setIdentifierExpansion(true);
      validator.setDefaultNullCollation(config.defaultNullCollation());

      preparedResult = preparingStmt.prepareSql(
          sqlNode, Object.class, validator, true);
      switch (sqlNode.getKind()) {
      case INSERT:
      case EXPLAIN:
        // FIXME: getValidatedNodeType is wrong for DML
        x = RelOptUtil.createDmlRowType(sqlNode.getKind(), typeFactory);
        break;
      default:
        x = validator.getValidatedNodeType(sqlNode);
      }
    } else if (query.queryable != null) {
      x = context.getTypeFactory().createType(elementType);
      preparedResult =
          preparingStmt.prepareQueryable(query.queryable, x);
      statementType = getStatementType(preparedResult);
    } else {
      assert query.rel != null;
      x = query.rel.getRowType();
      preparedResult = preparingStmt.prepareRel(query.rel);
      statementType = getStatementType(preparedResult);
    }

    final List<AvaticaParameter> parameters = new ArrayList<>();
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
    return new CalciteSignature<>(
        query.sql,
        parameters,
        preparingStmt.internalParameters,
        jdbcType,
        columns,
        preparingStmt.resultConvention == BindableConvention.INSTANCE
            ? Meta.CursorFactory.ARRAY
            : Meta.CursorFactory.deduce(columns, resultClazz),
        preparedResult instanceof Prepare.PreparedResultImpl
            ? ((Prepare.PreparedResultImpl) preparedResult).collations
            : ImmutableList.<RelCollation>of(),
        maxRowCount,
        bindable,
        statementType);
  }

  private SqlValidator createSqlValidator(Context context,
      CalciteCatalogReader catalogReader) {
    final SqlOperatorTable opTab0 =
        context.config().fun(SqlOperatorTable.class,
            SqlStdOperatorTable.instance());
    final SqlOperatorTable opTab =
        ChainedSqlOperatorTable.of(opTab0, catalogReader);
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    return new CalciteSqlValidator(opTab, catalogReader, typeFactory);
  }

  private List<ColumnMetaData> getColumnMetaDataList(
      JavaTypeFactory typeFactory, RelDataType x, RelDataType jdbcType,
      List<List<String>> originList) {
    final List<ColumnMetaData> columns = new ArrayList<>();
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
    final String typeName = getTypeName(type);
    if (type.getComponentType() != null) {
      final ColumnMetaData.AvaticaType componentType =
          avaticaType(typeFactory, type.getComponentType(), null);
      final Type clazz = typeFactory.getJavaClass(type.getComponentType());
      final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(clazz);
      assert rep != null;
      return ColumnMetaData.array(componentType, typeName, rep);
    } else {
      final int typeOrdinal = getTypeOrdinal(type);
      switch (typeOrdinal) {
      case Types.STRUCT:
        final List<ColumnMetaData> columns = new ArrayList<>();
        for (RelDataTypeField field : type.getFieldList()) {
          columns.add(
              metaData(typeFactory, field.getIndex(), field.getName(),
                  field.getType(), null, null));
        }
        return ColumnMetaData.struct(columns);
      default:
        final Type clazz =
            typeFactory.getJavaClass(Util.first(fieldType, type));
        final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(clazz);
        assert rep != null;
        return ColumnMetaData.scalar(typeOrdinal, typeName, rep);
      }
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

  /** Returns the type name in string form. Does not include precision, scale
   * or whether nulls are allowed. Example: "DECIMAL" not "DECIMAL(7, 2)";
   * "INTEGER" not "JavaType(int)". */
  private static String getTypeName(RelDataType type) {
    final SqlTypeName sqlTypeName = type.getSqlTypeName();
    switch (sqlTypeName) {
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY_TIME:
      // e.g. "INTERVAL_MONTH" or "INTERVAL_YEAR_MONTH"
      return "INTERVAL_"
          + type.getIntervalQualifier().toString().replace(' ', '_');
    case ARRAY:
    case MULTISET:
    case MAP:
    case ROW:
      return type.toString(); // e.g. "INTEGER ARRAY"
    default:
      return sqlTypeName.getName(); // e.g. "DECIMAL"
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
              materialization.viewSchemaPath,
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
    final CalciteSchema schema =
        action.getConfig().getDefaultSchema() != null
            ? CalciteSchema.from(action.getConfig().getDefaultSchema())
            : prepareContext.getRootSchema();
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(schema.root(),
            prepareContext.config().caseSensitive(),
            schema.path(null),
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
        final Queryable queryable,
        RelDataType resultType) {
      return prepare_(
          new Supplier<RelNode>() {
            public RelNode get() {
              final RelOptCluster cluster =
                  prepare.createCluster(planner, rexBuilder);
              return new LixToRelTranslator(cluster, CalcitePreparingStmt.this)
                  .translate(queryable);
            }
          }, resultType);
    }

    public PreparedResult prepareRel(final RelNode rel) {
      return prepare_(
          new Supplier<RelNode>() {
            public RelNode get() {
              return rel;
            }
          }, rel.getRowType());
    }

    private PreparedResult prepare_(Supplier<RelNode> fn,
        RelDataType resultType) {
      queryString = null;
      Class runtimeContextClass = Object.class;
      init(runtimeContextClass);

      final RelNode rel = fn.get();
      final RelDataType rowType = rel.getRowType();
      final List<Pair<Integer, String>> fields =
          Pair.zip(ImmutableIntList.identity(rowType.getFieldCount()),
              rowType.getFieldNames());
      final RelCollation collation =
          rel instanceof Sort
              ? ((Sort) rel).collation
              : RelCollations.EMPTY;
      RelRoot root = new RelRoot(rel, resultType, SqlKind.SELECT, fields,
          collation);

      if (timingTracer != null) {
        timingTracer.traceTime("end sql2rel");
      }

      final RelDataType jdbcType =
          makeStruct(rexBuilder.getTypeFactory(), resultType);
      fieldOrigins = Collections.nCopies(jdbcType.getFieldCount(), null);
      parameterRowType = rexBuilder.getTypeFactory().builder().build();

      // Structured type flattening, view expansion, and plugging in
      // physical storage.
      root = root.withRel(flattenTypes(root.rel, true));

      // Trim unused fields.
      root = trimUnusedFields(root);

      final List<Materialization> materializations = ImmutableList.of();
      final List<CalciteSchema.LatticeEntry> lattices = ImmutableList.of();
      root = optimize(root, materializations, lattices);

      if (timingTracer != null) {
        timingTracer.traceTime("end optimization");
      }

      return implement(root);
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

    @Override public RelRoot expandView(RelDataType rowType, String queryString,
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
      RelRoot root =
          sqlToRelConverter.convertQuery(sqlNode1, true, false);

      --expansionDepth;
      return root;
    }

    protected SqlValidator createSqlValidator(CatalogReader catalogReader) {
      return prepare.createSqlValidator(context,
          (CalciteCatalogReader) catalogReader);
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
        RelRoot root,
        boolean explainAsXml,
        SqlExplainLevel detailLevel) {
      return new CalcitePreparedExplain(
          resultType, parameterRowType, root, explainAsXml, detailLevel);
    }

    @Override protected PreparedResult implement(RelRoot root) {
      RelDataType resultType = root.rel.getRowType();
      boolean isDml = root.kind.belongsTo(SqlKind.DML);
      final Bindable bindable;
      if (resultConvention == BindableConvention.INSTANCE) {
        bindable = Interpreters.bindable(root.rel);
      } else {
        EnumerableRel enumerable = (EnumerableRel) root.rel;
        if (!root.isRefTrivial()) {
          final List<RexNode> projects = new ArrayList<>();
          final RexBuilder rexBuilder = enumerable.getCluster().getRexBuilder();
          for (int field : Pair.left(root.fields)) {
            projects.add(rexBuilder.makeInputRef(enumerable, field));
          }
          RexProgram program = RexProgram.create(enumerable.getRowType(),
              projects, null, root.validatedRowType, rexBuilder);
          enumerable = EnumerableCalc.create(enumerable, program);
        }

        try {
          CatalogReader.THREAD_LOCAL.set(catalogReader);
          bindable = EnumerableInterpretable.toBindable(internalParameters,
              context.spark(), enumerable, prefer);
        } finally {
          CatalogReader.THREAD_LOCAL.remove();
        }
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
          root.collation.getFieldCollations().isEmpty()
              ? ImmutableList.<RelCollation>of()
              : ImmutableList.of(root.collation),
          root.rel,
          mapTableModOp(isDml, root.kind),
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

    @Override protected List<Materialization> getMaterializations() {
      final List<Prepare.Materialization> materializations =
          context.config().materializationsEnabled()
              ? MaterializationService.instance().query(schema)
              : ImmutableList.<Prepare.Materialization>of();
      for (Prepare.Materialization materialization : materializations) {
        prepare.populateMaterializations(context, planner, materialization);
      }
      return materializations;
    }

    @Override protected List<LatticeEntry> getLattices() {
      return Schemas.getLatticeEntries(schema);
    }
  }

  /** An {@code EXPLAIN} statement, prepared and ready to execute. */
  private static class CalcitePreparedExplain extends Prepare.PreparedExplain {
    public CalcitePreparedExplain(
        RelDataType resultType,
        RelDataType parameterRowType,
        RelRoot root,
        boolean explainAsXml,
        SqlExplainLevel detailLevel) {
      super(resultType, parameterRowType, root, explainAsXml, detailLevel);
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
      final List<RexNode> list = new ArrayList<>();
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
      final List<RexNode> list = new ArrayList<>();
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
