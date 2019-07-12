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
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexMultisetUtil;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Rules and relational operators for
 * {@link JdbcConvention}
 * calling convention.
 */
public class JdbcRules {
  private JdbcRules() {
  }

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  static final RelFactories.ProjectFactory PROJECT_FACTORY =
      (input, projects, fieldNames) -> {
        final RelOptCluster cluster = input.getCluster();
        final RelDataType rowType =
            RexUtil.createStructType(cluster.getTypeFactory(), projects,
                fieldNames, SqlValidatorUtil.F_SUGGESTER);
        return new JdbcProject(cluster, input.getTraitSet(), input, projects,
            rowType);
      };

  static final RelFactories.FilterFactory FILTER_FACTORY =
      (input, condition, variablesSet) -> {
        Preconditions.checkArgument(variablesSet.isEmpty(),
            "JdbcFilter does not allow variables");
        return new JdbcFilter(input.getCluster(),
            input.getTraitSet(), input, condition);
      };

  static final RelFactories.JoinFactory JOIN_FACTORY =
      (left, right, condition, variablesSet, joinType, semiJoinDone) -> {
        final RelOptCluster cluster = left.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(left.getConvention());
        try {
          return new JdbcJoin(cluster, traitSet, left, right, condition,
              variablesSet, joinType);
        } catch (InvalidRelException e) {
          throw new AssertionError(e);
        }
      };

  static final RelFactories.CorrelateFactory CORRELATE_FACTORY =
      (left, right, correlationId, requiredColumns, joinType) -> {
        throw new UnsupportedOperationException("JdbcCorrelate");
      };

  public static final RelFactories.SortFactory SORT_FACTORY =
      (input, collation, offset, fetch) -> {
        throw new UnsupportedOperationException("JdbcSort");
      };

  public static final RelFactories.ExchangeFactory EXCHANGE_FACTORY =
      (input, distribution) -> {
        throw new UnsupportedOperationException("JdbcExchange");
      };

  public static final RelFactories.SortExchangeFactory SORT_EXCHANGE_FACTORY =
      (input, distribution, collation) -> {
        throw new UnsupportedOperationException("JdbcSortExchange");
      };

  public static final RelFactories.AggregateFactory AGGREGATE_FACTORY =
      (input, groupSet, groupSets, aggCalls) -> {
        final RelOptCluster cluster = input.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(input.getConvention());
        try {
          return new JdbcAggregate(cluster, traitSet, input, groupSet,
              groupSets, aggCalls);
        } catch (InvalidRelException e) {
          throw new AssertionError(e);
        }
      };

  public static final RelFactories.MatchFactory MATCH_FACTORY =
      (input, pattern, rowType, strictStart, strictEnd, patternDefinitions,
          measures, after, subsets, allRows, partitionKeys, orderKeys,
          interval) -> {
        throw new UnsupportedOperationException("JdbcMatch");
      };

  public static final RelFactories.SetOpFactory SET_OP_FACTORY =
      (kind, inputs, all) -> {
        RelNode input = inputs.get(0);
        RelOptCluster cluster = input.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(input.getConvention());
        switch (kind) {
        case UNION:
          return new JdbcUnion(cluster, traitSet, inputs, all);
        case INTERSECT:
          return new JdbcIntersect(cluster, traitSet, inputs, all);
        case EXCEPT:
          return new JdbcMinus(cluster, traitSet, inputs, all);
        default:
          throw new AssertionError("unknown: " + kind);
        }
      };

  public static final RelFactories.ValuesFactory VALUES_FACTORY =
      (cluster, rowType, tuples) -> {
        throw new UnsupportedOperationException();
      };

  public static final RelFactories.TableScanFactory TABLE_SCAN_FACTORY =
      (cluster, table) -> {
        throw new UnsupportedOperationException();
      };

  public static final RelFactories.SnapshotFactory SNAPSHOT_FACTORY =
      (input, period) -> {
        throw new UnsupportedOperationException();
      };

  /** A {@link RelBuilderFactory} that creates a {@link RelBuilder} that will
   * create JDBC relational expressions for everything. */
  public static final RelBuilderFactory JDBC_BUILDER =
      RelBuilder.proto(
          Contexts.of(PROJECT_FACTORY,
              FILTER_FACTORY,
              JOIN_FACTORY,
              SORT_FACTORY,
              EXCHANGE_FACTORY,
              SORT_EXCHANGE_FACTORY,
              AGGREGATE_FACTORY,
              MATCH_FACTORY,
              SET_OP_FACTORY,
              VALUES_FACTORY,
              TABLE_SCAN_FACTORY,
              SNAPSHOT_FACTORY));

  public static List<RelOptRule> rules(JdbcConvention out) {
    return rules(out, RelFactories.LOGICAL_BUILDER);
  }

  public static List<RelOptRule> rules(JdbcConvention out,
      RelBuilderFactory relBuilderFactory) {
    return ImmutableList.of(
        new JdbcToEnumerableConverterRule(out, relBuilderFactory),
        new JdbcJoinRule(out, relBuilderFactory),
        new JdbcCalcRule(out, relBuilderFactory),
        new JdbcProjectRule(out, relBuilderFactory),
        new JdbcFilterRule(out, relBuilderFactory),
        new JdbcAggregateRule(out, relBuilderFactory),
        new JdbcSortRule(out, relBuilderFactory),
        new JdbcUnionRule(out, relBuilderFactory),
        new JdbcIntersectRule(out, relBuilderFactory),
        new JdbcMinusRule(out, relBuilderFactory),
        new JdbcTableModificationRule(out, relBuilderFactory),
        new JdbcValuesRule(out, relBuilderFactory));
  }

  /** Abstract base class for rule that converts to JDBC. */
  abstract static class JdbcConverterRule extends ConverterRule {
    protected final JdbcConvention out;

    @SuppressWarnings("unchecked")
    @Deprecated // to be removed before 2.0
    JdbcConverterRule(Class<? extends RelNode> clazz, RelTrait in,
        JdbcConvention out, String description) {
      this(clazz, (Predicate<RelNode>) r -> true, in, out,
          RelFactories.LOGICAL_BUILDER, description);
    }

    <R extends RelNode> JdbcConverterRule(Class<R> clazz,
        Predicate<? super R> predicate, RelTrait in, JdbcConvention out,
        RelBuilderFactory relBuilderFactory, String description) {
      super(clazz, predicate, in, out, relBuilderFactory, description);
      this.out = out;
    }

    @SuppressWarnings({"Guava", "unchecked"})
    @Deprecated // to be removed before 2.0
    <R extends RelNode> JdbcConverterRule(Class<R> clazz,
        com.google.common.base.Predicate<? super R> predicate,
        RelTrait in, JdbcConvention out,
        RelBuilderFactory relBuilderFactory, String description) {
      this(clazz, (Predicate<R>) predicate, in, out, relBuilderFactory,
          description);
    }
  }

  /** Rule that converts a join to JDBC. */
  public static class JdbcJoinRule extends JdbcConverterRule {
    @Deprecated // to be removed before 2.0
    public JdbcJoinRule(JdbcConvention out) {
      this(out, RelFactories.LOGICAL_BUILDER);
    }

    /** Creates a JdbcJoinRule. */
    public JdbcJoinRule(JdbcConvention out,
        RelBuilderFactory relBuilderFactory) {
      super(Join.class, (Predicate<RelNode>) r -> true, Convention.NONE,
          out, relBuilderFactory, "JdbcJoinRule");
    }

    @Override public RelNode convert(RelNode rel) {
      final Join join = (Join) rel;
      switch (join.getJoinType()) {
      case SEMI:
      case ANTI:
        // It's not possible to convert semi-joins or anti-joins. They have fewer columns
        // than regular joins.
        return null;
      default:
        return convert(join, true);
      }
    }

    /**
     * Converts a {@code Join} into a {@code JdbcJoin}.
     *
     * @param join Join operator to convert
     * @param convertInputTraits Whether to convert input to {@code join}'s
     *                            JDBC convention
     * @return A new JdbcJoin
     */
    public RelNode convert(Join join, boolean convertInputTraits) {
      final List<RelNode> newInputs = new ArrayList<>();
      for (RelNode input : join.getInputs()) {
        if (convertInputTraits && input.getConvention() != getOutTrait()) {
          input =
              convert(input,
                  input.getTraitSet().replace(out));
        }
        newInputs.add(input);
      }
      if (convertInputTraits && !canJoinOnCondition(join.getCondition())) {
        return null;
      }
      try {
        return new JdbcJoin(
            join.getCluster(),
            join.getTraitSet().replace(out),
            newInputs.get(0),
            newInputs.get(1),
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType());
      } catch (InvalidRelException e) {
        LOGGER.debug(e.toString());
        return null;
      }
    }

    /**
     * Returns whether a condition is supported by {@link JdbcJoin}.
     *
     * <p>Corresponds to the capabilities of
     * {@link SqlImplementor#convertConditionToSqlNode}.
     *
     * @param node Condition
     * @return Whether condition is supported
     */
    private boolean canJoinOnCondition(RexNode node) {
      final List<RexNode> operands;
      switch (node.getKind()) {
      case AND:
      case OR:
        operands = ((RexCall) node).getOperands();
        for (RexNode operand : operands) {
          if (!canJoinOnCondition(operand)) {
            return false;
          }
        }
        return true;

      case EQUALS:
      case IS_NOT_DISTINCT_FROM:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        operands = ((RexCall) node).getOperands();
        if ((operands.get(0) instanceof RexInputRef)
            && (operands.get(1) instanceof RexInputRef)) {
          return true;
        }
        // fall through

      default:
        return false;
      }
    }
  }

  /** Join operator implemented in JDBC convention. */
  public static class JdbcJoin extends Join implements JdbcRel {
    /** Creates a JdbcJoin. */
    public JdbcJoin(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode left, RelNode right, RexNode condition,
        Set<CorrelationId> variablesSet, JoinRelType joinType)
        throws InvalidRelException {
      super(cluster, traitSet, left, right, condition, variablesSet, joinType);
    }

    @Deprecated // to be removed before 2.0
    protected JdbcJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        Set<String> variablesStopped)
        throws InvalidRelException {
      this(cluster, traitSet, left, right, condition,
          CorrelationId.setOf(variablesStopped), joinType);
    }

    @Override public JdbcJoin copy(RelTraitSet traitSet, RexNode condition,
        RelNode left, RelNode right, JoinRelType joinType,
        boolean semiJoinDone) {
      try {
        return new JdbcJoin(getCluster(), traitSet, left, right,
            condition, variablesSet, joinType);
      } catch (InvalidRelException e) {
        // Semantic error not possible. Must be a bug. Convert to
        // internal error.
        throw new AssertionError(e);
      }
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      // We always "build" the
      double rowCount = mq.getRowCount(this);

      return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
      final double leftRowCount = left.estimateRowCount(mq);
      final double rightRowCount = right.estimateRowCount(mq);
      return Math.max(leftRowCount, rightRowCount);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Calc} to an
   * {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcCalc}.
   */
  private static class JdbcCalcRule extends JdbcConverterRule {
    /** Creates a JdbcCalcRule. */
    private JdbcCalcRule(JdbcConvention out,
        RelBuilderFactory relBuilderFactory) {
      super(Calc.class, (Predicate<RelNode>) r -> true, Convention.NONE,
          out, relBuilderFactory, "JdbcCalcRule");
    }

    public RelNode convert(RelNode rel) {
      final Calc calc = (Calc) rel;

      // If there's a multiset, let FarragoMultisetSplitter work on it
      // first.
      if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
        return null;
      }

      return new JdbcCalc(rel.getCluster(), rel.getTraitSet().replace(out),
          convert(calc.getInput(), calc.getTraitSet().replace(out)),
          calc.getProgram());
    }
  }

  /** Calc operator implemented in JDBC convention.
   *
   * @see org.apache.calcite.rel.core.Calc */
  public static class JdbcCalc extends SingleRel implements JdbcRel {
    private final RexProgram program;

    public JdbcCalc(RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RexProgram program) {
      super(cluster, traitSet, input);
      assert getConvention() instanceof JdbcConvention;
      this.program = program;
      this.rowType = program.getOutputRowType();
    }

    @Deprecated // to be removed before 2.0
    public JdbcCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
        RexProgram program, int flags) {
      this(cluster, traitSet, input, program);
      Util.discard(flags);
    }

    public RelWriter explainTerms(RelWriter pw) {
      return program.explainCalc(super.explainTerms(pw));
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
      return RelMdUtil.estimateFilteredRows(getInput(), program, mq);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      double dRows = mq.getRowCount(this);
      double dCpu = mq.getRowCount(getInput())
          * program.getExprCount();
      double dIo = 0;
      return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new JdbcCalc(getCluster(), traitSet, sole(inputs), program);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Project} to
   * an {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProject}.
   */
  public static class JdbcProjectRule extends JdbcConverterRule {
    @Deprecated // to be removed before 2.0
    public JdbcProjectRule(final JdbcConvention out) {
      this(out, RelFactories.LOGICAL_BUILDER);
    }

    /** Creates a JdbcProjectRule. */
    public JdbcProjectRule(final JdbcConvention out,
        RelBuilderFactory relBuilderFactory) {
      super(Project.class, (Predicate<Project>) project ->
              (out.dialect.supportsWindowFunctions()
                  || !RexOver.containsOver(project.getProjects(), null))
                  && !userDefinedFunctionInProject(project),
          Convention.NONE, out, relBuilderFactory, "JdbcProjectRule");
    }

    private static boolean userDefinedFunctionInProject(Project project) {
      CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
      for (RexNode node : project.getChildExps()) {
        node.accept(visitor);
        if (visitor.containsUserDefinedFunction()) {
          return true;
        }
      }
      return false;
    }

    public RelNode convert(RelNode rel) {
      final Project project = (Project) rel;

      return new JdbcProject(
          rel.getCluster(),
          rel.getTraitSet().replace(out),
          convert(
              project.getInput(),
              project.getInput().getTraitSet().replace(out)),
          project.getProjects(),
          project.getRowType());
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Project} in
   * {@link JdbcConvention jdbc calling convention}. */
  public static class JdbcProject
      extends Project
      implements JdbcRel {
    public JdbcProject(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType) {
      super(cluster, traitSet, input, projects, rowType);
      assert getConvention() instanceof JdbcConvention;
    }

    @Deprecated // to be removed before 2.0
    public JdbcProject(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, List<RexNode> projects, RelDataType rowType, int flags) {
      this(cluster, traitSet, input, projects, rowType);
      Util.discard(flags);
    }

    @Override public JdbcProject copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> projects, RelDataType rowType) {
      return new JdbcProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return super.computeSelfCost(planner, mq)
          .multiplyBy(JdbcConvention.COST_MULTIPLIER);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Filter} to
   * an {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilter}.
   */
  public static class JdbcFilterRule extends JdbcConverterRule {
    @Deprecated // to be removed before 2.0
    public JdbcFilterRule(JdbcConvention out) {
      this(out, RelFactories.LOGICAL_BUILDER);
    }

    /** Creates a JdbcFilterRule. */
    public JdbcFilterRule(JdbcConvention out,
        RelBuilderFactory relBuilderFactory) {
      super(Filter.class,
          (Predicate<Filter>) r -> !userDefinedFunctionInFilter(r),
          Convention.NONE, out, relBuilderFactory, "JdbcFilterRule");
    }

    private static boolean userDefinedFunctionInFilter(Filter filter) {
      CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
      filter.getCondition().accept(visitor);
      return visitor.containsUserDefinedFunction();
    }

    public RelNode convert(RelNode rel) {
      final Filter filter = (Filter) rel;

      return new JdbcFilter(
          rel.getCluster(),
          rel.getTraitSet().replace(out),
          convert(filter.getInput(),
              filter.getInput().getTraitSet().replace(out)),
          filter.getCondition());
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Filter} in
   * {@link JdbcConvention jdbc calling convention}. */
  public static class JdbcFilter extends Filter implements JdbcRel {
    public JdbcFilter(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RexNode condition) {
      super(cluster, traitSet, input, condition);
      assert getConvention() instanceof JdbcConvention;
    }

    public JdbcFilter copy(RelTraitSet traitSet, RelNode input,
        RexNode condition) {
      return new JdbcFilter(getCluster(), traitSet, input, condition);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Aggregate}
   * to a {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcAggregate}.
   */
  public static class JdbcAggregateRule extends JdbcConverterRule {
    @Deprecated // to be removed before 2.0
    public JdbcAggregateRule(JdbcConvention out) {
      this(out, RelFactories.LOGICAL_BUILDER);
    }

    /** Creates a JdbcAggregateRule. */
    public JdbcAggregateRule(JdbcConvention out,
        RelBuilderFactory relBuilderFactory) {
      super(Aggregate.class, (Predicate<RelNode>) r -> true, Convention.NONE,
          out, relBuilderFactory, "JdbcAggregateRule");
    }

    public RelNode convert(RelNode rel) {
      final Aggregate agg = (Aggregate) rel;
      if (agg.getGroupSets().size() != 1) {
        // GROUPING SETS not supported; see
        // [CALCITE-734] Push GROUPING SETS to underlying SQL via JDBC adapter
        return null;
      }
      final RelTraitSet traitSet =
          agg.getTraitSet().replace(out);
      try {
        return new JdbcAggregate(rel.getCluster(), traitSet,
            convert(agg.getInput(), out), false, agg.getGroupSet(),
            agg.getGroupSets(), agg.getAggCallList());
      } catch (InvalidRelException e) {
        LOGGER.debug(e.toString());
        return null;
      }
    }
  }

  /** Returns whether this JDBC data source can implement a given aggregate
   * function. */
  private static boolean canImplement(SqlAggFunction aggregation, SqlDialect sqlDialect) {
    return sqlDialect.supportsAggregateFunction(aggregation.getKind());
  }

  /** Aggregate operator implemented in JDBC convention. */
  public static class JdbcAggregate extends Aggregate implements JdbcRel {
    public JdbcAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls)
        throws InvalidRelException {
      super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
      assert getConvention() instanceof JdbcConvention;
      assert this.groupSets.size() == 1 : "Grouping sets not supported";
      final SqlDialect dialect = ((JdbcConvention) getConvention()).dialect;
      for (AggregateCall aggCall : aggCalls) {
        if (!canImplement(aggCall.getAggregation(), dialect)) {
          throw new InvalidRelException("cannot implement aggregate function "
              + aggCall.getAggregation());
        }
      }
    }

    @Deprecated // to be removed before 2.0
    public JdbcAggregate(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, boolean indicator, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
        throws InvalidRelException {
      this(cluster, traitSet, input, groupSet, groupSets, aggCalls);
      checkIndicator(indicator);
    }

    @Override public JdbcAggregate copy(RelTraitSet traitSet, RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      try {
        return new JdbcAggregate(getCluster(), traitSet, input,
            groupSet, groupSets, aggCalls);
      } catch (InvalidRelException e) {
        // Semantic error not possible. Must be a bug. Convert to
        // internal error.
        throw new AssertionError(e);
      }
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to an
   * {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcSort}.
   */
  public static class JdbcSortRule extends JdbcConverterRule {
    @Deprecated // to be removed before 2.0
    public JdbcSortRule(JdbcConvention out) {
      this(out, RelFactories.LOGICAL_BUILDER);
    }

    /** Creates a JdbcSortRule. */
    public JdbcSortRule(JdbcConvention out,
        RelBuilderFactory relBuilderFactory) {
      super(Sort.class, (Predicate<RelNode>) r -> true, Convention.NONE, out,
          relBuilderFactory, "JdbcSortRule");
    }

    public RelNode convert(RelNode rel) {
      return convert((Sort) rel, true);
    }

    /**
     * Converts a {@code Sort} into a {@code JdbcSort}.
     *
     * @param sort Sort operator to convert
     * @param convertInputTraits Whether to convert input to {@code sort}'s
     *                            JDBC convention
     * @return A new JdbcSort
     */
    public RelNode convert(Sort sort, boolean convertInputTraits) {
      final RelTraitSet traitSet = sort.getTraitSet().replace(out);

      final RelNode input;
      if (convertInputTraits) {
        input = convert(sort.getInput(), traitSet);
      } else {
        input = sort.getInput();
      }

      return new JdbcSort(sort.getCluster(), traitSet,
          input, sort.getCollation(), sort.offset, sort.fetch);
    }
  }

  /** Sort operator implemented in JDBC convention. */
  public static class JdbcSort
      extends Sort
      implements JdbcRel {
    public JdbcSort(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelCollation collation,
        RexNode offset,
        RexNode fetch) {
      super(cluster, traitSet, input, collation, offset, fetch);
      assert getConvention() instanceof JdbcConvention;
      assert getConvention() == input.getConvention();
    }

    @Override public JdbcSort copy(RelTraitSet traitSet, RelNode newInput,
        RelCollation newCollation, RexNode offset, RexNode fetch) {
      return new JdbcSort(getCluster(), traitSet, newInput, newCollation,
          offset, fetch);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert an {@link org.apache.calcite.rel.core.Union} to a
   * {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcUnion}.
   */
  public static class JdbcUnionRule extends JdbcConverterRule {
    @Deprecated // to be removed before 2.0
    public JdbcUnionRule(JdbcConvention out) {
      this(out, RelFactories.LOGICAL_BUILDER);
    }

    /** Creates a JdbcUnionRule. */
    public JdbcUnionRule(JdbcConvention out,
        RelBuilderFactory relBuilderFactory) {
      super(Union.class, (Predicate<RelNode>) r -> true, Convention.NONE, out,
          relBuilderFactory, "JdbcUnionRule");
    }

    public RelNode convert(RelNode rel) {
      final Union union = (Union) rel;
      final RelTraitSet traitSet =
          union.getTraitSet().replace(out);
      return new JdbcUnion(rel.getCluster(), traitSet,
          convertList(union.getInputs(), out), union.all);
    }
  }

  /** Union operator implemented in JDBC convention. */
  public static class JdbcUnion extends Union implements JdbcRel {
    public JdbcUnion(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
    }

    public JdbcUnion copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new JdbcUnion(getCluster(), traitSet, inputs, all);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Intersect}
   * to a {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcIntersect}.
   */
  public static class JdbcIntersectRule extends JdbcConverterRule {
    /** Creates a JdbcIntersectRule. */
    private JdbcIntersectRule(JdbcConvention out,
        RelBuilderFactory relBuilderFactory) {
      super(Intersect.class, (Predicate<RelNode>) r -> true, Convention.NONE,
          out, relBuilderFactory, "JdbcIntersectRule");
    }

    public RelNode convert(RelNode rel) {
      final Intersect intersect = (Intersect) rel;
      if (intersect.all) {
        return null; // INTERSECT ALL not implemented
      }
      final RelTraitSet traitSet =
          intersect.getTraitSet().replace(out);
      return new JdbcIntersect(rel.getCluster(), traitSet,
          convertList(intersect.getInputs(), out), false);
    }
  }

  /** Intersect operator implemented in JDBC convention. */
  public static class JdbcIntersect
      extends Intersect
      implements JdbcRel {
    public JdbcIntersect(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    public JdbcIntersect copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new JdbcIntersect(getCluster(), traitSet, inputs, all);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Minus} to a
   * {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcMinus}.
   */
  public static class JdbcMinusRule extends JdbcConverterRule {
    /** Creates a JdbcMinusRule. */
    private JdbcMinusRule(JdbcConvention out,
        RelBuilderFactory relBuilderFactory) {
      super(Minus.class, (Predicate<RelNode>) r -> true, Convention.NONE, out,
          relBuilderFactory, "JdbcMinusRule");
    }

    public RelNode convert(RelNode rel) {
      final Minus minus = (Minus) rel;
      if (minus.all) {
        return null; // EXCEPT ALL not implemented
      }
      final RelTraitSet traitSet =
          rel.getTraitSet().replace(out);
      return new JdbcMinus(rel.getCluster(), traitSet,
          convertList(minus.getInputs(), out), false);
    }
  }

  /** Minus operator implemented in JDBC convention. */
  public static class JdbcMinus extends Minus implements JdbcRel {
    public JdbcMinus(RelOptCluster cluster, RelTraitSet traitSet,
        List<RelNode> inputs, boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    public JdbcMinus copy(RelTraitSet traitSet, List<RelNode> inputs,
        boolean all) {
      return new JdbcMinus(getCluster(), traitSet, inputs, all);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /** Rule that converts a table-modification to JDBC. */
  public static class JdbcTableModificationRule extends JdbcConverterRule {
    /** Creates a JdbcTableModificationRule. */
    private JdbcTableModificationRule(JdbcConvention out,
        RelBuilderFactory relBuilderFactory) {
      super(TableModify.class, (Predicate<RelNode>) r -> true,
          Convention.NONE, out, relBuilderFactory, "JdbcTableModificationRule");
    }

    @Override public RelNode convert(RelNode rel) {
      final TableModify modify =
          (TableModify) rel;
      final ModifiableTable modifiableTable =
          modify.getTable().unwrap(ModifiableTable.class);
      if (modifiableTable == null) {
        return null;
      }
      final RelTraitSet traitSet =
          modify.getTraitSet().replace(out);
      return new JdbcTableModify(
          modify.getCluster(), traitSet,
          modify.getTable(),
          modify.getCatalogReader(),
          convert(modify.getInput(), traitSet),
          modify.getOperation(),
          modify.getUpdateColumnList(),
          modify.getSourceExpressionList(),
          modify.isFlattened());
    }
  }

  /** Table-modification operator implemented in JDBC convention. */
  public static class JdbcTableModify extends TableModify implements JdbcRel {
    private final Expression expression;

    public JdbcTableModify(RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode input,
        Operation operation,
        List<String> updateColumnList,
        List<RexNode> sourceExpressionList,
        boolean flattened) {
      super(cluster, traitSet, table, catalogReader, input, operation,
          updateColumnList, sourceExpressionList, flattened);
      assert input.getConvention() instanceof JdbcConvention;
      assert getConvention() instanceof JdbcConvention;
      final ModifiableTable modifiableTable =
          table.unwrap(ModifiableTable.class);
      if (modifiableTable == null) {
        throw new AssertionError(); // TODO: user error in validator
      }
      this.expression = table.getExpression(Queryable.class);
      if (expression == null) {
        throw new AssertionError(); // TODO: user error in validator
      }
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new JdbcTableModify(
          getCluster(), traitSet, getTable(), getCatalogReader(),
          sole(inputs), getOperation(), getUpdateColumnList(),
          getSourceExpressionList(), isFlattened());
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /** Rule that converts a values operator to JDBC. */
  public static class JdbcValuesRule extends JdbcConverterRule {
    /** Creates a JdbcValuesRule. */
    private JdbcValuesRule(JdbcConvention out,
        RelBuilderFactory relBuilderFactory) {
      super(Values.class, (Predicate<RelNode>) r -> true, Convention.NONE,
          out, relBuilderFactory, "JdbcValuesRule");
    }

    @Override public RelNode convert(RelNode rel) {
      Values values = (Values) rel;
      return new JdbcValues(values.getCluster(), values.getRowType(),
          values.getTuples(), values.getTraitSet().replace(out));
    }
  }

  /** Values operator implemented in JDBC convention. */
  public static class JdbcValues extends Values implements JdbcRel {
    JdbcValues(RelOptCluster cluster, RelDataType rowType,
        ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
      super(cluster, rowType, tuples, traitSet);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();
      return new JdbcValues(getCluster(), rowType, tuples, traitSet);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Visitor for checking whether part of projection is a user defined function or not
   */
  private static class CheckingUserDefinedFunctionVisitor extends RexVisitorImpl<Void> {

    private boolean containsUsedDefinedFunction = false;

    CheckingUserDefinedFunctionVisitor() {
      super(true);
    }

    public boolean containsUserDefinedFunction() {
      return containsUsedDefinedFunction;
    }

    @Override public Void visitCall(RexCall call) {
      SqlOperator operator = call.getOperator();
      if (operator instanceof SqlFunction
          && ((SqlFunction) operator).getFunctionType().isUserDefined()) {
        containsUsedDefinedFunction |= true;
      }
      return super.visitCall(call);
    }

  }

}

// End JdbcRules.java
