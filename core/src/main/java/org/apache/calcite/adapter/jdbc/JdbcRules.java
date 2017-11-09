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
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexMultisetUtil;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Rules and relational operators for
 * {@link JdbcConvention}
 * calling convention.
 */
public class JdbcRules {
  private JdbcRules() {
  }

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public static List<RelOptRule> rules(JdbcConvention out) {
    return ImmutableList.<RelOptRule>of(
        new JdbcToEnumerableConverterRule(out, RelFactories.LOGICAL_BUILDER),
        new JdbcJoinRule(out),
        new JdbcCalcRule(out),
        new JdbcProjectRule(out),
        new JdbcFilterRule(out),
        new JdbcAggregateRule(out),
        new JdbcSortRule(out),
        new JdbcUnionRule(out),
        new JdbcIntersectRule(out),
        new JdbcMinusRule(out),
        new JdbcTableModificationRule(out),
        new JdbcValuesRule(out));
  }

  /** Abstract base class for rule that converts to JDBC. */
  abstract static class JdbcConverterRule extends ConverterRule {
    protected final JdbcConvention out;

    JdbcConverterRule(Class<? extends RelNode> clazz, RelTrait in,
        JdbcConvention out, String description) {
      super(clazz, in, out, description);
      this.out = out;
    }
  }

  /** Rule that converts a join to JDBC. */
  private static class JdbcJoinRule extends JdbcConverterRule {
    private JdbcJoinRule(JdbcConvention out) {
      super(LogicalJoin.class, Convention.NONE, out, "JdbcJoinRule");
    }

    @Override public RelNode convert(RelNode rel) {
      LogicalJoin join = (LogicalJoin) rel;
      final List<RelNode> newInputs = new ArrayList<>();
      for (RelNode input : join.getInputs()) {
        if (!(input.getConvention() == getOutTrait())) {
          input =
              convert(input,
                  input.getTraitSet().replace(out));
        }
        newInputs.add(input);
      }
      if (!canJoinOnCondition(join.getCondition())) {
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
    protected JdbcJoin(RelOptCluster cluster, RelTraitSet traitSet,
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
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalCalc} to an
   * {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcCalc}.
   */
  private static class JdbcCalcRule extends JdbcConverterRule {
    private JdbcCalcRule(JdbcConvention out) {
      super(LogicalCalc.class, Convention.NONE, out, "JdbcCalcRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalCalc calc = (LogicalCalc) rel;

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
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject} to
   * an {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProject}.
   */
  private static class JdbcProjectRule extends JdbcConverterRule {
    private JdbcProjectRule(JdbcConvention out) {
      super(LogicalProject.class, Convention.NONE, out, "JdbcProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;

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

  /** Implementation of {@link org.apache.calcite.rel.logical.LogicalProject} in
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
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to
   * an {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilter}.
   */
  private static class JdbcFilterRule extends JdbcConverterRule {
    private JdbcFilterRule(JdbcConvention out) {
      super(LogicalFilter.class, Convention.NONE, out, "JdbcFilterRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;

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
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalAggregate}
   * to a {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcAggregate}.
   */
  private static class JdbcAggregateRule extends JdbcConverterRule {
    private JdbcAggregateRule(JdbcConvention out) {
      super(LogicalAggregate.class, Convention.NONE, out, "JdbcAggregateRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalAggregate agg = (LogicalAggregate) rel;
      if (agg.getGroupSets().size() != 1) {
        // GROUPING SETS not supported; see
        // [CALCITE-734] Push GROUPING SETS to underlying SQL via JDBC adapter
        return null;
      }
      final RelTraitSet traitSet =
          agg.getTraitSet().replace(out);
      try {
        return new JdbcAggregate(rel.getCluster(), traitSet,
            convert(agg.getInput(), out), agg.indicator, agg.getGroupSet(),
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
        boolean indicator,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls)
        throws InvalidRelException {
      super(cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls);
      assert getConvention() instanceof JdbcConvention;
      assert this.groupSets.size() == 1 : "Grouping sets not supported";
      assert !this.indicator;
      final SqlDialect dialect = ((JdbcConvention) getConvention()).dialect;
      for (AggregateCall aggCall : aggCalls) {
        if (!canImplement(aggCall.getAggregation(), dialect)) {
          throw new InvalidRelException("cannot implement aggregate function "
              + aggCall.getAggregation());
        }
      }
    }

    @Override public JdbcAggregate copy(RelTraitSet traitSet, RelNode input,
        boolean indicator, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      try {
        return new JdbcAggregate(getCluster(), traitSet, input, indicator,
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
  private static class JdbcSortRule extends JdbcConverterRule {
    private JdbcSortRule(JdbcConvention out) {
      super(Sort.class, Convention.NONE, out, "JdbcSortRule");
    }

    public RelNode convert(RelNode rel) {
      final Sort sort = (Sort) rel;
      if (sort.offset != null || sort.fetch != null) {
        // Cannot implement "OFFSET n FETCH n" currently.
        return null;
      }
      final RelTraitSet traitSet = sort.getTraitSet().replace(out);
      return new JdbcSort(rel.getCluster(), traitSet,
          convert(sort.getInput(), traitSet), sort.getCollation());
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
        RelCollation collation) {
      super(cluster, traitSet, input, collation);
      assert getConvention() instanceof JdbcConvention;
      assert getConvention() == input.getConvention();
    }

    @Override public JdbcSort copy(RelTraitSet traitSet, RelNode newInput,
        RelCollation newCollation, RexNode offset, RexNode fetch) {
      if (offset != null || fetch != null) {
        throw new IllegalArgumentException("not supported: offset or fetch");
      }
      return new JdbcSort(getCluster(), traitSet, newInput, newCollation);
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert an {@link org.apache.calcite.rel.logical.LogicalUnion} to a
   * {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcUnion}.
   */
  private static class JdbcUnionRule extends JdbcConverterRule {
    private JdbcUnionRule(JdbcConvention out) {
      super(LogicalUnion.class, Convention.NONE, out, "JdbcUnionRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalUnion union = (LogicalUnion) rel;
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
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalIntersect}
   * to a {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcIntersect}.
   */
  private static class JdbcIntersectRule extends JdbcConverterRule {
    private JdbcIntersectRule(JdbcConvention out) {
      super(LogicalIntersect.class, Convention.NONE, out, "JdbcIntersectRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalIntersect intersect = (LogicalIntersect) rel;
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
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalMinus} to a
   * {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcMinus}.
   */
  private static class JdbcMinusRule extends JdbcConverterRule {
    private JdbcMinusRule(JdbcConvention out) {
      super(LogicalMinus.class, Convention.NONE, out, "JdbcMinusRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalMinus minus = (LogicalMinus) rel;
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
    private JdbcTableModificationRule(JdbcConvention out) {
      super(
          LogicalTableModify.class,
          Convention.NONE,
          out,
          "JdbcTableModificationRule");
    }

    @Override public RelNode convert(RelNode rel) {
      final LogicalTableModify modify =
          (LogicalTableModify) rel;
      final ModifiableTable modifiableTable =
          modify.getTable().unwrap(ModifiableTable.class);
      if (modifiableTable == null
          /* || modifiableTable.getExpression(tableInSchema) == null */) {
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
    private JdbcValuesRule(JdbcConvention out) {
      super(LogicalValues.class, Convention.NONE, out, "JdbcValuesRule");
    }

    @Override public RelNode convert(RelNode rel) {
      LogicalValues values = (LogicalValues) rel;
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
}

// End JdbcRules.java
