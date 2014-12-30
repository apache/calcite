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
package org.apache.calcite.interpreter;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.AggImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

/**
 * Utilities pertaining to {@link BindableRel} and {@link BindableConvention}.
 */
public class Bindables {
  private Bindables() {}

  public static final RelOptRule BINDABLE_TABLE_RULE =
      new BindableTableScanRule();

  public static final RelOptRule BINDABLE_FILTER_RULE =
      new BindableFilterRule();

  public static final RelOptRule BINDABLE_PROJECT_RULE =
      new BindableProjectRule();

  public static final RelOptRule BINDABLE_SORT_RULE =
      new BindableSortRule();

  public static final RelOptRule BINDABLE_JOIN_RULE =
      new BindableJoinRule();

  public static final RelOptRule BINDABLE_UNION_RULE =
      new BindableUnionRule();

  public static final RelOptRule BINDABLE_VALUES_RULE =
      new BindableValuesRule();

  public static final RelOptRule BINDABLE_AGGREGATE_RULE =
      new BindableAggregateRule();

  public static final RelOptRule BINDABLE_WINDOW_RULE =
      new BindableWindowRule();

  /** All rules that convert logical relational expression to bindable. */
  public static final ImmutableList<RelOptRule> RULES =
      ImmutableList.of(
          NoneToBindableConverterRule.INSTANCE,
          BINDABLE_TABLE_RULE,
          BINDABLE_FILTER_RULE,
          BINDABLE_PROJECT_RULE,
          BINDABLE_SORT_RULE,
          BINDABLE_JOIN_RULE,
          BINDABLE_UNION_RULE,
          BINDABLE_VALUES_RULE,
          BINDABLE_AGGREGATE_RULE,
          BINDABLE_WINDOW_RULE);

  /** Helper method that converts a bindable relational expression into a
   * record iterator.
   *
   * <p>Any bindable can be compiled; if its input is also bindable, it becomes
   * part of the same compilation unit.
   */
  private static Enumerable<Object[]> help(DataContext dataContext,
      BindableRel rel) {
    return new Interpreter(dataContext, rel);
  }

  /** Rule that converts a {@link ScannableTable} to bindable convention. */
  private static class BindableTableScanRule extends RelOptRule {
    public BindableTableScanRule() {
      super(operand(TableScan.class, none()));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final TableScan scan = call.rel(0);
      call.transformTo(
          new BindableTableScan(scan.getCluster(),
              scan.getTraitSet().replace(BindableConvention.INSTANCE),
              scan.getTable()));
    }
  }

  /** Scan of a table that implements {@link ScannableTable} and therefore can
   * be converted into an {@link Enumerable}. */
  private static class BindableTableScan
      extends TableScan implements BindableRel {
    BindableTableScan(RelOptCluster cluster, RelTraitSet traits,
        RelOptTable table) {
      super(cluster, traits, table);
    }

    public Class<Object[]> getElementType() {
      return Object[].class;
    }

    public Enumerable<Object[]> bind(DataContext dataContext) {
      return table.unwrap(ScannableTable.class).scan(dataContext);
    }

    public Node implement(InterpreterImplementor implementor) {
      throw new UnsupportedOperationException(); // TODO:
    }
  }

  /** Rule that converts a {@link Filter} to bindable convention. */
  private static class BindableFilterRule extends ConverterRule {
    private BindableFilterRule() {
      super(LogicalFilter.class, RelOptUtil.FILTER_PREDICATE, Convention.NONE,
          BindableConvention.INSTANCE, "BindableFilterRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      return new BindableFilter(rel.getCluster(),
          rel.getTraitSet().replace(BindableConvention.INSTANCE),
          convert(filter.getInput(),
              filter.getInput().getTraitSet()
                  .replace(BindableConvention.INSTANCE)),
          filter.getCondition());
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Filter}
   * in bindable convention. */
  public static class BindableFilter extends Filter implements BindableRel {
    public BindableFilter(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode child, RexNode condition) {
      super(cluster, traitSet, child, condition);
      assert getConvention() instanceof BindableConvention;
    }

    public BindableFilter copy(RelTraitSet traitSet, RelNode input,
        RexNode condition) {
      return new BindableFilter(getCluster(), traitSet, input, condition);
    }

    public Class<Object[]> getElementType() {
      return Object[].class;
    }

    public Enumerable<Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    public Node implement(InterpreterImplementor implementor) {
      return new FilterNode(implementor.interpreter, this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to a {@link BindableProject}.
   */
  private static class BindableProjectRule extends ConverterRule {
    BindableProjectRule() {
      super(LogicalProject.class, RelOptUtil.PROJECT_PREDICATE, Convention.NONE,
          BindableConvention.INSTANCE, "BindableProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      return new BindableProject(rel.getCluster(),
          rel.getTraitSet().replace(BindableConvention.INSTANCE),
          convert(project.getInput(),
              project.getInput().getTraitSet()
                  .replace(BindableConvention.INSTANCE)),
          project.getProjects(),
          project.getRowType(),
          Project.Flags.BOXED);
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Project} in
   * bindable calling convention. */
  public static class BindableProject extends Project implements BindableRel {
    public BindableProject(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode child, List<? extends RexNode> exps, RelDataType rowType,
        int flags) {
      super(cluster, traitSet, child, exps, rowType, flags);
      assert getConvention() instanceof BindableConvention;
    }

    public BindableProject copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> exps, RelDataType rowType) {
      return new BindableProject(getCluster(), traitSet, input, exps, rowType,
          flags);
    }

    public Class<Object[]> getElementType() {
      return Object[].class;
    }

    public Enumerable<Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    public Node implement(InterpreterImplementor implementor) {
      return new ProjectNode(implementor.interpreter, this);
    }
  }

  /**
   * Rule to convert an {@link org.apache.calcite.rel.core.Sort} to a
   * {@link org.apache.calcite.interpreter.Bindables.BindableSort}.
   */
  private static class BindableSortRule extends ConverterRule {
    BindableSortRule() {
      super(Sort.class, Convention.NONE, BindableConvention.INSTANCE,
          "BindableSortRule");
    }

    public RelNode convert(RelNode rel) {
      final Sort sort = (Sort) rel;
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(BindableConvention.INSTANCE);
      final RelNode input = sort.getInput();
      return new BindableSort(rel.getCluster(), traitSet,
          convert(input,
              input.getTraitSet().replace(BindableConvention.INSTANCE)),
          sort.getCollation(), sort.offset, sort.fetch);
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Sort}
   * bindable calling convention. */
  public static class BindableSort extends Sort implements BindableRel {
    public BindableSort(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
      super(cluster, traitSet, child, collation, offset, fetch);
      assert getConvention() instanceof BindableConvention;
      assert getConvention() == child.getConvention();
    }

    @Override public BindableSort copy(RelTraitSet traitSet, RelNode newInput,
        RelCollation newCollation, RexNode offset, RexNode fetch) {
      return new BindableSort(getCluster(), traitSet, newInput, newCollation,
          offset, fetch);
    }

    public Class<Object[]> getElementType() {
      return Object[].class;
    }

    public Enumerable<Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    public Node implement(InterpreterImplementor implementor) {
      return new SortNode(implementor.interpreter, this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalJoin}
   * to a {@link BindableJoin}.
   */
  private static class BindableJoinRule extends ConverterRule {
    BindableJoinRule() {
      super(LogicalJoin.class, Convention.NONE, BindableConvention.INSTANCE,
          "BindableJoinRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalJoin join = (LogicalJoin) rel;
      final BindableConvention out = BindableConvention.INSTANCE;
      final RelTraitSet traitSet = join.getTraitSet().replace(out);
      return new BindableJoin(rel.getCluster(), traitSet,
          convert(join.getLeft(),
              join.getLeft().getTraitSet()
                  .replace(BindableConvention.INSTANCE)),
          convert(join.getRight(),
              join.getRight().getTraitSet()
                  .replace(BindableConvention.INSTANCE)),
          join.getCondition(), join.getJoinType(), join.getVariablesStopped());
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Join} in
   * bindable calling convention. */
  public static class BindableJoin extends Join implements BindableRel {
    protected BindableJoin(RelOptCluster cluster, RelTraitSet traits,
        RelNode left, RelNode right, RexNode condition, JoinRelType joinType,
        Set<String> variablesStopped) {
      super(cluster, traits, left, right, condition, joinType,
          variablesStopped);
    }

    public BindableJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
        RelNode left, RelNode right, JoinRelType joinType,
        boolean semiJoinDone) {
      return new BindableJoin(getCluster(), traitSet, left, right,
          conditionExpr, joinType, variablesStopped);
    }

    public Class<Object[]> getElementType() {
      return Object[].class;
    }

    public Enumerable<Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    public Node implement(InterpreterImplementor implementor) {
      return new JoinNode(implementor.interpreter, this);
    }
  }

  /**
   * Rule to convert an {@link org.apache.calcite.rel.logical.LogicalUnion}
   * to a {@link BindableUnion}.
   */
  private static class BindableUnionRule extends ConverterRule {
    BindableUnionRule() {
      super(LogicalUnion.class, Convention.NONE, BindableConvention.INSTANCE,
          "BindableUnionRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalUnion union = (LogicalUnion) rel;
      final BindableConvention out = BindableConvention.INSTANCE;
      final RelTraitSet traitSet = union.getTraitSet().replace(out);
      return new BindableUnion(rel.getCluster(), traitSet,
          convertList(union.getInputs(), out), union.all);
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Union} in
   * bindable calling convention. */
  public static class BindableUnion extends Union implements BindableRel {
    public BindableUnion(RelOptCluster cluster, RelTraitSet traitSet,
        List<RelNode> inputs, boolean all) {
      super(cluster, traitSet, inputs, all);
    }

    public BindableUnion copy(RelTraitSet traitSet, List<RelNode> inputs,
        boolean all) {
      return new BindableUnion(getCluster(), traitSet, inputs, all);
    }

    public Class<Object[]> getElementType() {
      return Object[].class;
    }

    public Enumerable<Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    public Node implement(InterpreterImplementor implementor) {
      return new UnionNode(implementor.interpreter, this);
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Values}
   * in bindable calling convention. */
  public static class BindableValues extends Values implements BindableRel {
    BindableValues(RelOptCluster cluster, RelDataType rowType,
        ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
      super(cluster, rowType, tuples, traitSet);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();
      return new BindableValues(getCluster(), rowType, tuples, traitSet);
    }

    public Class<Object[]> getElementType() {
      return Object[].class;
    }

    public Enumerable<Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    public Node implement(InterpreterImplementor implementor) {
      return new ValuesNode(implementor.interpreter, this);
    }
  }

  /** Rule that converts a {@link Values} to bindable convention. */
  private static class BindableValuesRule extends ConverterRule {
    BindableValuesRule() {
      super(LogicalValues.class, Convention.NONE, BindableConvention.INSTANCE,
          "BindableValuesRule");
    }

    @Override public RelNode convert(RelNode rel) {
      LogicalValues values = (LogicalValues) rel;
      return new BindableValues(values.getCluster(), values.getRowType(),
          values.getTuples(),
          values.getTraitSet().replace(BindableConvention.INSTANCE));
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Aggregate}
   * in bindable calling convention. */
  public static class BindableAggregate extends Aggregate
      implements BindableRel {
    public BindableAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        boolean indicator,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls)
        throws InvalidRelException {
      super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
      assert getConvention() instanceof BindableConvention;

      for (AggregateCall aggCall : aggCalls) {
        if (aggCall.isDistinct()) {
          throw new InvalidRelException(
              "distinct aggregation not supported");
        }
        AggImplementor implementor2 =
            RexImpTable.INSTANCE.get(aggCall.getAggregation(), false);
        if (implementor2 == null) {
          throw new InvalidRelException(
              "aggregation " + aggCall.getAggregation() + " not supported");
        }
      }
    }

    @Override public BindableAggregate copy(RelTraitSet traitSet, RelNode input,
        boolean indicator, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      try {
        return new BindableAggregate(getCluster(), traitSet, input, indicator,
            groupSet, groupSets, aggCalls);
      } catch (InvalidRelException e) {
        // Semantic error not possible. Must be a bug. Convert to
        // internal error.
        throw new AssertionError(e);
      }
    }

    public Class<Object[]> getElementType() {
      return Object[].class;
    }

    public Enumerable<Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    public Node implement(InterpreterImplementor implementor) {
      return new AggregateNode(implementor.interpreter, this);
    }
  }

  /** Rule that converts an {@link Aggregate} to bindable convention. */
  private static class BindableAggregateRule extends ConverterRule {
    BindableAggregateRule() {
      super(LogicalAggregate.class, Convention.NONE,
          BindableConvention.INSTANCE, "BindableAggregateRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalAggregate agg = (LogicalAggregate) rel;
      final RelTraitSet traitSet =
          agg.getTraitSet().replace(BindableConvention.INSTANCE);
      try {
        return new BindableAggregate(rel.getCluster(), traitSet,
            convert(agg.getInput(), traitSet), agg.indicator, agg.getGroupSet(),
            agg.getGroupSets(), agg.getAggCallList());
      } catch (InvalidRelException e) {
        RelOptPlanner.LOGGER.fine(e.toString());
        return null;
      }
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Window}
   * in bindable convention. */
  public static class BindableWindow extends Window implements BindableRel {
    /** Creates an BindableWindowRel. */
    BindableWindow(RelOptCluster cluster, RelTraitSet traits, RelNode child,
        List<RexLiteral> constants, RelDataType rowType, List<Group> groups) {
      super(cluster, traits, child, constants, rowType, groups);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new BindableWindow(getCluster(), traitSet, sole(inputs),
          constants, rowType, groups);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner) {
      return super.computeSelfCost(planner)
          .multiplyBy(BindableConvention.COST_MULTIPLIER);
    }

    public Class<Object[]> getElementType() {
      return Object[].class;
    }

    public Enumerable<Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    public Node implement(InterpreterImplementor implementor) {
      return new WindowNode(implementor.interpreter, this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalWindow}
   * to a {@link BindableWindow}.
   */
  private static class BindableWindowRule extends ConverterRule {
    BindableWindowRule() {
      super(LogicalWindow.class, Convention.NONE, BindableConvention.INSTANCE,
          "BindableWindowRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalWindow winAgg = (LogicalWindow) rel;
      final RelTraitSet traitSet =
          winAgg.getTraitSet().replace(BindableConvention.INSTANCE);
      final RelNode child = winAgg.getInput();
      final RelNode convertedChild =
          convert(child,
              child.getTraitSet().replace(BindableConvention.INSTANCE));
      return new BindableWindow(rel.getCluster(), traitSet, convertedChild,
          winAgg.getConstants(), winAgg.getRowType(), winAgg.groups);
    }
  }
}

// End Bindables.java
