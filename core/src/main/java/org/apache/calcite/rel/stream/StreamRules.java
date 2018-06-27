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
package org.apache.calcite.rel.stream;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Rules and relational operators for streaming relational expressions.
 */
public class StreamRules {
  private StreamRules() {}

  public static final ImmutableList<RelOptRule> RULES =
      ImmutableList.of(
          new DeltaProjectTransposeRule(RelFactories.LOGICAL_BUILDER),
          new DeltaFilterTransposeRule(RelFactories.LOGICAL_BUILDER),
          new DeltaAggregateTransposeRule(RelFactories.LOGICAL_BUILDER),
          new DeltaSortTransposeRule(RelFactories.LOGICAL_BUILDER),
          new DeltaUnionTransposeRule(RelFactories.LOGICAL_BUILDER),
          new DeltaJoinTransposeRule(RelFactories.LOGICAL_BUILDER),
          new DeltaTableScanRule(RelFactories.LOGICAL_BUILDER),
          new DeltaTableScanToEmptyRule(RelFactories.LOGICAL_BUILDER));

  /** Planner rule that pushes a {@link Delta} through a {@link Project}. */
  public static class DeltaProjectTransposeRule extends RelOptRule {

    /**
     * Creates a DeltaProjectTransposeRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public DeltaProjectTransposeRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Delta.class,
              operand(Project.class, any())),
          relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      Util.discard(delta);
      final Project project = call.rel(1);
      final LogicalDelta newDelta = LogicalDelta.create(project.getInput());
      final LogicalProject newProject =
          LogicalProject.create(newDelta, project.getProjects(),
              project.getRowType().getFieldNames());
      call.transformTo(newProject);
    }
  }

  /** Planner rule that pushes a {@link Delta} through a {@link Filter}. */
  public static class DeltaFilterTransposeRule extends RelOptRule {

    /**
     * Creates a DeltaFilterTransposeRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public DeltaFilterTransposeRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Delta.class,
              operand(Filter.class, any())),
          relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      Util.discard(delta);
      final Filter filter = call.rel(1);
      final LogicalDelta newDelta = LogicalDelta.create(filter.getInput());
      final LogicalFilter newFilter =
          LogicalFilter.create(newDelta, filter.getCondition());
      call.transformTo(newFilter);
    }
  }

  /** Planner rule that pushes a {@link Delta} through an {@link Aggregate}. */
  public static class DeltaAggregateTransposeRule extends RelOptRule {

    /**
     * Creates a DeltaAggregateTransposeRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public DeltaAggregateTransposeRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Delta.class,
              operand(Aggregate.class, null, Aggregate.NO_INDICATOR, any())),
          relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      Util.discard(delta);
      final Aggregate aggregate = call.rel(1);
      final LogicalDelta newDelta =
          LogicalDelta.create(aggregate.getInput());
      final LogicalAggregate newAggregate =
          LogicalAggregate.create(newDelta, aggregate.getGroupSet(),
              aggregate.groupSets, aggregate.getAggCallList());
      call.transformTo(newAggregate);
    }
  }

  /** Planner rule that pushes a {@link Delta} through an {@link Sort}. */
  public static class DeltaSortTransposeRule extends RelOptRule {

    /**
     * Creates a DeltaSortTransposeRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public DeltaSortTransposeRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Delta.class,
              operand(Sort.class, any())),
          relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      Util.discard(delta);
      final Sort sort = call.rel(1);
      final LogicalDelta newDelta =
          LogicalDelta.create(sort.getInput());
      final LogicalSort newSort =
          LogicalSort.create(newDelta, sort.collation, sort.offset, sort.fetch);
      call.transformTo(newSort);
    }
  }

  /** Planner rule that pushes a {@link Delta} through an {@link Union}. */
  public static class DeltaUnionTransposeRule extends RelOptRule {

    /**
     * Creates a DeltaUnionTransposeRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public DeltaUnionTransposeRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Delta.class,
              operand(Union.class, any())),
          relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      Util.discard(delta);
      final Union union = call.rel(1);
      final List<RelNode> newInputs = Lists.newArrayList();
      for (RelNode input : union.getInputs()) {
        final LogicalDelta newDelta =
            LogicalDelta.create(input);
        newInputs.add(newDelta);
      }
      final LogicalUnion newUnion = LogicalUnion.create(newInputs, union.all);
      call.transformTo(newUnion);
    }
  }

  /** Planner rule that pushes a {@link Delta} into a {@link TableScan} of a
   * {@link org.apache.calcite.schema.StreamableTable}.
   *
   * <p>Very likely, the stream was only represented as a table for uniformity
   * with the other relations in the system. The Delta disappears and the stream
   * can be implemented directly. */
  public static class DeltaTableScanRule extends RelOptRule {

    /**
     * Creates a DeltaTableScanRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public DeltaTableScanRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Delta.class,
              operand(TableScan.class, none())),
          relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      final TableScan scan = call.rel(1);
      final RelOptCluster cluster = delta.getCluster();
      final RelOptTable relOptTable = scan.getTable();
      final StreamableTable streamableTable =
          relOptTable.unwrap(StreamableTable.class);
      if (streamableTable != null) {
        final Table table1 = streamableTable.stream();
        final RelOptTable relOptTable2 =
            RelOptTableImpl.create(relOptTable.getRelOptSchema(),
                relOptTable.getRowType(), table1,
                ImmutableList.<String>builder()
                    .addAll(relOptTable.getQualifiedName())
                    .add("(STREAM)").build());
        final LogicalTableScan newScan =
            LogicalTableScan.create(cluster, relOptTable2);
        call.transformTo(newScan);
      }
    }
  }

  /**
   * Planner rule that converts {@link Delta} over a {@link TableScan} of
   * a table other than {@link org.apache.calcite.schema.StreamableTable} to
   * an empty {@link Values}.
   */
  public static class DeltaTableScanToEmptyRule extends RelOptRule {

    /**
     * Creates a DeltaTableScanToEmptyRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public DeltaTableScanToEmptyRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Delta.class,
              operand(TableScan.class, none())),
          relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      final TableScan scan = call.rel(1);
      final RelOptTable relOptTable = scan.getTable();
      final StreamableTable streamableTable =
          relOptTable.unwrap(StreamableTable.class);
      final RelBuilder builder = call.builder();
      if (streamableTable == null) {
        call.transformTo(builder.values(delta.getRowType()).build());
      }
    }
  }

  /**
   * Planner rule that pushes a {@link Delta} through a {@link Join}.
   *
   * <p>We apply something analogous to the
   * <a href="https://en.wikipedia.org/wiki/Product_rule">product rule of
   * differential calculus</a> to implement the transpose:
   *
   * <blockquote><code>stream(x join y) &rarr;
   * x join stream(y) union all stream(x) join y</code></blockquote>
   */
  public static class DeltaJoinTransposeRule extends RelOptRule {

    @Deprecated // to be removed before 2.0
    public DeltaJoinTransposeRule() {
      this(RelFactories.LOGICAL_BUILDER);
    }

    /**
     * Creates a DeltaJoinTransposeRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public DeltaJoinTransposeRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Delta.class,
              operand(Join.class, any())),
          relBuilderFactory, null);
    }

    public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      Util.discard(delta);
      final Join join = call.rel(1);
      final RelNode left = join.getLeft();
      final RelNode right = join.getRight();

      final LogicalDelta rightWithDelta = LogicalDelta.create(right);
      final LogicalJoin joinL = LogicalJoin.create(left, rightWithDelta,
          join.getCondition(), join.getVariablesSet(), join.getJoinType(),
          join.isSemiJoinDone(),
          ImmutableList.copyOf(join.getSystemFieldList()));

      final LogicalDelta leftWithDelta = LogicalDelta.create(left);
      final LogicalJoin joinR = LogicalJoin.create(leftWithDelta, right,
          join.getCondition(), join.getVariablesSet(), join.getJoinType(),
          join.isSemiJoinDone(),
          ImmutableList.copyOf(join.getSystemFieldList()));

      List<RelNode> inputsToUnion = Lists.newArrayList();
      inputsToUnion.add(joinL);
      inputsToUnion.add(joinR);

      final LogicalUnion newNode = LogicalUnion.create(inputsToUnion, true);
      call.transformTo(newNode);
    }
  }
}

// End StreamRules.java
