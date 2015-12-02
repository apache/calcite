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
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
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
          new DeltaProjectTransposeRule(),
          new DeltaFilterTransposeRule(),
          new DeltaAggregateTransposeRule(),
          new DeltaSortTransposeRule(),
          new DeltaUnionTransposeRule(),
          new DeltaJoinTransposeRule(),
          new DeltaTableScanRule(),
          new DeltaTableScanToEmptyRule());

  /** Planner rule that pushes a {@link Delta} through a {@link Project}. */
  public static class DeltaProjectTransposeRule extends RelOptRule {
    private DeltaProjectTransposeRule() {
      super(
          operand(Delta.class,
              operand(Project.class, any())));
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
    private DeltaFilterTransposeRule() {
      super(
          operand(Delta.class,
              operand(Filter.class, any())));
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
    private DeltaAggregateTransposeRule() {
      super(
          operand(Delta.class,
              operand(Aggregate.class, any())));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      Util.discard(delta);
      final Aggregate aggregate = call.rel(1);
      final LogicalDelta newDelta =
          LogicalDelta.create(aggregate.getInput());
      final LogicalAggregate newAggregate =
          LogicalAggregate.create(newDelta, aggregate.indicator,
              aggregate.getGroupSet(), aggregate.groupSets,
              aggregate.getAggCallList());
      call.transformTo(newAggregate);
    }
  }

  /** Planner rule that pushes a {@link Delta} through an {@link Sort}. */
  public static class DeltaSortTransposeRule extends RelOptRule {
    private DeltaSortTransposeRule() {
      super(
          operand(Delta.class,
              operand(Sort.class, any())));
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
    private DeltaUnionTransposeRule() {
      super(
          operand(Delta.class,
              operand(Union.class, any())));
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
    private DeltaTableScanRule() {
      super(
          operand(Delta.class,
              operand(TableScan.class, none())));
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
                relOptTable.getRowType(), table1);
        final LogicalTableScan newScan =
            LogicalTableScan.create(cluster, relOptTable2);
        call.transformTo(newScan);
      }
    }
  }

  /**
   * Planner rule that converts {@link Delta} over a {@link TableScan} of
   * a table other than {@link org.apache.calcite.schema.StreamableTable} to Empty.
   */
  public static class DeltaTableScanToEmptyRule extends RelOptRule {
    private DeltaTableScanToEmptyRule() {
      super(
          operand(Delta.class,
              operand(TableScan.class, none())));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      final TableScan scan = call.rel(1);
      final RelOptCluster cluster = delta.getCluster();
      final RelOptTable relOptTable = scan.getTable();
      final StreamableTable streamableTable =
          relOptTable.unwrap(StreamableTable.class);
      if (streamableTable == null) {
        call.transformTo(LogicalValues.createEmpty(cluster, delta.getRowType()));
      }
    }
  }


  /**
   * Planner rule that pushes a {@link Delta} through a {@link Join}.
   *
   * Product rule [1] is applied to implement the transpose:
   * stream(x join y)" = "x join stream(y) union all stream(x) join y
   *
   * [1] https://en.wikipedia.org/wiki/Product_rule
   */
  public static class DeltaJoinTransposeRule extends RelOptRule {

    public DeltaJoinTransposeRule() {
      super(
          operand(Delta.class,
              operand(Join.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      final Join join = call.rel(1);
      final RelOptCluster cluster = delta.getCluster();
      RelNode left = join.getLeft();
      RelNode right = join.getRight();

      final LogicalDelta rightWithDelta = LogicalDelta.create(right);
      final LogicalJoin joinL = LogicalJoin.create(left, rightWithDelta, join.getCondition(),
          join.getJoinType(), join.getVariablesStopped(), join.isSemiJoinDone(),
          ImmutableList.copyOf(join.getSystemFieldList()));

      final LogicalDelta leftWithDelta = LogicalDelta.create(left);
      final LogicalJoin joinR = LogicalJoin.create(leftWithDelta, right, join.getCondition(),
          join.getJoinType(), join.getVariablesStopped(), join.isSemiJoinDone(),
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
