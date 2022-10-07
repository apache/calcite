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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
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
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Rules and relational operators for streaming relational expressions.
 */
public class StreamRules {
  private StreamRules() {}

  public static final ImmutableList<RelOptRule> RULES =
      ImmutableList.of(
          DeltaProjectTransposeRule.DeltaProjectTransposeRuleConfig.DEFAULT.toRule(),
          DeltaFilterTransposeRule.DeltaFilterTransposeRuleConfig.DEFAULT.toRule(),
          DeltaAggregateTransposeRule.DeltaAggregateTransposeRuleConfig.DEFAULT.toRule(),
          DeltaSortTransposeRule.DeltaSortTransposeRuleConfig.DEFAULT.toRule(),
          DeltaUnionTransposeRule.DeltaUnionTransposeRuleConfig.DEFAULT.toRule(),
          DeltaJoinTransposeRule.DeltaJoinTransposeRuleConfig.DEFAULT.toRule(),
          DeltaTableScanRule.DeltaTableScanRuleConfig.DEFAULT.toRule(),
          DeltaTableScanToEmptyRule.DeltaTableScanToEmptyRuleConfig.DEFAULT.toRule());

  /** Planner rule that pushes a {@link Delta} through a {@link Project}. */
  public static class DeltaProjectTransposeRule
      extends RelRule<DeltaProjectTransposeRule.DeltaProjectTransposeRuleConfig>
      implements TransformationRule {
    /** Creates a DeltaProjectTransposeRule. */
    protected DeltaProjectTransposeRule(DeltaProjectTransposeRuleConfig config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      Util.discard(delta);
      final Project project = call.rel(1);
      final LogicalDelta newDelta = LogicalDelta.create(project.getInput());
      final LogicalProject newProject =
          LogicalProject.create(newDelta,
              project.getHints(),
              project.getProjects(),
              project.getRowType().getFieldNames(),
              project.getVariablesSet());
      call.transformTo(newProject);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface DeltaProjectTransposeRuleConfig extends RelRule.Config {
      DeltaProjectTransposeRuleConfig DEFAULT = ImmutableDeltaProjectTransposeRuleConfig.of()
          .withOperandSupplier(b0 ->
              b0.operand(Delta.class).oneInput(b1 ->
                  b1.operand(Project.class).anyInputs()));

      @Override default DeltaProjectTransposeRule toRule() {
        return new DeltaProjectTransposeRule(this);
      }

      /** Defines an operand tree for the given classes. */
      default Config withOperandFor(Class<? extends RelNode> relClass) {
        return withOperandSupplier(b -> b.operand(relClass).anyInputs())
            .as(Config.class);
      }
    }
  }

  /** Planner rule that pushes a {@link Delta} through a {@link Filter}. */
  public static class DeltaFilterTransposeRule
      extends RelRule<DeltaFilterTransposeRule.DeltaFilterTransposeRuleConfig>
      implements TransformationRule {
    /** Creates a DeltaFilterTransposeRule. */
    protected DeltaFilterTransposeRule(DeltaFilterTransposeRuleConfig config) {
      super(config);
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

    /** Rule configuration. */
    @Value.Immutable
    public interface DeltaFilterTransposeRuleConfig extends RelRule.Config {
      DeltaFilterTransposeRuleConfig DEFAULT = ImmutableDeltaFilterTransposeRuleConfig.of()
          .withOperandSupplier(b0 ->
              b0.operand(Delta.class).oneInput(b1 ->
                  b1.operand(Filter.class).anyInputs()));

      @Override default DeltaFilterTransposeRule toRule() {
        return new DeltaFilterTransposeRule(this);
      }

      /** Defines an operand tree for the given classes. */
      default Config withOperandFor(Class<? extends RelNode> relClass) {
        return withOperandSupplier(b -> b.operand(relClass).anyInputs())
            .as(Config.class);
      }
    }
  }

  /** Planner rule that pushes a {@link Delta} through an {@link Aggregate}. */
  public static class DeltaAggregateTransposeRule
      extends RelRule<DeltaAggregateTransposeRule.DeltaAggregateTransposeRuleConfig>
      implements TransformationRule {
    /** Creates a DeltaAggregateTransposeRule. */
    protected DeltaAggregateTransposeRule(DeltaAggregateTransposeRuleConfig config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      Util.discard(delta);
      final Aggregate aggregate = call.rel(1);
      final LogicalDelta newDelta =
          LogicalDelta.create(aggregate.getInput());
      final LogicalAggregate newAggregate =
          LogicalAggregate.create(newDelta, aggregate.getHints(), aggregate.getGroupSet(),
              aggregate.groupSets, aggregate.getAggCallList());
      call.transformTo(newAggregate);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface DeltaAggregateTransposeRuleConfig extends RelRule.Config {
      DeltaAggregateTransposeRuleConfig DEFAULT = ImmutableDeltaAggregateTransposeRuleConfig.of()
          .withOperandSupplier(b0 ->
              b0.operand(Delta.class).oneInput(b1 ->
                  b1.operand(Aggregate.class)
                      .predicate(Aggregate::isSimple).anyInputs()));

      @Override default DeltaAggregateTransposeRule toRule() {
        return new DeltaAggregateTransposeRule(this);
      }

      /** Defines an operand tree for the given classes. */
      default Config withOperandFor(Class<? extends RelNode> relClass) {
        return withOperandSupplier(b -> b.operand(relClass).anyInputs())
            .as(Config.class);
      }
    }
  }

  /** Planner rule that pushes a {@link Delta} through an {@link Sort}. */
  public static class DeltaSortTransposeRule
      extends RelRule<DeltaSortTransposeRule.DeltaSortTransposeRuleConfig>
      implements TransformationRule {
    /** Creates a DeltaSortTransposeRule. */
    protected DeltaSortTransposeRule(DeltaSortTransposeRuleConfig config) {
      super(config);
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

    /** Rule configuration. */
    @Value.Immutable
    public interface DeltaSortTransposeRuleConfig extends RelRule.Config {
      DeltaSortTransposeRuleConfig DEFAULT = ImmutableDeltaSortTransposeRuleConfig.of()
          .withOperandSupplier(b0 ->
              b0.operand(Delta.class).oneInput(b1 ->
                  b1.operand(Sort.class).anyInputs()));

      @Override default DeltaSortTransposeRule toRule() {
        return new DeltaSortTransposeRule(this);
      }
    }
  }

  /** Planner rule that pushes a {@link Delta} through an {@link Union}. */
  public static class DeltaUnionTransposeRule
      extends RelRule<DeltaUnionTransposeRule.DeltaUnionTransposeRuleConfig>
      implements TransformationRule {
    /** Creates a DeltaUnionTransposeRule. */
    protected DeltaUnionTransposeRule(DeltaUnionTransposeRuleConfig config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      Util.discard(delta);
      final Union union = call.rel(1);
      final List<RelNode> newInputs = new ArrayList<>();
      for (RelNode input : union.getInputs()) {
        final LogicalDelta newDelta =
            LogicalDelta.create(input);
        newInputs.add(newDelta);
      }
      final LogicalUnion newUnion = LogicalUnion.create(newInputs, union.all);
      call.transformTo(newUnion);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface DeltaUnionTransposeRuleConfig extends RelRule.Config {
      DeltaUnionTransposeRuleConfig DEFAULT = ImmutableDeltaUnionTransposeRuleConfig.of()
          .withOperandSupplier(b0 ->
              b0.operand(Delta.class).oneInput(b1 ->
                  b1.operand(Union.class).anyInputs()));

      @Override default DeltaUnionTransposeRule toRule() {
        return new DeltaUnionTransposeRule(this);
      }
    }
  }

  /** Planner rule that pushes a {@link Delta} into a {@link TableScan} of a
   * {@link org.apache.calcite.schema.StreamableTable}.
   *
   * <p>Very likely, the stream was only represented as a table for uniformity
   * with the other relations in the system. The Delta disappears and the stream
   * can be implemented directly. */
  public static class DeltaTableScanRule
      extends RelRule<DeltaTableScanRule.DeltaTableScanRuleConfig>
      implements TransformationRule {
    /** Creates a DeltaTableScanRule. */
    protected DeltaTableScanRule(DeltaTableScanRuleConfig config) {
      super(config);
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
            LogicalTableScan.create(cluster, relOptTable2, scan.getHints());
        call.transformTo(newScan);
      }
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface DeltaTableScanRuleConfig extends RelRule.Config {
      DeltaTableScanRuleConfig DEFAULT = ImmutableDeltaTableScanRuleConfig.of()
          .withOperandSupplier(b0 ->
              b0.operand(Delta.class).oneInput(b1 ->
                  b1.operand(TableScan.class).anyInputs()));

      @Override default DeltaTableScanRule toRule() {
        return new DeltaTableScanRule(this);
      }
    }
  }

  /**
   * Planner rule that converts {@link Delta} over a {@link TableScan} of
   * a table other than {@link org.apache.calcite.schema.StreamableTable} to
   * an empty {@link Values}.
   */
  public static class DeltaTableScanToEmptyRule
      extends RelRule<DeltaTableScanToEmptyRule.DeltaTableScanToEmptyRuleConfig>
      implements TransformationRule {
    /** Creates a DeltaTableScanToEmptyRule. */
    protected DeltaTableScanToEmptyRule(DeltaTableScanToEmptyRuleConfig config) {
      super(config);
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

    /** Rule configuration. */
    @Value.Immutable
    public interface DeltaTableScanToEmptyRuleConfig extends RelRule.Config {
      ImmutableDeltaTableScanToEmptyRuleConfig DEFAULT =
          ImmutableDeltaTableScanToEmptyRuleConfig.of()
              .withOperandSupplier(b0 ->
                  b0.operand(Delta.class).oneInput(b1 ->
                      b1.operand(TableScan.class).anyInputs()));

      @Override default DeltaTableScanToEmptyRule toRule() {
        return new DeltaTableScanToEmptyRule(this);
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
  public static class DeltaJoinTransposeRule
      extends RelRule<DeltaJoinTransposeRule.DeltaJoinTransposeRuleConfig>
      implements TransformationRule {
    /** Creates a DeltaJoinTransposeRule. */
    protected DeltaJoinTransposeRule(DeltaJoinTransposeRuleConfig config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public DeltaJoinTransposeRule() {
      this(DeltaJoinTransposeRuleConfig.DEFAULT.toRule().config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      Util.discard(delta);
      final Join join = call.rel(1);
      final RelNode left = join.getLeft();
      final RelNode right = join.getRight();

      final LogicalDelta rightWithDelta = LogicalDelta.create(right);
      final LogicalJoin joinL = LogicalJoin.create(left,
          rightWithDelta,
          join.getHints(),
          join.getCondition(),
          join.getVariablesSet(),
          join.getJoinType(),
          join.isSemiJoinDone(),
          ImmutableList.copyOf(join.getSystemFieldList()));

      final LogicalDelta leftWithDelta = LogicalDelta.create(left);
      final LogicalJoin joinR = LogicalJoin.create(leftWithDelta,
          right,
          join.getHints(),
          join.getCondition(),
          join.getVariablesSet(),
          join.getJoinType(),
          join.isSemiJoinDone(),
          ImmutableList.copyOf(join.getSystemFieldList()));

      List<RelNode> inputsToUnion = new ArrayList<>();
      inputsToUnion.add(joinL);
      inputsToUnion.add(joinR);

      final LogicalUnion newNode = LogicalUnion.create(inputsToUnion, true);
      call.transformTo(newNode);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface DeltaJoinTransposeRuleConfig extends RelRule.Config {
      DeltaJoinTransposeRuleConfig DEFAULT = ImmutableDeltaJoinTransposeRuleConfig.of()
          .withOperandSupplier(b0 ->
              b0.operand(Delta.class).oneInput(b1 ->
                  b1.operand(Join.class).anyInputs()));

      @Override default DeltaJoinTransposeRule toRule() {
        return new DeltaJoinTransposeRule(this);
      }
    }
  }
}
