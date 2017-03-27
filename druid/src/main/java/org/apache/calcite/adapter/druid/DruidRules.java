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
package org.apache.calcite.adapter.druid;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.AggregateFilterTransposeRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectSortTransposeRule;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Rules and relational operators for {@link DruidQuery}.
 */
public class DruidRules {
  private DruidRules() {}

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public static final DruidFilterRule FILTER = new DruidFilterRule();
  public static final DruidProjectRule PROJECT = new DruidProjectRule();
  public static final DruidAggregateRule AGGREGATE = new DruidAggregateRule();
  public static final DruidAggregateProjectRule AGGREGATE_PROJECT =
      new DruidAggregateProjectRule();
  public static final DruidSortRule SORT = new DruidSortRule();
  public static final DruidSortProjectTransposeRule SORT_PROJECT_TRANSPOSE =
      new DruidSortProjectTransposeRule();
  public static final DruidProjectSortTransposeRule PROJECT_SORT_TRANSPOSE =
      new DruidProjectSortTransposeRule();
  public static final DruidProjectFilterTransposeRule PROJECT_FILTER_TRANSPOSE =
      new DruidProjectFilterTransposeRule();
  public static final DruidFilterProjectTransposeRule FILTER_PROJECT_TRANSPOSE =
      new DruidFilterProjectTransposeRule();
  public static final DruidAggregateFilterTransposeRule AGGREGATE_FILTER_TRANSPOSE =
      new DruidAggregateFilterTransposeRule();
  public static final DruidFilterAggregateTransposeRule FILTER_AGGREGATE_TRANSPOSE =
      new DruidFilterAggregateTransposeRule();

  public static final List<RelOptRule> RULES =
      ImmutableList.of(FILTER,
          PROJECT_FILTER_TRANSPOSE,
          // Disabled, per
          //   [CALCITE-1706] DruidAggregateFilterTransposeRule
          //   causes very fine-grained aggregations to be pushed to Druid
          // AGGREGATE_FILTER_TRANSPOSE,
          AGGREGATE_PROJECT,
          PROJECT,
          AGGREGATE,
          FILTER_AGGREGATE_TRANSPOSE,
          FILTER_PROJECT_TRANSPOSE,
          PROJECT_SORT_TRANSPOSE,
          SORT,
          SORT_PROJECT_TRANSPOSE);

  /** Predicate that returns whether Druid can not handle an aggregate. */
  private static final Predicate<Aggregate> BAD_AGG =
      new PredicateImpl<Aggregate>() {
        public boolean test(Aggregate aggregate) {
          final CalciteConnectionConfig config =
                  aggregate.getCluster().getPlanner().getContext()
                      .unwrap(CalciteConnectionConfig.class);
          for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            switch (aggregateCall.getAggregation().getKind()) {
            case COUNT:
            case SUM:
            case SUM0:
            case MIN:
            case MAX:
              final RelDataType type = aggregateCall.getType();
              final SqlTypeName sqlTypeName = type.getSqlTypeName();
              if (SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains(sqlTypeName)
                      || SqlTypeFamily.INTEGER.getTypeNames().contains(sqlTypeName)) {
                continue;
              } else if (SqlTypeFamily.EXACT_NUMERIC.getTypeNames().contains(sqlTypeName)) {
                // Decimal
                assert sqlTypeName == SqlTypeName.DECIMAL;
                if (type.getScale() == 0 || config.approximateDecimal()) {
                  // If scale is zero or we allow approximating decimal, we can proceed
                  continue;
                }
              }
              // Cannot handle this aggregate function
              return true;
            default:
              // Cannot handle this aggregate function
              return true;
            }
          }
          return false;
        }
      };

  /**
   * Rule to push a {@link org.apache.calcite.rel.core.Filter} into a {@link DruidQuery}.
   */
  private static class DruidFilterRule extends RelOptRule {
    private DruidFilterRule() {
      super(operand(Filter.class, operand(DruidQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final DruidQuery query = call.rel(1);
      final RelOptCluster cluster = filter.getCluster();
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      if (!DruidQuery.isValidSignature(query.signature() + 'f')
              || !query.isValidFilter(filter.getCondition())) {
        return;
      }
      // Timestamp
      int timestampFieldIdx = -1;
      for (int i = 0; i < query.getRowType().getFieldCount(); i++) {
        if (query.druidTable.timestampFieldName.equals(
                query.getRowType().getFieldList().get(i).getName())) {
          timestampFieldIdx = i;
          break;
        }
      }
      final RexExecutor executor =
          Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
      final RexSimplify simplify = new RexSimplify(rexBuilder, true, executor);
      final Pair<List<RexNode>, List<RexNode>> pair =
          splitFilters(rexBuilder, query,
              simplify.simplify(filter.getCondition()), timestampFieldIdx);
      if (pair == null) {
        // We can't push anything useful to Druid.
        return;
      }
      List<LocalInterval> intervals = null;
      if (!pair.left.isEmpty()) {
        intervals = DruidDateTimeUtils.createInterval(
            query.getRowType().getFieldList().get(timestampFieldIdx).getType(),
            RexUtil.composeConjunction(rexBuilder, pair.left, false));
        if (intervals == null) {
          // We can't push anything useful to Druid.
          return;
        }
      }
      DruidQuery newDruidQuery = query;
      if (!pair.right.isEmpty()) {
        final RelNode newFilter = filter.copy(filter.getTraitSet(), Util.last(query.rels),
            RexUtil.composeConjunction(rexBuilder, pair.right, false));
        newDruidQuery = DruidQuery.extendQuery(query, newFilter);
      }
      if (intervals != null) {
        newDruidQuery = DruidQuery.extendQuery(newDruidQuery, intervals);
      }
      call.transformTo(newDruidQuery);
    }

    /** Splits the filter condition in two groups: those that filter on the timestamp column
     * and those that filter on other fields. */
    private static Pair<List<RexNode>, List<RexNode>> splitFilters(final RexBuilder rexBuilder,
        final DruidQuery input, RexNode cond, final int timestampFieldIdx) {
      final List<RexNode> timeRangeNodes = new ArrayList<>();
      final List<RexNode> otherNodes = new ArrayList<>();
      List<RexNode> conjs = RelOptUtil.conjunctions(cond);
      if (conjs.isEmpty()) {
        // We do not transform
        return null;
      }
      // Number of columns with the dimensions and timestamp
      for (RexNode conj : conjs) {
        final RelOptUtil.InputReferencedVisitor visitor = new RelOptUtil.InputReferencedVisitor();
        conj.accept(visitor);
        if (visitor.inputPosReferenced.contains(timestampFieldIdx)) {
          if (visitor.inputPosReferenced.size() != 1) {
            // Complex predicate, transformation currently not supported
            return null;
          }
          timeRangeNodes.add(conj);
        } else {
          for (Integer i : visitor.inputPosReferenced) {
            if (input.druidTable.metricFieldNames.contains(
                    input.getRowType().getFieldList().get(i).getName())) {
              // Filter on metrics, not supported in Druid
              return null;
            }
          }
          otherNodes.add(conj);
        }
      }
      return Pair.of(timeRangeNodes, otherNodes);
    }
  }

  /**
   * Rule to push a {@link org.apache.calcite.rel.core.Project} into a {@link DruidQuery}.
   */
  private static class DruidProjectRule extends RelOptRule {
    private DruidProjectRule() {
      super(operand(Project.class, operand(DruidQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final DruidQuery query = call.rel(1);
      final RelOptCluster cluster = project.getCluster();
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      if (!DruidQuery.isValidSignature(query.signature() + 'p')) {
        return;
      }

      if (canProjectAll(project.getProjects())) {
        // All expressions can be pushed to Druid in their entirety.
        final RelNode newProject = project.copy(project.getTraitSet(),
                ImmutableList.of(Util.last(query.rels)));
        RelNode newNode = DruidQuery.extendQuery(query, newProject);
        call.transformTo(newNode);
        return;
      }
      final Pair<List<RexNode>, List<RexNode>> pair =
          splitProjects(rexBuilder, query, project.getProjects());
      if (pair == null) {
        // We can't push anything useful to Druid.
        return;
      }
      final List<RexNode> above = pair.left;
      final List<RexNode> below = pair.right;
      final RelDataTypeFactory.FieldInfoBuilder builder =
          cluster.getTypeFactory().builder();
      final RelNode input = Util.last(query.rels);
      for (RexNode e : below) {
        final String name;
        if (e instanceof RexInputRef) {
          name = input.getRowType().getFieldNames().get(((RexInputRef) e).getIndex());
        } else {
          name = null;
        }
        builder.add(name, e.getType());
      }
      final RelNode newProject = project.copy(project.getTraitSet(), input, below, builder.build());
      final DruidQuery newQuery = DruidQuery.extendQuery(query, newProject);
      final RelNode newProject2 = project.copy(project.getTraitSet(), newQuery, above,
              project.getRowType());
      call.transformTo(newProject2);
    }

    private static boolean canProjectAll(List<RexNode> nodes) {
      for (RexNode e : nodes) {
        if (!(e instanceof RexInputRef)) {
          return false;
        }
      }
      return true;
    }

    private static Pair<List<RexNode>, List<RexNode>> splitProjects(final RexBuilder rexBuilder,
            final RelNode input, List<RexNode> nodes) {
      final RelOptUtil.InputReferencedVisitor visitor = new RelOptUtil.InputReferencedVisitor();
      for (RexNode node : nodes) {
        node.accept(visitor);
      }
      if (visitor.inputPosReferenced.size() == input.getRowType().getFieldCount()) {
        // All inputs are referenced
        return null;
      }
      final List<RexNode> belowNodes = new ArrayList<>();
      final List<RelDataType> belowTypes = new ArrayList<>();
      final List<Integer> positions = Lists.newArrayList(visitor.inputPosReferenced);
      for (int i : positions) {
        final RexNode node = rexBuilder.makeInputRef(input, i);
        belowNodes.add(node);
        belowTypes.add(node.getType());
      }
      final List<RexNode> aboveNodes = new ArrayList<>();
      for (RexNode node : nodes) {
        aboveNodes.add(
          node.accept(
            new RexShuttle() {
              @Override public RexNode visitInputRef(RexInputRef ref) {
                final int index = positions.indexOf(ref.getIndex());
                return rexBuilder.makeInputRef(belowTypes.get(index), index);
              }
            }));
      }
      return Pair.of(aboveNodes, belowNodes);
    }
  }

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Aggregate} into a {@link DruidQuery}.
   */
  private static class DruidAggregateRule extends RelOptRule {
    private DruidAggregateRule() {
      super(operand(Aggregate.class, operand(DruidQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
      final Aggregate aggregate = call.rel(0);
      final DruidQuery query = call.rel(1);
      if (!DruidQuery.isValidSignature(query.signature() + 'a')) {
        return;
      }
      if (aggregate.indicator
              || aggregate.getGroupSets().size() != 1
              || BAD_AGG.apply(aggregate)
              || !validAggregate(aggregate, query)) {
        return;
      }
      final RelNode newAggregate = aggregate.copy(aggregate.getTraitSet(),
              ImmutableList.of(Util.last(query.rels)));
      call.transformTo(DruidQuery.extendQuery(query, newAggregate));
    }

    /* Check whether agg functions reference timestamp */
    private static boolean validAggregate(Aggregate aggregate, DruidQuery query) {
      ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        builder.addAll(aggCall.getArgList());
      }
      if (checkAggregateOnMetric(aggregate.getGroupSet(), aggregate, query)) {
        return false;
      }
      return !checkTimestampRefOnQuery(builder.build(), query.getTopNode(), query);
    }
  }

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Aggregate} and
   * {@link org.apache.calcite.rel.core.Project} into a {@link DruidQuery}.
   */
  private static class DruidAggregateProjectRule extends RelOptRule {
    private DruidAggregateProjectRule() {
      super(
          operand(Aggregate.class,
              operand(Project.class,
                  operand(DruidQuery.class, none()))));
    }

    public void onMatch(RelOptRuleCall call) {
      final Aggregate aggregate = call.rel(0);
      final Project project = call.rel(1);
      final DruidQuery query = call.rel(2);
      if (!DruidQuery.isValidSignature(query.signature() + 'p' + 'a')) {
        return;
      }
      int timestampIdx;
      if ((timestampIdx = validProject(project, query)) == -1) {
        return;
      }
      if (aggregate.indicator
              || aggregate.getGroupSets().size() != 1
              || BAD_AGG.apply(aggregate)
              || !validAggregate(aggregate, timestampIdx)) {
        return;
      }

      if (checkAggregateOnMetric(aggregate.getGroupSet(), project, query)) {
        return;
      }

      final RelNode newProject = project.copy(project.getTraitSet(),
              ImmutableList.of(Util.last(query.rels)));
      final DruidQuery projectDruidQuery = DruidQuery.extendQuery(query, newProject);
      final RelNode newAggregate = aggregate.copy(aggregate.getTraitSet(),
              ImmutableList.of(Util.last(projectDruidQuery.rels)));
      call.transformTo(DruidQuery.extendQuery(projectDruidQuery, newAggregate));
    }

    /* To be a valid Project, we allow it to contain references, and a single call
     * to an FLOOR function on the timestamp column. Returns the reference to
     * the timestamp, if any. */
    private static int validProject(Project project, DruidQuery query) {
      List<RexNode> nodes = project.getProjects();
      int idxTimestamp = -1;
      for (int i = 0; i < nodes.size(); i++) {
        final RexNode e = nodes.get(i);
        if (e instanceof RexCall) {
          // It is a call, check that it is EXTRACT and follow-up conditions
          final RexCall call = (RexCall) e;
          if (DruidDateTimeUtils.extractGranularity(call) == null) {
            return -1;
          }
          if (idxTimestamp != -1) {
            // Already one usage of timestamp column
            return -1;
          }
          if (!(call.getOperands().get(0) instanceof RexInputRef)) {
            return -1;
          }
          final RexInputRef ref = (RexInputRef) call.getOperands().get(0);
          if (!(checkTimestampRefOnQuery(ImmutableBitSet.of(ref.getIndex()),
                  query.getTopNode(), query))) {
            return -1;
          }
          idxTimestamp = i;
          continue;
        }
        if (!(e instanceof RexInputRef)) {
          // It needs to be a reference
          return -1;
        }
        final RexInputRef ref = (RexInputRef) e;
        if (checkTimestampRefOnQuery(ImmutableBitSet.of(ref.getIndex()),
                query.getTopNode(), query)) {
          if (idxTimestamp != -1) {
            // Already one usage of timestamp column
            return -1;
          }
          idxTimestamp = i;
        }
      }
      return idxTimestamp;
    }

    private static boolean validAggregate(Aggregate aggregate, int idx) {
      if (!aggregate.getGroupSet().get(idx)) {
        return false;
      }
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        if (aggCall.getArgList().contains(idx)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Sort} through a
   * {@link org.apache.calcite.rel.core.Project}. Useful to transform
   * to complex Druid queries.
   */
  private static class DruidSortProjectTransposeRule
      extends SortProjectTransposeRule {
    private DruidSortProjectTransposeRule() {
      super(
          operand(Sort.class,
              operand(Project.class, operand(DruidQuery.class, none()))));
    }
  }

  /**
   * Rule to push back {@link org.apache.calcite.rel.core.Project} through a
   * {@link org.apache.calcite.rel.core.Sort}. Useful if after pushing Sort,
   * we could not push it inside DruidQuery.
   */
  private static class DruidProjectSortTransposeRule
      extends ProjectSortTransposeRule {
    private DruidProjectSortTransposeRule() {
      super(
          operand(Project.class,
              operand(Sort.class, operand(DruidQuery.class, none()))));
    }
  }

  /**
   * Rule to push a {@link org.apache.calcite.rel.core.Sort}
   * into a {@link DruidQuery}.
   */
  private static class DruidSortRule extends RelOptRule {
    private DruidSortRule() {
      super(operand(Sort.class, operand(DruidQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      final DruidQuery query = call.rel(1);
      if (!DruidQuery.isValidSignature(query.signature() + 'l')) {
        return;
      }
      // Either it is:
      // - a sort without limit on the time column on top of
      //     Agg operator (transformable to timeseries query), or
      // - it is a sort w/o limit on columns that do not include
      //     the time column on top of Agg operator, or
      // - a simple limit on top of other operator than Agg
      if (!validSortLimit(sort, query)) {
        return;
      }
      final RelNode newSort = sort.copy(sort.getTraitSet(),
              ImmutableList.of(Util.last(query.rels)));
      call.transformTo(DruidQuery.extendQuery(query, newSort));
    }

    /** Checks whether sort is valid. */
    private static boolean validSortLimit(Sort sort, DruidQuery query) {
      if (sort.offset != null && RexLiteral.intValue(sort.offset) != 0) {
        // offset not supported by Druid
        return false;
      }
      if (query.getTopNode() instanceof Aggregate) {
        final Aggregate topAgg = (Aggregate) query.getTopNode();
        final ImmutableBitSet.Builder positionsReferenced = ImmutableBitSet.builder();
        int metricsRefs = 0;
        for (RelFieldCollation col : sort.collation.getFieldCollations()) {
          int idx = col.getFieldIndex();
          if (idx >= topAgg.getGroupCount()) {
            metricsRefs++;
            continue;
          }
          positionsReferenced.set(topAgg.getGroupSet().nth(idx));
        }
        boolean refsTimestamp =
                checkTimestampRefOnQuery(positionsReferenced.build(), topAgg.getInput(), query);
        if (refsTimestamp && metricsRefs != 0) {
          // Metrics reference timestamp too
          return false;
        }
        // If the aggregate is grouping by timestamp (or a function of the
        // timestamp such as month) then we cannot push Sort to Druid.
        // Druid's topN and groupBy operators would sort only within the
        // granularity, whereas we want global sort.
        final boolean aggregateRefsTimestamp =
            checkTimestampRefOnQuery(topAgg.getGroupSet(), topAgg.getInput(), query);
        if (aggregateRefsTimestamp && metricsRefs != 0) {
          return false;
        }
        if (refsTimestamp
            && sort.collation.getFieldCollations().size() == 1
            && topAgg.getGroupCount() == 1) {
          // Timeseries query: if it has a limit, we cannot push
          return !RelOptUtil.isLimit(sort);
        }
        return true;
      }
      // If it is going to be a Druid select operator, we push the limit if
      // it does not contain a sort specification (required by Druid)
      return RelOptUtil.isPureLimit(sort);
    }
  }

  /** Checks whether any of the references leads to the timestamp column. */
  private static boolean checkTimestampRefOnQuery(ImmutableBitSet set, RelNode top,
      DruidQuery query) {
    if (top instanceof Project) {
      ImmutableBitSet.Builder newSet = ImmutableBitSet.builder();
      final Project project = (Project) top;
      for (int index : set) {
        RexNode node = project.getProjects().get(index);
        if (node instanceof RexInputRef) {
          newSet.set(((RexInputRef) node).getIndex());
        } else if (node instanceof RexCall) {
          RexCall call = (RexCall) node;
          assert DruidDateTimeUtils.extractGranularity(call) != null;
          newSet.set(((RexInputRef) call.getOperands().get(0)).getIndex());
        }
      }
      top = project.getInput();
      set = newSet.build();
    }

    // Check if any references the timestamp column
    for (int index : set) {
      if (query.druidTable.timestampFieldName.equals(
              top.getRowType().getFieldNames().get(index))) {
        return true;
      }
    }

    return false;
  }

  /** Checks whether any of the references leads to a metric column. */
  private static boolean checkAggregateOnMetric(ImmutableBitSet set, RelNode topProject,
      DruidQuery query) {
    if (topProject instanceof Project) {
      ImmutableBitSet.Builder newSet = ImmutableBitSet.builder();
      final Project project = (Project) topProject;
      for (int index : set) {
        RexNode node = project.getProjects().get(index);
        if (node instanceof RexInputRef) {
          newSet.set(((RexInputRef) node).getIndex());
        } else if (node instanceof RexCall) {
          RexCall call = (RexCall) node;
          newSet.set(((RexInputRef) call.getOperands().get(0)).getIndex());
        }
      }
      set = newSet.build();
    }
    for (int index : set) {
      if (query.druidTable.metricFieldNames
              .contains(query.getTopNode().getRowType().getFieldNames().get(index))) {
        return true;
      }
    }

    return false;
  }

  /**
   * Rule to push a {@link org.apache.calcite.rel.core.Project}
   * past a {@link org.apache.calcite.rel.core.Filter}
   * when {@code Filter} is on top of a {@link DruidQuery}.
   */
  private static class DruidProjectFilterTransposeRule
      extends ProjectFilterTransposeRule {
    private DruidProjectFilterTransposeRule() {
      super(
          operand(Project.class,
              operand(Filter.class,
                  operand(DruidQuery.class, none()))),
          PushProjector.ExprCondition.FALSE,
          RelFactories.LOGICAL_BUILDER);
    }
  }

  /**
   * Rule to push a {@link org.apache.calcite.rel.core.Filter}
   * past a {@link org.apache.calcite.rel.core.Project}
   * when {@code Project} is on top of a {@link DruidQuery}.
   */
  private static class DruidFilterProjectTransposeRule
      extends FilterProjectTransposeRule {
    private DruidFilterProjectTransposeRule() {
      super(
          operand(Filter.class,
              operand(Project.class,
                  operand(DruidQuery.class, none()))),
          true, true, RelFactories.LOGICAL_BUILDER);
    }
  }

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Aggregate}
   * past a {@link org.apache.calcite.rel.core.Filter}
   * when {@code Filter} is on top of a {@link DruidQuery}.
   */
  private static class DruidAggregateFilterTransposeRule
      extends AggregateFilterTransposeRule {
    private DruidAggregateFilterTransposeRule() {
      super(
          operand(Aggregate.class,
              operand(Filter.class,
                  operand(DruidQuery.class, none()))),
          RelFactories.LOGICAL_BUILDER);
    }
  }

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Filter}
   * past an {@link org.apache.calcite.rel.core.Aggregate}
   * when {@code Aggregate} is on top of a {@link DruidQuery}.
   */
  private static class DruidFilterAggregateTransposeRule
      extends FilterAggregateTransposeRule {
    private DruidFilterAggregateTransposeRule() {
      super(
          operand(Filter.class,
              operand(Aggregate.class,
                  operand(DruidQuery.class, none()))),
          RelFactories.LOGICAL_BUILDER);
    }
  }

}

// End DruidRules.java
