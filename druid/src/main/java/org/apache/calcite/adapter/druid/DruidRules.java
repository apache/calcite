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
import org.apache.calcite.rel.logical.LogicalFilter;
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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
      final RelBuilder relBuilder = call.builder();
      final RexBuilder rexBuilder = cluster.getRexBuilder();

      if (!DruidQuery.isValidSignature(query.signature() + 'f')) {
        return;
      }

      final List<RexNode> validPreds = new ArrayList<>();
      final List<RexNode> nonValidPreds = new ArrayList<>();
      final RexExecutor executor =
          Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
      final RexSimplify simplify = new RexSimplify(rexBuilder, true, executor);
      final RexNode cond = simplify.simplify(filter.getCondition());
      if (!canPush(cond)) {
        return;
      }
      for (RexNode e : RelOptUtil.conjunctions(cond)) {
        if (query.isValidFilter(e)) {
          validPreds.add(e);
        } else {
          nonValidPreds.add(e);
        }
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

      final Triple<List<RexNode>, List<RexNode>, List<RexNode>> triple =
          splitFilters(rexBuilder, query, validPreds, nonValidPreds, timestampFieldIdx);
      if (triple.getLeft().isEmpty() && triple.getMiddle().isEmpty()) {
        // We can't push anything useful to Druid.
        return;
      }
      final List<RexNode> residualPreds = new ArrayList<>(triple.getRight());
      List<LocalInterval> intervals = null;
      if (!triple.getLeft().isEmpty()) {
        intervals = DruidDateTimeUtils.createInterval(
            query.getRowType().getFieldList().get(timestampFieldIdx).getType(),
            RexUtil.composeConjunction(rexBuilder, triple.getLeft(), false));
        if (intervals == null || intervals.isEmpty()) {
          // Case we have an filter with extract that can not be written as interval push down
          triple.getMiddle().addAll(triple.getLeft());
        }
      }
      RelNode newDruidQuery = query;
      if (!triple.getMiddle().isEmpty()) {
        final RelNode newFilter = filter.copy(filter.getTraitSet(), Util.last(query.rels),
            RexUtil.composeConjunction(rexBuilder, triple.getMiddle(), false));
        newDruidQuery = DruidQuery.extendQuery(query, newFilter);
      }
      if (intervals != null && !intervals.isEmpty()) {
        newDruidQuery = DruidQuery.extendQuery((DruidQuery) newDruidQuery, intervals);
      }
      if (!residualPreds.isEmpty()) {
        newDruidQuery = relBuilder
            .push(newDruidQuery)
            .filter(residualPreds)
            .build();
      }
      call.transformTo(newDruidQuery);
    }

    /**
     * Given a list of conditions that contain Druid valid operations and
     * a list that contains those that contain any non-supported operation,
     * it outputs a triple with three different categories:
     * 1-l) condition filters on the timestamp column,
     * 2-m) condition filters that can be pushed to Druid,
     * 3-r) condition filters that cannot be pushed to Druid.
     */
    private static Triple<List<RexNode>, List<RexNode>, List<RexNode>> splitFilters(
        final RexBuilder rexBuilder, final DruidQuery input, final List<RexNode> validPreds,
        final List<RexNode> nonValidPreds, final int timestampFieldIdx) {
      final List<RexNode> timeRangeNodes = new ArrayList<>();
      final List<RexNode> pushableNodes = new ArrayList<>();
      final List<RexNode> nonPushableNodes = new ArrayList<>(nonValidPreds);
      // Number of columns with the dimensions and timestamp
      for (RexNode conj : validPreds) {
        final RelOptUtil.InputReferencedVisitor visitor = new RelOptUtil.InputReferencedVisitor();
        conj.accept(visitor);
        if (visitor.inputPosReferenced.contains(timestampFieldIdx)) {
          if (visitor.inputPosReferenced.size() != 1) {
            // Complex predicate, transformation currently not supported
            nonPushableNodes.add(conj);
          } else {
            timeRangeNodes.add(conj);
          }
        } else {
          boolean filterOnMetrics = false;
          for (Integer i : visitor.inputPosReferenced) {
            if (input.druidTable.metricFieldNames.contains(
                    input.getRowType().getFieldList().get(i).getName())) {
              // Filter on metrics, not supported in Druid
              filterOnMetrics = true;
              break;
            }
          }
          if (filterOnMetrics) {
            nonPushableNodes.add(conj);
          } else {
            pushableNodes.add(conj);
          }
        }
      }
      return ImmutableTriple.of(timeRangeNodes, pushableNodes, nonPushableNodes);
    }

    /** Returns whether we can push an expression to Druid. */
    private static boolean canPush(RexNode cond) {
      // Druid cannot implement "where false"
      return !cond.isAlwaysFalse();
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
      DruidQuery query = call.rel(2);
      if (!DruidQuery.isValidSignature(query.signature() + 'p' + 'a')) {
        return;
      }

      int timestampIdx = validProject(project, query);
      List<Integer> filterRefs = getFilterRefs(aggregate.getAggCallList());

      if (timestampIdx == -1 && filterRefs.size() == 0) {
        return;
      }

      // Check that the filters that the Aggregate calls refer to are valid filters can be pushed
      // into Druid
      for (Integer i : filterRefs) {
        RexNode filterNode = project.getProjects().get(i);
        if (!query.isValidFilter(filterNode, project.getInput()) || filterNode.isAlwaysFalse()) {
          return;
        }
      }

      if (aggregate.indicator
              || aggregate.getGroupSets().size() != 1
              || BAD_AGG.apply(aggregate)
              || !validAggregate(aggregate, timestampIdx, filterRefs.size())) {
        return;
      }
      if (checkAggregateOnMetric(aggregate.getGroupSet(), project, query)) {
        return;
      }

      final RelNode newProject = project.copy(project.getTraitSet(),
              ImmutableList.of(Util.last(query.rels)));
      final RelNode newAggregate = aggregate.copy(aggregate.getTraitSet(),
              ImmutableList.of(newProject));

      if (filterRefs.size() > 0) {
        query = optimizeFilteredAggregations(query, (Project) newProject,
                (Aggregate) newAggregate);
      } else {
        query = DruidQuery.extendQuery(DruidQuery.extendQuery(query, newProject), newAggregate);
      }
      call.transformTo(query);
    }

    /**
     * Returns an array of unique filter references from
     * the given list of {@link org.apache.calcite.rel.core.AggregateCall}
     * */
    private Set<Integer> getUniqueFilterRefs(List<AggregateCall> calls) {
      Set<Integer> refs = new HashSet<>();
      for (AggregateCall call : calls) {
        if (call.hasFilter()) {
          refs.add(call.filterArg);
        }
      }
      return refs;
    }

    /**
     * Attempts to optimize any aggregations with filters in the DruidQuery by
     * 1. Trying to abstract common filters out into the "filter" field
     * 2. Eliminating expressions that are always true or always false when possible
     * 3. ANDing aggregate filters together with the outer filter to allow for pruning of data
     * Should be called before pushing both the aggregate and project into Druid.
     * Assumes that at least one aggregate call has a filter attached to it
     * */
    private DruidQuery optimizeFilteredAggregations(DruidQuery query, Project project,
                                                   Aggregate aggregate) {
      Filter filter = null;
      RexBuilder builder = query.getCluster().getRexBuilder();
      final RexExecutor executor =
              Util.first(query.getCluster().getPlanner().getExecutor(), RexUtil.EXECUTOR);
      RexSimplify simplifier = new RexSimplify(builder, true, executor);

      // if the druid query originally contained a filter
      boolean containsFilter = false;
      for (RelNode node : query.rels) {
        if (node instanceof Filter) {
          filter = (Filter) node;
          containsFilter = true;
          break;
        }
      }

      // if every aggregate call has a filter arg reference
      boolean allHaveFilters = allAggregatesHaveFilters(aggregate.getAggCallList());

      Set<Integer> uniqueFilterRefs = getUniqueFilterRefs(aggregate.getAggCallList());

      // One of the pre-conditions for this method
      assert uniqueFilterRefs.size() > 0;

      List<AggregateCall> newCalls = new ArrayList<>();

      // OR all the filters so that they can ANDed to the outer filter
      List<RexNode> disjunctions = new ArrayList<>();
      for (Integer i : uniqueFilterRefs) {
        disjunctions.add(stripFilter(project.getProjects().get(i)));
      }
      RexNode filterNode = RexUtil.composeDisjunction(builder, disjunctions);

      // Erase references to filters
      for (AggregateCall call : aggregate.getAggCallList()) {
        int newFilterArg = call.filterArg;
        if (!call.hasFilter()
                || (uniqueFilterRefs.size() == 1 && allHaveFilters) // filters get extracted
                || project.getProjects().get(newFilterArg).isAlwaysTrue()) {
          newFilterArg = -1;
        }
        newCalls.add(call.copy(call.getArgList(), newFilterArg));
      }
      aggregate = aggregate.copy(aggregate.getTraitSet(), aggregate.getInput(),
              aggregate.indicator, aggregate.getGroupSet(), aggregate.getGroupSets(),
              newCalls);

      if (containsFilter) {
        // AND the current filterNode with the filter node inside filter
        filterNode = builder.makeCall(SqlStdOperatorTable.AND, filterNode, filter.getCondition());
      }

      // Simplify the filter as much as possible
      RexNode tempFilterNode = filterNode;
      filterNode = simplifier.simplify(filterNode);

      // It's possible that after simplification that the expression is now always false.
      // Druid cannot handle such a filter.
      // This will happen when the below expression (f_n+1 may not exist):
      // f_n+1 AND (f_1 OR f_2 OR ... OR f_n) simplifies to be something always false.
      // f_n+1 cannot be false, since it came from a pushed filter rel node
      // and each f_i cannot be false, since DruidAggregateProjectRule would have caught that.
      // So, the only solution is to revert back to the un simplified version and let Druid
      // handle a filter that is ultimately unsatisfiable.
      if (filterNode.isAlwaysFalse()) {
        filterNode = tempFilterNode;
      }

      filter = LogicalFilter.create(query.rels.get(0), filterNode);

      boolean addNewFilter = !filter.getCondition().isAlwaysTrue() && allHaveFilters;
      // Assumes that Filter nodes are always right after
      // TableScan nodes (which are always present)
      int startIndex = containsFilter && addNewFilter ? 2 : 1;

      List<RelNode> newNodes = constructNewNodes(query.rels, addNewFilter, startIndex,
              filter, project, aggregate);

      return DruidQuery.create(query.getCluster(),
             aggregate.getTraitSet().replace(query.getConvention()),
             query.getTable(), query.druidTable, newNodes);
    }

    // Returns true if and only if every AggregateCall in calls has a filter argument.
    private static boolean allAggregatesHaveFilters(List<AggregateCall> calls) {
      for (AggregateCall call : calls) {
        if (!call.hasFilter()) {
          return false;
        }
      }
      return true;
    }

    /**
     * Returns a new List of RelNodes in the order of the given order of the oldNodes,
     * the given {@link Filter}, and any extra nodes.
     */
    private static List<RelNode> constructNewNodes(List<RelNode> oldNodes,
        boolean addFilter, int startIndex, RelNode filter, RelNode ... trailingNodes) {
      List<RelNode> newNodes = new ArrayList<>();

      // The first item should always be the Table scan, so any filter would go after that
      newNodes.add(oldNodes.get(0));

      if (addFilter) {
        newNodes.add(filter);
        // This is required so that each RelNode is linked to the one before it
        if (startIndex < oldNodes.size()) {
          RelNode next = oldNodes.get(startIndex);
          newNodes.add(next.copy(next.getTraitSet(), Collections.singletonList(filter)));
          startIndex++;
        }
      }

      // Add the rest of the nodes from oldNodes
      for (int i = startIndex; i < oldNodes.size(); i++) {
        newNodes.add(oldNodes.get(i));
      }

      // Add the trailing nodes (need to link them)
      for (RelNode node : trailingNodes) {
        newNodes.add(node.copy(node.getTraitSet(), Collections.singletonList(Util.last(newNodes))));
      }

      return newNodes;
    }

    // Removes the IS_TRUE in front of RexCalls, if they exist
    private static RexNode stripFilter(RexNode node) {
      if (node.getKind() == SqlKind.IS_TRUE) {
        return ((RexCall) node).getOperands().get(0);
      }
      return node;
    }

    private static List<Integer> getFilterRefs(List<AggregateCall> calls) {
      List<Integer> refs = new ArrayList<>();
      for (AggregateCall call : calls) {
        if (call.hasFilter()) {
          refs.add(call.filterArg);
        }
      }
      return refs;
    }

    /* To be a valid Project, we allow it to contain references, and a single call
     * to a FLOOR function on the timestamp column OR valid time EXTRACT on the timestamp column.
     * Returns the reference to the timestamp, if any. */
    private static int validProject(Project project, DruidQuery query) {
      List<RexNode> nodes = project.getProjects();
      int idxTimestamp = -1;
      boolean hasFloor = false;
      for (int i = 0; i < nodes.size(); i++) {
        final RexNode e = nodes.get(i);
        if (e instanceof RexCall) {
          // It is a call, check that it is EXTRACT and follow-up conditions
          final RexCall call = (RexCall) e;
          if (DruidDateTimeUtils.extractGranularity(call) == null) {
            return -1;
          }
          if (idxTimestamp != -1 && hasFloor) {
            // Already one usage of timestamp column
            return -1;
          }
          switch (call.getKind()) {
          case FLOOR:
            hasFloor = true;
            if (!(call.getOperands().get(0) instanceof RexInputRef)) {
              return -1;
            }
            final RexInputRef ref = (RexInputRef) call.getOperands().get(0);
            if (!(checkTimestampRefOnQuery(ImmutableBitSet.of(ref.getIndex()),
                query.getTopNode(),
                query))) {
              return -1;
            }
            idxTimestamp = i;
            break;
          case EXTRACT:
            idxTimestamp = RelOptUtil.InputFinder.bits(call).asList().get(0);
            break;
          default:
            throw new AssertionError();
          }
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

    private static boolean validAggregate(Aggregate aggregate, int idx, int numFilterRefs) {
      if (numFilterRefs > 0 && idx < 0) {
        return true;
      }
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
      // - a sort and limit on a dimension/metric part of the druid group by query or
      // - a sort without limit on the time column on top of
      //     Agg operator (transformable to timeseries query), or
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
        for (RelFieldCollation col : sort.collation.getFieldCollations()) {
          int idx = col.getFieldIndex();
          if (idx >= topAgg.getGroupCount()) {
            continue;
          }
          //has the indexes of the columns used for sorts
          positionsReferenced.set(topAgg.getGroupSet().nth(idx));
        }
        // Case it is a timeseries query
        if (checkIsFlooringTimestampRefOnQuery(topAgg.getGroupSet(), topAgg.getInput(), query)
            && topAgg.getGroupCount() == 1) {
          // do not push if it has a limit or more than one sort key or we have sort by
          // metric/dimension
          return !RelOptUtil.isLimit(sort) && sort.collation.getFieldCollations().size() == 1
              && checkTimestampRefOnQuery(positionsReferenced.build(), topAgg.getInput(), query);
        }
        return true;
      }
      // If it is going to be a Druid select operator, we push the limit if
      // it does not contain a sort specification (required by Druid)
      return RelOptUtil.isPureLimit(sort);
    }
  }

  /** Returns true if any of the grouping key is a floor operator over the timestamp column. */
  private static boolean checkIsFlooringTimestampRefOnQuery(ImmutableBitSet set, RelNode top,
      DruidQuery query) {
    if (top instanceof Project) {
      ImmutableBitSet.Builder newSet = ImmutableBitSet.builder();
      final Project project = (Project) top;
      for (int index : set) {
        RexNode node = project.getProjects().get(index);
        if (node instanceof RexCall) {
          RexCall call = (RexCall) node;
          assert DruidDateTimeUtils.extractGranularity(call) != null;
          if (call.getKind().equals(SqlKind.FLOOR)) {
            newSet.addAll(RelOptUtil.InputFinder.bits(call));
          }
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
          // when we have extract from time columnthe rexCall is in the form of /Reinterpret$0
          newSet.addAll(RelOptUtil.InputFinder.bits(call));
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
        ImmutableBitSet setOfBits = RelOptUtil.InputFinder.bits(node);
        newSet.addAll(setOfBits);
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
