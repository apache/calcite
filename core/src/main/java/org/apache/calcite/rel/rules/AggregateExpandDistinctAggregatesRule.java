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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import static java.util.Objects.requireNonNull;

/**
 * Planner rule that expands distinct aggregates
 * (such as {@code COUNT(DISTINCT x)}) from a
 * {@link org.apache.calcite.rel.core.Aggregate}.
 *
 * <p>How this is done depends upon the arguments to the function. If all
 * functions have the same argument
 * (e.g. {@code COUNT(DISTINCT x), SUM(DISTINCT x)} both have the argument
 * {@code x}) then one extra {@link org.apache.calcite.rel.core.Aggregate} is
 * sufficient.
 *
 * <p>If there are multiple arguments
 * (e.g. {@code COUNT(DISTINCT x), COUNT(DISTINCT y)})
 * the rule creates separate {@code Aggregate}s and combines using a
 * {@link org.apache.calcite.rel.core.Join}.
 *
 * @see CoreRules#AGGREGATE_EXPAND_DISTINCT_AGGREGATES
 * @see CoreRules#AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN
 */
@Value.Enclosing
public final class AggregateExpandDistinctAggregatesRule
    extends RelRule<AggregateExpandDistinctAggregatesRule.Config>
    implements TransformationRule {

  /** Creates an AggregateExpandDistinctAggregatesRule. */
  AggregateExpandDistinctAggregatesRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public AggregateExpandDistinctAggregatesRule(
      Class<? extends Aggregate> clazz,
      boolean useGroupingSets,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier(b ->
            b.operand(clazz).anyInputs())
        .as(Config.class)
        .withUsingGroupingSets(useGroupingSets));
  }

  @Deprecated // to be removed before 2.0
  public AggregateExpandDistinctAggregatesRule(
      Class<? extends LogicalAggregate> clazz,
      boolean useGroupingSets,
      RelFactories.JoinFactory joinFactory) {
    this(clazz, useGroupingSets, RelBuilder.proto(Contexts.of(joinFactory)));
  }

  @Deprecated // to be removed before 2.0
  public AggregateExpandDistinctAggregatesRule(
      Class<? extends LogicalAggregate> clazz,
      RelFactories.JoinFactory joinFactory) {
    this(clazz, false, RelBuilder.proto(Contexts.of(joinFactory)));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    if (!aggregate.containsDistinctCall()) {
      return;
    }

    if (!config.isUsingGroupingSets()
        && aggregate.groupSets.size() > 1) {
      // Grouping sets are not handled correctly
      // when generating joins.
      return;
    }

    // Find all of the agg expressions. We use a LinkedHashSet to ensure determinism.
    final List<AggregateCall> aggCalls = aggregate.getAggCallList();
    // Find all aggregate calls with distinct
    final List<AggregateCall> distinctAggCalls = aggCalls.stream()
        .filter(AggregateCall::isDistinct).collect(Collectors.toList());
    // Find all aggregate calls without distinct
    final List<AggregateCall> nonDistinctAggCalls = aggCalls.stream()
        .filter(aggCall -> !aggCall.isDistinct()).collect(Collectors.toList());
    final long filterCount = aggCalls.stream()
        .filter(aggCall -> aggCall.filterArg >= 0).count();
    final long unsupportedNonDistinctAggCallCount = nonDistinctAggCalls.stream()
        .filter(aggCall -> {
          final SqlKind aggCallKind = aggCall.getAggregation().getKind();
          // We only support COUNT/SUM/MIN/MAX for the "single" count distinct optimization
          switch (aggCallKind) {
          case COUNT:
          case SUM:
          case SUM0:
          case MIN:
          case MAX:
            return false;
          default:
            return true;
          }
        }).count();
    // Argument list of distinct agg calls.
    final Set<Pair<List<Integer>, Integer>> distinctCallArgLists = distinctAggCalls.stream()
        .map(aggCall -> Pair.of(aggCall.getArgList(), aggCall.filterArg))
        .collect(Collectors.toCollection(LinkedHashSet::new));

    checkState(!distinctCallArgLists.isEmpty(), "containsDistinctCall lied");

    // If all of the agg expressions are distinct and have the same
    // arguments then we can use a more efficient form.

    // MAX, MIN, BIT_AND, BIT_OR always ignore distinct attribute,
    // when they are mixed in with other distinct agg calls,
    // we can still use this promotion.

    // Treat the agg expression with Optionality.IGNORED as distinct and
    // re-statistic the non-distinct agg call count and the distinct agg
    // call arguments.
    final List<AggregateCall> nonDistinctAggCallsOfIgnoredOptionality =
        nonDistinctAggCalls.stream().filter(aggCall ->
            aggCall.getAggregation().getDistinctOptionality() == Optionality.IGNORED)
            .collect(Collectors.toList());
    // Different with distinctCallArgLists, this list also contains args that come from
    // agg call which can ignore the distinct constraint.
    final Set<Pair<List<Integer>, Integer>> distinctCallArgLists2 =
        Stream.of(distinctAggCalls, nonDistinctAggCallsOfIgnoredOptionality)
            .flatMap(Collection::stream)
            .map(aggCall -> Pair.of(aggCall.getArgList(), aggCall.filterArg))
            .collect(Collectors.toCollection(LinkedHashSet::new));

    if ((nonDistinctAggCalls.size() - nonDistinctAggCallsOfIgnoredOptionality.size()) == 0
        && distinctCallArgLists2.size() == 1
        && aggregate.getGroupType() == Group.SIMPLE) {
      final Pair<List<Integer>, Integer> pair =
          Iterables.getOnlyElement(distinctCallArgLists2);
      final RelBuilder relBuilder = call.builder();
      convertMonopole(relBuilder, aggregate, pair.left, pair.right);
      call.transformTo(relBuilder.build());
      return;
    }

    if (config.isUsingGroupingSets()) {
      rewriteUsingGroupingSets(call, aggregate);
      return;
    }

    // If only one distinct aggregate and one or more non-distinct aggregates,
    // we can generate multiphase aggregates
    if (distinctAggCalls.size() == 1 // one distinct aggregate
        // no filter
        && filterCount == 0
        // sum/min/max/count in non-distinct aggregate
        && unsupportedNonDistinctAggCallCount == 0
        // one or more non-distinct aggregates
        && !nonDistinctAggCalls.isEmpty()) {
      final RelBuilder relBuilder = call.builder();
      convertSingletonDistinct(relBuilder, aggregate, distinctCallArgLists);
      call.transformTo(relBuilder.build());
      return;
    }

    // Create a list of the expressions which will yield the final result.
    // Initially, the expressions point to the input field.
    final List<RelDataTypeField> aggFields =
        aggregate.getRowType().getFieldList();
    final List<@Nullable RexInputRef> refs = new ArrayList<>();
    final List<String> fieldNames = aggregate.getRowType().getFieldNames();
    final ImmutableBitSet groupSet = aggregate.getGroupSet();
    final int groupCount = aggregate.getGroupCount();
    for (int i : Util.range(groupCount)) {
      refs.add(RexInputRef.of(i, aggFields));
    }

    // Aggregate the original relation, including any non-distinct aggregates.
    final List<AggregateCall> newAggCallList = new ArrayList<>();
    int i = -1;
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      ++i;
      if (aggCall.isDistinct()) {
        refs.add(null);
        continue;
      }
      refs.add(
          new RexInputRef(
              groupCount + newAggCallList.size(),
              aggFields.get(groupCount + i).getType()));
      newAggCallList.add(aggCall);
    }

    // In the case where there are no non-distinct aggregates (regardless of
    // whether there are group bys), there's no need to generate the
    // extra aggregate and join.
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(aggregate.getInput());
    int n = 0;
    if (!newAggCallList.isEmpty()) {
      final RelBuilder.GroupKey groupKey =
          relBuilder.groupKey(groupSet, aggregate.getGroupSets());
      relBuilder.aggregate(groupKey, newAggCallList);
      ++n;
    }

    // For each set of operands, find and rewrite all calls which have that
    // set of operands.
    for (Pair<List<Integer>, Integer> argList : distinctCallArgLists) {
      doRewrite(relBuilder, aggregate, n++, argList.left, argList.right, refs);
    }
    // It is assumed doRewrite above replaces nulls in refs
    @SuppressWarnings("assignment.type.incompatible")
    List<RexInputRef> nonNullRefs = refs;
    relBuilder.project(nonNullRefs, fieldNames);
    call.transformTo(relBuilder.build());
  }

  /**
   * Converts an aggregate with one distinct aggregate and one or more
   * non-distinct aggregates to multi-phase aggregates (see reference example
   * below).
   *
   * @param relBuilder Contains the input relational expression
   * @param aggregate  Original aggregate
   * @param argLists   Arguments and filters to the distinct aggregate function
   *
   */
  private static RelBuilder convertSingletonDistinct(RelBuilder relBuilder,
      Aggregate aggregate, Set<Pair<List<Integer>, Integer>> argLists) {

    // In this case, we are assuming that there is a single distinct function.
    // So make sure that argLists is of size one.
    checkArgument(argLists.size() == 1);

    // For example,
    //    SELECT deptno, COUNT(*), SUM(bonus), MIN(DISTINCT sal)
    //    FROM emp
    //    GROUP BY deptno
    //
    // becomes
    //
    //    SELECT deptno, SUM(cnt), SUM(bonus), MIN(sal)
    //    FROM (
    //          SELECT deptno, COUNT(*) as cnt, SUM(bonus), sal
    //          FROM EMP
    //          GROUP BY deptno, sal)            // Aggregate B
    //    GROUP BY deptno                        // Aggregate A
    relBuilder.push(aggregate.getInput());

    final List<AggregateCall> originalAggCalls = aggregate.getAggCallList();
    final ImmutableBitSet originalGroupSet = aggregate.getGroupSet();

    // Add the distinct aggregate column(s) to the group-by columns,
    // if not already a part of the group-by
    final NavigableSet<Integer> bottomGroups = new TreeSet<>(aggregate.getGroupSet().asList());
    for (AggregateCall aggCall : originalAggCalls) {
      if (aggCall.isDistinct()) {
        bottomGroups.addAll(aggCall.getArgList());
        break;  // since we only have single distinct call
      }
    }
    final ImmutableBitSet bottomGroupSet = ImmutableBitSet.of(bottomGroups);

    // Generate the intermediate aggregate B, the one on the bottom that converts
    // a distinct call to group by call.
    // Bottom aggregate is the same as the original aggregate, except that
    // the bottom aggregate has converted the DISTINCT aggregate to a group by clause.
    final List<AggregateCall> bottomAggregateCalls = new ArrayList<>();
    for (AggregateCall aggCall : originalAggCalls) {
      // Project the column corresponding to the distinct aggregate. Project
      // as-is all the non-distinct aggregates
      if (!aggCall.isDistinct()) {
        final AggregateCall newCall =
            AggregateCall.create(aggCall.getParserPosition(), aggCall.getAggregation(), false,
                aggCall.isApproximate(), aggCall.ignoreNulls(), aggCall.rexList,
                aggCall.getArgList(), -1, aggCall.distinctKeys,
                aggCall.collation, false,
                relBuilder.peek(), null, aggCall.name);
        bottomAggregateCalls.add(newCall);
      }
    }
    // Generate the aggregate B (see the reference example above)
    relBuilder.push(
        aggregate.copy(aggregate.getTraitSet(), relBuilder.build(),
            bottomGroupSet, null, bottomAggregateCalls));

    // Add aggregate A (see the reference example above), the top aggregate
    // to handle the rest of the aggregation that the bottom aggregate hasn't handled
    final List<AggregateCall> topAggregateCalls = new ArrayList<>();
    // Use the remapped arguments for the (non)distinct aggregate calls
    int nonDistinctAggCallProcessedSoFar = 0;
    for (AggregateCall aggCall : originalAggCalls) {
      final AggregateCall newCall;
      if (aggCall.isDistinct()) {
        List<Integer> newArgList = new ArrayList<>();
        for (int arg : aggCall.getArgList()) {
          newArgList.add(bottomGroups.headSet(arg, false).size());
        }
        newCall =
            AggregateCall.create(aggCall.getParserPosition(),
                aggCall.getAggregation(),
                false,
                aggCall.isApproximate(),
                aggCall.ignoreNulls(),
                aggCall.rexList,
                newArgList,
                -1,
                aggCall.distinctKeys,
                aggCall.collation,
                aggregate.hasEmptyGroup(),
                relBuilder.peek(),
                aggCall.getType(),
                aggCall.name);
      } else {
        // If aggregate B had a COUNT aggregate call the corresponding aggregate at
        // aggregate A must be SUM. For other aggregates, it remains the same.
        final int arg = bottomGroups.size() + nonDistinctAggCallProcessedSoFar;
        final List<Integer> newArgs = ImmutableList.of(arg);
        if (aggCall.getAggregation().getKind() == SqlKind.COUNT) {
          newCall =
              AggregateCall.create(aggCall.getParserPosition(),
                  new SqlSumEmptyIsZeroAggFunction(), false,
                  aggCall.isApproximate(), aggCall.ignoreNulls(),
                  aggCall.rexList, newArgs, -1, aggCall.distinctKeys,
                  aggCall.collation, aggregate.hasEmptyGroup(),
                  relBuilder.peek(), null, aggCall.getName());
        } else {
          newCall =
              AggregateCall.create(aggCall.getParserPosition(),
                  aggCall.getAggregation(), false,
                  aggCall.isApproximate(), aggCall.ignoreNulls(),
                  aggCall.rexList, newArgs, -1, aggCall.distinctKeys,
                  aggCall.collation, aggregate.hasEmptyGroup(),
                  relBuilder.peek(), null, aggCall.name);
        }
        nonDistinctAggCallProcessedSoFar++;
      }

      topAggregateCalls.add(newCall);
    }

    // Populate the group-by keys with the remapped arguments for aggregate A
    // The top groupset is basically an identity (first X fields of aggregate B's
    // output), minus the distinct aggCall's input.
    final Set<Integer> topGroupSet = new HashSet<>();
    int groupSetToAdd = 0;
    for (int bottomGroup : bottomGroups) {
      if (originalGroupSet.get(bottomGroup)) {
        topGroupSet.add(groupSetToAdd);
      }
      groupSetToAdd++;
    }
    relBuilder.push(
        aggregate.copy(aggregate.getTraitSet(), relBuilder.build(),
            ImmutableBitSet.of(topGroupSet), null, topAggregateCalls));

    // Add projection node for case: SUM of COUNT(*):
    // Type of the SUM may be larger than type of COUNT.
    // CAST to original type must be added.
    relBuilder.convert(aggregate.getRowType(), true);

    return relBuilder;
  }

  /**
   * Rewrite aggregates that use GROUPING SETS. The following SQL/plan example
   * serves as the concrete blueprint, starting from the original statement and
   * plan-before outputs and then rebuilding the plan-after tree from the bottom
   * (line 7) back to the top (line 1):
   *
   * <p>Original SQL:
   * <pre>{@code
   * SELECT deptno, COUNT(DISTINCT sal)
   * FROM emp
   * GROUP BY ROLLUP(deptno)
   * }</pre>
   *
   * <p>Plan before rewrite:
   * <pre>{@code
   * LogicalAggregate(group=[{0}], groups=[[{0}, {}]], EXPR$1=[COUNT(DISTINCT $1)])
   *   LogicalProject(DEPTNO=[$7], SAL=[$5])
   *     LogicalTableScan(table=[[CATALOG, SALES, EMP]])
   * }</pre>
   *
   * <p>Plan after rewrite (lines referenced below):
   * <pre>{@code
   * 1 LogicalProject(DEPTNO=[$0],
   *     EXPR$1=[CAST(CASE(=($5, 0), $1, =($5, 1), $2, null:BIGINT)):BIGINT NOT NULL])
   * 2  LogicalFilter(condition=[OR(AND(=($5, 0), >($3, 0)), =($5, 1))])
   * 3    LogicalAggregate(group=[{0}], groups=[[{0}, {}]],
   *          EXPR$1_g0=[COUNT($1) FILTER $2],
   *          EXPR$1_g1=[COUNT($1) FILTER $4],
   *          $g_present_0=[COUNT() FILTER $3],
   *          $g_present_1=[COUNT() FILTER $5],
   *          $g_final=[GROUPING($0)])
   * 4      LogicalProject(DEPTNO=[$0], SAL=[$1],
   *            $g_0=[=($2, 0)], $g_1=[=($2, 1)],
   *            $g_2=[=($2, 2)], $g_3=[=($2, 3)])
   * 5        LogicalAggregate(group=[{0, 1}],
   *              groups=[[{0, 1}, {0}, {1}, {}]], $g=[GROUPING($0, $1)])
   * 6          LogicalProject(DEPTNO=[$7], SAL=[$5])
   * 7            LogicalTableScan(table=[[CATALOG, SALES, EMP]])
   * }</pre>
   *
   * <p>The method performs the following actions:
   * <ul>
   * <li>Reuse the incoming scan and projection (lines 7 and 6) by pushing the
   *   original aggregate input onto the builder.</li>
   * <li>Enumerate all grouping-set combinations and run the "bottom" aggregate
   *   over {@code fullGroupSet} to materialize line 5, including the internal
   *   {@code GROUPING()} value.</li>
   * <li>Project the boolean selector columns that compare {@code GROUPING()}
   *   outputs to the required combinations, which surfaces line 4.</li>
   * <li>Build the "upper" grouping-set aggregates with per-set FILTER clauses,
   *   reproducing line 3 and retaining presence counters / grouping ids.</li>
   * <li>Assemble {@code keepConditions} so we can emit the filter of line 2 that
   *   drops internal-only rows.</li>
   * <li>Produce the final projection (line 1) that routes each aggregate result
   *   to the user-visible columns.</li>
   * </ul>
   */
  private static void rewriteUsingGroupingSets(RelOptRuleCall call,
      Aggregate aggregate) {
    final ImmutableBitSet aggregateGroupSet = aggregate.getGroupSet();
    final ImmutableList<ImmutableBitSet> aggregateGroupingSets = aggregate.getGroupSets();

    final Set<ImmutableBitSet> groupSetTreeSet =
        new TreeSet<>(ImmutableBitSet.ORDERING);

    // Map from a set of group keys -> which filter args (if any) contributed
    // to that combination. Used to generate boolean marker columns later which
    // indicate whether a bottom-row should be considered for a particular
    // (grouping-set, filter) combination.
    final Map<ImmutableBitSet, Set<Integer>> distinctFilterArgMap = new HashMap<>();

    // Enumerating every required grouping-set combination, including distinct
    // args or filter columns relied on by the downstream projection(line 4).
    BiConsumer<ImmutableBitSet, Integer> addGroupSet = (groupSet, filterArg) -> {
      groupSetTreeSet.add(groupSet);
      distinctFilterArgMap.computeIfAbsent(groupSet, g -> new HashSet<>()).add(filterArg);
    };

    // Always include the base group set and each declared grouping set. -1 means "no filter".
    addGroupSet.accept(aggregateGroupSet, -1);
    for (ImmutableBitSet groupingSet : aggregateGroupingSets) {
      addGroupSet.accept(groupingSet, -1);
    }

    // For each DISTINCT aggregate, include grouping-set combinations:
    // (distinct args) ∪ (grouping set). This ensures we compute bottom rows
    // at a granularity sufficient to evaluate DISTINCT per grouping set. If
    // the DISTINCT agg has a FILTER, include that filter column in the
    // grouping so that the downstream boolean selector can distinguish
    // filtered rows. For example, with COUNT(DISTINCT sal) and grouping sets
    // (deptno) and (), we add {deptno, sal} and {sal}.
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (!aggCall.isDistinct()) {
        continue;
      }
      final ImmutableBitSet args = ImmutableBitSet.of(aggCall.getArgList());
      for (ImmutableBitSet groupingSet : aggregateGroupingSets) {
        ImmutableBitSet groupSet = args.union(groupingSet);
        if (aggCall.filterArg >= 0) {
          groupSet = groupSet.set(aggCall.filterArg);
        }
        addGroupSet.accept(groupSet, aggCall.filterArg);
      }
    }

    final ImmutableList<ImmutableBitSet> groupSets =
        ImmutableList.copyOf(groupSetTreeSet);
    // fullGroupSet is the union of all bits that appear in any grouping set.
    final ImmutableBitSet fullGroupSet = ImmutableBitSet.union(groupSets);
    // Whether the bottom aggregate must account for an "empty" grouping set.
    final boolean bottomHasEmptyGroup = groupSets.contains(ImmutableBitSet.of());

    final List<AggregateCall> distinctAggCalls = new ArrayList<>();
    for (Pair<AggregateCall, String> aggCall : aggregate.getNamedAggCalls()) {
      if (!aggCall.left.isDistinct()) {
        AggregateCall newAggCall =
            aggCall.left.adaptTo(aggregate.getInput(),
                aggCall.left.getArgList(), aggCall.left.filterArg,
                aggregate.hasEmptyGroup(), bottomHasEmptyGroup);
        distinctAggCalls.add(newAggCall.withName(aggCall.right));
      }
    }

    final RelBuilder relBuilder = call.builder();
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    // Lines 7 & 6: reuse the existing scan+projection feeding the original aggregate.
    relBuilder.push(aggregate.getInput());
    final int bottomGroupCount = fullGroupSet.cardinality();

    // Map each (groupSet, filterArg) pair to an output field index in the
    // bottom projection. These fields become boolean/marker columns used to
    // implement FILTER(...) and to detect whether a grouping set had rows.
    // The numbering starts after the bottom group fields and the distinct
    // aggregate columns; 'z' is the running output field index for these
    // selector markers.
    final Map<Pair<ImmutableBitSet, Integer>, Integer> filters = new LinkedHashMap<>();
    int z = bottomGroupCount + distinctAggCalls.size();
    for (ImmutableBitSet groupSet : groupSets) {
      Set<Integer> filterArgList = distinctFilterArgMap.get(groupSet);
      for (Integer filterArg : requireNonNull(filterArgList, "filterArgList")) {
        filters.put(Pair.of(groupSet, filterArg), z);
        z += 1;
      }
    }

    distinctAggCalls.add(
        AggregateCall.create(SqlStdOperatorTable.GROUPING, false, false, false,
            ImmutableList.of(), ImmutableIntList.copyOf(fullGroupSet), -1,
            null, RelCollations.EMPTY,
            bottomHasEmptyGroup, relBuilder.peek(), null, "$g"));

    // Line 5: bottom aggregate materializes every grouping-set combination and
    // produces the GROUPING() value needed by later steps.
    relBuilder.aggregate(
        relBuilder.groupKey(fullGroupSet, groupSets),
        distinctAggCalls);

    // Line 4: convert GROUPING() into named selector columns ($g_*) that pick
    // rows for each grouping-set/filter combination.
    if (!filters.isEmpty()) {
      final List<RexNode> nodes = new ArrayList<>(relBuilder.fields());
      final RexNode nodeZ = nodes.remove(nodes.size() - 1);
      for (Map.Entry<Pair<ImmutableBitSet, Integer>, Integer> entry : filters.entrySet()) {
        final long v = groupValue(fullGroupSet.asList(), entry.getKey().left);
        int distinctFilterArg = remap(fullGroupSet, entry.getKey().right);
        RexNode expr = relBuilder.equals(nodeZ, relBuilder.literal(v));
        if (distinctFilterArg > -1) {
          // 'AND' the filter of the distinct aggregate call and the group value.
          expr =
              relBuilder.and(expr,
                  relBuilder.call(SqlStdOperatorTable.IS_TRUE,
                      relBuilder.field(distinctFilterArg)));
        }
        // "f" means filter.
        nodes.add(
            relBuilder.alias(expr,
            "$g_" + v + (distinctFilterArg < 0 ? "" : "_f_" + distinctFilterArg)));
      }
      relBuilder.project(nodes);
    }

    // Compute the remapped top-group key and grouping sets. The top-group key
    // selects which fields of the bottom result correspond to the original
    // aggregate's group-by columns. Upper aggregates(line 3) will group by this key.
    final ImmutableBitSet topGroupKey = remap(fullGroupSet, aggregateGroupSet);
    final ImmutableList<ImmutableBitSet> topGroupingSets =
        remap(fullGroupSet, aggregate.getGroupSets());
    final int topGroupCount = topGroupKey.cardinality();
    final boolean needsGroupingIndicators = aggregate.getGroupType() != Group.SIMPLE;
    final List<Integer> groupingIndicatorOrdinals;
    if (needsGroupingIndicators) {
      groupingIndicatorOrdinals =
          new ArrayList<>(Collections.nCopies(aggregateGroupingSets.size(), -1));
    } else {
      groupingIndicatorOrdinals = ImmutableList.of();
    }

    int valueIndex = bottomGroupCount;
    // line 3 will be built from this list
    final List<AggregateCall> upperAggCalls = new ArrayList<>();
    final List<List<Integer>> aggCallOrdinals = new ArrayList<>();
    final List<AggregateCall> aggCalls = aggregate.getAggCallList();

    // The first part of line 3: Build upper aggregates per declared grouping set.
    // For each original aggCall we create one upper agg per declared grouping set.
    // The upper aggregate groups by {@code topGroupKey} and uses the boolean marker
    // columns (placed at known ordinals) as the FILTER argument for the
    // corresponding per-group aggregation. The list {@code aggCallOrdinals}
    // records, for each original aggCall, the output field ordinals of the
    // corresponding upper-aggregate results (one per grouping set).
    for (AggregateCall aggCall : aggCalls) {
      final List<Integer> ordinals = new ArrayList<>();
      if (!aggCall.isDistinct()) {
        final int inputIndex = valueIndex++;
        final List<Integer> args = ImmutableIntList.of(inputIndex);
        for (int g = 0; g < aggregateGroupingSets.size(); g++) {
          final ImmutableBitSet groupingSet = aggregateGroupingSets.get(g);
          final int newFilterArg =
              requireNonNull(filters.get(Pair.of(groupingSet, -1)),
                  () -> "filters.get(" + groupingSet + ", -1)");
          final String upperAggName = upperAggCallName(aggCall, g);
          // Each filtered grouping set emits exactly one row per group,
          // so MIN just passes that value through without re-aggregation
          final AggregateCall newCall =
              AggregateCall.create(aggCall.getParserPosition(),
                  SqlStdOperatorTable.MIN, false, aggCall.isApproximate(),
                  aggCall.ignoreNulls(), aggCall.rexList, args, newFilterArg,
                  aggCall.distinctKeys, aggCall.collation, aggregate.hasEmptyGroup(),
                  relBuilder.peek(), null, upperAggName);
          upperAggCalls.add(newCall);
          ordinals.add(topGroupCount + upperAggCalls.size() - 1);
        }
      } else {
        final List<Integer> newArgList = remap(fullGroupSet, aggCall.getArgList());
        for (int g = 0; g < aggregateGroupingSets.size(); g++) {
          final ImmutableBitSet groupingSet = aggregateGroupingSets.get(g);
          final ImmutableBitSet newGroupSet = ImmutableBitSet.of(aggCall.getArgList())
              .setIf(aggCall.filterArg, aggCall.filterArg >= 0)
              .union(groupingSet);
          final int newFilterArg =
              requireNonNull(filters.get(Pair.of(newGroupSet, aggCall.filterArg)),
                  () -> "filters.get(" + newGroupSet + ", " + aggCall.filterArg + ")");
          final String upperAggName = upperAggCallName(aggCall, g);
          final AggregateCall newCall =
              AggregateCall.create(aggCall.getParserPosition(), aggCall.getAggregation(), false,
                  aggCall.isApproximate(), aggCall.ignoreNulls(),
                  aggCall.rexList, newArgList, newFilterArg,
                  aggCall.distinctKeys, aggCall.collation,
                  aggregate.hasEmptyGroup(), relBuilder.peek(), null, upperAggName);
          upperAggCalls.add(newCall);
          ordinals.add(topGroupCount + upperAggCalls.size() - 1);
        }
      }
      aggCallOrdinals.add(ordinals);
    }

    // The second part of line 3: If grouping indicators are needed
    // (ROLLUP/CUBE/GROUPING SETS with more than one grouping set), add
    // COUNT(...) presence calls which are later used to determine whether
    // a grouping set produced any rows. These calls implement the
    // semantics where empty grouping sets must still produce a result.
    if (needsGroupingIndicators) {
      for (int g = 0; g < aggregateGroupingSets.size(); g++) {
        final ImmutableBitSet groupingSet = aggregateGroupingSets.get(g);
        final Integer filterField = filters.get(Pair.of(groupingSet, -1));
        if (filterField == null) {
          continue;
        }
        final AggregateCall presenceCall =
            AggregateCall.create(SqlStdOperatorTable.COUNT, false, false, false,
                ImmutableList.of(), ImmutableIntList.of(), filterField, null,
                RelCollations.EMPTY, aggregate.hasEmptyGroup(), relBuilder.peek(), null,
                "$g_present_" + g);
        upperAggCalls.add(presenceCall);
        groupingIndicatorOrdinals.set(g, topGroupCount + upperAggCalls.size() - 1);
      }
    }

    // The third part of line 3: If there are multiple declared grouping sets,
    // then we need a GROUPING() value in the upper aggregate so we can later
    // route results to the correct output using CASE expressions. Compute and
    // append that grouping-call if required.
    final boolean needsGroupingId = aggregateGroupingSets.size() > 1;
    final int groupingIdOrdinal;
    if (needsGroupingId) {
      final ImmutableBitSet remappedGroupSet = remap(fullGroupSet, aggregateGroupSet);
      final AggregateCall groupingCall =
          AggregateCall.create(SqlStdOperatorTable.GROUPING, false, false, false,
              ImmutableList.of(), ImmutableIntList.copyOf(remappedGroupSet.asList()), -1, null,
              RelCollations.EMPTY, aggregate.hasEmptyGroup(), relBuilder.peek(), null, "$g_final");
      upperAggCalls.add(groupingCall);
      groupingIdOrdinal = topGroupCount + upperAggCalls.size() - 1;
    } else {
      groupingIdOrdinal = -1;
    }

    // The final part of line 3: build the upper aggregate layer, grouping by the
    // original keys and applying FILTERs (and presence/grouping columns) per
    // declared set.
    relBuilder.aggregate(
        relBuilder.groupKey(topGroupKey, topGroupingSets),
        upperAggCalls);

    final ImmutableList<Integer> groupingIdColumns =
        ImmutableList.copyOf(Util.range(topGroupCount));
    final RexNode groupingIdRef = needsGroupingId ? relBuilder.field(groupingIdOrdinal) : null;

    if (needsGroupingIndicators) {
      final List<RexNode> keepConditions = new ArrayList<>();
      for (int g = 0; g < aggregateGroupingSets.size(); g++) {
        final int indicatorOrdinal = groupingIndicatorOrdinals.get(g);
        if (indicatorOrdinal < 0) {
          continue;
        }
        final ImmutableBitSet groupingSet = aggregateGroupingSets.get(g);
        final RexNode requiredRows;
        if (groupingSet.isEmpty()) {
          // Empty grouping sets must still produce a row even if the input is
          // empty, so do not require any contributing tuples.
          requiredRows = relBuilder.literal(true);
        } else {
          requiredRows =
              relBuilder.greaterThan(relBuilder.field(indicatorOrdinal),
                  relBuilder.literal(0));
        }

        final RexNode groupingMatches;
        if (needsGroupingId) {
          final long groupingValue =
              groupValue(groupingIdColumns, remap(aggregateGroupSet, groupingSet));
          groupingMatches =
              relBuilder.equals(requireNonNull(groupingIdRef, "groupingIdRef"),
                  relBuilder.literal(groupingValue));
        } else {
          groupingMatches = relBuilder.literal(true);
        }
        keepConditions.add(relBuilder.and(groupingMatches, requiredRows));
      }

      // Line 2: filter away rows produced solely for internal combinations.
      if (!keepConditions.isEmpty()) {
        RexNode condition = keepConditions.get(0);
        for (int i = 1; i < keepConditions.size(); i++) {
          condition = relBuilder.or(condition, keepConditions.get(i));
        }
        relBuilder.filter(condition);
      }
    }

    // Assemble the projections for line 1 here
    final List<RexNode> projects = new ArrayList<>();
    final List<String> finalFieldNames = aggregate.getRowType().getFieldNames();
    for (int i = 0; i < topGroupCount; i++) {
      projects.add(relBuilder.field(i));
    }

    for (int i = 0; i < aggCalls.size(); i++) {
      final AggregateCall aggCall = aggCalls.get(i);
      final List<Integer> ordinals = aggCallOrdinals.get(i);
      if (!needsGroupingId || ordinals.size() == 1) {
        projects.add(relBuilder.field(ordinals.get(0)));
        continue;
      }

      final List<RexNode> caseOperands = new ArrayList<>();
      for (int g = 0; g < aggregateGroupingSets.size(); g++) {
        final ImmutableBitSet groupingSet = aggregateGroupingSets.get(g);
        final long groupingValue =
            groupValue(groupingIdColumns, remap(aggregateGroupSet, groupingSet));
        caseOperands.add(
            relBuilder.equals(requireNonNull(groupingIdRef, "groupingIdRef"),
                relBuilder.literal(groupingValue)));
        caseOperands.add(relBuilder.field(ordinals.get(g)));
      }
      caseOperands.add(rexBuilder.makeNullLiteral(aggCall.getType()));
      projects.add(
          relBuilder.call(SqlStdOperatorTable.CASE,
              caseOperands.toArray(new RexNode[0])));
    }

    // Line 1: final projection routes per-set aggregates back into the original
    // output schema (including CASE routing when needed).
    relBuilder.project(projects, finalFieldNames);
    relBuilder.convert(aggregate.getRowType(), true);
    call.transformTo(relBuilder.build());
  }

  /** Returns the value that "GROUPING(fullGroupSet)" will return for
   * "groupSet".
   *
   * <p>It is important that {@code fullGroupSet} is not an
   * {@link ImmutableBitSet}; the order of the bits matters. */
  static long groupValue(Collection<Integer> fullGroupSet,
      ImmutableBitSet groupSet) {
    long v = 0;
    long x = 1L << (fullGroupSet.size() - 1);
    assert ImmutableBitSet.of(fullGroupSet).contains(groupSet);
    for (int i : fullGroupSet) {
      if (!groupSet.get(i)) {
        v |= x;
      }
      x >>= 1;
    }
    return v;
  }

  static ImmutableBitSet remap(ImmutableBitSet groupSet,
      ImmutableBitSet bitSet) {
    final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (Integer bit : bitSet) {
      builder.set(remap(groupSet, bit));
    }
    return builder.build();
  }

  static ImmutableList<ImmutableBitSet> remap(ImmutableBitSet groupSet,
      Iterable<ImmutableBitSet> bitSets) {
    final ImmutableList.Builder<ImmutableBitSet> builder =
        ImmutableList.builder();
    for (ImmutableBitSet bitSet : bitSets) {
      builder.add(remap(groupSet, bitSet));
    }
    return builder.build();
  }

  private static List<Integer> remap(ImmutableBitSet groupSet,
      List<Integer> argList) {
    ImmutableIntList list = ImmutableIntList.of();
    for (int arg : argList) {
      list = list.append(remap(groupSet, arg));
    }
    return list;
  }

  private static int remap(ImmutableBitSet groupSet, int arg) {
    return arg < 0 ? -1 : groupSet.indexOf(arg);
  }

  private static String upperAggCallName(AggregateCall aggCall,
      int groupingSetIndex) {
    String baseName = aggCall.getName();
    if (baseName == null || baseName.isEmpty()) {
      baseName = aggCall.getAggregation().getName();
    }
    return baseName + "_g" + groupingSetIndex;
  }

  /**
   * Converts an aggregate relational expression that contains just one
   * distinct aggregate function (or perhaps several over the same arguments)
   * and no non-distinct aggregate functions.
   */
  private static RelBuilder convertMonopole(RelBuilder relBuilder, Aggregate aggregate,
      List<Integer> argList, int filterArg) {
    // For example,
    //    SELECT deptno, COUNT(DISTINCT sal), SUM(DISTINCT sal)
    //    FROM emp
    //    GROUP BY deptno
    //
    // becomes
    //
    //    SELECT deptno, COUNT(distinct_sal), SUM(distinct_sal)
    //    FROM (
    //      SELECT DISTINCT deptno, sal AS distinct_sal
    //      FROM EMP GROUP BY deptno)
    //    GROUP BY deptno

    // Project the columns of the GROUP BY plus the arguments
    // to the agg function.
    final Map<Integer, Integer> sourceOf = new HashMap<>();
    createSelectDistinct(relBuilder, aggregate, argList, filterArg, sourceOf);

    // Create an aggregate on top, with the new aggregate list.
    final List<AggregateCall> newAggCalls =
        Lists.newArrayList(aggregate.getAggCallList());
    rewriteAggCalls(newAggCalls, argList, sourceOf);
    final int cardinality = aggregate.getGroupSet().cardinality();
    relBuilder.push(
        aggregate.copy(aggregate.getTraitSet(), relBuilder.build(),
            ImmutableBitSet.range(cardinality), null, newAggCalls));
    return relBuilder;
  }

  /**
   * Converts all distinct aggregate calls to a given set of arguments.
   *
   * <p>This method is called several times, one for each set of arguments.
   * Each time it is called, it generates a JOIN to a new SELECT DISTINCT
   * relational expression, and modifies the set of top-level calls.
   *
   * @param aggregate Original aggregate
   * @param n         Ordinal of this in a join. {@code relBuilder} contains the
   *                  input relational expression (either the original
   *                  aggregate, the output from the previous call to this
   *                  method. {@code n} is 0 if we're converting the
   *                  first distinct aggregate in a query with no non-distinct
   *                  aggregates)
   * @param argList   Arguments to the distinct aggregate function
   * @param filterArg Argument that filters input to aggregate function, or -1
   * @param refs      Array of expressions which will be the projected by the
   *                  result of this rule. Those relating to this arg list will
   *                  be modified
   */
  private static void doRewrite(RelBuilder relBuilder, Aggregate aggregate, int n,
      List<Integer> argList, int filterArg, List<@Nullable RexInputRef> refs) {
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final List<RelDataTypeField> leftFields;
    if (n == 0) {
      leftFields = null;
    } else {
      leftFields = relBuilder.peek().getRowType().getFieldList();
    }

    // Aggregate(
    //     child,
    //     {COUNT(DISTINCT 1), SUM(DISTINCT 1), SUM(2)})
    //
    // becomes
    //
    // Aggregate(
    //     Join(
    //         child,
    //         Aggregate(child, < all columns > {}),
    //         INNER,
    //         <f2 = f5>))
    //
    // E.g.
    //   SELECT deptno, SUM(DISTINCT sal), COUNT(DISTINCT gender), MAX(age)
    //   FROM Emps
    //   GROUP BY deptno
    //
    // becomes
    //
    //   SELECT e.deptno, adsal.sum_sal, adgender.count_gender, e.max_age
    //   FROM (
    //     SELECT deptno, MAX(age) as max_age
    //     FROM Emps GROUP BY deptno) AS e
    //   JOIN (
    //     SELECT deptno, COUNT(gender) AS count_gender FROM (
    //       SELECT DISTINCT deptno, gender FROM Emps) AS dgender
    //     GROUP BY deptno) AS adgender
    //     ON e.deptno = adgender.deptno
    //   JOIN (
    //     SELECT deptno, SUM(sal) AS sum_sal FROM (
    //       SELECT DISTINCT deptno, sal FROM Emps) AS dsal
    //     GROUP BY deptno) AS adsal
    //   ON e.deptno = adsal.deptno
    //   GROUP BY e.deptno
    //
    // Note that if a query contains no non-distinct aggregates, then the
    // very first join/group by is omitted.  In the example above, if
    // MAX(age) is removed, then the sub-select of "e" is not needed, and
    // instead the two other group by's are joined to one another.

    // Project the columns of the GROUP BY plus the arguments
    // to the agg function.
    final Map<Integer, Integer> sourceOf = new HashMap<>();
    createSelectDistinct(relBuilder, aggregate, argList, filterArg, sourceOf);

    // Now compute the aggregate functions on top of the distinct dataset.
    // Each distinct agg becomes a non-distinct call to the corresponding
    // field from the right; for example,
    //   "COUNT(DISTINCT e.sal)"
    // becomes
    //   "COUNT(distinct_e.sal)".
    final List<AggregateCall> aggCallList = new ArrayList<>();
    final List<AggregateCall> aggCalls = aggregate.getAggCallList();

    final int groupCount = aggregate.getGroupCount();
    int i = groupCount - 1;
    for (AggregateCall aggCall : aggCalls) {
      ++i;

      // Ignore agg calls which are not distinct or have the wrong set
      // arguments. If we're rewriting aggs whose args are {sal}, we will
      // rewrite COUNT(DISTINCT sal) and SUM(DISTINCT sal) but ignore
      // COUNT(DISTINCT gender) or SUM(sal).
      if (!aggCall.isDistinct()) {
        continue;
      }
      if (!aggCall.getArgList().equals(argList)) {
        continue;
      }
      if (aggCall.filterArg != filterArg) {
        continue;
      }

      // Re-map arguments.
      final int argCount = aggCall.getArgList().size();
      final List<Integer> newArgs = new ArrayList<>(argCount);
      for (Integer arg : aggCall.getArgList()) {
        newArgs.add(requireNonNull(sourceOf.get(arg), () -> "sourceOf.get(" + arg + ")"));
      }
      final AggregateCall newAggCall =
          AggregateCall.create(aggCall.getParserPosition(), aggCall.getAggregation(), false,
              aggCall.isApproximate(), aggCall.ignoreNulls(), aggCall.rexList,
              newArgs, -1, aggCall.distinctKeys, aggCall.collation,
              aggCall.getType(), aggCall.getName());
      assert refs.get(i) == null;
      if (leftFields == null) {
        refs.set(i,
            new RexInputRef(groupCount + aggCallList.size(),
                newAggCall.getType()));
      } else {
        refs.set(i,
            new RexInputRef(leftFields.size() + groupCount
                + aggCallList.size(), newAggCall.getType()));
      }
      aggCallList.add(newAggCall);
    }

    final Map<Integer, Integer> map = new HashMap<>();
    for (Integer key : aggregate.getGroupSet()) {
      map.put(key, map.size());
    }
    final ImmutableBitSet newGroupSet = aggregate.getGroupSet().permute(map);
    assert newGroupSet
        .equals(ImmutableBitSet.range(aggregate.getGroupSet().cardinality()));
    ImmutableList<ImmutableBitSet> newGroupingSets = null;

    relBuilder.push(
        aggregate.copy(aggregate.getTraitSet(), relBuilder.build(),
            newGroupSet, newGroupingSets, aggCallList));

    // If there's no left child yet, no need to create the join
    if (leftFields == null) {
      return;
    }

    // Create the join condition. It is of the form
    //  'left.f0 = right.f0 and left.f1 = right.f1 and ...'
    // where {f0, f1, ...} are the GROUP BY fields.
    final List<RelDataTypeField> distinctFields =
        relBuilder.peek().getRowType().getFieldList();
    final List<RexNode> conditions = new ArrayList<>();
    for (i = 0; i < groupCount; ++i) {
      // null values form its own group
      // use "is not distinct from" so that the join condition
      // allows null values to match.
      conditions.add(
          rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
              RexInputRef.of(i, leftFields),
              new RexInputRef(leftFields.size() + i,
                  distinctFields.get(i).getType())));
    }

    // Join in the new 'select distinct' relation.
    relBuilder.join(JoinRelType.INNER, conditions);
  }

  private static void rewriteAggCalls(
      List<AggregateCall> newAggCalls,
      List<Integer> argList,
      Map<Integer, Integer> sourceOf) {
    // Rewrite the agg calls. Each distinct agg becomes a non-distinct call
    // to the corresponding field from the right; for example,
    // "COUNT(DISTINCT e.sal)" becomes   "COUNT(distinct_e.sal)".
    for (int i = 0; i < newAggCalls.size(); i++) {
      final AggregateCall aggCall = newAggCalls.get(i);

      // Ignore agg calls which are not distinct or have the wrong set
      // arguments. If we're rewriting aggregates whose args are {sal}, we will
      // rewrite COUNT(DISTINCT sal) and SUM(DISTINCT sal) but ignore
      // COUNT(DISTINCT gender) or SUM(sal).
      if (!aggCall.isDistinct()
          && aggCall.getAggregation().getDistinctOptionality() != Optionality.IGNORED) {
        continue;
      }
      if (!aggCall.getArgList().equals(argList)) {
        continue;
      }

      // Re-map arguments.
      final int argCount = aggCall.getArgList().size();
      final List<Integer> newArgs = new ArrayList<>(argCount);
      for (int j = 0; j < argCount; j++) {
        final Integer arg = aggCall.getArgList().get(j);
        newArgs.add(
            requireNonNull(sourceOf.get(arg),
                () -> "sourceOf.get(" + arg + ")"));
      }
      final AggregateCall newAggCall =
          AggregateCall.create(aggCall.getParserPosition(), aggCall.getAggregation(), false,
              aggCall.isApproximate(), aggCall.ignoreNulls(),
              aggCall.rexList, newArgs, -1,
              aggCall.distinctKeys, aggCall.collation,
              aggCall.getType(), aggCall.getName());
      newAggCalls.set(i, newAggCall);
    }
  }

  /**
   * Given an {@link org.apache.calcite.rel.core.Aggregate}
   * and the ordinals of the arguments to a
   * particular call to an aggregate function, creates a 'select distinct'
   * relational expression which projects the group columns and those
   * arguments but nothing else.
   *
   * <p>For example, given
   *
   * <blockquote>
   * <pre>select f0, count(distinct f1), count(distinct f2)
   * from t group by f0</pre>
   * </blockquote>
   *
   * <p>and the argument list
   *
   * <blockquote>{2}</blockquote>
   *
   * <p>returns
   *
   * <blockquote>
   * <pre>select distinct f0, f2 from t</pre>
   * </blockquote>
   *
   * <p>The <code>sourceOf</code> map is populated with the source of each
   * column; in this case sourceOf.get(0) = 0, and sourceOf.get(1) = 2.
   *
   * @param relBuilder Relational expression builder
   * @param aggregate Aggregate relational expression
   * @param argList   Ordinals of columns to make distinct
   * @param filterArg Ordinal of column to filter on, or -1
   * @param sourceOf  Out parameter, is populated with a map of where each
   *                  output field came from
   * @return Aggregate relational expression which projects the required
   * columns
   */
  private static RelBuilder createSelectDistinct(RelBuilder relBuilder,
      Aggregate aggregate, List<Integer> argList, int filterArg,
      Map<Integer, Integer> sourceOf) {
    relBuilder.push(aggregate.getInput());
    final PairList<RexNode, String> projects = PairList.of();
    final List<RelDataTypeField> childFields =
        relBuilder.peek().getRowType().getFieldList();
    for (int i : aggregate.getGroupSet()) {
      sourceOf.put(i, projects.size());
      RexInputRef.add2(projects, i, childFields);
    }
    for (Integer arg : argList) {
      if (filterArg >= 0) {
        // Implement
        //   agg(DISTINCT arg) FILTER $f
        // by generating
        //   SELECT DISTINCT ... CASE WHEN $f THEN arg ELSE NULL END AS arg
        // and then applying
        //   agg(arg)
        // as usual.
        //
        // It works except for (rare) agg functions that need to see null
        // values.
        final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        final RexInputRef filterRef = RexInputRef.of(filterArg, childFields);
        final Pair<RexNode, String> argRef = RexInputRef.of2(arg, childFields);
        RexNode condition =
            rexBuilder.makeCall(SqlStdOperatorTable.CASE, filterRef,
                argRef.left,
                rexBuilder.makeNullLiteral(argRef.left.getType()));
        sourceOf.put(arg, projects.size());
        projects.add(condition, "i$" + argRef.right);
        continue;
      }
      if (sourceOf.get(arg) != null) {
        continue;
      }
      sourceOf.put(arg, projects.size());
      RexInputRef.add2(projects, arg, childFields);
    }
    relBuilder.project(projects.leftList(), projects.rightList());

    // Get the distinct values of the GROUP BY fields and the arguments
    // to the agg functions.
    relBuilder.push(
        aggregate.copy(aggregate.getTraitSet(), relBuilder.build(),
            ImmutableBitSet.range(projects.size()), null, ImmutableList.of()));
    return relBuilder;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateExpandDistinctAggregatesRule.Config.of()
        .withOperandSupplier(b ->
            b.operand(LogicalAggregate.class).anyInputs());

    Config JOIN = DEFAULT.withUsingGroupingSets(false);

    @Override default AggregateExpandDistinctAggregatesRule toRule() {
      return new AggregateExpandDistinctAggregatesRule(this);
    }

    /** Whether to use GROUPING SETS, default true. */
    @Value.Default default boolean isUsingGroupingSets() {
      return true;
    }

    /** Sets {@link #isUsingGroupingSets()}. */
    Config withUsingGroupingSets(boolean usingGroupingSets);
  }
}
