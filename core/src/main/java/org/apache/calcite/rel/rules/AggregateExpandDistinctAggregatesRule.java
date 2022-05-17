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
import org.apache.calcite.sql.SqlAggFunction;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    Preconditions.checkState(distinctCallArgLists.size() > 0,
        "containsDistinctCall lied");

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
    // we can generate multi-phase aggregates
    if (distinctAggCalls.size() == 1 // one distinct aggregate
        && filterCount == 0 // no filter
        && unsupportedNonDistinctAggCallCount == 0 // sum/min/max/count in non-distinct aggregate
        && nonDistinctAggCalls.size() > 0) { // one or more non-distinct aggregates
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
    Preconditions.checkArgument(argLists.size() == 1);

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
            AggregateCall.create(aggCall.getAggregation(), false,
                aggCall.isApproximate(), aggCall.ignoreNulls(),
                aggCall.getArgList(), -1, aggCall.distinctKeys,
                aggCall.collation, bottomGroupSet.cardinality(),
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
            AggregateCall.create(aggCall.getAggregation(),
                false,
                aggCall.isApproximate(),
                aggCall.ignoreNulls(),
                newArgList,
                -1,
                aggCall.distinctKeys,
                aggCall.collation,
                originalGroupSet.cardinality(),
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
              AggregateCall.create(new SqlSumEmptyIsZeroAggFunction(), false,
                  aggCall.isApproximate(), aggCall.ignoreNulls(),
                  newArgs, -1, aggCall.distinctKeys, aggCall.collation,
                  originalGroupSet.cardinality(), relBuilder.peek(),
                  null,
                  aggCall.getName());
        } else {
          newCall =
              AggregateCall.create(aggCall.getAggregation(), false,
                  aggCall.isApproximate(), aggCall.ignoreNulls(),
                  newArgs, -1, aggCall.distinctKeys, aggCall.collation,
                  originalGroupSet.cardinality(),
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

  private static void rewriteUsingGroupingSets(RelOptRuleCall call,
      Aggregate aggregate) {
    final Set<ImmutableBitSet> groupSetTreeSet =
        new TreeSet<>(ImmutableBitSet.ORDERING);
    // GroupSet to distinct filter arg map,
    // filterArg will be -1 for non-distinct agg call.

    // Using `Set` here because it's possible that two agg calls
    // have different filterArgs but same groupSet.
    final Map<ImmutableBitSet, Set<Integer>> distinctFilterArgMap = new HashMap<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      ImmutableBitSet groupSet;
      int filterArg;
      if (!aggCall.isDistinct()) {
        filterArg = -1;
        groupSet = aggregate.getGroupSet();
        groupSetTreeSet.add(aggregate.getGroupSet());
      } else {
        filterArg = aggCall.filterArg;
        groupSet =
            ImmutableBitSet.of(aggCall.getArgList())
                .setIf(filterArg, filterArg >= 0)
                .union(aggregate.getGroupSet());
        groupSetTreeSet.add(groupSet);
      }
      Set<Integer> filterList = distinctFilterArgMap
          .computeIfAbsent(groupSet, g -> new HashSet<>());
      filterList.add(filterArg);
    }

    final ImmutableList<ImmutableBitSet> groupSets =
        ImmutableList.copyOf(groupSetTreeSet);
    final ImmutableBitSet fullGroupSet = ImmutableBitSet.union(groupSets);

    final List<AggregateCall> distinctAggCalls = new ArrayList<>();
    for (Pair<AggregateCall, String> aggCall : aggregate.getNamedAggCalls()) {
      if (!aggCall.left.isDistinct()) {
        AggregateCall newAggCall =
            aggCall.left.adaptTo(aggregate.getInput(),
                aggCall.left.getArgList(), aggCall.left.filterArg,
                aggregate.getGroupCount(), fullGroupSet.cardinality());
        distinctAggCalls.add(newAggCall.withName(aggCall.right));
      }
    }

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(aggregate.getInput());
    final int groupCount = fullGroupSet.cardinality();

    // Get the base ordinal of filter args for different groupSets.
    final Map<Pair<ImmutableBitSet, Integer>, Integer> filters = new LinkedHashMap<>();
    int z = groupCount + distinctAggCalls.size();
    for (ImmutableBitSet groupSet: groupSets) {
      Set<Integer> filterArgList = distinctFilterArgMap.get(groupSet);
      for (Integer filterArg: requireNonNull(filterArgList, "filterArgList")) {
        filters.put(Pair.of(groupSet, filterArg), z);
        z += 1;
      }
    }

    distinctAggCalls.add(
        AggregateCall.create(SqlStdOperatorTable.GROUPING, false, false, false,
            ImmutableIntList.copyOf(fullGroupSet), -1,
            null, RelCollations.EMPTY,
            groupSets.size(), relBuilder.peek(), null, "$g"));

    relBuilder.aggregate(
        relBuilder.groupKey(fullGroupSet, groupSets),
        distinctAggCalls);

    // GROUPING returns an integer (0 or 1). Add a project to convert those
    // values to BOOLEAN.
    if (!filters.isEmpty()) {
      final List<RexNode> nodes = new ArrayList<>(relBuilder.fields());
      final RexNode nodeZ = nodes.remove(nodes.size() - 1);
      for (Map.Entry<Pair<ImmutableBitSet, Integer>, Integer> entry : filters.entrySet()) {
        final long v = groupValue(fullGroupSet.asList(), entry.getKey().left);
        int distinctFilterArg = remap(fullGroupSet, entry.getKey().right);
        RexNode expr = relBuilder.equals(nodeZ, relBuilder.literal(v));
        if (distinctFilterArg > -1) {
          // 'AND' the filter of the distinct aggregate call and the group value.
          expr = relBuilder.and(expr,
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

    int x = groupCount;
    final ImmutableBitSet groupSet = aggregate.getGroupSet();
    final List<AggregateCall> newCalls = new ArrayList<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      final int newFilterArg;
      final List<Integer> newArgList;
      final SqlAggFunction aggregation;
      if (!aggCall.isDistinct()) {
        aggregation = SqlStdOperatorTable.MIN;
        newArgList = ImmutableIntList.of(x++);
        newFilterArg = requireNonNull(filters.get(Pair.of(groupSet, -1)),
            "filters.get(Pair.of(groupSet, -1))");
      } else {
        aggregation = aggCall.getAggregation();
        newArgList = remap(fullGroupSet, aggCall.getArgList());
        final ImmutableBitSet newGroupSet = ImmutableBitSet.of(aggCall.getArgList())
            .setIf(aggCall.filterArg, aggCall.filterArg >= 0)
            .union(groupSet);
        newFilterArg = requireNonNull(filters.get(Pair.of(newGroupSet, aggCall.filterArg)),
            "filters.get(of(newGroupSet, aggCall.filterArg))");
      }
      final AggregateCall newCall =
          AggregateCall.create(aggregation, false,
              aggCall.isApproximate(), aggCall.ignoreNulls(),
              newArgList, newFilterArg, aggCall.distinctKeys, aggCall.collation,
              aggregate.getGroupCount(), relBuilder.peek(), null, aggCall.name);
      newCalls.add(newCall);
    }

    relBuilder.aggregate(
        relBuilder.groupKey(
            remap(fullGroupSet, groupSet),
            remap(fullGroupSet, aggregate.getGroupSets())),
        newCalls);
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

      // Re-map arguments.
      final int argCount = aggCall.getArgList().size();
      final List<Integer> newArgs = new ArrayList<>(argCount);
      for (Integer arg : aggCall.getArgList()) {
        newArgs.add(requireNonNull(sourceOf.get(arg), () -> "sourceOf.get(" + arg + ")"));
      }
      final int newFilterArg =
          aggCall.filterArg < 0 ? -1
              : requireNonNull(sourceOf.get(aggCall.filterArg),
                  () -> "sourceOf.get(" + aggCall.filterArg + ")");
      final AggregateCall newAggCall =
          AggregateCall.create(aggCall.getAggregation(), false,
              aggCall.isApproximate(), aggCall.ignoreNulls(),
              newArgs, newFilterArg, aggCall.distinctKeys, aggCall.collation,
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
          AggregateCall.create(aggCall.getAggregation(), false,
              aggCall.isApproximate(), aggCall.ignoreNulls(), newArgs, -1,
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
    final List<Pair<RexNode, String>> projects = new ArrayList<>();
    final List<RelDataTypeField> childFields =
        relBuilder.peek().getRowType().getFieldList();
    for (int i : aggregate.getGroupSet()) {
      sourceOf.put(i, projects.size());
      projects.add(RexInputRef.of2(i, childFields));
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
        projects.add(Pair.of(condition, "i$" + argRef.right));
        continue;
      }
      if (sourceOf.get(arg) != null) {
        continue;
      }
      sourceOf.put(arg, projects.size());
      projects.add(RexInputRef.of2(arg, childFields));
    }
    relBuilder.project(Pair.left(projects), Pair.right(projects));

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
