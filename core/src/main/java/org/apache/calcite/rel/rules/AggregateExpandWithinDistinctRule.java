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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

import static org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule.groupValue;
import static org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule.remap;

import static java.util.Objects.requireNonNull;

/**
 * Planner rule that rewrites an {@link Aggregate} that contains
 * {@code WITHIN DISTINCT} aggregate functions.
 *
 * <p>For example,
 * <blockquote>
 *   <pre>SELECT o.paymentType,
 *     COUNT(*) as "count",
 *     COUNT(*) WITHIN DISTINCT (o.orderId) AS orderCount,
 *     SUM(o.shipping) WITHIN DISTINCT (o.orderId) as sumShipping,
 *     SUM(i.units) as sumUnits
 *   FROM Orders AS o
 *   JOIN OrderItems AS i USING (orderId)
 *   GROUP BY o.paymentType</pre>
 * </blockquote>
 *
 * <p>becomes
 *
 * <blockquote>
 *   <pre>
 *     SELECT paymentType,
 *       COUNT(*) as "count",
 *       COUNT(*) FILTER (WHERE g = 0) AS orderCount,
 *       SUM(minShipping) FILTER (WHERE g = 0) AS sumShipping,
 *       SUM(sumUnits) FILTER (WHERE g = 1) as sumUnits
 *    FROM (
 *      SELECT o.paymentType,
 *        GROUPING(o.orderId) AS g,
 *        SUM(o.shipping) AS sumShipping,
 *        MIN(o.shipping) AS minShipping,
 *        SUM(i.units) AS sumUnits
 *        FROM Orders AS o
 *        JOIN OrderItems ON o.orderId = i.orderId
 *        GROUP BY GROUPING SETS ((o.paymentType), (o.paymentType, o.orderId)))
 *     GROUP BY o.paymentType</pre>
 * </blockquote>
 *
 * <p>By the way, note that {@code COUNT(*) WITHIN DISTINCT (o.orderId)}
 * is identical to {@code COUNT(DISTINCT o.orderId)}.
 * {@code WITHIN DISTINCT} is a generalization of aggregate(DISTINCT).
 * So, it is perhaps not surprising that the rewrite to {@code GROUPING SETS}
 * is similar.
 *
 * <p>If there are multiple arguments
 * (e.g. {@code SUM(a) WITHIN DISTINCT (x), SUM(a) WITHIN DISTINCT (y)})
 * the rule creates separate {@code GROUPING SET}s.
 */
@Value.Enclosing
public class AggregateExpandWithinDistinctRule
    extends RelRule<AggregateExpandWithinDistinctRule.Config> {

  /** Creates an AggregateExpandWithinDistinctRule. */
  protected AggregateExpandWithinDistinctRule(Config config) {
    super(config);
  }

  private static boolean hasWithinDistinct(Aggregate aggregate) {
    return aggregate.getAggCallList().stream()
            .anyMatch(c -> c.distinctKeys != null)
        // Wait until AggregateReduceFunctionsRule has dealt with AVG etc.
        && aggregate.getAggCallList().stream()
           .noneMatch(CoreRules.AGGREGATE_REDUCE_FUNCTIONS::canReduce)
        // Don't think we can handle GROUPING SETS yet
        && aggregate.getGroupType() == Aggregate.Group.SIMPLE;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);

    // Throughout this method, assume we are working on the following SQL:
    //
    //   SELECT deptno, SUM(sal), SUM(sal) WITHIN DISTINCT (job)
    //   FROM emp
    //   GROUP BY deptno
    //
    // or in algebra,
    //
    //   Aggregate($0, SUM($2), SUM($2) WITHIN DISTINCT ($4))
    //     Scan(emp)
    //
    // We plan to generate the following:
    //
    //   SELECT deptno, SUM(sal), SUM(sal) WITHIN DISTINCT (job)
    //   FROM (
    //     SELECT deptno, GROUPING(deptno, job), SUM(sal), MIN(sal)
    //     FROM emp
    //     GROUP BY GROUPING SETS ((deptno), (deptno, job)))
    //   GROUP BY deptno
    //
    // This rewrite also handles DISTINCT aggregates. We treat
    //
    //   SUM(DISTINCT sal)
    //   SUM(DISTINCT sal) WITHIN DISTINCT (job)
    //
    // as if the user had written
    //
    //   SUM(sal) WITHIN DISTINCT (sal)
    //

    final List<AggregateCall> aggCallList =
        aggregate.getAggCallList()
            .stream()
            .map(c -> unDistinct(c, aggregate.getInput()::fieldIsNullable))
            .collect(toImmutableList());

    // Find all within-distinct expressions.
    final Multimap<ImmutableBitSet, AggregateCall> argLists =
        ArrayListMultimap.create();
    // A bit set that represents an aggregate with no WITHIN DISTINCT.
    // Different from "WITHIN DISTINCT ()".
    final ImmutableBitSet notDistinct =
        ImmutableBitSet.of(aggregate.getInput().getRowType().getFieldCount());
    for (AggregateCall aggCall : aggCallList) {
      ImmutableBitSet distinctKeys = aggCall.distinctKeys;
      if (distinctKeys == null) {
        distinctKeys = notDistinct;
      } else {
        if (distinctKeys.intersects(aggregate.getGroupSet())) {
          // Remove group keys. E.g.
          //   sum(x) within distinct (y, z) ... group by y
          // can be simplified to
          //   sum(x) within distinct (z) ... group by y
          // Note that this assumes a single grouping set for the original agg.
          distinctKeys = distinctKeys.rebuild()
              .removeAll(aggregate.getGroupSet()).build();
        }
      }
      argLists.put(distinctKeys, aggCall);
    }

    // Compute the set of all grouping sets that will be used in the output
    // query. For each WITHIN DISTINCT aggregate call, we will need a grouping
    // set that is the union of the aggregate call's unique keys and the input
    // query's overall grouping. Redundant grouping sets can be reused for
    // multiple aggregate calls.
    final Set<ImmutableBitSet> groupSetTreeSet =
        new TreeSet<>(ImmutableBitSet.ORDERING);
    for (ImmutableBitSet key : argLists.keySet()) {
      groupSetTreeSet.add(
          (key == notDistinct)
              ? aggregate.getGroupSet()
              : ImmutableBitSet.of(key).union(aggregate.getGroupSet()));
    }

    final ImmutableList<ImmutableBitSet> groupSets =
        ImmutableList.copyOf(groupSetTreeSet);
    final boolean hasMultipleGroupSets = groupSets.size() > 1;
    final ImmutableBitSet fullGroupSet = ImmutableBitSet.union(groupSets);
    final Set<Integer> fullGroupOrderedSet = new LinkedHashSet<>();
    fullGroupOrderedSet.addAll(aggregate.getGroupSet().asSet());
    fullGroupOrderedSet.addAll(fullGroupSet.asSet());
    final ImmutableIntList fullGroupList =
        ImmutableIntList.copyOf(fullGroupOrderedSet);

    // Build the inner query
    //
    //   SELECT deptno, SUM(sal) AS sum_sal, MIN(sal) AS min_sal,
    //       MAX(sal) AS max_sal, GROUPING(deptno, job) AS g
    //   FROM emp
    //   GROUP BY GROUPING SETS ((deptno), (deptno, job))
    //
    // or in algebra,
    //
    //   Aggregate([($0), ($0, $4)], SUM($2), MIN($2), MAX($2), GROUPING($0, $4))
    //     Scan(emp)

    final RelBuilder b = call.builder();
    b.push(aggregate.getInput());
    final List<RelBuilder.AggCall> aggCalls = new ArrayList<>();

    // Helper class for building the inner query.
    // CHECKSTYLE: IGNORE 1
    class Registrar {
      final int g = fullGroupSet.cardinality();
      /** Map of input fields (below the original aggregation) and filter args
       * to inner query aggregate calls. */
      final Map<IntPair, Integer> args = new HashMap<>();
      /** Map of aggregate calls from the original aggregation to inner query
       * aggregate calls. */
      final Map<Integer, Integer> aggs = new HashMap<>();
      /** Map of aggregate calls from the original aggregation to inner-query
       * {@code COUNT(*)} calls, which are only needed for filters in the outer
       * aggregate when the original aggregate call does not ignore null
       * inputs. */
      final Map<Integer, Integer> counts = new HashMap<>();

      List<Integer> fields(List<Integer> fields, int filterArg) {
        return Util.transform(fields, f -> this.field(f, filterArg));
      }

      int field(int field, int filterArg) {
        return requireNonNull(args.get(IntPair.of(field, filterArg)));
      }

      /** Computes an aggregate call argument's values for a
       * {@code WITHIN DISTINCT} aggregate call.
       *
       * <p>For example, to compute
       * {@code SUM(x) WITHIN DISTINCT (y) GROUP BY (z)},
       * the inner aggregate must first group {@code x} by {@code (y, z)}
       * &mdash; using {@code MIN} to select the (hopefully) unique value of
       * {@code x} for each {@code (y, z)} group. Actually summing over the
       * grouped {@code x} values must occur in an outer aggregate.
       *
       * @param field Index of an input field that's used in a
       *         {@code WITHIN DISTINCT} aggregate call
       * @param filterArg Filter arg used in the original aggregate call, or
       *         {@code -1} if there is no filter. We use the same filter in
       *         the inner query.
       * @return Index of the inner query aggregate call representing the
       *         grouped field, which can be referenced in the outer query
       *         aggregate call
       */
      int register(int field, int filterArg) {
        return args.computeIfAbsent(IntPair.of(field, filterArg), j -> {
          final int ordinal = g + aggCalls.size();
          RelBuilder.AggCall groupedField =
              b.aggregateCall(SqlStdOperatorTable.MIN, b.field(field));
          aggCalls.add(
              filterArg < 0
                  ? groupedField
                  : groupedField.filter(b.field(filterArg)));
          if (config.throwIfNotUnique()) {
            groupedField =
                b.aggregateCall(SqlStdOperatorTable.MAX, b.field(field));
            aggCalls.add(
                filterArg < 0
                    ? groupedField
                    : groupedField.filter(b.field(filterArg)));
          }
          return ordinal;
        });
      }

      /** Registers an aggregate call that is <em>not</em> a
       * {@code WITHIN DISTINCT} call.
       *
       * <p>Unlike the case handled by {@link #register(int, int)} above,
       * aggregate calls without any distinct keys do not need a second round
       * of aggregation in the outer query, so they can be computed "as-is" in
       * the inner query.
       *
       * @param i Index of the aggregate call in the original aggregation
       * @param aggregateCall Original aggregate call
       * @return Index of the aggregate call in the computed inner query
       */
      int registerAgg(int i, RelBuilder.AggCall aggregateCall) {
        final int ordinal = g + aggCalls.size();
        aggs.put(i, ordinal);
        aggCalls.add(aggregateCall);
        return ordinal;
      }

      int getAgg(int i) {
        return requireNonNull(aggs.get(i));
      }

      /** Registers an extra {@code COUNT} aggregate call when it's needed to
       * filter out null inputs in the outer aggregate.
       *
       * <p>This should only be called for aggregate calls with filters. It's
       * possible that the filter would eliminate all input rows to the
       * {@code MIN} call in the inner query, so calls in the outer
       * aggregate may need to be aware of this. See usage of
       * {@link AggregateExpandWithinDistinctRule#mustBeCounted(AggregateCall)}.
       *
       * @param filterArg The original aggregate call's filter; must be
       *                 non-negative
       * @return Index of the {@code COUNT} call in the computed inner query
       */
      int registerCount(int filterArg) {
        assert filterArg >= 0;
        return counts.computeIfAbsent(filterArg, i -> {
          final int ordinal = g + aggCalls.size();
          aggCalls.add(b.aggregateCall(SqlStdOperatorTable.COUNT)
              .filter(b.field(filterArg)));
          return ordinal;
        });
      }

      int getCount(int filterArg) {
        return requireNonNull(counts.get(filterArg));
      }
    }

    final Registrar registrar = new Registrar();
    Ord.forEach(aggCallList, (c, i) -> {
      if (c.distinctKeys == null) {
        RelBuilder.AggCall aggCall =
            b.aggregateCall(c.getParserPosition(), c.getAggregation(), b.fields(c.getArgList()));
        if (c.hasFilter()) {
          aggCall = aggCall.filter(b.field(c.filterArg));
        }
        if (c.hasCollation()) {
          aggCall = aggCall.sort(b.fields(c.getCollation()));
        }
        registrar.registerAgg(i, aggCall);
      } else {
        for (int inputIdx : c.getArgList()) {
          registrar.register(inputIdx, c.filterArg);
        }
        if (mustBeCounted(c)) {
          registrar.registerCount(c.filterArg);
        }
      }
    });
    // Add an additional GROUPING() aggregate call so we can select only the
    // relevant inner-aggregate rows from the outer aggregate. If there is only
    // 1 grouping set (i.e. every aggregate call has the same distinct keys),
    // no GROUPING() call is necessary.
    final int grouping =
        hasMultipleGroupSets
            ? registrar.registerAgg(-1,
                b.aggregateCall(
                    SqlStdOperatorTable.GROUPING,
                    b.fields(fullGroupList)))
            : -1;
    b.aggregate(b.groupKey(fullGroupSet, groupSets), aggCalls);

    // Build the outer query
    //
    //   SELECT deptno,
    //     MIN(sum_sal) FILTER (g = 0),
    //     SUM(min_sal) FILTER (g = 1)
    //   FROM ( ... )
    //   GROUP BY deptno
    //
    // or in algebra,
    //
    //   Aggregate($0, SUM($2 WHERE $4 = 0), SUM($3 WHERE $4 = 1))
    //     Aggregate([($0), ($0, $2)], SUM($2), MIN($2), GROUPING($0, $4))
    //       Scan(emp)
    //
    // If throwIfNotUnique, the "SUM(min_sal) FILTER (g = 1)" term above becomes
    //
    //     SUM(min_sal) FILTER (
    //         $THROW_UNLESS(g <> 1 OR min_sal IS NOT DISTINCT FROM max_sal,
    //             'more than one distinct value in agg UNIQUE_VALUE')
    //         AND g = 1)

    aggCalls.clear();
    Ord.forEach(aggCallList, (c, i) -> {
      final List<RexNode> filters = new ArrayList<>();
      RexNode groupFilter = null;
      if (hasMultipleGroupSets) {
        groupFilter =
            b.equals(
                b.field(grouping),
                b.literal(
                    groupValue(fullGroupList,
                        union(aggregate.getGroupSet(), c.distinctKeys))));
        filters.add(groupFilter);
      }
      RelBuilder.AggCall aggCall;
      if (c.distinctKeys == null) {
        aggCall =
            b.aggregateCall(c.getParserPosition(), SqlStdOperatorTable.MIN,
                b.field(registrar.getAgg(i)));
      } else {
        // The inputs to this aggregate are outputs from MIN() calls from the
        // inner agg, and MIN() returns null iff it has no non-null inputs,
        // which can only happen if an original aggregate's filter causes all
        // non-null input rows to be discarded for a particular group in the
        // inner aggregate. In this case, it should be ignored by the outer
        // aggregate as well. In case the aggregate call does not naturally
        // ignore null inputs, we add a filter based on a COUNT() in the inner
        // aggregate.
        aggCall =
            b.aggregateCall(c.getParserPosition(), c.getAggregation(),
                b.fields(registrar.fields(c.getArgList(), c.filterArg)));

        if (mustBeCounted(c)) {
          filters.add(
              b.greaterThan(b.field(registrar.getCount(c.filterArg)),
                  b.literal(0)));
        }

        if (config.throwIfNotUnique()) {
          for (int j : c.getArgList()) {
            RexNode isUniqueCondition =
                b.isNotDistinctFrom(
                    b.field(registrar.field(j, c.filterArg)),
                    b.field(registrar.field(j, c.filterArg) + 1));
            if (groupFilter != null) {
              isUniqueCondition = b.or(b.not(groupFilter), isUniqueCondition);
            }
            String message = "more than one distinct value in agg UNIQUE_VALUE";
            filters.add(
                b.call(SqlInternalOperators.THROW_UNLESS, isUniqueCondition,
                    b.literal(message)));
          }
        }
      }
      if (!filters.isEmpty()) {
        aggCall = aggCall.filter(b.and(filters));
      }
      aggCalls.add(aggCall);
    });

    b.aggregate(
        b.groupKey(
            remap(fullGroupSet, aggregate.getGroupSet()),
            remap(fullGroupSet, aggregate.getGroupSets())),
        aggCalls);
    b.convert(aggregate.getRowType(), false);
    call.transformTo(b.build());
  }

  private static boolean mustBeCounted(AggregateCall aggCall) {
    // Always count filtered inner aggregates to be safe.
    //
    // It's possible that, for some aggregate calls (namely, those that
    // completely ignore null inputs), we could neglect counting the
    // grouped-and-filtered rows of the inner aggregate and filtering the empty
    // ones out from the outer aggregate, since those empty groups would produce
    // null values as the result of MIN and thus be ignored by the outer
    // aggregate anyway.
    //
    // Note that using "aggCall.ignoreNulls()" is not sufficient to determine
    // when it's safe to do this, since for COUNT the value of ignoreNulls()
    // should generally be true even though COUNT(*) will never ignore anything.
    return aggCall.hasFilter();
  }

  /** Converts a {@code DISTINCT} aggregate call into an equivalent one with
   * {@code WITHIN DISTINCT}.
   *
   * <p>Examples:
   * <ul>
   * <li>{@code SUM(DISTINCT x)} &rarr;
   *     {@code SUM(x) WITHIN DISTINCT (x)} has distinct key (x);
   * <li>{@code SUM(DISTINCT x)} WITHIN DISTINCT (y) &rarr;
   *     {@code SUM(x) WITHIN DISTINCT (x)} has distinct key (x);
   * <li>{@code SUM(x)} WITHIN DISTINCT (y, z) has distinct key (y, z);
   * <li>{@code SUM(x)} has no distinct key.
   * </ul>
   */
  private static AggregateCall unDistinct(AggregateCall aggregateCall,
      IntPredicate isNullable) {
    if (aggregateCall.isDistinct()) {
      final List<Integer> newArgList = aggregateCall.getArgList()
          .stream()
          .filter(i ->
              aggregateCall.getAggregation().getKind() != SqlKind.COUNT
                  || aggregateCall.hasFilter()
                  || isNullable.test(i))
          .collect(Collectors.toList());
      return aggregateCall.withDistinct(false)
          .withDistinctKeys(ImmutableBitSet.of(aggregateCall.getArgList()))
          .withArgList(newArgList);
    }
    return aggregateCall;
  }

  private static ImmutableBitSet union(ImmutableBitSet s0,
      @Nullable ImmutableBitSet s1) {
    return s1 == null ? s0 : s0.union(s1);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateExpandWithinDistinctRule.Config.of()
        .withOperandSupplier(b -> b.operand(LogicalAggregate.class)
            .predicate(AggregateExpandWithinDistinctRule::hasWithinDistinct)
            .anyInputs());

    @Override default AggregateExpandWithinDistinctRule toRule() {
      return new AggregateExpandWithinDistinctRule(this);
    }

    /** Whether the code generated by the rule should throw if the arguments
     * are not functionally dependent.
     *
     * <p>For example, if implementing {@code SUM(sal) WITHIN DISTINCT job)} ...
     * {@code GROUP BY deptno},
     * suppose that within department 10, (job, sal) has the values
     * ('CLERK', 100), ('CLERK', 120), ('MANAGER', 150), ('MANAGER', 150). If
     * {@code throwIfNotUnique} is true, the query would throw because of the
     * values [100, 120]; if false, the query would sum the distinct values
     * [100, 120, 150]. */
    @Value.Default default boolean throwIfNotUnique() {
      return true;
    }

    /** Sets {@link #throwIfNotUnique()}. */
    Config withThrowIfNotUnique(boolean throwIfNotUnique);
  }
}
