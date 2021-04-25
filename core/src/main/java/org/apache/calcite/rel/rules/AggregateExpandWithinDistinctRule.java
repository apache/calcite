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
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import static org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule.groupValue;
import static org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule.remap;

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
        // Don't know that we can handle FILTER yet
        && aggregate.getAggCallList().stream().noneMatch(c -> c.filterArg >= 0)
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
    //   Aggregate($0, SUM($2), SUM($3) WITHIN DISTINCT ($4))
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

    // TODO: handle "agg(x) filter (b)"

    final List<AggregateCall> aggCallList =
        aggregate.getAggCallList()
            .stream()
            .map(c -> unDistinct(c, aggregate.getInput()::fieldIsNullable))
            .collect(Util.toImmutableList());

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
          distinctKeys = distinctKeys.rebuild()
              .removeAll(aggregate.getGroupSet()).build();
        }
      }
      argLists.put(distinctKeys, aggCall);
      assert aggCall.filterArg < 0;
    }

    final Set<ImmutableBitSet> groupSetTreeSet =
        new TreeSet<>(ImmutableBitSet.ORDERING);
    groupSetTreeSet.add(aggregate.getGroupSet());
    for (ImmutableBitSet key : argLists.keySet()) {
      if (key == notDistinct) {
        continue;
      }
      groupSetTreeSet.add(
          ImmutableBitSet.of(key).union(aggregate.getGroupSet()));
    }

    final ImmutableList<ImmutableBitSet> groupSets =
        ImmutableList.copyOf(groupSetTreeSet);
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
    //   Aggregate([($0), ($0, $2)], SUM($2), MIN($2), MAX($2), GROUPING($0, $4))
    //     Scan(emp)

    final RelBuilder b = call.builder();
    b.push(aggregate.getInput());
    final List<RelBuilder.AggCall> aggCalls = new ArrayList<>();

    // CHECKSTYLE: IGNORE 1
    class Registrar {
      final int g = fullGroupSet.cardinality();
      final Map<Integer, Integer> args = new HashMap<>();
      final Map<Integer, Integer> aggs = new HashMap<>();

      List<Integer> fields(List<Integer> fields) {
        return Util.transform(fields, this::field);
      }

      int field(int field) {
        return Objects.requireNonNull(args.get(field));
      }

      int register(int field) {
        return args.computeIfAbsent(field, j -> {
          final int ordinal = g + aggCalls.size();
          aggCalls.add(
              b.aggregateCall(SqlStdOperatorTable.MIN, b.field(j)));
          if (config.throwIfNotUnique()) {
            aggCalls.add(
                b.aggregateCall(SqlStdOperatorTable.MAX, b.field(j)));
          }
          return ordinal;
        });
      }

      int registerAgg(int i, RelBuilder.AggCall aggregateCall) {
        final int ordinal = g + aggCalls.size();
        aggs.put(i, ordinal);
        aggCalls.add(aggregateCall);
        return ordinal;
      }

      int getAgg(int i) {
        return Objects.requireNonNull(aggs.get(i));
      }
    }

    final Registrar registrar = new Registrar();
    Ord.forEach(aggCallList, (c, i) -> {
      if (c.distinctKeys == null) {
        registrar.registerAgg(i,
            b.aggregateCall(c.getAggregation(),
                b.fields(c.getArgList())));
      } else {
        c.getArgList().forEach(registrar::register);
      }
    });
    final int grouping =
        registrar.registerAgg(-1,
            b.aggregateCall(SqlStdOperatorTable.GROUPING,
                b.fields(fullGroupList)));
    b.aggregate(
        b.groupKey(fullGroupSet,
            (Iterable<ImmutableBitSet>) groupSets), aggCalls);

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
      final RexNode groupFilter = b.equals(b.field(grouping),
          b.literal(
              groupValue(fullGroupList,
                  union(aggregate.getGroupSet(), c.distinctKeys))));
      filters.add(groupFilter);
      final RelBuilder.AggCall aggCall;
      if (c.distinctKeys == null) {
        aggCall = b.aggregateCall(SqlStdOperatorTable.MIN,
            b.field(registrar.getAgg(i)));
      } else {
        aggCall = b.aggregateCall(c.getAggregation(),
            b.fields(registrar.fields(c.getArgList())));

        if (config.throwIfNotUnique()) {
          for (int j : c.getArgList()) {
            String message = "more than one distinct value in agg UNIQUE_VALUE";
            filters.add(
                b.call(SqlInternalOperators.THROW_UNLESS,
                    b.or(b.not(groupFilter),
                        b.isNotDistinctFrom(b.field(registrar.field(j)),
                            b.field(registrar.field(j) + 1))),
                    b.literal(message)));
          }
        }
      }
      aggCalls.add(aggCall.filter(b.and(filters)));
    });

    b.aggregate(
        b.groupKey(
            remap(fullGroupSet, aggregate.getGroupSet()),
            (Iterable<ImmutableBitSet>)
                remap(fullGroupSet, aggregate.getGroupSets())),
        aggCalls);
    b.convert(aggregate.getRowType(), false);
    call.transformTo(b.build());
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
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
        .withOperandSupplier(b -> b.operand(LogicalAggregate.class)
            .predicate(AggregateExpandWithinDistinctRule::hasWithinDistinct)
            .anyInputs())
        .as(AggregateExpandWithinDistinctRule.Config.class);

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
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean throwIfNotUnique();

    /** Sets {@link #throwIfNotUnique()}. */
    Config withThrowIfNotUnique(boolean throwIfNotUnique);
  }
}
