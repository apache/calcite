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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.Util.skipLast;

/**
 * Planner rule that translates a distinct
 * {@link org.apache.calcite.rel.core.Intersect}
 * (<code>all</code> = <code>false</code>)
 * into a group of operators composed of
 * {@link org.apache.calcite.rel.core.Union},
 * {@link org.apache.calcite.rel.core.Aggregate}, etc.
 *
 * <p>The rule has a configuration option to control whether it should also perform
 * a (partial) aggregation pushdown in the union branches (default behavior).
 *
 * @see org.apache.calcite.rel.rules.UnionToDistinctRule
 * @see CoreRules#INTERSECT_TO_DISTINCT
 * @see CoreRules#INTERSECT_TO_DISTINCT_NO_AGGREGATE_PUSHDOWN
 */
@Value.Enclosing
public class IntersectToDistinctRule
    extends RelRule<IntersectToDistinctRule.Config>
    implements TransformationRule {

  /** Creates an IntersectToDistinctRule. */
  protected IntersectToDistinctRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public IntersectToDistinctRule(Class<? extends Intersect> intersectClass,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(intersectClass));
  }

  //~ Methods ----------------------------------------------------------------
  @Override public void onMatch(RelOptRuleCall call) {
    if (config.isAggregatePushdown()) {
      onMatchAggregatePushdown(call);
    } else {
      onMatchAggregateOnUnion(call);
    }
  }

  /**
   * Variant not performing a partial aggregation pushdown.
   *
   * <p>Original query:
   * <pre>{@code
   * SELECT job FROM "scott".emp WHERE deptno = 10
   * INTERSECT
   * SELECT job FROM "scott".emp WHERE deptno = 20
   * }</pre>
   *
   * <p>Query after conversion:
   * <pre>{@code
   * SELECT job
   * FROM (
   *   SELECT job, 0 AS i FROM "scott".emp WHERE deptno = 10
   *   UNION ALL
   *   SELECT job, 1 AS i FROM "scott".emp WHERE deptno = 20
   * )
   * GROUP BY job
   * HAVING COUNT(*) FILTER (WHERE i = 0) > 0
   *    AND COUNT(*) FILTER (WHERE i = 1) > 0
   * }</pre>
   */
  public void onMatchAggregateOnUnion(RelOptRuleCall call) {
    final Intersect intersect = call.rel(0);
    if (intersect.all) {
      return; // nothing we can do
    }
    final RelBuilder relBuilder = call.builder();
    final int oriFieldCount = intersect.getRowType().getFieldCount();
    final int branchCount = intersect.getInputs().size();

    List<AggCall> aggCalls = new ArrayList<>(branchCount);
    for (int i = 0; i < branchCount; ++i) {
      relBuilder.push(intersect.getInputs().get(i));
      List<RexNode> fields = new ArrayList<>(relBuilder.fields());
      fields.add(relBuilder.alias(relBuilder.literal(i), "i"));
      relBuilder.project(fields);
      aggCalls.add(
          relBuilder.countStar(null).filter(
              relBuilder.equals(relBuilder.field(oriFieldCount),
                  relBuilder.literal(i)))
              .as("count_i" + i));
    }

    // create union and aggregate above all the branches
    relBuilder.union(true, branchCount)
        .aggregate(relBuilder.groupKey(ImmutableBitSet.range(oriFieldCount)), aggCalls);

    // Generate filter count_i{n} > 0 for each branch
    List<RexNode> filters = new ArrayList<>(branchCount);
    for (int i = 0; i < branchCount; i++) {
      filters.add(
          relBuilder.greaterThan(relBuilder.field(oriFieldCount + i),
          relBuilder.literal(0)));
    }
    relBuilder.filter(filters);

    // Project all but the last added field (e.g. count_i{n})
    relBuilder.project(skipLast(relBuilder.fields(), branchCount));
    call.transformTo(relBuilder.build());
  }

  /**
   * Variant performing a partial aggregation pushdown.
   *
   * <p>Original query:
   * <pre>{@code
   * SELECT job FROM "scott".emp WHERE deptno = 10
   * INTERSECT
   * SELECT job FROM "scott".emp WHERE deptno = 20
   * }</pre>
   *
   * <p>Query after conversion:
   * <pre>{@code
   * SELECT job
   * FROM (
   *   SELECT job, COUNT(*) AS c
   *   FROM (
   *     SELECT job, COUNT(*) FROM "scott".emp
   *     WHERE deptno = 10 GROUP BY job
   *     UNION ALL
   *     SELECT job, COUNT(*) FROM "scott".emp
   *     WHERE deptno = 20 GROUP BY job)
   *   GROUP BY job)
   * WHERE c = 2
   * }</pre>
   */
  public void onMatchAggregatePushdown(RelOptRuleCall call) {
    final Intersect intersect = call.rel(0);
    if (intersect.all) {
      return; // nothing we can do
    }
    final RelOptCluster cluster = intersect.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelBuilder relBuilder = call.builder();

    // 1st level aggregate: create an aggregate(col_0, ..., col_n, count(*)), for each branch
    for (RelNode input : intersect.getInputs()) {
      relBuilder.push(input);
      relBuilder.aggregate(relBuilder.groupKey(relBuilder.fields()),
          relBuilder.countStar(null));
    }

    // create a union above all the branches
    final int branchCount = intersect.getInputs().size();
    relBuilder.union(true, branchCount);
    final RelNode union = relBuilder.peek();

    // 2nd level aggregate: create an aggregate(col_0, ..., col_n, count(*)), for each branch
    // the index of the counter is union.getRowType().getFieldList().size() - 1
    final int fieldCount = union.getRowType().getFieldCount();

    final ImmutableBitSet groupSet =
        ImmutableBitSet.range(fieldCount - 1);
    relBuilder.aggregate(relBuilder.groupKey(groupSet),
        relBuilder.countStar(null));

    // add a filter count(*) = #branches
    relBuilder.filter(
        relBuilder.equals(relBuilder.field(fieldCount - 1),
            rexBuilder.makeBigintLiteral(new BigDecimal(branchCount))));

    // Project all but the last field
    relBuilder.project(Util.skipLast(relBuilder.fields()));

    // the schema for intersect distinct matches that of the relation,
    // built here with an extra last column for the count,
    // which is projected out by the final project we added
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableIntersectToDistinctRule.Config.of()
        .withOperandFor(LogicalIntersect.class);

    Config NO_AGGREGATE_PUSHDOWN = DEFAULT
        .withDescription("IntersectToDistinctRule(NoAggregatePushDown)")
        .as(Config.class)
        .withAggregatePushdown(false);

    @Override default IntersectToDistinctRule toRule() {
      return new IntersectToDistinctRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Intersect> intersectClass) {
      return withOperandSupplier(b -> b.operand(intersectClass).anyInputs())
          .as(Config.class);
    }

    /** Whether to apply partial aggregate pushdown; default true. */
    @Value.Default default boolean isAggregatePushdown() {
      return true;
    }

    /** Sets {@link #isAggregatePushdown()} ()}. */
    Config withAggregatePushdown(boolean aggregatePushdown);
  }
}
