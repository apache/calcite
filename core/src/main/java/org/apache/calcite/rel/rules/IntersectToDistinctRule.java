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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;

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
 * <h2>Example</h2>
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
 *
 * @see org.apache.calcite.rel.rules.UnionToDistinctRule
 * @see CoreRules#INTERSECT_TO_DISTINCT
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

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableIntersectToDistinctRule.Config.of()
        .withOperandFor(LogicalIntersect.class);

    @Override default IntersectToDistinctRule toRule() {
      return new IntersectToDistinctRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Intersect> intersectClass) {
      return withOperandSupplier(b -> b.operand(intersectClass).anyInputs())
          .as(Config.class);
    }
  }
}
