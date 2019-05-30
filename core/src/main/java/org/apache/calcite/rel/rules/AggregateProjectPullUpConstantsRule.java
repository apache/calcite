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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Planner rule that removes constant keys from an
 * {@link org.apache.calcite.rel.core.Aggregate}.
 *
 * <p>Constant fields are deduced using
 * {@link RelMetadataQuery#getPulledUpPredicates(RelNode)}; the input does not
 * need to be a {@link org.apache.calcite.rel.core.Project}.
 *
 * <p>This rules never removes the last column, because {@code Aggregate([])}
 * returns 1 row even if its input is empty.
 *
 * <p>Since the transformed relational expression has to match the original
 * relational expression, the constants are placed in a projection above the
 * reduced aggregate. If those constants are not used, another rule will remove
 * them from the project.
 */
public class AggregateProjectPullUpConstantsRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final AggregateProjectPullUpConstantsRule INSTANCE =
      new AggregateProjectPullUpConstantsRule(LogicalAggregate.class,
          LogicalProject.class, RelFactories.LOGICAL_BUILDER,
          "AggregateProjectPullUpConstantsRule");

  /** More general instance that matches any relational expression. */
  public static final AggregateProjectPullUpConstantsRule INSTANCE2 =
      new AggregateProjectPullUpConstantsRule(LogicalAggregate.class,
          RelNode.class, RelFactories.LOGICAL_BUILDER,
          "AggregatePullUpConstantsRule");

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AggregateProjectPullUpConstantsRule.
   *
   * @param aggregateClass Aggregate class
   * @param inputClass Input class, such as {@link LogicalProject}
   * @param relBuilderFactory Builder for relational expressions
   * @param description Description, or null to guess description
   */
  public AggregateProjectPullUpConstantsRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends RelNode> inputClass,
      RelBuilderFactory relBuilderFactory, String description) {
    super(
        operandJ(aggregateClass, null, Aggregate::isSimple,
            operand(inputClass, any())),
        relBuilderFactory, description);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = call.rel(1);

    final int groupCount = aggregate.getGroupCount();
    if (groupCount == 1) {
      // No room for optimization since we cannot convert from non-empty
      // GROUP BY list to the empty one.
      return;
    }

    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelOptPredicateList predicates =
        mq.getPulledUpPredicates(aggregate.getInput());
    if (predicates == null) {
      return;
    }
    final NavigableMap<Integer, RexNode> map = new TreeMap<>();
    for (int key : aggregate.getGroupSet()) {
      final RexInputRef ref =
          rexBuilder.makeInputRef(aggregate.getInput(), key);
      if (predicates.constantMap.containsKey(ref)) {
        map.put(key, predicates.constantMap.get(ref));
      }
    }

    // None of the group expressions are constant. Nothing to do.
    if (map.isEmpty()) {
      return;
    }

    if (groupCount == map.size()) {
      // At least a single item in group by is required.
      // Otherwise "GROUP BY 1, 2" might be altered to "GROUP BY ()".
      // Removing of the first element is not optimal here,
      // however it will allow us to use fast path below (just trim
      // groupCount).
      map.remove(map.navigableKeySet().first());
    }

    ImmutableBitSet newGroupSet = aggregate.getGroupSet();
    for (int key : map.keySet()) {
      newGroupSet = newGroupSet.clear(key);
    }
    final int newGroupCount = newGroupSet.cardinality();

    // If the constants are on the trailing edge of the group list, we just
    // reduce the group count.
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(input);

    // Clone aggregate calls.
    final List<AggregateCall> newAggCalls = new ArrayList<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      newAggCalls.add(
          aggCall.adaptTo(input, aggCall.getArgList(), aggCall.filterArg,
              groupCount, newGroupCount));
    }
    relBuilder.aggregate(relBuilder.groupKey(newGroupSet), newAggCalls);

    // Create a projection back again.
    List<Pair<RexNode, String>> projects = new ArrayList<>();
    int source = 0;
    for (RelDataTypeField field : aggregate.getRowType().getFieldList()) {
      RexNode expr;
      final int i = field.getIndex();
      if (i >= groupCount) {
        // Aggregate expressions' names and positions are unchanged.
        expr = relBuilder.field(i - map.size());
      } else {
        int pos = aggregate.getGroupSet().nth(i);
        if (map.containsKey(pos)) {
          // Re-generate the constant expression in the project.
          RelDataType originalType =
              aggregate.getRowType().getFieldList().get(projects.size()).getType();
          if (!originalType.equals(map.get(pos).getType())) {
            expr = rexBuilder.makeCast(originalType, map.get(pos), true);
          } else {
            expr = map.get(pos);
          }
        } else {
          // Project the aggregation expression, in its original
          // position.
          expr = relBuilder.field(source);
          ++source;
        }
      }
      projects.add(Pair.of(expr, field.getName()));
    }
    relBuilder.project(Pair.left(projects), Pair.right(projects)); // inverse
    call.transformTo(relBuilder.build());
  }

}

// End AggregateProjectPullUpConstantsRule.java
