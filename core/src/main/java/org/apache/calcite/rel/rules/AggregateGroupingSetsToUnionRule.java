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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that converts a {@link org.apache.calcite.rel.core.Aggregate} with
 * {@code GROUPING SETS} into a {@code UNION ALL} of simpler aggregates.
 *
 * <p>Example transformation:
 * <pre>{@code
 *   SELECT a, b, c FROM t GROUP BY GROUPING SETS ((a,b), (a,c))
 * }</pre>
 *
 * <p>Transformed to:
 *
 * <pre>{@code
 *   SELECT a, b, NULL AS c FROM t GROUP BY a, b
 *   UNION ALL
 *   SELECT a, NULL AS b, c FROM t GROUP BY a, c
 * }</pre>
 */
@Value.Enclosing
public class AggregateGroupingSetsToUnionRule
    extends RelRule<AggregateGroupingSetsToUnionRule.Config>
    implements SubstitutionRule {

  /** Creates an AggregateGroupingSetsToUnionRule. */
  protected AggregateGroupingSetsToUnionRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);

    if (Aggregate.isSimple(aggregate)) {
      return;
    }

    final RelBuilder relBuilder = call.builder();
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();

    final RelNode input = aggregate.getInput();
    final RelDataType rowType = aggregate.getRowType();
    final ImmutableBitSet oriGroupSet = aggregate.getGroupSet();
    final List<RelNode> unionInputs = new ArrayList<>();

    for (ImmutableBitSet subGroupSet : aggregate.getGroupSets()) {
      relBuilder.push(input);
      final List<RexNode> subProjects = new ArrayList<>();

      // Process aggregate group set
      RelDataType subAggregateType =
          Aggregate.deriveRowType(relBuilder.getTypeFactory(), relBuilder.peek().getRowType(),
              false, subGroupSet, ImmutableList.of(subGroupSet), ImmutableList.of());
      for (int i = 0; i < oriGroupSet.cardinality(); i++) {
        int groupKey = oriGroupSet.nth(i);
        if (subGroupSet.get(groupKey)) {
          subProjects.add(
              RexInputRef.of(
                  subGroupSet.indexOf(groupKey),
                  subAggregateType));
        } else {
          // If the groupKey is not in the GroupSet, use null as a placeholder.
          subProjects.add(rexBuilder.makeNullLiteral(relBuilder.field(groupKey).getType()));
        }
      }

      // Process aggregate calls
      List<AggregateCall> subAggCalls = new ArrayList<>();
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        switch (aggCall.getAggregation().getKind()) {
        case GROUPING:
          int groupingValue = evaluateGroupingFunction(subGroupSet, aggCall.getArgList());
          subProjects.add(
              rexBuilder.makeLiteral(groupingValue, aggCall.getType(), true));
          break;
        case GROUP_ID:
          // GROUP_ID is removed during RelNode conversion, no handling needed here.
          return;
        case GROUPING_ID:
          // The GROUPING_ID aggregate function has been marked as deprecated
          // and is no longer supported.
          return;
        default:
          subProjects.add(
              new RexInputRef(
                  subGroupSet.cardinality() + subAggCalls.size(),
                  aggCall.getType()));
          subAggCalls.add(aggCall);
          break;
        }
      }

      relBuilder.aggregate(relBuilder.groupKey(subGroupSet), subAggCalls)
          .project(subProjects, rowType.getFieldNames());

      unionInputs.add(relBuilder.build());
    }

    relBuilder.pushAll(unionInputs)
        .union(true, unionInputs.size());

    call.transformTo(relBuilder.build());
  }

  private static int evaluateGroupingFunction(ImmutableBitSet groupSet, List<Integer> argIndices) {
    final int argCount = argIndices.size();
    if (argCount >= Integer.SIZE) {
      throw new IllegalArgumentException(
          "Too many grouping keys. Maximum is " + (Integer.SIZE - 1) + " for grouping functions.");
    }

    int result = 0;
    for (int k = 0; k < argCount; k++) {
      int index = argIndices.get(argCount - 1 - k);
      if (!groupSet.get(index)) {
        result |= 1 << k;
      }
    }
    return result;
  }


  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateGroupingSetsToUnionRule.Config.of()
        .withOperandFor(Aggregate.class, Values.class);

    @Override default AggregateGroupingSetsToUnionRule toRule() {
      return new AggregateGroupingSetsToUnionRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass,
        Class<? extends Values> valuesClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass)
              .predicate(aggregate -> aggregate.getGroupType() != Aggregate.Group.SIMPLE)
              .anyInputs())
          .as(Config.class);
    }
  }
}
