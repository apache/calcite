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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    final Aggregate agg = call.rel(0);
    final RelBuilder relBuilder = call.builder();
    final RexBuilder rexBuilder = agg.getCluster().getRexBuilder();

    final RelNode oriAggInput = agg.getInput();
    final RelDataType oriRowType = agg.getRowType();
    final int oriGroupCount = agg.getGroupCount();
    final ImmutableBitSet oriGroupSet = agg.getGroupSet();

    List<RelNode> unionInputs = new ArrayList<>();
    for (ImmutableBitSet groupSet : agg.getGroupSets()) {
      Map<Integer, RexNode> specialProjections = new HashMap<>();

      List<Integer> missingGroups = oriGroupSet.except(groupSet).toList();
      for (int i : missingGroups) {
        specialProjections.put(
            i,
            rexBuilder.makeNullLiteral(oriRowType.getFieldList().get(i).getType()));
      }

      List<AggregateCall> aggCalls = new ArrayList<>();
      for (int i = 0; i < agg.getAggCallList().size(); i++) {
        AggregateCall oriAggCall = agg.getAggCallList().get(i);
        switch (oriAggCall.getAggregation().getKind()) {
        case GROUPING:
          int groupingValue = calculateGroupingValue(groupSet, oriAggCall.getArgList());
          specialProjections.put(
              oriGroupCount + i,
              rexBuilder.makeLiteral(groupingValue, oriAggCall.getType(), true));
          break;
        case GROUP_ID:
          // GROUP_ID is removed during RelNode conversion, no handling needed here.
          return;
        case GROUPING_ID:
          // The GROUPING_ID aggregate function has been marked as deprecated
          // and is no longer supported.
          return;
        default:
          aggCalls.add(oriAggCall);
        }
      }

      relBuilder.push(oriAggInput)
          .aggregate(relBuilder.groupKey(groupSet), aggCalls);

      int index = 0;
      List<RexNode> projects = new ArrayList<>();
      for (int i = 0; i < oriRowType.getFieldCount(); i++) {
        RexNode node = specialProjections.get(i);
        if (node != null) {
          projects.add(node);
          continue;
        }
        projects.add(
            rexBuilder.makeInputRef(
                oriRowType.getFieldList().get(i).getType(), index++));
      }

      relBuilder.project(projects);
      unionInputs.add(relBuilder.build());
    }

    relBuilder.pushAll(unionInputs)
        .union(true, unionInputs.size())
        .convert(oriRowType, false);
    call.transformTo(relBuilder.build());
  }

  private static int calculateGroupingValue(ImmutableBitSet groupSet, List<Integer> argIndices) {
    if (argIndices.size() == 1) {
      return groupSet.get(argIndices.get(0)) ? 0 : 1;
    }

    int groupingValue = 0;
    int n = argIndices.size();
    for (int k = 0; k < n; k++) {
      int index = argIndices.get(n - 1 - k);
      if (!groupSet.get(index)) {
        groupingValue |= 1 << k;
      }
    }
    return groupingValue;
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
