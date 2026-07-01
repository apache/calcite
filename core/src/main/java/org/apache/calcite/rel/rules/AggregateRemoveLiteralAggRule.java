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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Planner rule that removes {@code LITERAL_AGG} aggregate calls from an
 * {@link Aggregate}.
 *
 * <p>{@code LITERAL_AGG} is an internal aggregate used by Calcite to mark
 * whether a group exists, and many external databases cannot implement it. This
 * rule keeps the grouping operation, removes the literal aggregate calls, and
 * adds a {@code Project} that restores the original row type with the literal
 * values.
 *
 * <p>For example,
 *
 * <pre>{@code
 * LogicalAggregate(group=[{0}], i=[LITERAL_AGG(true)])
 *   LogicalTableScan(table=[[EMP]])
 * }</pre>
 *
 * <p>becomes
 *
 * <pre>{@code
 * LogicalProject(DEPTNO=[$0], i=[true])
 *   LogicalAggregate(group=[{0}])
 *     LogicalTableScan(table=[[EMP]])
 * }</pre>
 */
@Value.Enclosing
public class AggregateRemoveLiteralAggRule
    extends RelRule<AggregateRemoveLiteralAggRule.Config>
    implements TransformationRule {

  /** Creates an AggregateRemoveLiteralAggRule. */
  protected AggregateRemoveLiteralAggRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final RelBuilder relBuilder = call.builder();
    final Aggregate aggregate = call.rel(0);
    final List<AggregateCall> aggCalls = aggregate.getAggCallList();
    if (aggCalls.isEmpty()) {
      return;
    }

    final boolean[] literalAggs = new boolean[aggCalls.size()];
    int literalAggCount = 0;
    for (int i = 0; i < aggCalls.size(); i++) {
      if (aggCalls.get(i).getAggregation().getKind() == SqlKind.LITERAL_AGG) {
        literalAggs[i] = true;
        literalAggCount++;
      }
    }
    if (literalAggCount == 0) {
      return;
    }

    final List<AggregateCall> newAggCalls =
        new ArrayList<>(aggCalls.size() - literalAggCount);
    final int[] oldAggIndexToNewAggIndex = new int[aggCalls.size()];
    int newAggPos = 0;
    for (int i = 0; i < aggCalls.size(); i++) {
      if (!literalAggs[i]) {
        newAggCalls.add(aggCalls.get(i));
        oldAggIndexToNewAggIndex[i] = newAggPos++;
      }
    }
    if (newAggCalls.isEmpty() && aggregate.getGroupCount() == 0) {
      newAggCalls.add(
          AggregateCall.create(SqlStdOperatorTable.COUNT, false, false, false,
              Collections.emptyList(), Collections.emptyList(), -1,
              null, RelCollations.EMPTY,
              aggregate.getGroupSets().contains(ImmutableBitSet.of()),
              aggregate.getInput(), null, null));
    }

    final RelNode newAggregate =
        aggregate.copy(aggregate.getTraitSet(), aggregate.getInput(),
            aggregate.getGroupSet(), aggregate.getGroupSets(), newAggCalls);
    relBuilder.push(newAggregate);

    final int groupCount = aggregate.getGroupCount();
    final int outputCount = aggregate.getRowType().getFieldCount();
    final List<RexNode> projects = new ArrayList<>(outputCount);
    for (int outPos = 0; outPos < outputCount; outPos++) {
      if (outPos < groupCount) {
        projects.add(relBuilder.field(outPos));
      } else {
        final int aggIndex = outPos - groupCount;
        final AggregateCall aggCall = aggCalls.get(aggIndex);
        if (literalAggs[aggIndex]) {
          projects.add(aggCall.rexList.get(0));
        } else {
          projects.add(relBuilder.field(groupCount + oldAggIndexToNewAggIndex[aggIndex]));
        }
      }
    }

    relBuilder.project(projects, aggregate.getRowType().getFieldNames());
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateRemoveLiteralAggRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalAggregate.class).anyInputs());

    @Override default AggregateRemoveLiteralAggRule toRule() {
      return new AggregateRemoveLiteralAggRule(this);
    }

    /** Defines an operand tree for the given aggregate class. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass).anyInputs())
          .as(Config.class);
    }
  }
}
