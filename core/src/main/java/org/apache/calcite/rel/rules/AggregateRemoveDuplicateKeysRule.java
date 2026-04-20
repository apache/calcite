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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that removes redundant grouping keys from an
 * {@link Aggregate} when the later keys are functionally determined by the
 * earlier retained keys.
 *
 * <p>The original output schema is preserved by adding {@code SINGLE_VALUE}
 * aggregate calls for removed grouping keys and then projecting the row back
 * into the original field order.
 *
 * <p>The original SQL:
 * <pre>{@code
 * SELECT deptno, name, count(*) AS c
 * FROM sales.dept
 * GROUP BY deptno, name
 * }</pre>
 *
 * <p>The original logical plan:
 * <pre>
 * LogicalAggregate(group=[{0, 1}], C=[COUNT()])
 *   LogicalTableScan(table=[[CATALOG, SALES, DEPT]])
 * </pre>
 *
 * <p>After optimization:
 * <pre>
 * LogicalProject(DEPTNO=[$0], NAME=[$1], C=[$2])
 *   LogicalAggregate(group=[{0}], NAME=[SINGLE_VALUE($1)], C=[COUNT()])
 *     LogicalTableScan(table=[[CATALOG, SALES, DEPT]])
 * </pre>
 */
@Value.Enclosing
public class AggregateRemoveDuplicateKeysRule
    extends RelRule<AggregateRemoveDuplicateKeysRule.Config>
    implements TransformationRule {

  /** Creates an AggregateRemoveDuplicateKeysRule. */
  protected AggregateRemoveDuplicateKeysRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    if (!Aggregate.isSimple(aggregate) || aggregate.getGroupCount() <= 1) {
      return;
    }

    final RelMetadataQuery mq = call.getMetadataQuery();
    final List<Integer> groupKeys = aggregate.getGroupSet().asList();
    final int keyCount = groupKeys.size();

    // Classify each group-key position (index into groupKeys) as retained or redundant.
    // retainedBits tracks *positions* (not input field indices) already retained, so that
    // mq.determinesSet can check whether they functionally determine the candidate position
    // on the aggregate's own output schema.
    final List<Integer> retainedPos = new ArrayList<>();
    final List<Integer> removedPos  = new ArrayList<>();
    final ImmutableBitSet.Builder newGroupSetBuilder = ImmutableBitSet.builder();
    ImmutableBitSet retainedBits = ImmutableBitSet.of();

    for (int i = 0; i < keyCount; i++) {
      if (!retainedBits.isEmpty()
          && mq.determinesSet(aggregate, retainedBits, ImmutableBitSet.of(i))) {
        removedPos.add(i);
      } else {
        retainedPos.add(i);
        retainedBits = retainedBits.union(ImmutableBitSet.of(i));
        newGroupSetBuilder.set(groupKeys.get(i));
      }
    }

    if (removedPos.isEmpty()) {
      return;
    }

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(aggregate.getInput());

    // Build new agg calls. SINGLE_VALUE calls for removed keys are placed first so
    // their output indices are contiguous with the retained group-key columns:
    //
    // new aggregate output layout:
    //   [0 .. retainedCount-1]                           retained group keys
    //   [retainedCount .. retainedCount+removedCount-1]  SINGLE_VALUE calls
    //   [retainedCount + removedCount .. ...]            original aggregate calls
    //
    final List<String> inputFieldNames = aggregate.getInput().getRowType().getFieldNames();
    final List<RelBuilder.AggCall> newAggCalls = new ArrayList<>();
    for (int removedKeyPos : removedPos) {
      final int inputIdx = groupKeys.get(removedKeyPos);
      newAggCalls.add(
          relBuilder.aggregateCall(SqlStdOperatorTable.SINGLE_VALUE,
                  relBuilder.field(inputIdx))
              .as(inputFieldNames.get(inputIdx)));
    }
    for (org.apache.calcite.rel.core.AggregateCall aggCall : aggregate.getAggCallList()) {
      newAggCalls.add(relBuilder.aggregateCall(aggCall));
    }

    relBuilder.aggregate(relBuilder.groupKey(newGroupSetBuilder.build()), newAggCalls);

    // Build a project to restore the original field order.
    // Precompute position-in-groupKeys → new-output-column-index to avoid O(n) indexOf.
    final int retainedCount = retainedPos.size();
    final int removedCount  = removedPos.size();
    final int origAggCount  = aggregate.getAggCallList().size();
    final int[] posToCol = new int[keyCount];
    for (int ri = 0; ri < retainedCount; ri++) {
      posToCol[retainedPos.get(ri)] = ri;
    }
    for (int di = 0; di < removedCount; di++) {
      posToCol[removedPos.get(di)] = retainedCount + di;
    }

    final List<RexNode> projects = new ArrayList<>(aggregate.getRowType().getFieldCount());
    for (int pos = 0; pos < keyCount; pos++) {
      projects.add(relBuilder.field(posToCol[pos]));
    }
    for (int i = 0; i < origAggCount; i++) {
      projects.add(relBuilder.field(retainedCount + removedCount + i));
    }

    relBuilder.project(projects, aggregate.getRowType().getFieldNames());

    call.getPlanner().prune(aggregate);
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateRemoveDuplicateKeysRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalAggregate.class)
                .predicate(Aggregate::isSimple)
                .anyInputs());

    @Override default AggregateRemoveDuplicateKeysRule toRule() {
      return new AggregateRemoveDuplicateKeysRule(this);
    }

    /** Defines an operand tree for the given aggregate class. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass)
              .predicate(Aggregate::isSimple)
              .anyInputs())
          .as(Config.class);
    }
  }
}
