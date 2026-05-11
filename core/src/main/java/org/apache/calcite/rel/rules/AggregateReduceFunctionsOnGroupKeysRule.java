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
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that eliminates aggregate functions of GROUP BY keys.
 *
 * <p>For example,
 * {@code SELECT sal, max(sal) FROM emp GROUP BY sal}
 * can be simplified to
 * {@code SELECT sal, sal FROM emp GROUP BY sal}.
 *
 * <p>Currently supports the following aggregate functions when their
 * arguments exist in the aggregate's group set:
 * <ul>
 *   <li>{@code MAX}</li>
 *   <li>{@code MIN}</li>
 *   <li>{@code AVG}</li>
 *   <li>{@code ANY_VALUE}</li>
 * </ul>
 *
 * @see CoreRules#AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS
 */
@Value.Enclosing
public class AggregateReduceFunctionsOnGroupKeysRule
    extends RelRule<AggregateReduceFunctionsOnGroupKeysRule.Config>
    implements TransformationRule {

  /** Creates an AggregateReduceFunctionsOnGroupKeysRule. */
  protected AggregateReduceFunctionsOnGroupKeysRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final List<AggregateCall> oldCalls = aggregate.getAggCallList();
    final int groupCount = aggregate.getGroupCount();
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();

    final List<AggregateCall> newCalls = new ArrayList<>();
    final List<RexNode> projects = new ArrayList<>();

    // Pass through group keys.
    for (int i = 0; i < groupCount; i++) {
      projects.add(rexBuilder.makeInputRef(aggregate, i));
    }

    boolean changed = false;
    int newCallOrdinal = 0;
    for (AggregateCall oldCall : oldCalls) {
      final @Nullable RexNode reduced = reduce(aggregate, oldCall, rexBuilder);
      if (reduced != null) {
        projects.add(reduced);
        changed = true;
      } else {
        newCalls.add(oldCall);
        projects.add(
            rexBuilder.makeInputRef(
                oldCall.getType(), groupCount + newCallOrdinal));
        newCallOrdinal++;
      }
    }

    if (!changed) {
      return;
    }

    final RelNode newAggregate =
        aggregate.copy(
            aggregate.getTraitSet(),
            aggregate.getInput(),
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            newCalls);
    relBuilder.push(newAggregate);
    relBuilder.project(projects);
    call.transformTo(relBuilder.build());
  }

  /**
   * Tries to reduce an aggregate call to a reference to a group-by key.
   *
   * @return the reduced expression, or null if cannot reduce
   */
  private static @Nullable RexNode reduce(
      Aggregate aggregate,
      AggregateCall call,
      RexBuilder rexBuilder) {
    if (!Aggregate.isSimple(aggregate)) {
      return null;
    }
    if (call.hasFilter()
        || call.distinctKeys != null
        || call.collation != RelCollations.EMPTY) {
      return null;
    }
    final List<Integer> argList = call.getArgList();
    if (argList.size() != 1) {
      return null;
    }
    final int arg = argList.get(0);
    if (!aggregate.getGroupSet().get(arg)) {
      return null;
    }
    final SqlKind kind = call.getAggregation().getKind();
    switch (kind) {
    case AVG:
    case MAX:
    case MIN:
    case ANY_VALUE:
      break;
    default:
      return null;
    }
    final int groupIndex = aggregate.getGroupSet().asList().indexOf(arg);
    RexNode ref = RexInputRef.of(groupIndex, aggregate.getRowType().getFieldList());
    if (!ref.getType().equals(call.getType())) {
      ref = rexBuilder.makeCast(call.getParserPosition(), call.getType(), ref);
    }
    return ref;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateReduceFunctionsOnGroupKeysRule.Config.of()
        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
        .withOperandFor(LogicalAggregate.class);

    @Override default AggregateReduceFunctionsOnGroupKeysRule toRule() {
      return new AggregateReduceFunctionsOnGroupKeysRule(this);
    }

    /** Defines an operand tree for the given class. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass) {
      return withOperandSupplier(b ->
          b.operand(aggregateClass)
              .predicate(Aggregate::isSimple)
              .anyInputs())
          .as(Config.class);
    }
  }
}
