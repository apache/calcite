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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Planner rule that removes constant keys from an
 * a {@link Aggregate}.
 *
 * <p>It never removes the last column, because {@code Aggregate([])} returns
 * 1 row even if its input is empty.
 */
public class AggregateConstantKeyRule extends RelOptRule {
  public static final AggregateConstantKeyRule INSTANCE =
      new AggregateConstantKeyRule(RelFactories.LOGICAL_BUILDER,
          "AggregateConstantKeyRule");

  //~ Constructors -----------------------------------------------------------

  /** Creates an AggregateConstantKeyRule. */
  private AggregateConstantKeyRule(RelBuilderFactory relBuilderFactory,
      String description) {
    super(operand(Aggregate.class, null, Aggregate.IS_SIMPLE, any()),
        relBuilderFactory, description);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    assert !aggregate.indicator : "predicate ensured no grouping sets";

    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final RelOptPredicateList predicates =
        RelMetadataQuery.getPulledUpPredicates(aggregate.getInput());
    if (predicates == null) {
      return;
    }
    final ImmutableMap<RexNode, RexLiteral> constants =
        ReduceExpressionsRule.predicateConstants(rexBuilder, predicates);
    final NavigableMap<Integer, RexLiteral> map = new TreeMap<>();
    for (int key : aggregate.getGroupSet()) {
      final RexInputRef ref =
          rexBuilder.makeInputRef(aggregate.getInput(), key);
      if (constants.containsKey(ref)) {
        map.put(key, constants.get(ref));
      }
    }

    if (map.isEmpty()) {
      return; // none of the keys are constant
    }

    if (map.size() == aggregate.getGroupCount()) {
      if (map.size() == 1) {
        // There is one key, and it is constant. We cannot remove it.
        return;
      }
      map.remove(map.descendingKeySet().descendingIterator().next());
    }

    ImmutableBitSet newGroupSet = aggregate.getGroupSet();
    for (int key : map.keySet()) {
      newGroupSet = newGroupSet.clear(key);
    }
    final Aggregate newAggregate =
        aggregate.copy(aggregate.getTraitSet(), aggregate.getInput(),
            false, newGroupSet, ImmutableList.of(newGroupSet),
            aggregate.getAggCallList());
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(newAggregate);

    final List<RexNode> projects = new ArrayList<>();
    int offset = 0;
    for (RelDataTypeField field : aggregate.getRowType().getFieldList()) {
      RexNode node = null;
      if (field.getIndex() < aggregate.getGroupCount()) {
        node = map.get(aggregate.getGroupSet().nth(field.getIndex()));
        if (node != null) {
          node = relBuilder.getRexBuilder().makeCast(field.getType(), node, true);
          node = relBuilder.alias(node, field.getName());
          ++offset;
        }
      }
      if (node == null) {
        node = relBuilder.field(field.getIndex() - offset);
      }
      projects.add(node);
    }
    call.transformTo(relBuilder.project(projects).build());
  }
}

// End AggregateConstantKeyRule.java
