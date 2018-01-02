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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Rule to extract a {@link org.apache.calcite.rel.core.Project} from
 * {@link org.apache.calcite.rel.core.Aggregate} based on the fields used in the aggregate.
 * NOTE - This rule will not extract Project if the Aggregate is already on top of a Project.
 */
public class AggregateExtractProjectRule extends RelOptRule {

  // Predicate to prevent matching against Aggregate which is already on top of project.
  // This will prevent firing of this rule repeatedly.
  private static final Predicate<RelNode> PREDICATE = new Predicate<RelNode>() {
    @Override public boolean apply(@Nullable RelNode relNode) {
      return !(relNode instanceof Project);
    }
  };
  /**
   * Creates a AggregateExtractProjectRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public AggregateExtractProjectRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends RelNode> inputClass,
      RelBuilderFactory relBuilderFactory) {
    this(operand(aggregateClass, operand(inputClass, null, PREDICATE, any())),
        relBuilderFactory);
  }

  public AggregateExtractProjectRule(RelOptRuleOperand operand,
      RelBuilderFactory builderFactory) {
    super(operand, builderFactory, null);
  }

  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode child = call.rel(1);
    // Compute which input fields are used.
    // 1. group fields are always used
    final ImmutableBitSet.Builder inputFieldsUsed =
        aggregate.getGroupSet().rebuild();
    // 2. agg functions
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      for (int i : aggCall.getArgList()) {
        inputFieldsUsed.set(i);
      }
      if (aggCall.filterArg >= 0) {
        inputFieldsUsed.set(aggCall.filterArg);
      }
    }
    RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    RelBuilder relBuilder = call.builder();

    List<RexNode> projects = Lists.newArrayList();
    final Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            aggregate.getInput().getRowType().getFieldCount(),
            inputFieldsUsed.cardinality());
    int j = 0;
    for (int i : inputFieldsUsed.build()) {
      projects.add(rexBuilder.makeInputRef(aggregate.getInput(), i));
      mapping.set(i, j++);
    }

    relBuilder = relBuilder.push(child).project(projects);

    final ImmutableBitSet newGroupSet =
        Mappings.apply(mapping, aggregate.getGroupSet());

    final ImmutableList<ImmutableBitSet> newGroupSets =
        ImmutableList.copyOf(
            Iterables.transform(aggregate.getGroupSets(),
                new Function<ImmutableBitSet, ImmutableBitSet>() {
                  public ImmutableBitSet apply(ImmutableBitSet input) {
                    return Mappings.apply(mapping, input);
                  }
                }));
    final List<RelBuilder.AggCall> newAggCallList = new ArrayList<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      final ImmutableList<RexNode> args =
          relBuilder.fields(
              Mappings.apply2(mapping, aggCall.getArgList()));
      final RexNode filterArg = aggCall.filterArg < 0 ? null
          : relBuilder.field(Mappings.apply(mapping, aggCall.filterArg));
      RelBuilder.AggCall newAggCall =
          relBuilder.aggregateCall(aggCall.getAggregation(),
              aggCall.isDistinct(), aggCall.isApproximate(),
              filterArg, aggCall.name, args);
      newAggCallList.add(newAggCall);
    }

    final RelBuilder.GroupKey groupKey =
        relBuilder.groupKey(newGroupSet, newGroupSets);
    RelNode rel = relBuilder.aggregate(groupKey, newAggCallList).build();
    call.transformTo(rel);
  }
}

// End AggregateExtractProjectRule.java
