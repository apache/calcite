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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule to extract a {@link org.apache.calcite.rel.core.Project}
 * from an {@link org.apache.calcite.rel.core.Aggregate}
 * and push it down towards the input.
 *
 * <p>What projections can be safely pushed down depends upon which fields the
 * Aggregate uses.
 *
 * <p>To prevent cycles, this rule will not extract a {@code Project} if the
 * {@code Aggregate}s input is already a {@code Project}.
 */
public class AggregateExtractProjectRule
    extends RelRule<AggregateExtractProjectRule.Config>
    implements TransformationRule {
  public static final AggregateExtractProjectRule SCAN =
      Config.DEFAULT.toRule();

  /** Creates an AggregateExtractProjectRule. */
  protected AggregateExtractProjectRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public AggregateExtractProjectRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends RelNode> inputClass,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(aggregateClass, inputClass));
  }

  @Deprecated // to be removed before 2.0
  public AggregateExtractProjectRule(RelOptRuleOperand operand,
      RelBuilderFactory builderFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(builderFactory)
        .withOperandSupplier(b -> b.exactly(operand))
        .as(Config.class));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = call.rel(1);
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
    final RelBuilder relBuilder = call.builder().push(input);
    final List<RexNode> projects = new ArrayList<>();
    final Mapping mapping =
        Mappings.create(MappingType.INVERSE_SURJECTION,
            aggregate.getInput().getRowType().getFieldCount(),
            inputFieldsUsed.cardinality());
    int j = 0;
    for (int i : inputFieldsUsed.build()) {
      projects.add(relBuilder.field(i));
      mapping.set(i, j++);
    }

    relBuilder.project(projects);

    final ImmutableBitSet newGroupSet =
        Mappings.apply(mapping, aggregate.getGroupSet());

    final Iterable<ImmutableBitSet> newGroupSets =
        Util.transform(aggregate.getGroupSets(),
            bitSet -> Mappings.apply(mapping, bitSet));
    final List<RelBuilder.AggCall> newAggCallList = new ArrayList<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      final ImmutableList<RexNode> args =
          relBuilder.fields(
              Mappings.apply2(mapping, aggCall.getArgList()));
      final RexNode filterArg = aggCall.filterArg < 0 ? null
          : relBuilder.field(Mappings.apply(mapping, aggCall.filterArg));
      newAggCallList.add(
          relBuilder.aggregateCall(aggCall.getAggregation(), args)
              .distinct(aggCall.isDistinct())
              .filter(filterArg)
              .approximate(aggCall.isApproximate())
              .sort(relBuilder.fields(aggCall.collation))
              .as(aggCall.name));
    }

    final RelBuilder.GroupKey groupKey =
        relBuilder.groupKey(newGroupSet, newGroupSets);
    relBuilder.aggregate(groupKey, newAggCallList);
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
        .as(Config.class)
        .withOperandFor(Aggregate.class, LogicalTableScan.class);

    @Override default AggregateExtractProjectRule toRule() {
      return new AggregateExtractProjectRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass,
        Class<? extends RelNode> inputClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass).oneInput(b1 ->
              b1.operand(inputClass)
                  // Predicate prevents matching against an Aggregate whose
                  // input is already a Project. Prevents this rule firing
                  // repeatedly.
                  .predicate(r -> !(r instanceof Project)).anyInputs()))
          .as(Config.class);
    }
  }
}
