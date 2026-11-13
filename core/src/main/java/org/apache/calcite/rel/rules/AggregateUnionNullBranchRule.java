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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Planner rule that removes work from {@code UNION ALL} branches that cannot
 * contribute to the result of an enclosing {@link Aggregate}.
 *
 * <p>A branch is irrelevant to the aggregate if every aggregate call ignores
 * NULL inputs, for example {@code SUM}, {@code MIN}, {@code MAX}, {@code AVG}
 * or {@code COUNT(x)}, and reads only columns that the branch produces as
 * constant NULL. Then:
 *
 * <ul>
 * <li>If the aggregate has no {@code GROUP BY}, the branch cannot change the
 * result and is removed.  At least one branch is always kept.
 * <li>Otherwise the branch still contributes group keys, so it is kept, but a
 * pure {@code GROUP BY} with no aggregate calls inside it is redundant, because
 * duplicate rows cannot change the enclosing aggregate, and is replaced by a
 * projection of the grouping columns.
 * </ul>
 *
 * @see CoreRules#AGGREGATE_UNION_NULL_BRANCH
 */
@Value.Enclosing
public class AggregateUnionNullBranchRule
    extends RelRule<AggregateUnionNullBranchRule.Config>
    implements SubstitutionRule {

  /** Creates an AggregateUnionNullBranchRule. */
  protected AggregateUnionNullBranchRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Union union = call.rel(1);
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelBuilder relBuilder = call.builder();

    final List<RelNode> inputs = union.getInputs();
    final List<Boolean> irrelevant = new ArrayList<>();
    boolean anyIrrelevant = false;
    for (RelNode input : inputs) {
      final ImmutableBitSet nullColumns = nullColumns(mq, input);
      final boolean ignored = aggregate.getAggCallList().stream()
          .allMatch(c -> ignoresRowsWithNullColumns(c, nullColumns));
      irrelevant.add(ignored);
      anyIrrelevant |= ignored;
    }
    if (!anyIrrelevant) {
      return;
    }

    final List<RelNode> newInputs = new ArrayList<>();
    if (aggregate.getGroupSet().isEmpty()) {
      for (int i = 0; i < inputs.size(); i++) {
        if (!irrelevant.get(i)) {
          newInputs.add(inputs.get(i));
        }
      }
      if (newInputs.isEmpty()) {
        newInputs.add(inputs.get(0));
      }
      if (newInputs.size() == inputs.size()) {
        return;
      }
    } else {
      boolean changed = false;
      for (int i = 0; i < inputs.size(); i++) {
        final RelNode input = inputs.get(i);
        final RelNode simplified =
            irrelevant.get(i) ? removeRedundantGroupBy(input, relBuilder) : input;
        changed |= simplified != input;
        newInputs.add(simplified);
      }
      if (!changed) {
        return;
      }
    }

    relBuilder.pushAll(newInputs);
    relBuilder.union(true, newInputs.size());
    if (newInputs.size() == inputs.size()) {
      // Same number of branches: only the branches themselves changed.
      relBuilder.convert(union.getRowType(), false);
      call.transformTo(
          aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(relBuilder.build())));
      call.getPlanner().prune(aggregate);
      return;
    }
    // Branches were removed. Re-derive the aggregate over the remaining input
    // so that the types are inferred and only cast if they differ.
    final List<RelBuilder.AggCall> aggCalls = new ArrayList<>();
    for (AggregateCall c : aggregate.getAggCallList()) {
      RelBuilder.AggCall aggCall =
          relBuilder.aggregateCall(c.getAggregation(), relBuilder.fields(c.getArgList()))
              .distinct(c.isDistinct())
              .approximate(c.isApproximate())
              .ignoreNulls(c.ignoreNulls())
              .as(c.name);
      if (c.hasFilter()) {
        aggCall = aggCall.filter(relBuilder.field(c.filterArg));
      }
      if (!c.getCollation().getFieldCollations().isEmpty()) {
        aggCall = aggCall.sort(relBuilder.fields(c.getCollation()));
      }
      aggCalls.add(aggCall);
    }
    relBuilder.aggregate(relBuilder.groupKey(aggregate.getGroupSet()), aggCalls);
    relBuilder.convert(aggregate.getRowType(), false);
    call.transformTo(relBuilder.build());
    call.getPlanner().prune(aggregate);
  }

  /** Returns the columns that {@code input} always produces as NULL. */
  private static ImmutableBitSet nullColumns(RelMetadataQuery mq, RelNode input) {
    final ImmutableBitSet.Builder columns = ImmutableBitSet.builder();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(input);
    if (predicates == null) {
      return columns.build();
    }
    for (RexNode predicate : predicates.pulledUpPredicates) {
      if (predicate.getKind() == SqlKind.IS_NULL) {
        final RexNode operand = ((RexCall) predicate).getOperands().get(0);
        if (operand instanceof RexInputRef) {
          columns.set(((RexInputRef) operand).getIndex());
        }
      }
    }
    for (Map.Entry<RexNode, RexNode> e : predicates.constantMap.entrySet()) {
      if (e.getKey() instanceof RexInputRef && RexUtil.isNullLiteral(e.getValue(), true)) {
        columns.set(((RexInputRef) e.getKey()).getIndex());
      }
    }
    return columns.build();
  }

  /** Returns whether rows whose arguments in {@code nullColumns} are NULL
   * leave the result of {@code call} unchanged. */
  private static boolean ignoresRowsWithNullColumns(AggregateCall call,
      ImmutableBitSet nullColumns) {
    final List<Integer> args = call.getArgList();
    if (args.isEmpty()) {
      return false;
    }
    switch (call.getAggregation().getKind()) {
    case SUM:
    case SUM0:
    case MIN:
    case MAX:
    case AVG:
      return nullColumns.get(args.get(0));
    case COUNT:
      return args.stream().anyMatch(nullColumns::get);
    default:
      return false;
    }
  }

  /** Replaces a pure GROUP BY {@link Aggregate} without any calls at the top of
   * a union branch, by a projection of its grouping columns.
   * Returns the input if there is nothing to do. */
  private static RelNode removeRedundantGroupBy(RelNode branch, RelBuilder relBuilder) {
    for (RelNode node : logicalAlternatives(branch)) {
      if (node instanceof Project) {
        final Project project = (Project) node;
        for (RelNode child : logicalAlternatives(project.getInput())) {
          if (isPureGroupBy(child)) {
            relBuilder.push(groupColumns((Aggregate) child, relBuilder));
            relBuilder.project(project.getProjects(), project.getRowType().getFieldNames(), true);
            return relBuilder.build();
          }
        }
      } else if (isPureGroupBy(node)) {
        return groupColumns((Aggregate) node, relBuilder);
      }
    }
    return branch;
  }

  /** Returns the logical expressions that {@code node} stands for: for a
   * {@link RelSubset} returns its logical members, otherwise the node itself
   * (looking through a {@code HepRelVertex}). */
  private static List<RelNode> logicalAlternatives(RelNode node) {
    final List<RelNode> rels = node instanceof RelSubset
        ? ((RelSubset) node).getRelList()
        : ImmutableList.of(node.stripped());
    final List<RelNode> logical = new ArrayList<>();
    for (RelNode rel : rels) {
      if (isLogical(rel)) {
        logical.add(rel);
      }
    }
    return logical;
  }

  /** Returns whether {@code rel} is not yet implemented in a physical
   * convention. */
  private static boolean isLogical(RelNode rel) {
    final Convention convention = rel.getConvention();
    return convention == null || convention == Convention.NONE;
  }

  private static boolean isPureGroupBy(RelNode node) {
    return node instanceof Aggregate
        && ((Aggregate) node).getAggCallList().isEmpty()
        && ((Aggregate) node).getGroupType() == Aggregate.Group.SIMPLE;
  }

  private static RelNode groupColumns(Aggregate aggregate, RelBuilder relBuilder) {
    return relBuilder.push(aggregate.getInput())
        .project(relBuilder.fields(aggregate.getGroupSet().asList()))
        .build();
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateUnionNullBranchRule.Config.of()
        .withOperandFor(Aggregate.class, Union.class);

    @Override default AggregateUnionNullBranchRule toRule() {
      return new AggregateUnionNullBranchRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass,
        Class<? extends Union> unionClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass)
              .predicate(a -> isLogical(a) && !a.getAggCallList().isEmpty())
              .oneInput(b1 ->
                  b1.operand(unionClass)
                      .predicate(u -> u.all && isLogical(u))
                      .anyInputs()))
          .as(Config.class);
    }
  }
}
