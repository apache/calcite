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
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlConstantValueAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Planner rule that reduces aggregate functions whose arguments are constant
 * within each group. Such arguments include GROUP BY keys, deterministic
 * expressions of those keys, and constants even in a global aggregate.
 *
 * <p>For example,
 * {@code SELECT sal, max(sal) FROM emp GROUP BY sal}
 * can be simplified to
 * {@code SELECT sal, sal FROM emp GROUP BY sal}.
 *
 * <p>MIN, MAX, AVG and ANY_VALUE return the argument value for a non-empty
 * group. Functions implementing {@link SqlConstantValueAggFunction}, such as
 * STDDEV_POP, STDDEV_SAMP, VAR_POP and VAR_SAMP, declare their constant result
 * and the minimum number of non-null values needed to produce it.
 *
 * <p>A global aggregate produces a row even on empty input. Therefore,
 * {@code SELECT MIN(1) FROM emp} becomes
 * {@code SELECT CASE WHEN COUNT(*) = 0 THEN NULL ELSE 1 END FROM emp},
 * rather than an unconditional constant. Sample variance and standard
 * deviation also need a count, since fewer than two non-null values yield NULL.
 * Counts respect DISTINCT when the minimum is greater than one, and identical
 * counts are shared. Ordinary non-empty groups need no count when one value
 * suffices; nullable arguments still preserve their NULL semantics.
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
    final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();
    final List<RexNode> projects = new ArrayList<>();
    final List<String> fieldNames =
        new ArrayList<>(aggregate.getRowType().getFieldNames());

    // Pass through group keys.
    for (int i = 0; i < groupCount; i++) {
      projects.add(rexBuilder.makeInputRef(aggregate, i));
    }

    boolean changed = false;
    for (AggregateCall oldCall : oldCalls) {
      final @Nullable RexNode reduced =
          reduce(aggregate, oldCall, rexBuilder, newCalls, aggCallMapping);
      if (reduced != null) {
        projects.add(reduced);
        changed = true;
      } else {
        projects.add(
            rexBuilder.addAggCall(oldCall, groupCount, newCalls, aggCallMapping,
                aggregate.getInput()::fieldIsNullable));
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
    relBuilder.project(projects, fieldNames);
    call.transformTo(relBuilder.build());
  }

  /**
   * Tries to reduce an aggregate call to an expression over group-by keys
   * and constants, adding counts when empty or single-value inputs need
   * to be distinguished.
   *
   * @return the reduced expression, or null if cannot reduce
   */
  private static @Nullable RexNode reduce(
      Aggregate aggregate,
      AggregateCall call,
      RexBuilder rexBuilder,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping) {
    if (!Aggregate.isSimple(aggregate)) {
      return null;
    }
    if (call.hasFilter()
        || call.distinctKeys != null
        || call.collation != RelCollations.EMPTY) {
      return null;
    }
    final SqlKind kind = call.getAggregation().getKind();
    final boolean isConstantValueAgg =
        call.getAggregation() instanceof SqlConstantValueAggFunction;
    switch (kind) {
    case AVG:
    case MAX:
    case MIN:
    case ANY_VALUE:
      break;
    default:
      if (!isConstantValueAgg) {
        return null;
      }
    }
    final List<Integer> argList = call.getArgList();
    if (argList.size() != 1) {
      return null;
    }
    final int arg = argList.get(0);

    final RexNode argument;
    if (aggregate.getGroupSet().get(arg)) {
      final int groupIndex = aggregate.getGroupSet().asList().indexOf(arg);
      argument = RexInputRef.of(groupIndex, aggregate.getRowType().getFieldList());
    } else {
      RelNode input = aggregate.getInput();
      if (input instanceof HepRelVertex) {
        input = ((HepRelVertex) input).getCurrentRel();
      }
      if (!(input instanceof Project)) {
        return null;
      }
      final Project project = (Project) input;
      if (arg < 0 || arg >= project.getProjects().size()) {
        return null;
      }
      final RexNode expr = project.getProjects().get(arg);
      if (!RexUtil.isDeterministic(expr)) {
        return null;
      }
      // Constants have no input refs, so they also pass this check when the
      // group set is empty.
      final @Nullable RexNode translated =
          translateToGroupRefs(expr, project, aggregate);
      if (translated == null) {
        return null;
      }
      argument = translated;
    }

    RexNode result = argument;
    int minCount = 1;
    if (isConstantValueAgg) {
      final SqlConstantValueAggFunction function =
          (SqlConstantValueAggFunction) call.getAggregation();
      final @Nullable RexNode constantResult =
          function.getConstantResult(rexBuilder, call.getType());
      if (constantResult != null) {
        result = constantResult;
        minCount = function.getMinCountForConstantResult();
      } else if (kind != SqlKind.AVG && kind != SqlKind.MIN
          && kind != SqlKind.MAX && kind != SqlKind.ANY_VALUE) {
        return null;
      }
    }

    final @Nullable RexNode condition;
    if (aggregate.getGroupCount() == 0 || minCount > 1) {
      // COUNT ignores NULL arguments. DISTINCT only affects the threshold
      // when more than one value is required. addAggCall also turns a count
      // of a non-nullable argument into COUNT(*) and reuses existing counts.
      final AggregateCall countCall =
          AggregateCall.create(call.getParserPosition(), SqlStdOperatorTable.COUNT,
              call.isDistinct() && minCount > 1, false, false,
              ImmutableList.of(), call.getArgList(), -1, null, RelCollations.EMPTY,
              aggregate.hasEmptyGroup(), aggregate.getInput(), null, null);
      final RexNode count =
          rexBuilder.addAggCall(countCall, aggregate.getGroupCount(), newCalls,
              aggCallMapping, aggregate.getInput()::fieldIsNullable);
      condition =
          rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, count,
              rexBuilder.makeExactLiteral(BigDecimal.valueOf(minCount), count.getType()));
    } else if (result != argument && argument.getType().isNullable()) {
      condition = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, argument);
    } else {
      condition = null;
    }
    if (condition != null) {
      final RexNode nullResult = rexBuilder.makeNullLiteral(call.getType());
      result = rexBuilder.makeCall(SqlStdOperatorTable.CASE, condition, nullResult, result);
    }
    if (!result.getType().equals(call.getType())) {
      result = rexBuilder.makeCast(call.getParserPosition(), call.getType(), result);
    }
    return result;
  }

  /**
   * Translates an expression so that its {@link RexInputRef}s reference
   * the group keys of the aggregate rather than the input to the project.
   *
   * @return the translated expression, or null if the expression references
   * columns that are not group-by keys
   */
  private static @Nullable RexNode translateToGroupRefs(
      RexNode expr, Project project, Aggregate aggregate) {
    final List<RexNode> projects = project.getProjects();
    final GroupRefTranslator translator = new GroupRefTranslator(projects, aggregate);
    final RexNode result = expr.accept(translator);
    return translator.failed ? null : result;
  }

  /**
   * Shuttle that translates input refs to aggregate group key refs.
   *
   * <p>For each column reference in the expression being examined:
   * 1. If the expression is a direct pass-through of a project column,
   *    check if that project column is in the GROUP BY set
   * 2. If the expression contains references to input columns,
   *    verify that those input columns are in the GROUP BY set
   * 3. Map to the corresponding group key index in the aggregate
   *
   * <p>This ensures the expression references only columns that are constant
   * within each group.
   */
  private static class GroupRefTranslator extends RexShuttle {
    private final List<RexNode> projects;
    private final Aggregate aggregate;
    private boolean failed = false;

    GroupRefTranslator(List<RexNode> projects, Aggregate aggregate) {
      this.projects = projects;
      this.aggregate = aggregate;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      if (failed) {
        return inputRef;
      }
      final int inputIndex = inputRef.getIndex();
      // Look for a project column that is a direct pass-through of this input.
      // For example, if a project has SAL=[$5], and the expression references $5,
      // we need to map it to the corresponding group key.
      int projectOutputIndex = -1;
      for (int i = 0; i < projects.size(); i++) {
        final RexNode projExpr = projects.get(i);
        if (projExpr instanceof RexInputRef
            && ((RexInputRef) projExpr).getIndex() == inputIndex) {
          projectOutputIndex = i;
          break;
        }
      }
      // The input column must be available through a project column that is in
      // the GROUP BY set. If not found, the input is embedded in a computed
      // expression, which means the optimization cannot proceed safely.
      if (projectOutputIndex < 0
          || !aggregate.getGroupSet().get(projectOutputIndex)) {
        failed = true;
        return inputRef;
      }
      final int groupIndex =
          aggregate.getGroupSet().asList().indexOf(projectOutputIndex);
      return RexInputRef.of(groupIndex, aggregate.getRowType().getFieldList());
    }
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
