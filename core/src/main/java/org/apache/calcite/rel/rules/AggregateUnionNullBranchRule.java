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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Planner rule that removes work from {@code UNION ALL} branches that cannot
 * contribute to the result of an enclosing {@link Aggregate}.
 *
 * <p>For example, a "metrics" view pads each branch with NULL for the measures
 * of the other branches. In
 *
 * <blockquote><pre>{@code
 * SELECT SUM(m) FROM (
 *   SELECT deptno, SUM(sal) AS m FROM emp GROUP BY deptno
 *   UNION ALL
 *   SELECT deptno, CAST(NULL AS INTEGER) FROM emp GROUP BY deptno) AS v
 * }</pre></blockquote>
 *
 * <p>the second branch only produces NULLs for {@code m}, so it cannot change
 * the sum. The plan
 *
 * <blockquote><pre>
 * LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])
 *   LogicalUnion(all=[true])
 *     LogicalProject(M=[$1])
 *       LogicalAggregate(group=[{0}], M=[SUM($1)])
 *         ...
 *     LogicalProject(EXPR$1=[null:INTEGER])
 *       LogicalAggregate(group=[{0}])
 *         ...
 * </pre></blockquote>
 *
 * <p>becomes
 *
 * <blockquote><pre>
 * LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])
 *   LogicalProject(M=[$1])
 *     LogicalAggregate(group=[{0}], M=[SUM($1)])
 *       ...
 * </pre></blockquote>
 *
 * <p>If the enclosing aggregate has a {@code GROUP BY}, the branch is kept,
 * because it still contributes group keys, but a redundant {@code GROUP BY}
 * inside it is replaced by a projection. If every branch is irrelevant, the
 * aggregate is replaced by a constant row. See the comments in the code, and
 * {@code RelOptRulesTest}, for the individual cases.
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
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();

    // A branch is irrelevant if every aggregate call ignores NULL inputs, for
    // example SUM, MIN, MAX, AVG or COUNT(x), and reads only columns that the
    // branch produces as constant NULL.
    final List<RelNode> inputs = union.getInputs();
    final boolean[] irrelevant = new boolean[inputs.size()];
    boolean anyIrrelevant = false;
    for (int i = 0; i < irrelevant.length; i++) {
      final ImmutableBitSet nullColumns = nullColumns(mq, inputs.get(i));
      irrelevant[i] = aggregate.getAggCallList().stream()
          .allMatch(c -> ignoresRowsWithNullColumns(c, nullColumns, rexBuilder));
      anyIrrelevant |= irrelevant[i];
    }
    if (!anyIrrelevant) {
      return;
    }

    if (aggregate.getGroupSet().isEmpty()) {
      // Without GROUP BY, an irrelevant branch cannot change the result.
      removeIrrelevantBranches(call, aggregate, union, irrelevant);
    } else {
      // With GROUP BY, an irrelevant branch still contributes group keys.
      simplifyIrrelevantBranches(call, aggregate, union, irrelevant);
    }
  }

  /** Handles an aggregate without GROUP BY.
   * Branches that cannot contribute are removed. */
  private void removeIrrelevantBranches(RelOptRuleCall call,
      Aggregate aggregate, Union union, boolean[] irrelevant) {
    final List<RelNode> inputs = union.getInputs();
    final List<RelNode> newInputs = new ArrayList<>();
    for (int i = 0; i < inputs.size(); i++) {
      // Drop the branches that cannot contribute.
      if (!irrelevant[i]) {
        newInputs.add(inputs.get(i));
      }
    }
    if (newInputs.size() == inputs.size()) {
      return;
    }
    final RelBuilder relBuilder = call.builder();
    if (newInputs.isEmpty()) {
      // Every branch is irrelevant, so the aggregate sees no contributing
      // rows. A global aggregate returns exactly one row, whose value for
      // each call is a constant: 0 for COUNT and SUM0, NULL for the others.
      relBuilder.values(ImmutableList.of(constantRow(aggregate, relBuilder.getRexBuilder())),
          aggregate.getRowType());
      call.transformTo(relBuilder.build());
      call.getPlanner().prune(aggregate);
      return;
    }
    relBuilder.pushAll(newInputs);
    relBuilder.union(true, newInputs.size());
    // The removed branches may have determined the type of the union,
    // for example, a BIGINT column that the remaining branch has as SMALLINT.
    // Cast back, so that the aggregate runs at its original input type.
    // We do not call RelBuilder.convert(union.getRowType(), ...) directly,
    // because it would also add a cast where the types differ only in
    // nullability.  The remaining branch is often NOT NULL while the union
    // column is nullable, which is a no op that clutters the plan.
    castToTypesOf(union, relBuilder);
    relBuilder.hints(aggregate.getHints());
    relBuilder.aggregate(relBuilder.groupKey(aggregate.getGroupSet()),
        aggregateCalls(aggregate, relBuilder));
    relBuilder.convert(aggregate.getRowType(), false);
    call.transformTo(relBuilder.build());
    call.getPlanner().prune(aggregate);
  }

  /** Handles an aggregate with GROUP BY: irrelevant branches still contribute
   * group keys, so they are kept, but a redundant GROUP BY inside them is
   * removed. */
  private static void simplifyIrrelevantBranches(RelOptRuleCall call,
      Aggregate aggregate, Union union, boolean[] irrelevant) {
    final List<RelNode> inputs = union.getInputs();
    final RelBuilder relBuilder = call.builder();
    final List<RelNode> newInputs = new ArrayList<>();
    boolean changed = false;
    for (int i = 0; i < inputs.size(); i++) {
      final RelNode input = inputs.get(i);
      // The branch is kept, but a pure GROUP BY inside it is redundant: its
      // duplicate rows cannot change the enclosing aggregate, so replace it by
      // a projection of the grouping columns.
      final RelNode simplified =
          irrelevant[i] ? removeRedundantGroupBy(input, relBuilder) : input;
      changed |= simplified != input;
      newInputs.add(simplified);
    }
    if (!changed) {
      return;
    }
    // Same number of branches: only the branches themselves changed.
    relBuilder.pushAll(newInputs);
    relBuilder.union(true, newInputs.size());
    relBuilder.convert(union.getRowType(), false);
    call.transformTo(
        aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(relBuilder.build())));
    call.getPlanner().prune(aggregate);
  }

  /** Returns the single row produced by {@code aggregate} when it has no
   * GROUP BY and no row contributes: 0 for COUNT and SUM0, NULL otherwise. */
  private List<RexLiteral> constantRow(Aggregate aggregate, RexBuilder rexBuilder) {
    final List<RexLiteral> literals = new ArrayList<>();
    final List<RelDataTypeField> fields = aggregate.getRowType().getFieldList();
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      final AggregateCall call = aggregate.getAggCallList().get(i);
      final RelDataType type = fields.get(aggregate.getGroupCount() + i).getType();
      // Only aggregates accepted by ignoresRowsWithNullColumns get here.
      final RexLiteral value = valueOnNoRows(call, type, rexBuilder);
      if (value == null) {
        throw new AssertionError("no empty-input value for " + call);
      }
      literals.add(value);
    }
    return literals;
  }

  /** Casts the top of the {@code relBuilder} stack to the column types of
   * {@code union}, ignoring nullability, so that no cast is added unless a type
   * really differs. */
  private static void castToTypesOf(Union union, RelBuilder relBuilder) {
    final RelDataTypeFactory typeFactory = relBuilder.getTypeFactory();
    final List<RelDataTypeField> actual = relBuilder.peek().getRowType().getFieldList();
    final List<RelDataTypeField> expected = union.getRowType().getFieldList();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    boolean differ = false;
    for (int i = 0; i < actual.size(); i++) {
      final RelDataType actualType = actual.get(i).getType();
      final RelDataType expectedType = expected.get(i).getType();
      differ |= !SqlTypeUtil.equalSansNullability(typeFactory, actualType, expectedType);
      builder.add(actual.get(i).getName(),
          typeFactory.createTypeWithNullability(expectedType, actualType.isNullable()));
    }
    if (differ) {
      relBuilder.convert(builder.build(), false);
    }
  }

  /** Re-creates the calls of {@code aggregate} on the top of the {@code
   * relBuilder} stack. */
  private static List<RelBuilder.AggCall> aggregateCalls(Aggregate aggregate,
      RelBuilder relBuilder) {
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
    return aggCalls;
  }

  /** Returns the columns that {@code input} always produces as NULL. */
  private static ImmutableBitSet nullColumns(RelMetadataQuery mq, RelNode input) {
    final ImmutableBitSet.Builder columns = ImmutableBitSet.builder();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(input);
    // A column that is always NULL shows up either as a predicate
    // IS NULL($i), for example from a WHERE x IS NULL filter, or as a constant
    // $i = NULL. Only direct column references are recognized.  Other operands
    // are ignored, which at worst means a branch is not pruned.
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
  private boolean ignoresRowsWithNullColumns(AggregateCall call,
      ImmutableBitSet nullColumns, RexBuilder rexBuilder) {
    final List<Integer> args = call.getArgList();
    if (args.isEmpty()
        || call.distinctKeys != null // WITHIN DISTINCT is not supported
        || !call.rexList.isEmpty()) {
      return false;
    }
    final SqlKind kind = call.getAggregation().getKind();
    if (valueOnNoRows(call, call.getType(), rexBuilder) == null) {
      // Unknown aggregate, including user-defined ones (for example ARRAY_AGG,
      // which may return an empty array): we do not know that NULL inputs, or
      // no input at all, leave its result unchanged.
      return false;
    }
    return kind == SqlKind.COUNT
        ? args.stream().anyMatch(nullColumns::get)
        : nullColumns.get(args.get(0));
  }

  /** Returns the value that {@code call} yields when it aggregates no rows,
   * provided that rows whose argument is NULL do not affect its result, or null
   * if that is not known. Delegates to {@link Config#emptyInputValue()}. */
  private @Nullable RexLiteral valueOnNoRows(AggregateCall call,
      RelDataType type, RexBuilder rexBuilder) {
    return config.emptyInputValue().apply(call, type, rexBuilder);
  }

  /** The default {@link EmptyInputValue}: recognizes the standard aggregate
   * functions by kind, 0 for COUNT and SUM0, NULL for SUM, MIN, MAX and AVG. */
  private static @Nullable RexLiteral standardValueOnNoRows(AggregateCall call,
      RelDataType type, RexBuilder rexBuilder) {
    switch (call.getAggregation().getKind()) {
    case COUNT:
    case SUM0:
      return rexBuilder.makeZeroLiteral(type);
    case SUM:
    case MIN:
    case MAX:
    case AVG:
      return rexBuilder.makeNullLiteral(type);
    default:
      return null;
    }
  }

  /** Describes the result of an aggregate call over no rows, and thereby
   * whether the rule may prune branches for it. */
  @FunctionalInterface
  public interface EmptyInputValue {
    /** Returns the value that {@code call} yields when it aggregates no rows,
     * provided that rows whose argument is NULL do not affect its result;
     * returns null if that is not known, in which case the rule leaves the
     * call alone.
     *
     * @param call Aggregate call
     * @param type Type of the call's result
     * @param rexBuilder Builder for the returned literal
     */
    @Nullable RexLiteral apply(AggregateCall call, RelDataType type,
        RexBuilder rexBuilder);
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

  /** Returns the logical expressions that {@code node}, an input of another
   * relational expression, stands for.
   *
   * <p>The planner does not hand a rule the real operator, but a wrapper:
   *
   * <ul>
   * <li>In the Hep planner, an input is a {@code HepRelVertex}; we look through
   * it ({@link RelNode#stripped()}) and return the single wrapped expression.
   * <li>In the Volcano planner, an input is a {@link RelSubset}, a set of
   * equivalent expressions, for example both a {@code LogicalAggregate} and an
   * {@code EnumerableAggregate}. We return all of its members, so that a
   * pattern is found whichever member happens to match.
   * </ul>
   *
   * <p>Only logical expressions are returned (see {@link #isLogical}). */
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

  /** Returns whether {@code rel} is logical, that is, has not been implemented
   * in a physical convention such as Enumerable.
   *
   * <p>The rule rebuilds a branch using {@link RelBuilder}, which creates
   * logical nodes. Building them on top of the inputs of a physical node would
   * mix conventions, so physical expressions are ignored. For the same reason
   * the rule's operands only match a logical aggregate and union.
   *
   * <p>A null convention is treated as logical, to be safe. */
  private static boolean isLogical(RelNode rel) {
    final Convention convention = rel.getConvention();
    return convention == null || convention == Convention.NONE;
  }

  private static boolean isPureGroupBy(RelNode node) {
    // GROUP BY () is excluded: it returns one row even for empty input.
    return node instanceof Aggregate
        && !((Aggregate) node).getGroupSet().isEmpty()
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

    /** Returns what an aggregate call yields over no rows, which decides which
     * calls the rule may prune. The default recognizes the standard functions
     * by kind: 0 for COUNT and SUM0, NULL for SUM, MIN, MAX and AVG. Override
     * it to describe a custom aggregate function that ignores NULL inputs, for
     * example a user-defined function that returns NULL on empty input. */
    @Value.Default
    default EmptyInputValue emptyInputValue() {
      return AggregateUnionNullBranchRule::standardValueOnNoRows;
    }

    /** Sets {@link #emptyInputValue()}. */
    Config withEmptyInputValue(EmptyInputValue emptyInputValue);

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
