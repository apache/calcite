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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rule to expand disjunction for single table in condition of a {@link Filter} or {@link Join},
 * It makes sense to make this optimization part of the predicate pushdown. For example:
 *
 * <blockquote><pre>{@code
 * select t1.name from t1, t2
 * where t1.id = t2.id
 * and (
 *  (t1.id > 20 and t2.height < 50)
 *  or
 *  (t1.weight < 200 and t2.sales > 100)
 * )
 * }</pre></blockquote>
 *
 * <p>we can expand to obtain the condition
 *
 * <blockquote><pre>{@code
 * select t1.name from t1, t2
 * where t1.id = t2.id
 * and (
 *  (t1.id > 20 and t2.height < 50)
 *  or
 *  (t1.weight < 200 and t2.sales > 100)
 * )
 * and (t1.id > 20 or t1.weight < 200)
 * and (t2.height < 50 or t2.sales > 100)
 * }</pre></blockquote>
 *
 * <p>new generated predicates are redundant, but they could be pushed down to
 * scan operator of t1/t2 and reduce the cardinality.
 *
 * <p>Note: This rule should only be applied once and it should be used before
 * {@link CoreRules#FILTER_INTO_JOIN} and {@link CoreRules#JOIN_CONDITION_PUSH}.
 * In order to avoid it interacting with other expression-related rules and causing
 * an infinite loop, it is recommended to use it alone in HepPlanner first,
 * and then do predicate pushdown.
 *
 * @see CoreRules#EXPAND_FILTER_DISJUNCTION_GLOBAL
 * @see CoreRules#EXPAND_JOIN_DISJUNCTION_GLOBAL
 */
@Value.Enclosing
public class ExpandDisjunctionForTableRule
    extends RelRule<ExpandDisjunctionForTableRule.Config>
    implements TransformationRule {

  /**
   * Creates a ExpandDisjunctionForTableRule.
   */
  protected ExpandDisjunctionForTableRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    config.matchHandler().accept(this, call);
  }

  /**
   * Expand predicates for condition of a {@link Filter} or {@link Join}.
   *
   * @param condition   Condition to be expanded in Filter or Join
   * @param relNode     The Filter or Join node
   * @param fieldList   The field list of the Filter or Join inputs.
   * @param relBuilder  Builder
   * @param mq          RelMetadataQuery, which is used to get the expression lineage
   *
   * @return The redundant predicate expanded from the condition parameter. It is formed by
   * concatenating the predicates that can be pushed down to single table with AND.
   */
  private RexNode apply(
      RexNode condition,
      RelNode relNode,
      List<RelDataTypeField> fieldList,
      RelBuilder relBuilder,
      RelMetadataQuery mq) {
    final Map<Integer, RexTableInputRef.RelTableRef> inputRefToTableRef = new HashMap<>();

    ImmutableBitSet columnBits = RelOptUtil.InputFinder.bits(condition);
    if (columnBits.isEmpty()) {
      return condition;
    }

    // For each table discover the conditions that refer to columns of that
    // table (and refer to no other tables)
    for (int columnBit : columnBits.asList()) {
      Set<RexNode> exprLineage =
          mq.getExpressionLineage(
              relNode,
              RexInputRef.of(columnBit, fieldList));
      // If mq.getExpressionLineage cannot get result, skip it
      if (exprLineage == null) {
        continue;
      }
      Set<RexTableInputRef.RelTableRef> relTableRefs =
          RexUtil.gatherTableReferences(Lists.newArrayList(exprLineage));
      // If the column is computed by an expression that depends on multiple tables, skip it
      if (relTableRefs.size() != 1) {
        continue;
      }
      inputRefToTableRef.put(columnBit, relTableRefs.iterator().next());
    }

    ExpandDisjunctionForTableHelper expandHelper =
        new ExpandDisjunctionForTableHelper(inputRefToTableRef, relBuilder, config.bloat());
    Map<RexTableInputRef.RelTableRef, RexNode> expandResult = expandHelper.expand(condition);

    RexNode newCondition = condition;
    for (RexNode expandCondition : expandResult.values()) {
      newCondition = relBuilder.and(newCondition, expandCondition);
    }
    return newCondition;
  }

  private static void matchFilter(ExpandDisjunctionForTableRule rule, RelOptRuleCall call) {
    Filter filter = call.rel(0);
    RelMetadataQuery mq = call.getMetadataQuery();
    RelBuilder relBuilder = call.builder();

    RexNode newCondition =
        rule.apply(
            filter.getCondition(),
            filter,
            filter.getRowType().getFieldList(),
            relBuilder,
            mq);
    if (newCondition.equals(filter.getCondition())) {
      return;
    }
    Filter newFilter = filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);
    call.transformTo(newFilter);
  }

  private static void matchJoin(ExpandDisjunctionForTableRule rule, RelOptRuleCall call) {
    Join join = call.rel(0);
    RelMetadataQuery mq = call.getMetadataQuery();
    RelBuilder relBuilder = call.builder();

    List<RelDataTypeField> fieldList =
        Lists.newArrayList(join.getLeft().getRowType().getFieldList());
    fieldList.addAll(join.getRight().getRowType().getFieldList());
    RexNode newCondition =
        rule.apply(
            join.getCondition(),
            join,
            fieldList,
            relBuilder,
            mq);
    if (newCondition.equals(join.getCondition())) {
      return;
    }
    Join newJoin =
        join.copy(
            join.getTraitSet(),
            newCondition,
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());
    call.transformTo(newJoin);
  }

  /**
   * Helper class to expand predicates.
   */
  private static class ExpandDisjunctionForTableHelper {
    private final Map<Integer, RexTableInputRef.RelTableRef> inputRefToTableRef;

    private final RelBuilder relBuilder;

    private final int maxNodeCount;

    // Used to record the number of redundant expressions expanded.
    private int currentCount;

    private ExpandDisjunctionForTableHelper(
        Map<Integer, RexTableInputRef.RelTableRef> inputRefToTableRef,
        RelBuilder relBuilder,
        int maxNodeCount) {
      this.inputRefToTableRef = inputRefToTableRef;
      this.relBuilder = relBuilder;
      this.maxNodeCount = maxNodeCount;
    }

    private Map<RexTableInputRef.RelTableRef, RexNode> expand(RexNode condition) {
      try {
        this.currentCount = 0;
        return expandDeep(condition);
      } catch (OverLimitException e) {
        return new HashMap<>();
      }
    }

    /**
     * Expand predicates recursively that can be pushed down to single table.
     *
     * @param condition   Predicate to be expanded
     * @return  A map from a table to a (combined) predicate that can be pushed down
     * and only depends on columns of the table
     */
    private Map<RexTableInputRef.RelTableRef, RexNode> expandDeep(RexNode condition) {
      Map<RexTableInputRef.RelTableRef, RexNode> additionalConditions = new HashMap<>();

      ImmutableBitSet inputRefs = RelOptUtil.InputFinder.bits(condition);
      if (inputRefs.isEmpty()) {
        return additionalConditions;
      }

      RexTableInputRef.RelTableRef tableRef = inputRefsBelongToOneTable(inputRefs);
      if (tableRef != null) {
        // The condition already belongs to one table, return it directly
        checkExpandCount(condition.nodeCount());
        additionalConditions.put(tableRef, condition);
        return additionalConditions;
      }

      // Recursively expand the expression according to whether it is a conjunction
      // or a disjunction. If it is neither a disjunction nor a conjunction, it cannot
      // be expanded further and an empty Map is returned.
      switch (condition.getKind()) {
      case AND:
        List<RexNode> andOperands = RexUtil.flattenAnd(((RexCall) condition).getOperands());
        for (RexNode andOperand : andOperands) {
          Map<RexTableInputRef.RelTableRef, RexNode> operandResult = expandDeep(andOperand);
          combinePredicatesUsingAnd(additionalConditions, operandResult);
        }
        break;
      case OR:
        List<RexNode> orOperands = RexUtil.flattenOr(((RexCall) condition).getOperands());
        additionalConditions.putAll(expandDeep(orOperands.get(0)));
        for (int i = 1; i < orOperands.size(); i++) {
          Map<RexTableInputRef.RelTableRef, RexNode> operandResult =
              expandDeep(orOperands.get(i));
          combinePredicatesUsingOr(additionalConditions, operandResult);

          if (additionalConditions.isEmpty()) {
            break;
          }
        }
        break;
      default:
        break;
      }

      return additionalConditions;
    }

    private RexTableInputRef.@Nullable RelTableRef inputRefsBelongToOneTable(
        ImmutableBitSet inputRefs) {
      RexTableInputRef.RelTableRef tableRef = inputRefToTableRef.get(inputRefs.nth(0));
      if (tableRef == null) {
        return null;
      }
      for (int inputBit : inputRefs.asList()) {
        RexTableInputRef.RelTableRef inputBitTableRef = inputRefToTableRef.get(inputBit);
        if (!tableRef.equals(inputBitTableRef)) {
          return null;
        }
      }
      return tableRef;
    }

    /**
     * Combine predicates that depend on the same table using conjunctions.
     * The result is returned by modifying baseMap. For example:
     *
     * <p>baseMap: {t1: p1, t2: p2}
     *
     * <p>forMergeMap: {t1: p11, t3: p3}
     *
     * <p>result: {t1: p1 AND p11, t2: p2, t3: p3}
     *
     * @param baseMap       Additional predicates that current conjunction has already saved
     * @param forMergeMap   Additional predicates that current operand has expanded
     */
    private void combinePredicatesUsingAnd(
        Map<RexTableInputRef.RelTableRef, RexNode> baseMap,
        Map<RexTableInputRef.RelTableRef, RexNode> forMergeMap) {
      for (Map.Entry<RexTableInputRef.RelTableRef, RexNode> entry : forMergeMap.entrySet()) {
        RexNode mergedRex =
            relBuilder.and(
                entry.getValue(),
                baseMap.getOrDefault(entry.getKey(), relBuilder.literal(true)));
        int oriCount = entry.getValue().nodeCount()
            + (baseMap.containsKey(entry.getKey())
                ? baseMap.get(entry.getKey()).nodeCount()
                : 0);
        checkExpandCount(mergedRex.nodeCount() - oriCount);
        baseMap.put(entry.getKey(), mergedRex);
      }
    }

    /**
     * Combine predicates that depend on the same table using disjunctions.
     * The result is returned by modifying baseMap. For example:
     *
     * <p>baseMap: {t1: p1, t2: p2}
     *
     * <p>forMergeMap: {t1: p11, t3: p3}
     *
     * <p>result: {t1: p1 OR p11}
     *
     * @param baseMap       Additional predicates that current disjunction has already saved
     * @param forMergeMap   Additional predicates that current operand has expanded
     */
    private void combinePredicatesUsingOr(
        Map<RexTableInputRef.RelTableRef, RexNode> baseMap,
        Map<RexTableInputRef.RelTableRef, RexNode> forMergeMap) {
      if (baseMap.isEmpty()) {
        return;
      }

      Iterator<Map.Entry<RexTableInputRef.RelTableRef, RexNode>> iterator =
          baseMap.entrySet().iterator();
      while (iterator.hasNext()) {
        int forMergeNodeCount = 0;

        Map.Entry<RexTableInputRef.RelTableRef, RexNode> entry = iterator.next();
        if (!forMergeMap.containsKey(entry.getKey())) {
          checkExpandCount(-entry.getValue().nodeCount());
          iterator.remove();
          continue;
        } else {
          forMergeNodeCount = forMergeMap.get(entry.getKey()).nodeCount();
        }
        RexNode mergedRex =
            relBuilder.or(
                entry.getValue(),
                forMergeMap.get(entry.getKey()));
        int oriCount = entry.getValue().nodeCount() + forMergeNodeCount;
        checkExpandCount(mergedRex.nodeCount() - oriCount);
        baseMap.put(entry.getKey(), mergedRex);
      }
    }

    /**
     * Check whether the number of redundant expressions generated in the expansion
     * exceeds the limit.
     */
    private void checkExpandCount(int changeCount) {
      currentCount += changeCount;
      if (maxNodeCount > 0 && currentCount > maxNodeCount) {
        throw OverLimitException.INSTANCE;
      }
    }

    /** Exception to catch when we pass the limit. */
    private static class OverLimitException extends ControlFlowException {
      protected static final OverLimitException INSTANCE = new OverLimitException();

      private OverLimitException() {}
    }
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config FILTER = ImmutableExpandDisjunctionForTableRule.Config.builder()
        .withMatchHandler(ExpandDisjunctionForTableRule::matchFilter)
        .build()
        .withOperandSupplier(b -> b.operand(Filter.class).anyInputs());

    Config JOIN = ImmutableExpandDisjunctionForTableRule.Config.builder()
        .withMatchHandler(ExpandDisjunctionForTableRule::matchJoin)
        .build()
        .withOperandSupplier(b -> b.operand(Join.class).anyInputs());

    @Override default ExpandDisjunctionForTableRule toRule() {
      return new ExpandDisjunctionForTableRule(this);
    }

    /**
     * Limit to the size growth of the condition allowed during expansion.
     * Default is {@link RelOptUtil#DEFAULT_BLOAT} (100).
     */
    default int bloat() {
      return RelOptUtil.DEFAULT_BLOAT;
    }

    /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
    MatchHandler<ExpandDisjunctionForTableRule> matchHandler();

    /** Sets {@link #matchHandler()}. */
    Config withMatchHandler(MatchHandler<ExpandDisjunctionForTableRule> matchHandler);
  }
}
