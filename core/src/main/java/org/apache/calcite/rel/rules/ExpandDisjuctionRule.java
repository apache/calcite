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
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rule to expand disjuction in condition of a {@link Filter} or {@link Join},
 * It makes sense to make this optimization part of the predicate pushdown. For example:
 *
 * <blockquote><pre>
 * select t1.name from t1, t2
 * where t1.id = t2.id
 * and (
 *  (t1.id > 20 and t2.height < 50)
 *  or
 *  (t1.weight < 200 and t2.sales > 100)
 * )
 * </pre></blockquote>
 *
 * we can expand to obtain the condition
 *
 * <blockquote><pre>
 * t1.id > 20 or t1.weight < 200
 * t2.height < 50 or t2.sales > 100
 * </pre></blockquote>
 *
 * new generated predicates are redundant, but they could be pushed down to
 * scan operator of t1/t2 and reduce the cardinality.
 *
 * <p>This rule should only be applied once to avoid generate same redundant expression and
 * it should be used before {@link CoreRules#FILTER_INTO_JOIN} and {@link CoreRules#JOIN_CONDITION_PUSH}.
 *
 * @see CoreRules#EXPAND_FILTER_DISJUCTION
 * @see CoreRules#EXPAND_JOIN_DISJUCTION
 */
@Value.Enclosing
public class ExpandDisjuctionRule
    extends RelRule<ExpandDisjuctionRule.Config>
    implements TransformationRule {

  /**
   * Creates a ExpandDisjuctionRule.
   */
  protected ExpandDisjuctionRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    config.matchHandler().accept(this, call);
  }

  /**
   * Expand predicates for condition of a {@link Filter} or {@link Join}.
   */
  private static RexNode apply(
      RexNode condition,
      RelNode relNode,
      List<RelDataTypeField> fieldList,
      RelBuilder relBuilder,
      RelMetadataQuery mq) {
    HashMap<Integer, RexTableInputRef.RelTableRef> inputRefToTableRef = new HashMap<>();
    ImmutableBitSet columnBits = RelOptUtil.InputFinder.bits(condition);
    if (columnBits.isEmpty()) {
      return condition;
    }
    // Trace which table does the column referenced in the condition come from
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
      // If the column come from multiple tables, skip it
      if (relTableRefs.isEmpty() || relTableRefs.size() > 1) {
        continue;
      }
      inputRefToTableRef.put(columnBit, relTableRefs.iterator().next());
    }

    ExpandDisjuctionHelper expandHelper =
        new ExpandDisjuctionHelper(inputRefToTableRef, relBuilder);
    Map<RexTableInputRef.RelTableRef, RexNode> expandResult = expandHelper.expandDeep(condition);
    RexNode newCondition = condition;
    for (RexNode expandCondition : expandResult.values()) {
      newCondition = relBuilder.and(newCondition, expandCondition);
    }
    return newCondition;
  }

  private static void matchFilter(ExpandDisjuctionRule rule, RelOptRuleCall call) {
    Filter filter = call.rel(0);
    RelMetadataQuery mq = call.getMetadataQuery();
    RelBuilder relBuilder = call.builder();

    RexNode newCondition =
        apply(
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

  private static void matchJoin(ExpandDisjuctionRule rule, RelOptRuleCall call) {
    Join join = call.rel(0);
    RelMetadataQuery mq = call.getMetadataQuery();
    RelBuilder relBuilder = call.builder();

    List<RelDataTypeField> fieldList =
        Lists.newArrayList(join.getLeft().getRowType().getFieldList());
    fieldList.addAll(join.getRight().getRowType().getFieldList());
    RexNode newCondition =
        apply(
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
  private static class ExpandDisjuctionHelper {

    private final Map<Integer, RexTableInputRef.RelTableRef> inputRefToTableRef;

    private final RelBuilder relBuilder;

    private ExpandDisjuctionHelper(
        Map<Integer, RexTableInputRef.RelTableRef> inputRefToTableRef,
        RelBuilder relBuilder) {
      this.inputRefToTableRef = inputRefToTableRef;
      this.relBuilder = relBuilder;
    }

    /**
     * Expand predicates recursively that can be pushed down to single table.
     *
     * @param condition   Predicate to be expanded
     * @return  Additional predicates that can be pushed down for each table
     */
    private Map<RexTableInputRef.RelTableRef, RexNode> expandDeep(RexNode condition) {
      Map<RexTableInputRef.RelTableRef, RexNode> additionalConditions = new HashMap<>();
      ImmutableBitSet inputRefs = RelOptUtil.InputFinder.bits(condition);
      if (inputRefs.isEmpty()) {
        return additionalConditions;
      }
      RexTableInputRef.RelTableRef tableRef = inputRefsBelongOneTable(inputRefs);
      // The condition already belongs to one table, return it directly
      if (tableRef != null) {
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
          mergeAnd(additionalConditions, operandResult);
        }
        break;
      case OR:
        List<RexNode> orOperands = RexUtil.flattenOr(((RexCall) condition).getOperands());
        additionalConditions.putAll(expandDeep(orOperands.get(0)));
        for (int i = 1; i < orOperands.size(); i++) {
          Map<RexTableInputRef.RelTableRef, RexNode> operandResult =
              expandDeep(orOperands.get(i));
          mergeOr(additionalConditions, operandResult);
        }
        break;
      }

      return additionalConditions;
    }

    private RexTableInputRef.@Nullable RelTableRef inputRefsBelongOneTable(
        ImmutableBitSet inputRefs) {
      RexTableInputRef.RelTableRef tableRef = inputRefToTableRef.get(inputRefs.nth(0));
      if (tableRef == null) {
        return null;
      }
      for (int inputBit : inputRefs.asList()) {
        RexTableInputRef.RelTableRef inputBitTableRef = inputRefToTableRef.get(inputBit);
        if (inputBitTableRef == null || !inputBitTableRef.equals(tableRef)) {
          return null;
        }
      }
      return tableRef;
    }

    /**
     * For additional predicates of each operand in conjuction, all of them should be retained and
     * use 'AND' to combine expressions.
     *
     * @param baseMap       Additional predicates that current conjunction has already saved
     * @param forMergeMap   Additional predicates that current operand has expanded
     */
    private void mergeAnd(
        Map<RexTableInputRef.RelTableRef, RexNode> baseMap,
        Map<RexTableInputRef.RelTableRef, RexNode> forMergeMap) {
      for (Map.Entry<RexTableInputRef.RelTableRef, RexNode> entry : forMergeMap.entrySet()) {
        RexNode mergeExpression =
            relBuilder.and(
                entry.getValue(),
                baseMap.getOrDefault(entry.getKey(), relBuilder.literal(true)));
        baseMap.put(entry.getKey(), mergeExpression);
      }
    }

    /**
     * Only if all operands in disjunction have additional predicates for table t1, we use 'OR'
     * to combine expressions and retain them as additional predicates of table t1.
     *
     * @param baseMap       Additional predicates that current disjunction has already saved
     * @param forMergeMap   Additional predicates that current operand has expanded
     */
    private void mergeOr(
        Map<RexTableInputRef.RelTableRef, RexNode> baseMap,
        Map<RexTableInputRef.RelTableRef, RexNode> forMergeMap) {
      if (baseMap.isEmpty()) {
        return;
      }
      for (Map.Entry<RexTableInputRef.RelTableRef, RexNode> entry : baseMap.entrySet()) {
        if (!forMergeMap.containsKey(entry.getKey())) {
          baseMap.remove(entry.getKey());
          continue;
        }
        RexNode mergedRex =
            relBuilder.or(
                entry.getValue(),
                forMergeMap.get(entry.getKey()));
        baseMap.put(entry.getKey(), mergedRex);
      }
    }
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config FILTER = ImmutableExpandDisjuctionRule.Config.builder()
        .withMatchHandler(ExpandDisjuctionRule::matchFilter)
        .build()
        .withOperandSupplier(b ->
            b.operand(Filter.class).anyInputs());

    Config JOIN = ImmutableExpandDisjuctionRule.Config.builder()
        .withMatchHandler(ExpandDisjuctionRule::matchJoin)
        .build()
        .withOperandSupplier(b ->
            b.operand(Join.class).anyInputs());

    @Override default ExpandDisjuctionRule toRule() {
      return new ExpandDisjuctionRule(this);
    }

    /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
    MatchHandler<ExpandDisjuctionRule> matchHandler();

    /** Sets {@link #matchHandler()}. */
    ExpandDisjuctionRule.Config withMatchHandler(MatchHandler<ExpandDisjuctionRule> matchHandler);
  }
}
