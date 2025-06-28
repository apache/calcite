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
  private static class ExpandDisjunctionForTableHelper
      extends RexUtil.ExpandDisjunctionHelper<RexTableInputRef.RelTableRef> {
    private final Map<Integer, RexTableInputRef.RelTableRef> inputRefToTableRef;

    private ExpandDisjunctionForTableHelper(
        Map<Integer, RexTableInputRef.RelTableRef> inputRefToTableRef,
        RelBuilder relBuilder,
        int maxNodeCount) {
      super(relBuilder, maxNodeCount);
      this.inputRefToTableRef = inputRefToTableRef;
    }

    @Override protected boolean canReturnEarly(
        RexNode condition,
        Map<RexTableInputRef.RelTableRef, RexNode> additionalConditions) {
      ImmutableBitSet inputRefs = RelOptUtil.InputFinder.bits(condition);
      if (inputRefs.isEmpty()) {
        return true;
      }

      RexTableInputRef.RelTableRef tableRef = inputRefsBelongToOneTable(inputRefs);
      if (tableRef != null) {
        // The condition already belongs to one table, return it directly
        checkExpandCount(condition.nodeCount());
        additionalConditions.put(tableRef, condition);
        return true;
      }

      return false;
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
