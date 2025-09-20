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
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.math.BigDecimal;

/**
 * This rule try to merge the double {@link Sort},one is Limit semantics,
 * another sort is Limit or TOPN semantics.
 *
 * <p> It generally used with the {@link SortProjectTransposeRule} rule.
 *
 * <p> For example:
 * <blockquote><pre>{@code
 * select
 *   concat('-', N_REGIONKEY) from
 *   (
 *     select
 *       *
 *     from nation limit 10000) limit 10}
 *  </pre></blockquote>
 *
 * <p> will convert to
 * <blockquote><pre>{@code
 * select
 *   concat('-', N_REGIONKEY)
 * from
 *   nation limit 10
 * }</pre></blockquote>
 *
 * <p> The sql :
 * <blockquote><pre>{@code
 * select concat('-',N_REGIONKEY) from
 * (SELECT * FROM nation order BY N_REGIONKEY DESC LIMIT 10000) limit 10
 * }</pre></blockquote>
 *
 * <p> will convert to
 * <blockquote><pre>{@code
 * SELECT concat('-',N_REGIONKEY) FROM nation order BY N_REGIONKEY DESC LIMIT 10
 * }</pre></blockquote>
 *
 * <p> In the future,we could also extend other sort merge logic in this rule.
 *
 * @see CoreRules#LIMIT_MERGE
 */
@Value.Enclosing
public class SortMergeRule
    extends RelRule<SortMergeRule.Config>
    implements TransformationRule {

  protected SortMergeRule(final SortMergeRule.Config config) {
    super(config);
  }

  @Override public void onMatch(final RelOptRuleCall call) {
    config.matchHandler().accept(this, call);
  }

  private static void limitMerge(SortMergeRule rule,
      RelOptRuleCall call) {
    final Sort topSort = call.rel(0);
    final Sort bottomSort = call.rel(1);

    if (bottomSort.offset != null || bottomSort.fetch == null) {
      // we could do nothing here
      return;
    }

    final RelBuilder builder = call.builder();

    final Number topFetch = topSort.fetch instanceof RexLiteral
        ? RexLiteral.numberValue(topSort.fetch) : null;

    final Number bottomFetch = bottomSort.fetch instanceof RexLiteral
        ? RexLiteral.numberValue(bottomSort.fetch) : null;

    if (topFetch == null || bottomFetch == null) {
      return;
    }

    // Get the minimum limit value from parent and child sort RelNode
    final Number minFetch = ((BigDecimal) topFetch).min((BigDecimal) bottomFetch);

    builder.push(bottomSort.getInput());

    // Get the collation fields
    final ImmutableList<RexNode> fields = builder.fields(bottomSort.getCollation());
    builder.sortLimit(-1, minFetch, fields);
    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    SortMergeRule.Config LIMIT_MERGE = ImmutableSortMergeRule.Config.builder()
        .withMatchHandler(SortMergeRule::limitMerge)
        .build()
        .withOperandSupplier(b ->
            b.operand(Sort.class)
                .predicate(p ->
                    RelOptUtil.isPureLimit(p) && !RelOptUtil.isOffset(p))
                .oneInput(b1 ->
                    b1.operand(Sort.class).anyInputs()))
        .withDescription("SortMergeRule:LIMIT_MERGE");


    @Override default SortMergeRule toRule() {
      return new SortMergeRule(this);
    }

    /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
    MatchHandler<SortMergeRule> matchHandler();

    /** Sets {@link #matchHandler()}. */
    SortMergeRule.Config withMatchHandler(MatchHandler<SortMergeRule> matchHandler);
  }
}
