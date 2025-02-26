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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.tools.RelBuilderFactory;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Sort} past a
 * {@link org.apache.calcite.rel.core.Union}.
 *
 * @see CoreRules#SORT_UNION_TRANSPOSE
 * @see CoreRules#SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH
 */
@Value.Enclosing
public class SortUnionTransposeRule
    extends RelRule<SortUnionTransposeRule.Config>
    implements TransformationRule {

  /** Creates a SortUnionTransposeRule. */
  protected SortUnionTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public SortUnionTransposeRule(
      Class<? extends Sort> sortClass,
      Class<? extends Union> unionClass,
      boolean matchNullFetch,
      RelBuilderFactory relBuilderFactory,
      String description) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .as(Config.class)
        .withOperandFor(sortClass, unionClass)
        .withMatchNullFetch(matchNullFetch));
  }

  // ~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Union union = call.rel(1);
    // We only apply this rule if Union.all is true, Sort.offset is null and Sort.fetch is not
    // a dynamic param.
    // There is a flag indicating if this rule should be applied when
    // Sort.fetch is null.
    return union.all
        && sort.offset == null
        && !(sort.fetch instanceof RexDynamicParam)
        && (config.matchNullFetch() || sort.fetch != null);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Union union = call.rel(1);
    List<RelNode> inputs = new ArrayList<>();
    // Thus we use 'ret' as a flag to identify if we have finished pushing the
    // sort past a union.
    boolean ret = true;
    final RelMetadataQuery mq = call.getMetadataQuery();
    for (RelNode input : union.getInputs()) {
      if (!RelMdUtil.checkInputForCollationAndLimit(mq, input,
          sort.getCollation(), sort.offset, sort.fetch)) {
        ret = false;
        Sort branchSort =
            sort.copy(sort.getTraitSet(), input,
                sort.getCollation(), sort.offset, sort.fetch);
        inputs.add(branchSort);
      } else {
        inputs.add(input);
      }
    }
    // there is nothing to change
    if (ret) {
      return;
    }
    // create new union and sort
    Union unionCopy = (Union) union
        .copy(union.getTraitSet(), inputs, union.all);
    Sort result =
        sort.copy(sort.getTraitSet(), unionCopy, sort.getCollation(),
            sort.offset, sort.fetch);
    call.transformTo(result);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSortUnionTransposeRule.Config.of()
        .withOperandFor(Sort.class, Union.class)
        .withMatchNullFetch(false);

    @Override default SortUnionTransposeRule toRule() {
      return new SortUnionTransposeRule(this);
    }

    /** Whether to match a Sort whose {@link Sort#fetch} is null. Generally
     * this only makes sense if the Union preserves order (and merges). */
    @Value.Default default boolean matchNullFetch() {
      return false;
    }

    /** Sets {@link #matchNullFetch()}. */
    Config withMatchNullFetch(boolean matchNullFetch);

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Sort> sortClass,
        Class<? extends Union> unionClass) {
      return withOperandSupplier(b0 ->
          b0.operand(sortClass).oneInput(b1 ->
              b1.operand(unionClass).anyInputs()))
          .as(Config.class);
    }
  }
}
