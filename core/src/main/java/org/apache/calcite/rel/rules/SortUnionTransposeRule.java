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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Sort} past a
 * {@link org.apache.calcite.rel.core.Union}.
 *
 */
public class SortUnionTransposeRule extends RelOptRule {

  /** Rule instance for Union implementation that does not preserve the
   * ordering of its inputs. Thus, it makes no sense to match this rule
   * if the Sort does not have a limit, i.e., {@link Sort#fetch} is null. */
  public static final SortUnionTransposeRule INSTANCE = new SortUnionTransposeRule(false);

  /** Rule instance for Union implementation that preserves the ordering
   * of its inputs. It is still worth applying this rule even if the Sort
   * does not have a limit, for the merge of already sorted inputs that
   * the Union can do is usually cheap. */
  public static final SortUnionTransposeRule MATCH_NULL_FETCH = new SortUnionTransposeRule(true);

  /** Whether to match a Sort whose {@link Sort#fetch} is null. Generally
   * this only makes sense if the Union preserves order (and merges). */
  private final boolean matchNullFetch;

  // ~ Constructors -----------------------------------------------------------

  private SortUnionTransposeRule(boolean matchNullFetch) {
    this(Sort.class, Union.class, matchNullFetch, RelFactories.LOGICAL_BUILDER,
        "SortUnionTransposeRule:default");
  }

  /**
   * Creates a SortUnionTransposeRule.
   */
  public SortUnionTransposeRule(
      Class<? extends Sort> sortClass,
      Class<? extends Union> unionClass,
      boolean matchNullFetch,
      RelBuilderFactory relBuilderFactory,
      String description) {
    super(
        operand(sortClass,
            operand(unionClass, any())),
        relBuilderFactory, description);
    this.matchNullFetch = matchNullFetch;
  }

  // ~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Union union = call.rel(1);
    // We only apply this rule if Union.all is true and Sort.offset is null.
    // There is a flag indicating if this rule should be applied when
    // Sort.fetch is null.
    return union.all
        && sort.offset == null
        && (matchNullFetch || sort.fetch != null);
  }

  public void onMatch(RelOptRuleCall call) {
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
        Sort branchSort = sort.copy(sort.getTraitSet(), input,
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
    Sort result = sort.copy(sort.getTraitSet(), unionCopy, sort.getCollation(),
        sort.offset, sort.fetch);
    call.transformTo(result);
  }
}

// End SortUnionTransposeRule.java
