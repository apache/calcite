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
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMdUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Sort} past a
 * {@link org.apache.calcite.rel.core.Union}.
 *
 */
public class SortUnionTransposeRule extends RelOptRule {
  public static final SortUnionTransposeRule INSTANCE = new SortUnionTransposeRule();

  // ~ Constructors -----------------------------------------------------------

  /**
   * Creates a SortUnionTransposeRule.
   */
  private SortUnionTransposeRule() {
    super(operand(Sort.class, operand(Union.class, any())));
  }

  // ~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Union union = call.rel(1);
    // It is not valid to apply the rule if
    // Union.all is false;
    // or Sort.offset is not null
    // or Sort.fetch is null.
    if (!union.all || sort.offset != null || sort.fetch == null) {
      return;
    }
    List<RelNode> inputs = new ArrayList<>();
    // Thus we use 'ret' as a flag to identify if we have finished pushing the
    // sort past a union.
    boolean ret = true;
    for (RelNode input : union.getInputs()) {
      if (!RelMdUtil.checkInputForCollationAndLimit(input, sort.getCollation(),
          sort.offset, sort.fetch)) {
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
