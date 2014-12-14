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
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * UnionMergeRule implements the rule for combining two
 * non-distinct {@link org.apache.calcite.rel.logical.LogicalUnion}s
 * into a single {@link org.apache.calcite.rel.logical.LogicalUnion}.
 */
public class UnionMergeRule extends RelOptRule {
  public static final UnionMergeRule INSTANCE =
      new UnionMergeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a UnionMergeRule.
   */
  private UnionMergeRule() {
    super(
        operand(
            LogicalUnion.class,
            operand(RelNode.class, any()),
            operand(RelNode.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    LogicalUnion topUnion = call.rel(0);
    LogicalUnion bottomUnion;

    // We want to combine the Union that's in the second input first.
    // Hence, that's why the rule pattern matches on generic RelNodes
    // rather than explicit UnionRels.  By doing so, and firing this rule
    // in a bottom-up order, it allows us to only specify a single
    // pattern for this rule.
    if (call.rel(2) instanceof LogicalUnion) {
      bottomUnion = call.rel(2);
    } else if (call.rel(1) instanceof LogicalUnion) {
      bottomUnion = call.rel(1);
    } else {
      return;
    }

    // If distincts haven't been removed yet, defer invoking this rule
    if (!topUnion.all || !bottomUnion.all) {
      return;
    }

    // Combine the inputs from the bottom union with the other inputs from
    // the top union
    List<RelNode> unionInputs = new ArrayList<RelNode>();
    if (call.rel(2) instanceof LogicalUnion) {
      assert topUnion.getInputs().size() == 2;
      unionInputs.add(topUnion.getInput(0));
      unionInputs.addAll(bottomUnion.getInputs());
    } else {
      unionInputs.addAll(bottomUnion.getInputs());
      unionInputs.addAll(Util.skip(topUnion.getInputs()));
    }
    assert unionInputs.size()
        == bottomUnion.getInputs().size()
        + topUnion.getInputs().size()
        - 1;
    LogicalUnion newUnion = LogicalUnion.create(unionInputs, true);
    call.transformTo(newUnion);
  }
}

// End UnionMergeRule.java
