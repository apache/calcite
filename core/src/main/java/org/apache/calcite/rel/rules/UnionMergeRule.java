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
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;

/**
 * UnionMergeRule implements the rule for combining two
 * non-distinct {@link org.apache.calcite.rel.core.Union}s
 * into a single {@link org.apache.calcite.rel.core.Union}.
 */
public class UnionMergeRule extends RelOptRule {
  public static final UnionMergeRule INSTANCE =
      new UnionMergeRule(LogicalUnion.class, RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /** Creates a UnionMergeRule. */
  public UnionMergeRule(Class<? extends Union> unionClazz,
      RelBuilderFactory relBuilderFactory) {
    super(
        operand(unionClazz,
            operand(RelNode.class, any()),
            operand(RelNode.class, any())),
        relBuilderFactory, null);
  }

  @Deprecated // to be removed before 2.0
  public UnionMergeRule(Class<? extends Union> unionClazz,
      RelFactories.SetOpFactory setOpFactory) {
    this(unionClazz, RelBuilder.proto(setOpFactory));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Union topUnion = call.rel(0);

    // We want to combine the Union that's in the second input first.
    // Hence, that's why the rule pattern matches on generic RelNodes
    // rather than explicit UnionRels.  By doing so, and firing this rule
    // in a bottom-up order, it allows us to only specify a single
    // pattern for this rule.
    final Union bottomUnion;
    if (call.rel(2) instanceof Union) {
      bottomUnion = call.rel(2);
    } else if (call.rel(1) instanceof Union) {
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
    final RelBuilder relBuilder = call.builder();
    if (call.rel(2) instanceof Union) {
      assert topUnion.getInputs().size() == 2;
      relBuilder.push(topUnion.getInput(0));
      relBuilder.pushAll(bottomUnion.getInputs());
    } else {
      relBuilder.pushAll(bottomUnion.getInputs());
      relBuilder.pushAll(Util.skip(topUnion.getInputs()));
    }
    int n = bottomUnion.getInputs().size()
        + topUnion.getInputs().size()
        - 1;
    relBuilder.union(true, n);
    call.transformTo(relBuilder.build());
  }
}

// End UnionMergeRule.java
