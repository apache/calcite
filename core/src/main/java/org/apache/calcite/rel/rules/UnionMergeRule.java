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
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;

/**
 * UnionMergeRule implements the rule for combining two
 * non-distinct {@link org.apache.calcite.rel.core.SetOp}s
 * into a single {@link org.apache.calcite.rel.core.SetOp}.
 *
 * <p>Originally written for {@link Union} (hence the name),
 * but now also applies to {@link Intersect}.
 */
public class UnionMergeRule extends RelOptRule {
  public static final UnionMergeRule INSTANCE =
      new UnionMergeRule(LogicalUnion.class, "UnionMergeRule",
          RelFactories.LOGICAL_BUILDER);
  public static final UnionMergeRule INTERSECT_INSTANCE =
      new UnionMergeRule(LogicalIntersect.class, "IntersectMergeRule",
          RelFactories.LOGICAL_BUILDER);
  public static final UnionMergeRule MINUS_INSTANCE =
      new UnionMergeRule(LogicalMinus.class, "MinusMergeRule",
          RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /** Creates a UnionMergeRule. */
  public UnionMergeRule(Class<? extends SetOp> unionClazz, String description,
      RelBuilderFactory relBuilderFactory) {
    super(
        operand(unionClazz,
            operand(RelNode.class, any()),
            operand(RelNode.class, any())),
        relBuilderFactory, description);
  }

  @Deprecated // to be removed before 2.0
  public UnionMergeRule(Class<? extends Union> unionClazz,
      RelFactories.SetOpFactory setOpFactory) {
    this(unionClazz, null, RelBuilder.proto(setOpFactory));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final SetOp topOp = call.rel(0);
    @SuppressWarnings("unchecked") final Class<? extends SetOp> setOpClass =
        (Class) operands.get(0).getMatchedClass();

    // For Union and Intersect, we want to combine the set-op that's in the
    // second input first.
    //
    // For example, we reduce
    //    Union(Union(a, b), Union(c, d))
    // to
    //    Union(Union(a, b), c, d)
    // in preference to
    //    Union(a, b, Union(c, d))
    //
    // But for Minus, we can only reduce the left input. It is not valid to
    // reduce
    //    Minus(a, Minus(b, c))
    // to
    //    Minus(a, b, c)
    //
    // Hence, that's why the rule pattern matches on generic RelNodes rather
    // than explicit sub-classes of SetOp.  By doing so, and firing this rule
    // in a bottom-up order, it allows us to only specify a single
    // pattern for this rule.
    final SetOp bottomOp;
    if (setOpClass.isInstance(call.rel(2))
        && !Minus.class.isAssignableFrom(setOpClass)) {
      bottomOp = call.rel(2);
    } else if (setOpClass.isInstance(call.rel(1))) {
      bottomOp = call.rel(1);
    } else {
      return;
    }

    // Can only combine (1) if all operators are ALL,
    // or (2) top operator is DISTINCT (i.e. not ALL).
    // In case (2), all operators become DISTINCT.
    if (topOp.all && !bottomOp.all) {
      return;
    }

    // Combine the inputs from the bottom set-op with the other inputs from
    // the top set-op.
    final RelBuilder relBuilder = call.builder();
    if (setOpClass.isInstance(call.rel(2))
        && !Minus.class.isAssignableFrom(setOpClass)) {
      relBuilder.push(topOp.getInput(0));
      relBuilder.pushAll(bottomOp.getInputs());
      // topOp.getInputs().size() may be more than 2
      for (int index = 2; index < topOp.getInputs().size(); index++) {
        relBuilder.push(topOp.getInput(index));
      }
    } else {
      relBuilder.pushAll(bottomOp.getInputs());
      relBuilder.pushAll(Util.skip(topOp.getInputs()));
    }
    int n = bottomOp.getInputs().size()
        + topOp.getInputs().size()
        - 1;
    if (topOp instanceof Union) {
      relBuilder.union(topOp.all, n);
    } else if (topOp instanceof Intersect) {
      relBuilder.intersect(topOp.all, n);
    } else if (topOp instanceof Minus) {
      relBuilder.minus(topOp.all, n);
    }
    call.transformTo(relBuilder.build());
  }
}

// End UnionMergeRule.java
