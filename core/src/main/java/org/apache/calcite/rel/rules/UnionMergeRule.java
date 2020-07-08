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
 * but now also applies to {@link Intersect} and {@link Minus}.
 */
public class UnionMergeRule
    extends RelRule<UnionMergeRule.Config>
    implements TransformationRule {
  /** @deprecated Use {@link CoreRules#UNION_MERGE}. */
  @Deprecated // to be removed before 1.25
  public static final UnionMergeRule INSTANCE =
      Config.DEFAULT.toRule();

  /** @deprecated Use {@link CoreRules#INTERSECT_MERGE}. */
  @Deprecated // to be removed before 1.25
  public static final UnionMergeRule INTERSECT_INSTANCE =
      Config.INTERSECT.toRule();

  /** @deprecated Use {@link CoreRules#MINUS_MERGE}. */
  @Deprecated // to be removed before 1.25
  public static final UnionMergeRule MINUS_INSTANCE =
      Config.MINUS.toRule();

  //~ Constructors -----------------------------------------------------------

  /** Creates a UnionMergeRule. */
  protected UnionMergeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public UnionMergeRule(Class<? extends SetOp> setOpClass, String description,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .as(Config.class)
        .withOperandFor(setOpClass));
  }

  @Deprecated // to be removed before 2.0
  public UnionMergeRule(Class<? extends Union> setOpClass,
      RelFactories.SetOpFactory setOpFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(RelBuilder.proto(setOpFactory))
        .as(Config.class)
        .withOperandFor(setOpClass));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    // It avoids adding the rule match to the match queue in case the rule is known to be a no-op
    final SetOp topOp = call.rel(0);
    @SuppressWarnings("unchecked") final Class<? extends SetOp> setOpClass =
        (Class<? extends SetOp>) operands.get(0).getMatchedClass();
    final SetOp bottomOp;
    if (setOpClass.isInstance(call.rel(2))
        && !Minus.class.isAssignableFrom(setOpClass)) {
      bottomOp = call.rel(2);
    } else if (setOpClass.isInstance(call.rel(1))) {
      bottomOp = call.rel(1);
    } else {
      return false;
    }

    if (topOp.all && !bottomOp.all) {
      return false;
    }

    return true;
  }

  @Override public void onMatch(RelOptRuleCall call) {
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

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.withDescription("UnionMergeRule")
        .as(Config.class)
        .withOperandFor(LogicalUnion.class);

    Config INTERSECT = EMPTY.withDescription("IntersectMergeRule")
        .as(Config.class)
        .withOperandFor(LogicalIntersect.class);

    Config MINUS = EMPTY.withDescription("MinusMergeRule")
        .as(Config.class)
        .withOperandFor(LogicalMinus.class);

    @Override default UnionMergeRule toRule() {
      return new UnionMergeRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends RelNode> setOpClass) {
      return withOperandSupplier(b0 ->
          b0.operand(setOpClass).inputs(
              b1 -> b1.operand(RelNode.class).anyInputs(),
              b2 -> b2.operand(RelNode.class).anyInputs()))
          .as(Config.class);
    }
  }
}
