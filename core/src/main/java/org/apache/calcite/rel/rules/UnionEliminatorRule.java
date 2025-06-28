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
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.tools.RelBuilderFactory;

import org.immutables.value.Value;

/**
 * <code>UnionEliminatorRule</code> checks to see if its possible to optimize a
 * Union call by eliminating the Union operator altogether in the case the call
 * consists of only one input.
 *
 * <p>Originally written for {@link Union} (hence the name),
 * but now also applies to {@link Intersect} and {@link Minus}.
 *
 * @see CoreRules#UNION_REMOVE
 */
@Value.Enclosing
public class UnionEliminatorRule
    extends RelRule<UnionEliminatorRule.Config>
    implements SubstitutionRule {

  /** Creates a UnionEliminatorRule. */
  protected UnionEliminatorRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public UnionEliminatorRule(Class<? extends SetOp> setOpClass,
      String description, RelBuilderFactory relBuilderFactory) {
    super(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .as(Config.class)
        .withOperandFor(setOpClass));
  }

  @Deprecated // to be removed before 2.0
  public UnionEliminatorRule(Class<? extends Union> unionClass,
      RelBuilderFactory relBuilderFactory) {
    super(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(unionClass));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    SetOp setOp = call.rel(0);
    return setOp.all && setOp.getInputs().size() == 1;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    SetOp setOp = call.rel(0);
    call.transformTo(setOp.getInputs().get(0));
  }

  @Override public boolean autoPruneOld() {
    return true;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableUnionEliminatorRule.Config.of()
            .withDescription("UnionEliminatorRule")
            .withOperandFor(LogicalUnion.class);

    Config INTERSECT = ImmutableUnionEliminatorRule.Config.of()
            .withDescription("IntersectEliminatorRule")
            .withOperandFor(LogicalIntersect.class);

    Config MINUS = ImmutableUnionEliminatorRule.Config.of()
            .withDescription("MinusEliminatorRule")
            .withOperandFor(LogicalMinus.class);

    @Override default UnionEliminatorRule toRule() {
      return new UnionEliminatorRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends SetOp> setOpClass) {
      return withOperandSupplier(b -> b.operand(setOpClass).anyInputs())
          .as(Config.class);
    }
  }
}
