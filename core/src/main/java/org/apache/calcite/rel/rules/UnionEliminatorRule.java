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
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * <code>UnionEliminatorRule</code> checks to see if its possible to optimize a
 * Union call by eliminating the Union operator altogether in the case the call
 * consists of only one input.
 *
 * @see CoreRules#UNION_REMOVE
 */
public class UnionEliminatorRule
    extends RelRule<UnionEliminatorRule.Config>
    implements SubstitutionRule {

  /** Creates a UnionEliminatorRule. */
  protected UnionEliminatorRule(Config config) {
    super(config);
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
    Union union = call.rel(0);
    return union.all && union.getInputs().size() == 1;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Union union = call.rel(0);
    call.transformTo(union.getInputs().get(0));
  }

  @Override public boolean autoPruneOld() {
    return true;
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandFor(LogicalUnion.class);

    @Override default UnionEliminatorRule toRule() {
      return new UnionEliminatorRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Union> unionClass) {
      return withOperandSupplier(b -> b.operand(unionClass).anyInputs())
          .as(Config.class);
    }
  }
}
