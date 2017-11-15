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
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * <code>UnionEliminatorRule</code> checks to see if its possible to optimize a
 * Union call by eliminating the Union operator altogether in the case the call
 * consists of only one input.
 */
public class UnionEliminatorRule extends RelOptRule {
  public static final UnionEliminatorRule INSTANCE =
      new UnionEliminatorRule(LogicalUnion.class, RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a UnionEliminatorRule.
   */
  public UnionEliminatorRule(Class<? extends Union> clazz,
      RelBuilderFactory relBuilderFactory) {
    super(operand(clazz, any()), relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    Union union = call.rel(0);
    if (union.getInputs().size() != 1) {
      return;
    }
    if (!union.all) {
      return;
    }

    // REVIEW jvs 14-Mar-2006:  why don't we need to register
    // the equivalence here like we do in AggregateRemoveRule?

    call.transformTo(union.getInputs().get(0));
  }
}

// End UnionEliminatorRule.java
