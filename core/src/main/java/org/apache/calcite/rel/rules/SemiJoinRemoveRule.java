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
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that removes a {@link org.apache.calcite.rel.core.SemiJoin}s
 * from a join tree.
 *
 * <p>It is invoked after attempts have been made to convert a SemiJoin to an
 * indexed scan on a join factor have failed. Namely, if the join factor does
 * not reduce to a single table that can be scanned using an index.
 *
 * <p>It should only be enabled if all SemiJoins in the plan are advisory; that
 * is, they can be safely dropped without affecting the semantics of the query.
 */
public class SemiJoinRemoveRule extends RelOptRule {
  public static final SemiJoinRemoveRule INSTANCE =
      new SemiJoinRemoveRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /** Creates a SemiJoinRemoveRule. */
  public SemiJoinRemoveRule(RelBuilderFactory relBuilderFactory) {
    super(operand(SemiJoin.class, any()), relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    call.transformTo(call.rel(0).getInput(0));
  }
}

// End SemiJoinRemoveRule.java
