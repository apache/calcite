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
package org.apache.calcite.rel.rules.custom;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.BestMatch;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * BestMatchReduceRule is able to reduce two consecutive best-match operators
 * into one. In the current implementation, the outer one will be eliminated.
 */
public class BestMatchReduceRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** Instance of the rule that reduces two best-match operators. */
  public static final BestMatchReduceRule INSTANCE = new BestMatchReduceRule(
      operand(BestMatch.class, operand(BestMatch.class, any())), null);

  //~ Constructors -----------------------------------------------------------

  public BestMatchReduceRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public BestMatchReduceRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    // Gets the inner best-match operator.
    BestMatch innerBestMatch = call.rel(1);

    // Eliminates the outer one.
    RelNode reducedNode = call.builder().push(innerBestMatch).build();
    call.transformTo(reducedNode);
  }
}

// End BestMatchReduceRule.java
