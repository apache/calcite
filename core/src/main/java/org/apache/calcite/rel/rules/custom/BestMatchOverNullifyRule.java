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
import org.apache.calcite.rel.core.Nullify;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

/**
 * BestMatchOverNullifyRule eliminates all inner best-match operators (sandwiched
 * by a nullification operator) as long as there is a best-match operator at the
 * very end. It will check whether the nullification predicate is null-intolerant.
 * The conversion is from `B(Nullify(B(R)))` to `B(Nullify(R))`.
 */
public class BestMatchOverNullifyRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** Instance of the current rule. */
  public static final BestMatchOverNullifyRule INSTANCE = new BestMatchOverNullifyRule(
      operand(BestMatch.class,
          operand(Nullify.class,
              operand(BestMatch.class, any()))), null);

  //~ Constructors -----------------------------------------------------------

  public BestMatchOverNullifyRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public BestMatchOverNullifyRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    RelBuilder builder = call.builder();

    // Gets the old nullification operator.
    Nullify nullify = call.rel(1);
    RexNode oldPredicate = nullify.getPredicate();
    List<RexNode> oldAttributes = nullify.getAttributes();

    // Makes sure the nullification predicate is null-intolerant.
    if (isNullTolerant(oldPredicate)) {
      throw new AssertionError("The nullification predicate is not null-intolerant.");
    }

    // Gets the base relation.
    BestMatch innerBestMatch = call.rel(2);
    RelNode base = innerBestMatch.getInput();

    // Constructs the new expression.
    RelNode newNode = builder.push(base).nullify(oldPredicate, oldAttributes).bestMatch().build();
    call.transformTo(newNode);
  }

  /**
   * Checks whether a given predicate tolerates NULL values. A predicate is
   * null-intolerant if it cannot evaluate to TRUE when referring a NULL
   * value.
   *
   * @param predicate is the predicate to be tested.
   * @return true if null tolerant; false otherwise.
   */
  private boolean isNullTolerant(RexNode predicate) {
    // An OR connective is null-tolerant if any of its child expressions is null-tolerant.
    if (predicate.isA(SqlKind.OR)) {
      RexCall call = (RexCall) predicate;
      for (RexNode operand: call.getOperands()) {
        if (isNullTolerant(operand)) {
          return true;
        }
      }
      return false;
    }

    // IS NULL and TRUE are both null-tolerant.
    return predicate.isA(SqlKind.IS_NULL) || predicate.isA(SqlKind.IS_TRUE);
  }
}

// End BestMatchOverNullifyRule.java
