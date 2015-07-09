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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;

/**
 * Planner rule that removes
 * a {@link org.apache.calcite.rel.logical.LogicalAggregate}
 * if it computes no aggregate functions
 * (that is, it is implementing {@code SELECT DISTINCT})
 * and the underlying relational expression is already distinct.
 */
public class AggregateRemoveRule extends RelOptRule {
  public static final AggregateRemoveRule INSTANCE =
      new AggregateRemoveRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a AggregateRemoveRule.
   */
  private AggregateRemoveRule() {
    // REVIEW jvs 14-Mar-2006: We have to explicitly mention the child here
    // to make sure the rule re-fires after the child changes (e.g. via
    // ProjectRemoveRule), since that may change our information
    // about whether the child is distinct.  If we clean up the inference of
    // distinct to make it correct up-front, we can get rid of the reference
    // to the child here.
    super(
        operand(LogicalAggregate.class,
            operand(RelNode.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final LogicalAggregate aggregate = call.rel(0);
    final RelNode input = call.rel(1);
    if (!aggregate.getAggCallList().isEmpty()
        || aggregate.indicator
        || !input.isKey(aggregate.getGroupSet())) {
      return;
    }
    // Distinct is "GROUP BY c1, c2" (where c1, c2 are a set of columns on
    // which the input is unique, i.e. contain a key) and has no aggregate
    // functions. It can be removed.
    final RelNode newInput = convert(input, aggregate.getTraitSet().simplify());

    // If aggregate was projecting a subset of columns, add a project for the
    // same effect.
    RelNode rel;
    if (newInput.getRowType().getFieldCount()
        > aggregate.getRowType().getFieldCount()) {
      rel = RelOptUtil.createProject(newInput,
          aggregate.getGroupSet().toList());
    } else {
      rel = newInput;
    }
    call.transformTo(rel);
  }
}

// End AggregateRemoveRule.java
