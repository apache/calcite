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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * NullifyJoinRule nullifies a 1-sided outer join or inner join operator in the
 * following way:
 * 1) 1-sided outer join: nullify the null-producing side;
 * 2) inner join: nullify both sides;
 */
public class NullifyJoinRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the rule that nullifies inner, left outer or right outer join. */
  public static final NullifyJoinRule INSTANCE =
      new NullifyJoinRule(operand(Join.class, any()), null);

  //~ Constructors -----------------------------------------------------------

  public NullifyJoinRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public NullifyJoinRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    RelBuilder builder = call.builder();

    // The join operator at the current node.
    final Join join = call.rel(0);
    final JoinRelType joinType = join.getJoinType();
    if (join.getCondition().equals(builder.literal(true))) {
      LOGGER.debug("No need to nullify cartesian product");
      return;
    } else if (!joinType.canApplyNullify()) {
      LOGGER.debug("Invalid join relation type");
      return;
    }

    // The new join operator (as outer-cartesian product).
    final RelNode outerCartesianJoin = join.copy(
        join.getTraitSet(),
        builder.literal(true),  // Uses a literal condition which is always true.
        join.getLeft(),
        join.getRight(),
        JoinRelType.OUTER_CARTESIAN,
        join.isSemiJoinDone());

    // Determines the nullification attribute list based on the join type.
    List<RelDataTypeField> joinFieldList = outerCartesianJoin.getRowType().getFieldList();
    List<RelDataTypeField> nullifyFieldList;
    int leftFieldCount = join.getLeft().getRowType().getFieldCount();
    switch (joinType) {
    case LEFT:
      nullifyFieldList = joinFieldList.subList(leftFieldCount, joinFieldList.size());
      break;
    case RIGHT:
      nullifyFieldList = joinFieldList.subList(0, leftFieldCount);
      break;
    case INNER:
      nullifyFieldList = joinFieldList;
      break;
    default:
      throw new AssertionError(joinType);
    }
    List<RexNode> nullificationList = nullifyFieldList.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());

    // Determines the nullification condition.
    RexNode nullificationCondition = join.getCondition();

    // Builds the transformed relational tree.
    final RelNode transformedNode = builder.push(outerCartesianJoin)
        .nullify(nullificationCondition, nullificationList)
        .bestMatch()
        .build();
    call.transformTo(transformedNode);
  }
}

// End NullifyJoinRule.java
