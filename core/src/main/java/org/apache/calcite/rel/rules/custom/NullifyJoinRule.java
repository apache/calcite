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
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * NullifyJoinRule
 */
public class NullifyJoinRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

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
    Join join = call.rel(0);

    // Determines the nullification attribute list based on the join type.
    List<RexNode> nullificationList = new ArrayList<>();
    List<RelDataTypeField> leftFieldList = join.getLeft().getRowType().getFieldList();
    List<RelDataTypeField> rightFieldList = join.getRight().getRowType().getFieldList();
    List<RexNode> leftList = leftFieldList.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());
    List<RexNode> rightList = rightFieldList.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());

    switch (join.getJoinType()) {
    case LEFT:
      nullificationList.addAll(rightList);
      break;
    case RIGHT:
      nullificationList.addAll(leftList);
      break;
    case INNER:
      nullificationList.addAll(leftList);
      nullificationList.addAll(rightList);
      break;
    default:
      throw new AssertionError(join.getJoinType());
    }

    // Determines the nullification condition.
    RexNode nullificationCondition = join.getCondition();

    // Builds the transformed relational tree.
    final RelNode cartesianJoin =
        join.copy(
            join.getTraitSet(),
            builder.literal(true),  // Uses a literal condition which is always true.
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());
    builder.push(cartesianJoin).nullify(nullificationCondition, nullificationList).bestMatch();
    call.transformTo(builder.build());
  }
}

// End NullifyJoinRule.java
