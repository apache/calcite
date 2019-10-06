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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * AsscomOuterOuterRule applies limited r-asscom property on outer join and outer join.
 *
 * Rule 25.
 */
public class AsscomOuterOuterRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final AsscomOuterOuterRule INSTANCE = new AsscomOuterOuterRule(
      operand(Join.class,
          operand(RelSubset.class, any()),
          operand(Join.class, any())), null);

  //~ Constructors -----------------------------------------------------------

  public AsscomOuterOuterRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public AsscomOuterOuterRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    // Gets the two original join operators.
    Join topLeftJoin = call.rel(0);
    Join bottomLeftJoin = call.rel(1);

    // Makes sure the join types match the rule.
    if (topLeftJoin.getJoinType() != JoinRelType.LEFT) {
      LOGGER.debug("The top join is not an left outer join.");
      return;
    } else if (bottomLeftJoin.getJoinType() != JoinRelType.LEFT) {
      LOGGER.debug("The bottom join is not a left outer join.");
      return;
    }

    // Makes sure the join condition is referring to the correct set of fields.
    int topLeftJoinLeft = topLeftJoin.getLeft().getRowType().getFieldCount();
    int bottomLeftJoinRight = bottomLeftJoin.getRight().getRowType().getFieldCount();
    List<RelDataTypeField> fields = topLeftJoin.getRowType().getFieldList();
    if (!RelOptUtil.isNotReferringTo(topLeftJoin.getCondition(),
        fields.subList(topLeftJoinLeft, fields.size() - bottomLeftJoinRight))) {
      LOGGER.debug("Not a subset of attributes.");
      return;
    }

    // The new operators.
    final Join newBottomLeftJoin = topLeftJoin.copy(
        topLeftJoin.getTraitSet(),
        topLeftJoin.getCondition(),
        topLeftJoin.getLeft(),
        bottomLeftJoin.getRight(),
        JoinRelType.LEFT,
        topLeftJoin.isSemiJoinDone());
    final Join newTopLeftJoin = bottomLeftJoin.copy(
        bottomLeftJoin.getTraitSet(),
        bottomLeftJoin.getCondition(),
        newBottomLeftJoin,
        bottomLeftJoin.getLeft(),
        JoinRelType.LEFT,
        bottomLeftJoin.isSemiJoinDone());

    // Determines the nullification attribute.
    List<RelDataTypeField> nullifyFieldList =
        bottomLeftJoin.getRight().getRowType().getFieldList();
    List<RexNode> nullificationList = nullifyFieldList.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());

    // Builds the transformed relational tree.
    final RelNode transformedNode = call.builder().push(newTopLeftJoin)
        .nullify(bottomLeftJoin.getCondition(), nullificationList).bestMatch().build();
    call.transformTo(transformedNode);
  }
}

// End AsscomOuterOuterRule.java
