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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;

import java.util.List;

/**
 * AsscomOuterInnerRule applies limited r-asscom property on outer join and inner join.
 *
 * Rule 23.
 */
public class AsscomOuterInnerRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final AsscomOuterInnerRule INSTANCE = new AsscomOuterInnerRule(
      operand(Join.class,
          operand(RelSubset.class, any()),
          operand(Join.class, any())), null);

  //~ Constructors -----------------------------------------------------------

  public AsscomOuterInnerRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public AsscomOuterInnerRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    // Gets the two original join operators.
    Join topInnerJoin = call.rel(0);
    Join bottomLeftJoin = call.rel(2);

    // Makes sure the join types match the rule.
    if (topInnerJoin.getJoinType() != JoinRelType.INNER) {
      LOGGER.debug("The top join is not an inner join.");
      return;
    } else if (bottomLeftJoin.getJoinType() != JoinRelType.LEFT) {
      LOGGER.debug("The bottom join is not a left outer join.");
      return;
    }

    // Makes sure the join condition is referring to the correct set of fields.
    int topInnerJoinLeft = topInnerJoin.getLeft().getRowType().getFieldCount();
    int bottomLeftJoinRight = bottomLeftJoin.getRight().getRowType().getFieldCount();
    List<RelDataTypeField> fields = topInnerJoin.getRowType().getFieldList();
    if (!RelOptUtil.isNotReferringTo(topInnerJoin.getCondition(),
        fields.subList(topInnerJoinLeft, fields.size() - bottomLeftJoinRight))) {
      LOGGER.debug("Not a subset of attributes.");
      return;
    }

    // Replaces the variables in the predicates later.
    final RexBuilder rexBuilder = topInnerJoin.getCluster().getRexBuilder();
    int bottomLeftJoinLeft = bottomLeftJoin.getLeft().getRowType().getFieldCount();
    final VariableReplacer replacer = new VariableReplacer(
        rexBuilder, topInnerJoinLeft, bottomLeftJoinLeft, bottomLeftJoinRight);

    // The new operators.
    final Join newBottomInnerJoin = topInnerJoin.copy(
        topInnerJoin.getTraitSet(),
        replacer.replace(topInnerJoin.getCondition(), bottomLeftJoinLeft),
        topInnerJoin.getLeft(),
        bottomLeftJoin.getRight(),
        JoinRelType.INNER,
        topInnerJoin.isSemiJoinDone());
    final Join newTopInnerJoin = bottomLeftJoin.copy(
        bottomLeftJoin.getTraitSet(),
        replacer.replace(bottomLeftJoin.getCondition(), 0),
        bottomLeftJoin.getLeft(),
        newBottomInnerJoin,
        JoinRelType.INNER,
        bottomLeftJoin.isSemiJoinDone());

    // Builds the transformed relational tree.
    final RelNode transformedNode = call.builder().push(newTopInnerJoin).build();
    call.transformTo(transformedNode);
  }

  /**
   * A utility inner class to replace the index of attributes.
   */
  private static class VariableReplacer {
    private final RexBuilder rexBuilder;
    private final int topLeft;
    private final int bottomLeft;
    private final int bottomRight;

    VariableReplacer(RexBuilder rexBuilder, int topLeft, int bottomLeft, int bottomRight) {
      this.rexBuilder = rexBuilder;
      this.topLeft = topLeft;
      this.bottomLeft = bottomLeft;
      this.bottomRight = bottomRight;
    }

    RexNode replace(RexNode rex, int offset) {
      if (rex instanceof RexCall) {
        final RexCall call = (RexCall) rex;

        // Converts each operand in the predicate.
        ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        call.operands.forEach(operand -> builder.add(replace(operand, offset)));

        // Re-builds the predicate.
        return call.clone(call.getType(), builder.build());
      } else if (rex instanceof RexInputRef) {
        final RexInputRef var = (RexInputRef) rex;

        // Computes its index after transformation.
        int newIndex;
        if (var.getIndex() < topLeft) {
          newIndex = var.getIndex() + bottomLeft;
        } else if (var.getIndex() < topLeft + bottomLeft) {
          newIndex = var.getIndex() - topLeft;
        } else {
          newIndex = var.getIndex();
        }

        // Re-builds the attribute.
        return rexBuilder.makeInputRef(var.getType(), newIndex - offset);
      } else {
        return rex;
      }
    }
  }
}

// End AsscomOuterInnerRule.java
