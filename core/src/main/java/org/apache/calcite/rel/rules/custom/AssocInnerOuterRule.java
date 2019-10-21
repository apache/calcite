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
import java.util.stream.Collectors;

/**
 * AssocInnerOuterRule applies limited associativity on inner join and outer join.
 *
 * Rule 22.
 */
public class AssocInnerOuterRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final AssocInnerOuterRule INSTANCE = new AssocInnerOuterRule(
      operand(Join.class,
          operand(RelSubset.class, any()),
          operand(Join.class, any())), null);

  //~ Constructors -----------------------------------------------------------

  public AssocInnerOuterRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public AssocInnerOuterRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    // Gets the two original join operators.
    Join topLeftJoin = call.rel(0);
    Join bottomInnerJoin = call.rel(2);

    // Makes sure the join types match the rule.
    if (topLeftJoin.getJoinType() != JoinRelType.LEFT) {
      LOGGER.debug("The top join is not an left outer join.");
      return;
    } else if (bottomInnerJoin.getJoinType() != JoinRelType.INNER) {
      LOGGER.debug("The bottom join is not a inner join.");
      return;
    }

    // Makes sure the join condition is referring to the correct set of fields.
    int bottomInnerJoinRight = bottomInnerJoin.getRight().getRowType().getFieldCount();
    int total = topLeftJoin.getRowType().getFieldCount();
    if (!RelOptUtil.isNotReferringTo(topLeftJoin.getCondition(),
        topLeftJoin.getRowType().getFieldList().subList(total - bottomInnerJoinRight, total))) {
      LOGGER.debug("Not a subset of attributes.");
      return;
    }

    // Replaces the variables in the predicates later.
    final RexBuilder rexBuilder = topLeftJoin.getCluster().getRexBuilder();
    final VariableReplacer replacer = new VariableReplacer(rexBuilder);

    // The new operators.
    final Join newBottomLeftJoin = topLeftJoin.copy(
        topLeftJoin.getTraitSet(),
        replacer.replace(topLeftJoin.getCondition(), 0),
        topLeftJoin.getLeft(),
        bottomInnerJoin.getLeft(),
        JoinRelType.LEFT,
        topLeftJoin.isSemiJoinDone());
    final Join newTopLeftJoin = bottomInnerJoin.copy(
        bottomInnerJoin.getTraitSet(),
        replacer.replace(bottomInnerJoin.getCondition(), 0),
        newBottomLeftJoin,
        bottomInnerJoin.getRight(),
        JoinRelType.LEFT,
        bottomInnerJoin.isSemiJoinDone());

    // Determines the nullification attribute.
    List<RelDataTypeField> nullifyFieldList =
        bottomInnerJoin.getLeft().getRowType().getFieldList();
    List<RexNode> nullificationList = nullifyFieldList.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());

    // Builds the transformed relational tree.
    final RelNode transformedNode = call.builder().push(newTopLeftJoin)
        .nullify(bottomInnerJoin.getCondition(), nullificationList).bestMatch().build();
    call.transformTo(transformedNode);
  }

  /**
   * A utility inner class to replace the index of attributes.
   */
  private static class VariableReplacer {
    private final RexBuilder rexBuilder;

    VariableReplacer(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
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

        // Re-builds the attribute.
        int newIndex = var.getIndex() - offset;
        return rexBuilder.makeInputRef(var.getType(), newIndex);
      } else {
        return rex;
      }
    }
  }
}

// End AssocInnerOuterRule.java
