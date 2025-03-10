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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that translates a {@link org.apache.calcite.rel.core.Intersect}
 * to a series of {@link org.apache.calcite.rel.core.Join} that type is
 * {@link org.apache.calcite.rel.core.JoinRelType#SEMI}.
 */
@Value.Enclosing
public class IntersectToSemiJoinRule
    extends RelRule<IntersectToSemiJoinRule.Config>
    implements TransformationRule {

  /** Creates an IntersectToSemiJoinRule. */
  protected IntersectToSemiJoinRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Intersect intersect = call.rel(0);
    final RelBuilder builder = call.builder();
    final RexBuilder rexBuilder = builder.getRexBuilder();

    List<RelNode> inputs = intersect.getInputs();
    RelNode root = inputs.get(0);
    for (int i = 1; i < inputs.size(); i++) {
      RelNode right = inputs.get(i);
      List<RexNode> conditions = new ArrayList<>();
      int fieldCount = root.getRowType().getFieldCount();
      for (int j = 0; j < fieldCount; j++) {
        conditions.add(
            rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(
                root.getRowType().getFieldList().get(j).getType(), j),
            rexBuilder.makeInputRef(
                right.getRowType().getFieldList().get(j).getType(), j + fieldCount)));
      }
      RexNode condition = RexUtil.composeConjunction(rexBuilder, conditions);

      root = builder.push(root)
          .push(right)
          .join(JoinRelType.SEMI, condition)
          .build();
    }

    if (!intersect.all) {
      root = builder.push(root).distinct().build();
    }
    call.transformTo(root);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableIntersectToSemiJoinRule.Config.of()
        .withOperandFor(LogicalIntersect.class);

    @Override default IntersectToSemiJoinRule toRule() {
      return new IntersectToSemiJoinRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Intersect> intersectClass) {
      return withOperandSupplier(b -> b.operand(intersectClass).anyInputs())
          .as(Config.class);
    }
  }
}
