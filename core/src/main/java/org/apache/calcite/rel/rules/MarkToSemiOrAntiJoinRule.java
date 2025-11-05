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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.plan.RelOptUtil.conjunctions;

/**
 * Rule to simplify a mark join to semi join or anti join. This rule is applies by default after
 * general decorrelation.
 *
 * @see org.apache.calcite.sql2rel.TopDownGeneralDecorrelator
 */
@Value.Enclosing
public class MarkToSemiOrAntiJoinRule
    extends RelRule<MarkToSemiOrAntiJoinRule.Config>
    implements TransformationRule {


  /** Creates a MarkToSemiOrAntiJoinRule. */
  protected MarkToSemiOrAntiJoinRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Filter filter = call.rel(1);
    final Join join = call.rel(2);
    final RelBuilder builder = call.builder();

    int markIndex = join.getRowType().getFieldCount() - 1;
    ImmutableBitSet projectColumns = RelOptUtil.InputFinder.bits(project.getProjects(), null);
    ImmutableBitSet filterColumns = RelOptUtil.InputFinder.bits(filter.getCondition());
    if (projectColumns.get(markIndex) || !filterColumns.get(markIndex)) {
      return;
    }

    // Proj        <- no result of the project depends on marker
    //   Filter    <- condition depends on marker
    //     Join    <- mark join
    // After expressing the filter condition as a conjunction, there are only two cases to simplify:
    // 1. only reference the marker, simplify to semi join
    // 2. NOT(marker), and the join condition will only return TRUE/FALSE
    //    (will not return NULL values), simplify to anti join
    boolean toSemi = false;
    boolean toAnti = false;
    List<RexNode> filterConditions = RelOptUtil.conjunctions(filter.getCondition());
    List<RexNode> newFilterConditions = new ArrayList<>();
    for (RexNode condition : filterConditions) {
      final ImmutableBitSet inputBits = RelOptUtil.InputFinder.bits(condition);
      // marker is not referenced
      if (!inputBits.get(markIndex)) {
        newFilterConditions.add(condition);
        continue;
      }

      // only reference the marker, to semi join
      if (condition instanceof RexInputRef && !toAnti) {
        toSemi = true;
        continue;
      }
      // NOT(marker), and the join condition will only return TRUE/FALSE, to anti join
      if (condition instanceof RexCall
          && condition.isA(SqlKind.NOT)
          && ((RexCall) condition).getOperands().get(0) instanceof RexInputRef
          && isJoinConditionNotStrong(join.getCondition())
          && !toSemi) {
        toAnti = true;
        continue;
      }
      // other forms cannot be simplified, for example, disjunction
      return;
    }
    JoinRelType newJoinType = toSemi ? JoinRelType.SEMI : JoinRelType.ANTI;
    RelNode result
        = builder.push(join.getLeft()).push(join.getRight())
            .join(newJoinType, join.getCondition())
            .filter(newFilterConditions)
            .project(project.getProjects())
            .build();
    call.transformTo(result);
  }

  private static boolean isJoinConditionNotStrong(RexNode condition) {
    List<RexNode> conjunctions = conjunctions(condition);
    for (RexNode expr : conjunctions) {
      if (Strong.isStrong(expr)) {
        return false;
      }
    }
    return true;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableMarkToSemiOrAntiJoinRule.Config.of()
        .withOperandSupplier(b1 ->
            b1.operand(Project.class).oneInput(b2 ->
                b2.operand(Filter.class).oneInput(b3 ->
                    b3.operand(Join.class).predicate(join ->
                            join.getJoinType() == JoinRelType.LEFT_MARK).anyInputs())));

    @Override default MarkToSemiOrAntiJoinRule toRule() {
      return new MarkToSemiOrAntiJoinRule(this);
    }
  }

}
