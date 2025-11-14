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
 * Rule to simplify a mark join to semi join or anti join.
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
    // Proj       <- does not project marker
    //  Filter    <- use marker in condition
    //    Join    <- mark join
    if (projectColumns.get(markIndex) || !filterColumns.get(markIndex)) {
      return;
    }

    // After decompose the filter condition by AND, there are only two cases to simplify:
    // 1. only reference the marker, simplify to semi join
    // 2. NOT(marker), and the join condition only contains IS [NOT] DISTINCT FROM,
    //    simplify to anti join
    boolean toSemi = false;
    boolean toAnti = false;
    List<RexNode> filterConditions = RelOptUtil.conjunctions(filter.getCondition());
    List<RexNode> newFilterConditions = new ArrayList<>();
    for (RexNode condition : filterConditions) {
      final ImmutableBitSet inputBits = RelOptUtil.InputFinder.bits(condition);
      // marker not referenced
      if (!inputBits.get(markIndex)) {
        newFilterConditions.add(condition);
        continue;
      }

      // only marker referenced, to semi join
      if (condition instanceof RexInputRef && !toAnti) {
        toSemi = true;
        continue;
      }
      // NOT(marker), and the join condition only contains IS [NOT] DISTINCT FROM, to anti join
      if (condition instanceof RexCall
          && condition.isA(SqlKind.NOT)
          && ((RexCall) condition).getOperands().get(0) instanceof RexInputRef
          && onlyContainsDistinctFrom(join.getCondition())
          && !toSemi) {
        toAnti = true;
        continue;
      }
      // other forms cannot be eliminated
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

  private static boolean onlyContainsDistinctFrom(RexNode condition) {
    List<RexNode> conjunctions = conjunctions(condition);
    for (RexNode expr : conjunctions) {
      if (!expr.isA(SqlKind.IS_DISTINCT_FROM) && !expr.isA(SqlKind.IS_NOT_DISTINCT_FROM)) {
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
                    b3.operand(Join.class).predicate(join -> join.getJoinType() == JoinRelType.MARK)
                        .anyInputs())));

    @Override default MarkToSemiOrAntiJoinRule toRule() {
      return new MarkToSemiOrAntiJoinRule(this);
    }
  }

}
