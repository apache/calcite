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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that converts true filtered aggregates into CASE-style filtered aggregates.
 *
 * <p>For example,
 *
 * <blockquote>
 *   <code>SELECT SUM(salary) FILTER (WHERE gender = 'F')<br>
 *   FROM Emp</code>
 * </blockquote>
 *
 * <p>becomes
 *
 * <blockquote>
 *   <code>SELECT SUM(CASE WHEN gender = 'F' THEN salary END)<br>
 *   FROM Emp</code>
 * </blockquote>
 *
 * @see CoreRules#AGGREGATE_FILTER_TO_CASE
 */
@Value.Enclosing
public class AggregateFilterToCaseRule
    extends RelRule<AggregateFilterToCaseRule.Config>
    implements TransformationRule {

  /** Creates an AggregateFilterToCaseRule. */
  protected AggregateFilterToCaseRule(Config config) {
    super(config);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      if (aggregateCall.hasFilter()) {
        return true;
      }
    }
    return false;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final RelBuilder relBuilder = call.builder();
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    final List<AggregateCall> newCalls =
        new ArrayList<>(aggregate.getAggCallList().size());
    final List<RexNode> newProjects = new ArrayList<>(project.getProjects());

    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      AggregateCall newCall =
          transform(
              aggregateCall,
              relBuilder.getRexBuilder(),
              relBuilder.getTypeFactory(),
              newProjects);
      newCalls.add(newCall);
    }

    if (newCalls.equals(aggregate.getAggCallList())) {
      return;
    }

    relBuilder
        .push(project.getInput())
        .project(newProjects);
    final RelBuilder.GroupKey groupKey =
        relBuilder.groupKey(aggregate.getGroupSet(), aggregate.getGroupSets());
    relBuilder.aggregate(groupKey, newCalls);
    call.transformTo(relBuilder.build());
  }

  private static AggregateCall transform(AggregateCall call,
      RexBuilder rexBuilder, RelDataTypeFactory typeFactory, List<RexNode> newProjects) {
    if (!call.hasFilter()) {
      return call;
    }
    final SqlKind kind = call.getAggregation().getKind();
    final RexNode condition = newProjects.get(call.filterArg);
    final RexNode arg1;
    final RexNode arg2;

    if (kind == SqlKind.COUNT) {
      // COUNT function may have no argument. When building the CASE expression,
      // fill arg1 with "dummy":
      // COUNT() FILTER (x = 'foo') => COUNT(CASE WHEN x = 'foo' THEN 0 END)
      arg1 = call.getArgList().size() == 0
          ? rexBuilder.makeZeroLiteral(typeFactory.createSqlType(SqlTypeName.INTEGER))
          : newProjects.get(call.getArgList().get(0));
    } else if (call.isDistinct() || call.getArgList().size() != 1) {
      // ensure the reversibility of transformation, refer to AggregateCaseToFilterRule,
      // when the aggregate function is with distinct, only the COUNT can be converted.
      return call;
    } else {
      arg1 = newProjects.get(call.getArgList().get(0));
    }

    arg2 = rexBuilder.makeNullLiteral(arg1.getType());
    final RexNode caseWhen = rexBuilder.makeCall(SqlStdOperatorTable.CASE, condition, arg1, arg2);
    newProjects.add(caseWhen);
    return AggregateCall.create(call.getParserPosition(), call.getAggregation(), call.isDistinct(),
        call.isApproximate(), call.ignoreNulls(), call.rexList,
        ImmutableList.of(newProjects.size() - 1), -1, call.distinctKeys, RelCollations.EMPTY,
        call.getType(), call.getName());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateFilterToCaseRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Aggregate.class).oneInput(b1 ->
                b1.operand(Project.class).anyInputs()));

    @Override default AggregateFilterToCaseRule toRule() {
      return new AggregateFilterToCaseRule(this);
    }
  }
}
