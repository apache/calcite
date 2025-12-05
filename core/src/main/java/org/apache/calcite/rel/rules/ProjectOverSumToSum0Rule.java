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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.List;

/**
 * Planner rule that converts SUM to SUM0 when it is the aggregate for an OVER clause inside
 * the project list. Need to ensure that the result of the aggregate function for an OVER
 * clause is not NULL.
 */
public class ProjectOverSumToSum0Rule
    extends RelRule<ProjectOverSumToSum0Rule.Config>
    implements TransformationRule {

  /** Creates a ProjectOverSumToSum0Rule. */
  protected ProjectOverSumToSum0Rule(ProjectOverSumToSum0Rule.Config config) {
    super(config);
  }

  @Override public void onMatch(final RelOptRuleCall call) {
    Project project = call.rel(0);
    assert project.containsOver();

    // For OVER clauses, convert SUM to SUM0
    final RexShuttle sumToSum0 = new RexShuttle() {
      @Override public SqlAggFunction visitOverAggFunction(SqlAggFunction op) {
        if (op == SqlStdOperatorTable.SUM) {
          return SqlStdOperatorTable.SUM0;
        } else {
          return op;
        }
      }
    };

    // Modify the top LogicalProject
    final List<RexNode> topProjExps =
        sumToSum0.visitList(project.getProjects());

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(project.getInput());
    relBuilder.project(topProjExps, project.getRowType().getFieldNames());
    call.transformTo(relBuilder.build());
  }

  private static boolean projectContainsOverWithSum(Project project) {
    if (project.containsOver()) {
      final HaveOverWithSumRexShuttle rexShuttle = new HaveOverWithSumRexShuttle();
      rexShuttle.visitList(project.getProjects());
      return rexShuttle.haveOverWithSum;
    }

    return false;
  }

  /** A RexShuttle that looks for a SUM aggregate in an OVER clause.
   */
  private static class HaveOverWithSumRexShuttle extends RexShuttle {
    private boolean haveOverWithSum;

    @Override public SqlAggFunction visitOverAggFunction(SqlAggFunction op) {
      if (op == SqlStdOperatorTable.SUM) {
        haveOverWithSum = true;
      }

      return op;
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    ProjectOverSumToSum0Rule.Config DEFAULT =
        ImmutableConfig.of()
            .withOperandSupplier(b ->
                b.operand(Project.class)
                    .predicate(ProjectOverSumToSum0Rule::projectContainsOverWithSum)
                    .anyInputs())
            .withDescription("ProjectOverSumToSum0Rule");

    @Override default ProjectOverSumToSum0Rule toRule() {
      return new ProjectOverSumToSum0Rule(this);
    }
  }
}
