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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Planner rule that slices a
 * {@link org.apache.calcite.rel.core.Project}
 * into sections which contain windowed
 * aggregate functions and sections which do not.
 *
 * <p>The sections which contain windowed agg functions become instances of
 * {@link org.apache.calcite.rel.logical.LogicalWindow}.
 * If the {@link org.apache.calcite.rel.logical.LogicalCalc} does not contain
 * any windowed agg functions, does nothing.
 *
 * <p>There is also a variant that matches
 * {@link org.apache.calcite.rel.core.Calc} rather than {@code Project}.
 */
public abstract class ProjectToWindowRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  private static final Predicate<Calc> PREDICATE =
      new Predicate<Calc>() {
        public boolean apply(Calc calc) {
          return RexOver.containsOver(calc.getProgram());
        }
      };

  private static final Predicate<Project> PREDICATE2 =
      new Predicate<Project>() {
        public boolean apply(Project project) {
          return RexOver.containsOver(project.getProjects(), null);
        }
      };

  /**
   * Instance of the rule that applies to a
   * {@link org.apache.calcite.rel.core.Calc} that contains
   * windowed aggregates and converts it into a mixture of
   * {@link org.apache.calcite.rel.logical.LogicalWindow} and {@code Calc}.
   */
  public static final ProjectToWindowRule INSTANCE =
      new ProjectToWindowRule(
        operand(Calc.class, null, PREDICATE, any()),
        "ProjectToWindowRule") {
        public void onMatch(RelOptRuleCall call) {
          Calc calc = call.rel(0);
          assert RexOver.containsOver(calc.getProgram());
          CalcRelSplitter transform = new WindowedAggRelSplitter(calc);
          RelNode newRel = transform.execute();
          call.transformTo(newRel);
        }
      };

  /**
   * Instance of the rule that can be applied to a
   * {@link org.apache.calcite.rel.core.Project} and that produces, in turn,
   * a mixture of {@code LogicalProject}
   * and {@link org.apache.calcite.rel.logical.LogicalWindow}.
   */
  public static final ProjectToWindowRule PROJECT =
      new ProjectToWindowRule(
        operand(Project.class, null, PREDICATE2, any()),
        "ProjectToWindowRule:project") {
        @Override public void onMatch(RelOptRuleCall call) {
          Project project = call.rel(0);
          assert RexOver.containsOver(project.getProjects(), null);
          final RelNode input = project.getInput();
          final RexProgram program =
              RexProgram.create(
                  input.getRowType(),
                  project.getProjects(),
                  null,
                  project.getRowType(),
                  project.getCluster().getRexBuilder());
          // temporary LogicalCalc, never registered
          final LogicalCalc calc = LogicalCalc.create(input, program);
          CalcRelSplitter transform = new WindowedAggRelSplitter(calc) {
            @Override protected RelNode handle(RelNode rel) {
              if (rel instanceof LogicalCalc) {
                LogicalCalc calc = (LogicalCalc) rel;
                final RexProgram program = calc.getProgram();
                rel = calc.getInput();
                if (program.getCondition() != null) {
                  rel = LogicalFilter.create(rel,
                      program.expandLocalRef(
                          program.getCondition()));
                }
                if (!program.projectsOnlyIdentity()) {
                  rel = RelOptUtil.createProject(
                      rel,
                      Lists.transform(
                          program.getProjectList(),
                          new Function<RexLocalRef, RexNode>() {
                            public RexNode apply(RexLocalRef a0) {
                              return program.expandLocalRef(a0);
                            }
                          }),
                      calc.getRowType().getFieldNames());
                }
              }
              return rel;
            }
          };
          RelNode newRel = transform.execute();
          call.transformTo(newRel);
        }
      };

  //~ Constructors -----------------------------------------------------------

  /** Creates a ProjectToWindowRule. */
  private ProjectToWindowRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Splitter which distinguishes between windowed aggregation expressions
   * (calls to {@link RexOver}) and ordinary expressions.
   */
  static class WindowedAggRelSplitter extends CalcRelSplitter {
    WindowedAggRelSplitter(Calc calc) {
      super(
          calc,
          new RelType[]{
            new CalcRelSplitter.RelType("CalcRelType") {
              protected boolean canImplement(RexFieldAccess field) {
                return true;
              }

              protected boolean canImplement(RexDynamicParam param) {
                return true;
              }

              protected boolean canImplement(RexLiteral literal) {
                return true;
              }

              protected boolean canImplement(RexCall call) {
                return !(call instanceof RexOver);
              }

              protected RelNode makeRel(
                  RelOptCluster cluster,
                  RelTraitSet traits,
                  RelNode child,
                  RexProgram program) {
                assert !program.containsAggs();
                program = RexProgramBuilder.normalize(cluster.getRexBuilder(),
                    program);
                return super.makeRel(
                    cluster,
                    traits,
                    child,
                    program);
              }
            },
            new CalcRelSplitter.RelType("WinAggRelType") {
              protected boolean canImplement(RexFieldAccess field) {
                return false;
              }

              protected boolean canImplement(RexDynamicParam param) {
                return false;
              }

              protected boolean canImplement(RexLiteral literal) {
                return false;
              }

              protected boolean canImplement(RexCall call) {
                return call instanceof RexOver;
              }

              protected boolean supportsCondition() {
                return false;
              }

              protected RelNode makeRel(
                  RelOptCluster cluster,
                  RelTraitSet traits,
                  RelNode child,
                  RexProgram program) {
                Util.permAssert(
                    program.getCondition() == null,
                    "WindowedAggregateRel cannot accept a condition");
                return LogicalWindow.create(
                    cluster, traits, child, program);
              }
            }
          });
    }

    @Override protected List<Set<Integer>> getCohorts() {
      // Here used to be the implementation that treats all the RexOvers
      // as a single Cohort. This is flawed if the RexOvers
      // depend on each other (i.e. the second one uses the result
      // of the first).
      return Collections.emptyList();
    }
  }
}

// End ProjectToWindowRule.java
