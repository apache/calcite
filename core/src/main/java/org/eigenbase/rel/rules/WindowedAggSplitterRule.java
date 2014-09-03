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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Rule that slices the {@link CalcRel} into sections which contain windowed
 * agg functions and sections which do not.
 *
 * <p>The sections which contain windowed agg functions become instances of
 * {@link org.eigenbase.rel.WindowRel}. If the {@link CalcRel} does not contain any
 * windowed agg functions, does nothing.
 */
public abstract class WindowedAggSplitterRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Instance of the rule that applies to a {@link CalcRelBase} that contains
   * windowed aggregates and converts it into a mixture of
   * {@link org.eigenbase.rel.WindowRel} and {@code CalcRelBase}.
   */
  public static final WindowedAggSplitterRule INSTANCE =
      new WindowedAggSplitterRule(
        new RelOptRuleOperand(CalcRelBase.class, null, any()) {
          @Override
          public boolean matches(RelNode rel) {
            return super.matches(rel)
                && RexOver.containsOver(((CalcRelBase) rel).getProgram());
          }
        },
        "WindowedAggSplitterRule") {
        public void onMatch(RelOptRuleCall call) {
          CalcRelBase calc = call.rel(0);
          assert RexOver.containsOver(calc.getProgram());
          CalcRelSplitter transform = new WindowedAggRelSplitter(calc);
          RelNode newRel = transform.execute();
          call.transformTo(newRel);
        }
      };

  /**
   * Instance of the rule that can be applied to a
   * {@link org.eigenbase.rel.ProjectRelBase} and that produces, in turn,
   * a mixture of {@code ProjectRel} and {@link org.eigenbase.rel.WindowRel}.
   */
  public static final WindowedAggSplitterRule PROJECT =
      new WindowedAggSplitterRule(
        new RelOptRuleOperand(
            ProjectRelBase.class, null, any()) {
          @Override
          public boolean matches(RelNode rel) {
            return super.matches(rel)
                && RexOver.containsOver(((ProjectRelBase) rel).getProjects(),
                     null);
          }
        },
        "WindowedAggSplitterRule:project") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          ProjectRelBase project = call.rel(0);
          assert RexOver.containsOver(project.getProjects(), null);
          final RelNode child = project.getChild();
          final RelDataType rowType = project.getRowType();
          final RexProgram program =
              RexProgram.create(
                  child.getRowType(),
                  project.getProjects(),
                  null,
                  project.getRowType(),
                  project.getCluster().getRexBuilder());
          // temporary CalcRel, never registered
          final CalcRel calc =
              new CalcRel(
                  project.getCluster(),
                  project.getTraitSet(),
                  child,
                  rowType,
                  program,
                  ImmutableList.<RelCollation>of());
          CalcRelSplitter transform = new WindowedAggRelSplitter(calc) {
            @Override
            protected RelNode handle(RelNode rel) {
              if (rel instanceof CalcRel) {
                CalcRel calc = (CalcRel) rel;
                final RexProgram program = calc.getProgram();
                rel = calc.getChild();
                if (program.getCondition() != null) {
                  rel = new FilterRel(
                      calc.getCluster(),
                      rel,
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

  /**
   * Creates a rule.
   */
  private WindowedAggSplitterRule(
      RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Splitter which distinguishes between windowed aggregation expressions
   * (calls to {@link RexOver}) and ordinary expressions.
   */
  static class WindowedAggRelSplitter extends CalcRelSplitter {
    WindowedAggRelSplitter(CalcRelBase calc) {
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
                  RelDataType rowType,
                  RelNode child,
                  RexProgram program) {
                assert !program.containsAggs();
                program = RexProgramBuilder.normalize(cluster.getRexBuilder(),
                    program);
                return super.makeRel(
                    cluster,
                    traits,
                    rowType,
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
                  RelDataType rowType,
                  RelNode child,
                  RexProgram program) {
                Util.permAssert(
                    program.getCondition() == null,
                    "WindowedAggregateRel cannot accept a condition");
                return WindowRel.create(
                    cluster, traits, child, program, rowType);
              }
            }
          });
    }

    @Override
    protected List<Set<Integer>> getCohorts() {
      // Here used to be the implementation that treats all the RexOvers
      // as a single Cohort. This is flawed if the RexOvers
      // depend on each other (i.e. the second one uses the result
      // of the first).
      return Collections.emptyList();
    }
  }
}

// End WindowedAggSplitterRule.java
