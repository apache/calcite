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

import java.util.List;

import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLocalRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexProgram;
import org.eigenbase.rex.RexShuttle;

import net.hydromatic.optiq.ProjectableFilterableTable;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.JavaRules;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

/**
 * Planner rule that pushes a project into a scan of a
 * {@link net.hydromatic.optiq.ProjectableFilterableTable}.
 *
 * @see org.eigenbase.rel.rules.FilterTableRule
 */
public abstract class ProjectTableRule extends RelOptRule {
  private static final Predicate<TableAccessRelBase> PREDICATE =
      new Predicate<TableAccessRelBase>() {
        public boolean apply(TableAccessRelBase scan) {
          // We can only push projects into a ProjectableFilterableTable.
          final RelOptTable table = scan.getTable();
          return table.unwrap(ProjectableFilterableTable.class) != null;
        }
      };

  public static final ProjectTableRule INSTANCE =
      new ProjectTableRule(
          operand(ProjectRelBase.class,
              operand(JavaRules.EnumerableInterpreterRel.class,
                  operand(TableAccessRelBase.class, null, PREDICATE, none()))),
          "ProjectTableRule:basic") {
        @Override public void onMatch(RelOptRuleCall call) {
          final ProjectRelBase project = call.rel(0);
          final JavaRules.EnumerableInterpreterRel interpreter = call.rel(1);
          final TableAccessRelBase scan = call.rel(2);
          final RelOptTable table = scan.getTable();
          assert table.unwrap(ProjectableFilterableTable.class) != null;
          apply(call, project, null, interpreter);
        }
      };

  public static final ProjectTableRule INSTANCE2 =
      new ProjectTableRule(
          operand(ProjectRelBase.class,
              operand(FilterRelBase.class,
                  operand(JavaRules.EnumerableInterpreterRel.class,
                      operand(TableAccessRelBase.class, null, PREDICATE,
                          none())))),
          "ProjectTableRule:filter") {
        @Override public void onMatch(RelOptRuleCall call) {
          final ProjectRelBase project = call.rel(0);
          final FilterRelBase filter = call.rel(1);
          final JavaRules.EnumerableInterpreterRel interpreter = call.rel(2);
          final TableAccessRelBase scan = call.rel(3);
          final RelOptTable table = scan.getTable();
          assert table.unwrap(ProjectableFilterableTable.class) != null;
          apply(call, project, filter, interpreter);
        }
      };

  //~ Constructors -----------------------------------------------------------

  /** Creates a FilterTableRule. */
  private ProjectTableRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  //~ Methods ----------------------------------------------------------------

  protected void apply(RelOptRuleCall call, ProjectRelBase project,
      FilterRelBase filter, JavaRules.EnumerableInterpreterRel interpreter) {
    // Split the projects into column references and expressions on top of them.
    // Creating a RexProgram is a convenient way to do this.
    final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    final RexProgram program = RexProgram.create(interpreter.getRowType(),
        project.getProjects(), null, project.getRowType(), rexBuilder);
    final List<Integer> projectOrdinals = Lists.newArrayList();
    final List<RexNode> extraProjects;
    if (program.getExprList().size()
        == program.getInputRowType().getFieldCount()) {
      // There are only field references, no non-trivial expressions.
      for (RexLocalRef ref : program.getProjectList()) {
        projectOrdinals.add(ref.getIndex());
      }
      extraProjects = null;
    } else {
      extraProjects = Lists.newArrayList();
      RexShuttle shuttle = new RexShuttle() {
        final List<RexInputRef> inputRefs = Lists.newArrayList();

        @Override public RexNode visitInputRef(RexInputRef inputRef) {
          final int source = inputRef.getIndex();
          int target = projectOrdinals.indexOf(source);
          final RexInputRef ref;
          if (target < 0) {
            target = projectOrdinals.size();
            projectOrdinals.add(source);
            ref = rexBuilder.makeInputRef(inputRef.getType(), target);
            inputRefs.add(ref);
          } else {
            ref = inputRefs.get(target);
          }
          return ref;
        }
      };
      for (RexNode node : project.getProjects()) {
        extraProjects.add(node.accept(shuttle));
      }
    }

    RelNode input = interpreter.getChild();
    if (filter != null) {
      input = RelOptUtil.createFilter(input, filter.getCondition(),
          EnumerableRel.FILTER_FACTORY);
    }
    final RelNode newProject =
        RelOptUtil.createProject(EnumerableRel.PROJECT_FACTORY, input,
            projectOrdinals);
    final RelNode newInterpreter =
        new JavaRules.EnumerableInterpreterRel(interpreter.getCluster(),
            interpreter.getTraitSet(), newProject, 0.15d);
    final RelNode residue;
    if (extraProjects != null) {
      residue = RelOptUtil.createProject(newInterpreter, extraProjects,
          project.getRowType().getFieldNames());
    } else {
      residue = newInterpreter;
    }
    call.transformTo(residue);
  }
}

// End ProjectTableRule.java
