/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.rel.rules;

import org.apache.optiq.rel.*;
import org.apache.optiq.relopt.*;
import org.apache.optiq.reltype.*;
import org.apache.optiq.rex.*;

import com.google.common.collect.ImmutableList;

/**
 * Rule to convert a {@link ProjectRel} to a {@link CalcRel}
 *
 * <p>The rule does not fire if the child is a {@link ProjectRel}, {@link
 * FilterRel} or {@link CalcRel}. If it did, then the same {@link CalcRel} would
 * be formed via several transformation paths, which is a waste of effort.</p>
 *
 * @see FilterToCalcRule
 */
public class ProjectToCalcRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final ProjectToCalcRule INSTANCE = new ProjectToCalcRule();

  //~ Constructors -----------------------------------------------------------

  private ProjectToCalcRule() {
    super(operand(ProjectRel.class, any()));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final ProjectRel project = call.rel(0);
    final RelNode child = project.getChild();
    final RelDataType rowType = project.getRowType();
    final RexProgram program =
        RexProgram.create(
            child.getRowType(),
            project.getProjects(),
            null,
            project.getRowType(),
            project.getCluster().getRexBuilder());
    final CalcRel calc =
        new CalcRel(
            project.getCluster(),
            project.getTraitSet(),
            child,
            rowType,
            program,
            ImmutableList.<RelCollation>of());
    call.transformTo(calc);
  }
}

// End ProjectToCalcRule.java
