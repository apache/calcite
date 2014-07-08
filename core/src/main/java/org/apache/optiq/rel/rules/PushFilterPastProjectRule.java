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
import org.apache.optiq.rex.*;

/**
 * PushFilterPastProjectRule implements the rule for pushing a {@link FilterRel}
 * past a {@link ProjectRel}.
 */
public class PushFilterPastProjectRule extends RelOptRule {
  public static final PushFilterPastProjectRule INSTANCE =
      new PushFilterPastProjectRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a PushFilterPastProjectRule.
   */
  private PushFilterPastProjectRule() {
    super(
        operand(
            FilterRel.class,
            operand(ProjectRel.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    FilterRel filterRel = call.rel(0);
    ProjectRel projRel = call.rel(1);

    // convert the filter to one that references the child of the project
    RexNode newCondition =
        RelOptUtil.pushFilterPastProject(filterRel.getCondition(), projRel);

    FilterRel newFilterRel =
        new FilterRel(
            filterRel.getCluster(),
            projRel.getChild(),
            newCondition);

    ProjectRel newProjRel =
        (ProjectRel) CalcRel.createProject(
            newFilterRel,
            projRel.getNamedProjects(),
            false);

    call.transformTo(newProjRel);
  }
}

// End PushFilterPastProjectRule.java
