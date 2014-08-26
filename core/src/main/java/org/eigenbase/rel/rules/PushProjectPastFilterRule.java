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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

/**
 * PushProjectPastFilterRule implements the rule for pushing a projection past a
 * filter.
 */
public class PushProjectPastFilterRule extends RelOptRule {
  public static final PushProjectPastFilterRule INSTANCE =
      new PushProjectPastFilterRule(PushProjector.ExprCondition.FALSE);

  //~ Instance fields --------------------------------------------------------

  /**
   * Expressions that should be preserved in the projection
   */
  private final PushProjector.ExprCondition preserveExprCondition;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a PushProjectPastFilterRule.
   *
   * @param preserveExprCondition Condition for expressions that should be
   *                              preserved in the projection
   */
  private PushProjectPastFilterRule(
      PushProjector.ExprCondition preserveExprCondition) {
    super(
        operand(
            ProjectRel.class,
            operand(FilterRel.class, any())));
    this.preserveExprCondition = preserveExprCondition;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    ProjectRel origProj;
    FilterRel filterRel;

    if (call.rels.length == 2) {
      origProj = call.rel(0);
      filterRel = call.rel(1);
    } else {
      origProj = null;
      filterRel = call.rel(0);
    }
    RelNode rel = filterRel.getChild();
    RexNode origFilter = filterRel.getCondition();

    if ((origProj != null)
        && RexOver.containsOver(origProj.getProjects(), null)) {
      // Cannot push project through filter if project contains a windowed
      // aggregate -- it will affect row counts. Abort this rule
      // invocation; pushdown will be considered after the windowed
      // aggregate has been implemented. It's OK if the filter contains a
      // windowed aggregate.
      return;
    }

    PushProjector pushProjector =
        new PushProjector(
            origProj, origFilter, rel, preserveExprCondition);
    ProjectRel topProject = pushProjector.convertProject(null);

    if (topProject != null) {
      call.transformTo(topProject);
    }
  }
}

// End PushProjectPastFilterRule.java
