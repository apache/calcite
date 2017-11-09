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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * MultiJoinProjectTransposeRule implements the rule for pulling
 * {@link org.apache.calcite.rel.logical.LogicalProject}s that are on top of a
 * {@link MultiJoin} and beneath a
 * {@link org.apache.calcite.rel.logical.LogicalJoin} so the
 * {@link org.apache.calcite.rel.logical.LogicalProject} appears above the
 * {@link org.apache.calcite.rel.logical.LogicalJoin}.
 *
 * <p>In the process of doing
 * so, also save away information about the respective fields that are
 * referenced in the expressions in the
 * {@link org.apache.calcite.rel.logical.LogicalProject} we're pulling up, as
 * well as the join condition, in the resultant {@link MultiJoin}s
 *
 * <p>For example, if we have the following sub-query:
 *
 * <blockquote><pre>
 * (select X.x1, Y.y1 from X, Y
 *  where X.x2 = Y.y2 and X.x3 = 1 and Y.y3 = 2)</pre></blockquote>
 *
 * <p>The {@link MultiJoin} associated with (X, Y) associates x1 with X and
 * y1 with Y. Although x3 and y3 need to be read due to the filters, they are
 * not required after the row scan has completed and therefore are not saved.
 * The join fields, x2 and y2, are also tracked separately.
 *
 * <p>Note that by only pulling up projects that are on top of
 * {@link MultiJoin}s, we preserve projections on top of row scans.
 *
 * <p>See the superclass for details on restrictions regarding which
 * {@link org.apache.calcite.rel.logical.LogicalProject}s cannot be pulled.
 */
public class MultiJoinProjectTransposeRule extends JoinProjectTransposeRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final MultiJoinProjectTransposeRule MULTI_BOTH_PROJECT =
      new MultiJoinProjectTransposeRule(
          operand(LogicalJoin.class,
              operand(LogicalProject.class,
                  operand(MultiJoin.class, any())),
              operand(LogicalProject.class,
                  operand(MultiJoin.class, any()))),
          RelFactories.LOGICAL_BUILDER,
          "MultiJoinProjectTransposeRule: with two LogicalProject children");

  public static final MultiJoinProjectTransposeRule MULTI_LEFT_PROJECT =
      new MultiJoinProjectTransposeRule(
          operand(LogicalJoin.class,
              some(
                  operand(LogicalProject.class,
                      operand(MultiJoin.class, any())))),
          RelFactories.LOGICAL_BUILDER,
          "MultiJoinProjectTransposeRule: with LogicalProject on left");

  public static final MultiJoinProjectTransposeRule MULTI_RIGHT_PROJECT =
      new MultiJoinProjectTransposeRule(
          operand(LogicalJoin.class,
              operand(RelNode.class, any()),
              operand(LogicalProject.class,
                  operand(MultiJoin.class, any()))),
          RelFactories.LOGICAL_BUILDER,
          "MultiJoinProjectTransposeRule: with LogicalProject on right");

  //~ Constructors -----------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public MultiJoinProjectTransposeRule(
      RelOptRuleOperand operand,
      String description) {
    this(operand, RelFactories.LOGICAL_BUILDER, description);
  }

  /** Creates a MultiJoinProjectTransposeRule. */
  public MultiJoinProjectTransposeRule(
      RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory,
      String description) {
    super(operand, description, false, relBuilderFactory);
  }

  //~ Methods ----------------------------------------------------------------

  // override JoinProjectTransposeRule
  protected boolean hasLeftChild(RelOptRuleCall call) {
    return call.rels.length != 4;
  }

  // override JoinProjectTransposeRule
  protected boolean hasRightChild(RelOptRuleCall call) {
    return call.rels.length > 3;
  }

  // override JoinProjectTransposeRule
  protected LogicalProject getRightChild(RelOptRuleCall call) {
    if (call.rels.length == 4) {
      return call.rel(2);
    } else {
      return call.rel(3);
    }
  }

  // override JoinProjectTransposeRule
  protected RelNode getProjectChild(
      RelOptRuleCall call,
      LogicalProject project,
      boolean leftChild) {
    // locate the appropriate MultiJoin based on which rule was fired
    // and which projection we're dealing with
    MultiJoin multiJoin;
    if (leftChild) {
      multiJoin = call.rel(2);
    } else if (call.rels.length == 4) {
      multiJoin = call.rel(3);
    } else {
      multiJoin = call.rel(4);
    }

    // create a new MultiJoin that reflects the columns in the projection
    // above the MultiJoin
    return RelOptUtil.projectMultiJoin(multiJoin, project);
  }
}

// End MultiJoinProjectTransposeRule.java
