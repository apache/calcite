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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Planner rule that pushes a {@link Project} under {@link Correlate} to apply
 * on Correlate's left and right inputs.
 *
 * @see CoreRules#PROJECT_CORRELATE_TRANSPOSE
 */
@Value.Enclosing
public class ProjectCorrelateTransposeRule
    extends RelRule<ProjectCorrelateTransposeRule.Config>
    implements TransformationRule {

  /** Creates a ProjectCorrelateTransposeRule. */
  protected ProjectCorrelateTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public ProjectCorrelateTransposeRule(
      PushProjector.ExprCondition preserveExprCondition,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withPreserveExprCondition(preserveExprCondition));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Project origProject = call.rel(0);
    final Correlate correlate = call.rel(1);

    // locate all fields referenced in the projection
    // determine which inputs are referenced in the projection;
    // if all fields are being referenced and there are no
    // special expressions, no point in proceeding any further
    final PushProjector pushProjector =
        new PushProjector(origProject, call.builder().literal(true), correlate,
            config.preserveExprCondition(), call.builder());
    if (pushProjector.locateAllRefs()) {
      return;
    }

    // create left and right projections, projecting only those
    // fields referenced on each side
    final RelNode leftProject =
        pushProjector.createProjectRefsAndExprs(
            correlate.getLeft(),
            true,
            false);
    RelNode rightProject =
        pushProjector.createProjectRefsAndExprs(
            correlate.getRight(),
            true,
            true);

    final Map<Integer, Integer> requiredColsMap = new HashMap<>();

    // adjust requiredColumns that reference the projected columns
    int[] adjustments = pushProjector.getAdjustments();
    BitSet updatedBits = new BitSet();
    for (Integer col : correlate.getRequiredColumns()) {
      int newCol = col + adjustments[col];
      updatedBits.set(newCol);
      requiredColsMap.put(col, newCol);
    }

    final RexBuilder rexBuilder = call.builder().getRexBuilder();

    CorrelationId correlationId = correlate.getCluster().createCorrel();
    RexCorrelVariable rexCorrel =
        (RexCorrelVariable) rexBuilder.makeCorrel(
            leftProject.getRowType(),
            correlationId);

    // updates RexCorrelVariable and sets actual RelDataType for RexFieldAccess
    rightProject =
        rightProject.accept(
            new RelNodesExprsHandler(
                new RexFieldAccessReplacer(correlate.getCorrelationId(),
                    rexCorrel, rexBuilder, requiredColsMap)));

    // create a new correlate with the projected children
    final Correlate newCorrelate =
        correlate.copy(
            correlate.getTraitSet(),
            leftProject,
            rightProject,
            correlationId,
            ImmutableBitSet.of(BitSets.toIter(updatedBits)),
            correlate.getJoinType());

    // put the original project on top of the correlate, converting it to
    // reference the modified projection list
    final RelNode topProject =
        pushProjector.createNewProject(newCorrelate, adjustments);

    call.transformTo(topProject);
  }

  /**
   * Visitor for RexNodes which replaces {@link RexCorrelVariable} with specified.
   */
  public static class RexFieldAccessReplacer extends RexShuttle {
    private final RexBuilder builder;
    private final CorrelationId rexCorrelVariableToReplace;
    private final RexCorrelVariable rexCorrelVariable;
    private final Map<Integer, Integer> requiredColsMap;

    public RexFieldAccessReplacer(
        CorrelationId rexCorrelVariableToReplace,
        RexCorrelVariable rexCorrelVariable,
        RexBuilder builder,
        Map<Integer, Integer> requiredColsMap) {
      this.rexCorrelVariableToReplace = rexCorrelVariableToReplace;
      this.rexCorrelVariable = rexCorrelVariable;
      this.builder = builder;
      this.requiredColsMap = requiredColsMap;
    }

    @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
      if (variable.id.equals(rexCorrelVariableToReplace)) {
        return rexCorrelVariable;
      }
      return variable;
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      RexNode refExpr = fieldAccess.getReferenceExpr().accept(this);
      // creates new RexFieldAccess instance for the case when referenceExpr was replaced.
      // Otherwise calls super method.
      if (refExpr == rexCorrelVariable) {
        int fieldIndex = fieldAccess.getField().getIndex();
        return builder.makeFieldAccess(
            refExpr,
            requireNonNull(requiredColsMap.get(fieldIndex),
                () -> "no entry for field " + fieldIndex + " in " + requiredColsMap));
      }
      return super.visitFieldAccess(fieldAccess);
    }
  }

  /**
   * Visitor for RelNodes which applies specified {@link RexShuttle} visitor
   * for every node in the tree.
   */
  public static class RelNodesExprsHandler extends RelShuttleImpl {
    private final RexShuttle rexVisitor;

    public RelNodesExprsHandler(RexShuttle rexVisitor) {
      this.rexVisitor = rexVisitor;
    }

    @Override protected RelNode visitChild(RelNode parent, int i,
        RelNode input) {
      return super.visitChild(parent, i, input.stripped())
          .accept(rexVisitor);
    }
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableProjectCorrelateTransposeRule.Config.builder()
        .withPreserveExprCondition(expr -> !(expr instanceof RexOver))
        .build()
        .withOperandFor(Project.class, Correlate.class);

    @Override default ProjectCorrelateTransposeRule toRule() {
      return new ProjectCorrelateTransposeRule(this);
    }

    /** Defines when an expression should not be pushed. */
    PushProjector.ExprCondition preserveExprCondition();

    /** Sets {@link #preserveExprCondition()}. */
    Config withPreserveExprCondition(PushProjector.ExprCondition condition);

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Project> projectClass,
        Class<? extends Correlate> correlateClass) {
      return withOperandSupplier(b0 ->
          b0.operand(projectClass).oneInput(b1 ->
              b1.operand(correlateClass).anyInputs()))
          .as(Config.class);
    }
  }
}
