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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** Convert representations of a projected Unnest that use LogicalCorrelate into
 * simple Unnest representations.
 *
 * <p>Original plan:
 * LogicalProject // only uses rightmost columns of correlate, outerProject
 *   LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{...}])
 *     LeftSubquery
 *     LogicalProject (optional; innerProject)
 *       Uncollect
 *         LogicalProject(COL=[$cor0.ARRAY])
 *           LogicalValues(tuples=[[{ 0 }]])
 *
 * <p>is converted to
 *
 * <p>Resulting plan:
 * LogicalProject
 *   LogicalProject (optional)
 *     Uncollect
 *       LogicalProject
 *         LeftSubquery
 */
@Value.Enclosing
public class UnnestDecorrelateRule extends RelRule<UnnestDecorrelateRule.Config>
    implements TransformationRule {

  protected UnnestDecorrelateRule(UnnestDecorrelateRule.Config config) {
    super(config);
  }

  /** Given an expression and a correlationId, find whether the expression is a
   * sequence of field accesses that starts in the correlationId, i.e., it
   * has the form corId.field1.field2.
   *
   * @param expr   Expression to analyze
   * @param corId  Correlation id to search for
   * @param fieldsAccessed  On successful return, contains the list of fields accessed
   *                        in reverse order, e.g., (field2, field1)
   * @return  True if {@code expr} has the expected shape, false otherwise.
   */
  private boolean extractFieldReferences(
      RexNode expr, CorrelationId corId, List<RelDataTypeField> fieldsAccessed) {
    if (expr instanceof RexCorrelVariable) {
      RexCorrelVariable cv = (RexCorrelVariable) expr;
      return cv.id == corId;
    } else if (expr instanceof RexFieldAccess) {
      RexFieldAccess fieldAccess = (RexFieldAccess) expr;
      fieldsAccessed.add(fieldAccess.getField());
      return extractFieldReferences(fieldAccess.getReferenceExpr(), corId, fieldsAccessed);
    } else {
      return false;
    }
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Project outerProject = call.rel(0);
    Correlate cor = call.rel(1);
    CorrelationId corId = cor.getCorrelationId();

    RelNode left = call.rel(2);
    int leftCount = left.getRowType().getFieldCount();
    ImmutableBitSet used = RelOptUtil.InputFinder.bits(outerProject.getProjects(), null);
    int firstUsed = used.nextSetBit(0);
    if (firstUsed != -1 && firstUsed < leftCount) {
      return;
    }

    int uncollectIndex = 3;
    Project innerProject = null;
    if (call.rel(uncollectIndex) instanceof Project) {
      innerProject = call.rel(3);
      uncollectIndex = 4;
    }

    Uncollect uncollect = call.rel(uncollectIndex);
    Project project = call.rel(uncollectIndex + 1);

    List<RexNode> projects = project.getProjects();
    if (projects.size() != 1) {
      return;
    }

    final RexNode projected = projects.get(0);
    final ArrayList<RelDataTypeField> fieldsAccessed = new ArrayList<>();
    if (!extractFieldReferences(projected, corId, fieldsAccessed)) {
      return;
    }

    final RelBuilder builder = call.builder();
    builder.push(left);

    // Last field constructed by builder
    RexNode field = null;
    // Fields are in reverse order
    Collections.reverse(fieldsAccessed);
    for (RelDataTypeField index : fieldsAccessed) {
      if (field != null) {
        field = builder.field(field, index.getName());
      } else {
        field = builder.field(index.getName());
      }
    }
    builder.project(requireNonNull(field, "field"))
        .uncollect(uncollect.getItemAliases(), uncollect.withOrdinality);
    if (innerProject != null) {
      builder.project(innerProject.getProjects());
    }
    final List<RexNode> shifted = RexUtil.shift(outerProject.getProjects(), -leftCount);
    builder.project(shifted);
    RelNode result = builder.build();
    call.transformTo(result);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    UnnestDecorrelateRule.Config BASE = ImmutableUnnestDecorrelateRule.Config.of();

    RelRule.Config DEFAULT = BASE
          .withOperandSupplier(b0 -> b0.operand(Project.class)
              .oneInput(b1 -> b1.operand(Correlate.class)
                  .inputs(b2 -> b2.operand(RelNode.class).anyInputs(),
                      b3 -> b3.operand(Uncollect.class)
                          .oneInput(b4 -> b4.operand(Project.class)
                              .oneInput(b5 -> b5.operand(LogicalValues.class).anyInputs())))));

    RelRule.Config WITH_PROJECT = BASE
        .withOperandSupplier(b0 -> b0.operand(Project.class)
            .oneInput(b1 -> b1.operand(Correlate.class)
                .inputs(b2 -> b2.operand(RelNode.class).anyInputs(),
                    b3 -> b3.operand(Project.class)
                        .oneInput(b4 -> b4.operand(Uncollect.class)
                            .oneInput(b5 -> b5.operand(Project.class)
                                .oneInput(b6 -> b6.operand(LogicalValues.class).anyInputs()))))));

    @Override default UnnestDecorrelateRule toRule() {
      return new UnnestDecorrelateRule(this);
    }
  }
}
