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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Rule that converts a {@link Correlate} over an {@link Uncollect} into a
 * single, generalized {@link Uncollect} that reads directly from the
 * correlate's left input, when possible.
 *
 * <p>Input plan:
 * <pre>
 * LogicalProject (outerProject)
 *   LogicalCorrelate(cor=[$cor0], joinType=[inner|left|...])
 *     left (any RelNode)
 *     Uncollect
 *       LogicalProject($cor0.f_i, $cor0.f_j, ...)  (innerProject)
 *         LogicalValues
 * </pre>
 *
 * <p>Converted to:
 * <pre>
 * LogicalProject (outerProject, remapped to the merged Uncollect's output indices)
 *   Uncollect(collectionFields=[f_i, f_j, ...])
 *     left
 * </pre>
 *
 * <p>The rule fires only when every expression in {@code innerProject} is a
 * direct field access on the correlation variable (no nested struct paths).
 * Collection fields referenced in {@code outerProject} are passed through
 * as raw values alongside their element-column expansion.
 *
 * <p>This rule is a generalization of {@link UnnestDecorrelateRule}.
 *
 * <p>This merge cannot be performed by
 * {@link org.apache.calcite.sql2rel.SqlToRelConverter}, which builds the {@code Correlate}
 * over {@code Uncollect} shape. During conversion, each FROM-item alias ({@code t1} and
 * {@code t2} in {@code SELECT t2.x FROM t1, UNNEST(t1.arr) AS t2(x)}) is resolved to a
 * column range by locating a distinct sub-node of the plan for its namespace. A
 * {@code Correlate} has one child per namespace, but the merged {@code Uncollect} has a
 * single input, so references to {@code t2} converted after the merge (the SELECT list,
 * for example) would find no node to resolve to. A planner rule fires after conversion,
 * when every reference is already a fixed {@link RexInputRef} offset.
 */
@Value.Enclosing
public class CorrelateUncollectMergeRule
    extends RelRule<CorrelateUncollectMergeRule.Config>
    implements TransformationRule {

  protected CorrelateUncollectMergeRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Project outerProject = call.rel(0);
    final Correlate correlate = call.rel(1);
    final RelNode left = call.rel(2);
    final Uncollect uncollect = call.rel(3);
    final Project innerProject = call.rel(4);

    // Only INNER and LEFT join types map to Uncollect's passthrough/outer semantics.
    // Other types should have been rejected by the validator.
    final JoinRelType joinType = correlate.getJoinType();
    if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
      return;
    }

    final CorrelationId corId = correlate.getCorrelationId();
    final int leftCount = left.getRowType().getFieldCount();

    // Check that each expression in innerProject is a direct $cor0.field access;
    // bail out if not
    final List<Integer> collectionFieldIndices = new ArrayList<>();
    for (RexNode expr : innerProject.getProjects()) {
      if (!(expr instanceof RexFieldAccess)) {
        return;
      }
      final RexFieldAccess fa = (RexFieldAccess) expr;
      if (!(fa.getReferenceExpr() instanceof RexCorrelVariable)) {
        return;
      }
      if (((RexCorrelVariable) fa.getReferenceExpr()).id != corId) {
        return;
      }
      collectionFieldIndices.add(fa.getField().getIndex());
    }

    if (collectionFieldIndices.isEmpty()) {
      return;
    }

    final ImmutableBitSet collBitSet = ImmutableBitSet.of(collectionFieldIndices);

    // Scan outerProject to determine which left fields it references.
    // Any referenced left field — including collection fields — becomes a
    // passthrough field in the merged Uncollect (so the raw value appears in the output).
    // Unreferenced fields are skipped.
    final Set<Integer> passthroughSet = new HashSet<>();
    for (RexNode proj : outerProject.getProjects()) {
      for (int ref : RelOptUtil.InputFinder.bits(proj)) {
        if (ref < leftCount) {
          passthroughSet.add(ref);
        }
        // refs >= leftCount are collections element or ordinality fields
      }
    }
    final ImmutableBitSet passthroughBitSet = ImmutableBitSet.of(passthroughSet);
    final int passthroughCount = passthroughBitSet.cardinality();

    // Map passthrough left fields to the merged Uncollect's positions 0 to passthroughCount-1
    // (only referenced left fields are mapped; unreferenced fields will be mapped to -1).
    final int[] passthroughToUncollect = new int[leftCount];
    Arrays.fill(passthroughToUncollect, -1);
    int pos = 0;
    for (int i : passthroughBitSet) {
      passthroughToUncollect[i] = pos++;
    }

    final boolean isOuter = correlate.getJoinType() == JoinRelType.LEFT;
    final RelBuilder builder = call.builder();
    builder.push(left)
        .uncollect(passthroughBitSet, collBitSet, uncollect.withOrdinality, isOuter);

    // Remap outerProject to the merged Uncollect's output indices.
    final RexBuilder rexBuilder = correlate.getCluster().getRexBuilder();
    final List<RelDataTypeField> mergedFields = builder.peek().getRowType().getFieldList();
    final RexShuttle remapper = new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        int idx = inputRef.getIndex();
        int newIdx = idx < leftCount
            ? passthroughToUncollect[idx]
            : passthroughCount + idx - leftCount;
        return rexBuilder.makeInputRef(mergedFields.get(newIdx).getType(), newIdx);
      }
    };

    final List<RexNode> newProjects =
        Util.transform(outerProject.getProjects(), proj -> proj.accept(remapper));
    builder.project(newProjects, outerProject.getRowType().getFieldNames());
    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config BASE = ImmutableCorrelateUncollectMergeRule.Config.of();

    RelRule.Config DEFAULT = BASE
        .withOperandSupplier(b0 -> b0.operand(Project.class)
            .oneInput(b1 -> b1.operand(Correlate.class)
                .inputs(
                    b2 -> b2.operand(RelNode.class).anyInputs(),
                    b3 -> b3.operand(Uncollect.class)
                        .oneInput(b4 -> b4.operand(Project.class)
                            .oneInput(b5 -> b5.operand(LogicalValues.class)
                                .anyInputs())))));

    @Override default CorrelateUncollectMergeRule toRule() {
      return new CorrelateUncollectMergeRule(this);
    }
  }
}
