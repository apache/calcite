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
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql2rel.CorrelateProjectExtractor;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that converts a {@link Correlate} over an {@link Uncollect} into a
 * single, generalized {@link Uncollect} that reads directly from the
 * correlate's left input, when possible.
 *
 * <p>Input plan:
 * <pre>
 * LogicalCorrelate(cor=[$cor0], joinType=[inner|left])
 *   left (any RelNode)
 *   Uncollect
 *     LogicalProject($cor0.f_i, $cor0.f_j, ...)  (innerProject)
 *       LogicalValues
 * </pre>
 *
 * <p>Converted to:
 * <pre>
 * LogicalProject (restores the correlate's columns)
 *   Uncollect(passthrough=[all left fields], collectionFields=[f_i, f_j, ...])
 *     left
 * </pre>
 *
 * <p>Every left field becomes a passthrough field, so the merged
 * {@code Uncollect} preserves the correlate's full output; a LEFT correlate
 * folds into {@code isOuter}. The merged {@code Uncollect} expands
 * collections in ascending input-index order; the projection on top restores
 * the order in which {@code innerProject} lists them (collections may
 * repeat).
 *
 * <p>The merge requires every expression in {@code innerProject} to be a
 * direct field access on the correlation variable (no nested struct paths,
 * no computed collections).  This can be done using {@link CorrelateProjectExtractor}.
 *
 * <p>With {@link Config#OUTER} ({@link CoreRules#CORRELATE_UNCOLLECT_OUTER})
 * the rule instead performs a smaller rewrite that has no such requirement:
 * it moves the outer join semantics of a LEFT correlate onto the
 * {@code Uncollect}, leaving an INNER {@code Correlate} that
 * {@link UnnestDecorrelateRule} may then remove.
 *
 * <p>This rule is a generalization of {@link UnnestDecorrelateRule}.
 */
@Value.Enclosing
public class CorrelateUncollectMergeRule
    extends RelRule<CorrelateUncollectMergeRule.Config>
    implements TransformationRule {

  protected CorrelateUncollectMergeRule(Config config) {
    super(config);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    // Only INNER and LEFT join types should be possible.
    // Other kinds of join should have been rejected by the validator.
    final Correlate correlate = call.rel(0);
    final JoinRelType joinType = correlate.getJoinType();
    if (config.outerOnly()
        ? joinType != JoinRelType.LEFT
        : joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
      return false;
    }
    // Both rewrites assume the Uncollect's input produces is "LogicalValues { 0 }".
    final LogicalValues values = call.rel(4);
    return values.getTuples().size() == 1;
  }

  /** Returns the number of output columns produced by unnesting a collection
   * of {@code type}, which must be a collection.
   *
   * <p>Must mirror the row-type derivation of {@link Uncollect}: a SQL MAP expands to
   * a key and a value column; a ROW element expands to one column per
   * ROW field when {@code expandStructFields}, else to a single column. */
  private static int expansionWidth(RelDataType type, boolean expandStructFields) {
    if (type instanceof MapSqlType) {
      return 2;
    }
    final RelDataType componentType = type.getComponentType();
    assert componentType != null : type + " is not a collection";
    if (expandStructFields && componentType.isStruct()) {
      return componentType.getFieldCount();
    }
    return 1;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    if (config.outerOnly()) {
      pushOuterSemantics(call);
    } else {
      merge(call);
    }
  }

  private static void merge(RelOptRuleCall call) {
    final Correlate correlate = call.rel(0);
    final RelNode left = call.rel(1);
    final Uncollect uncollect = call.rel(2);
    final Project innerProject = call.rel(3);
    final JoinRelType joinType = correlate.getJoinType();

    final CorrelationId corId = correlate.getCorrelationId();
    final int leftCount = left.getRowType().getFieldCount();

    // Check that each expression in innerProject is a direct $cor0.field access;
    // bail out if not.
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
    final List<RelDataTypeField> leftFields = left.getRowType().getFieldList();
    final boolean expandStructFields = uncollect.expandStructFields;

    // Start offset of each collection's expansion block in the merged
    // Uncollect's output, which expands collections in ascending input-index
    // order after the passthrough fields.
    final int[] blockStart = new int[leftCount];
    int nextStart = leftCount;
    for (int c : collBitSet) {
      final RelDataType collType = leftFields.get(c).getType();
      if (collType.getSqlTypeName() == SqlTypeName.ANY) {
        // If type is not precisely known bail out
        return;
      }
      blockStart[c] = nextStart;
      nextStart += expansionWidth(collType, expandStructFields);
    }
    final int mergedOrdinality = nextStart;

    // For each output column the source column that produces it
    final List<Integer> outputSource = new ArrayList<>();
    for (int i = 0; i < leftCount; i++) {
      outputSource.add(i);
    }
    for (int c : collectionFieldIndices) {
      final int width = expansionWidth(leftFields.get(c).getType(), expandStructFields);
      for (int j = 0; j < width; j++) {
        outputSource.add(blockStart[c] + j);
      }
    }
    if (uncollect.withOrdinality) {
      outputSource.add(mergedOrdinality);
    }

    final List<RelDataTypeField> corFields = correlate.getRowType().getFieldList();
    if (outputSource.size() != corFields.size()) {
      return;
    }

    // An outer Uncollect emits a NULL-padded row for an empty collection even
    // under an INNER correlate, so the merged Uncollect must stay outer.
    final boolean isOuter = uncollect.isOuter || joinType == JoinRelType.LEFT;
    final RelBuilder builder = call.builder();
    builder.push(left)
        .uncollect(ImmutableBitSet.range(leftCount), collBitSet,
            uncollect.withOrdinality, expandStructFields, isOuter);

    final RexBuilder rexBuilder = correlate.getCluster().getRexBuilder();
    final List<RelDataTypeField> mergedFields = builder.peek().getRowType().getFieldList();
    final List<RexNode> exprs = new ArrayList<>();
    for (int i = 0; i < outputSource.size(); i++) {
      final int source = outputSource.get(i);
      final RelDataType mergedType = mergedFields.get(source).getType();
      final RelDataType corType = corFields.get(i).getType();
      RexNode ref = rexBuilder.makeInputRef(mergedType, source);
      if (!mergedType.equals(corType)) {
        if (!corType.isNullable()
            || !SqlTypeUtil.equalSansNullability(
                correlate.getCluster().getTypeFactory(), mergedType, corType)) {
          // Type derived for result is unexpected -- bail out
          return;
        }
        ref = rexBuilder.makeCast(corType, ref);
      }
      exprs.add(ref);
    }

    // Restore the correlate's column order and field names
    if (RexUtil.isIdentity(exprs, builder.peek().getRowType())) {
      builder.rename(correlate.getRowType().getFieldNames());
    } else {
      builder.project(exprs, correlate.getRowType().getFieldNames());
    }
    call.transformTo(builder.build());
  }

  /** Convert a Correlate[join=left]+Unnest into Correlate[join=inner]+Unnest(isOuter). */
  private static void pushOuterSemantics(RelOptRuleCall call) {
    final Correlate correlate = call.rel(0);
    if (correlate.getJoinType() != JoinRelType.LEFT) {
      return;
    }
    final Uncollect uncollect = call.rel(2);
    // Note: this is correct even if uncollect(isOuter=[true]) already
    final Uncollect outerUncollect =
        Uncollect.create(uncollect.getTraitSet(), uncollect.getInput(),
            uncollect.withOrdinality, uncollect.getItemAliases(),
            uncollect.getPassthroughFieldIndices(),
            uncollect.getCollectionFieldIndices(),
            uncollect.expandStructFields, true);
    final RelNode newCorrelate =
        correlate.copy(correlate.getTraitSet(), correlate.getLeft(),
            outerUncollect, correlate.getCorrelationId(),
            correlate.getRequiredColumns(), JoinRelType.INNER);
    call.transformTo(newCorrelate);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableCorrelateUncollectMergeRule.Config.of()
        .withOperandSupplier(b0 -> b0.operand(Correlate.class)
            .inputs(
                b1 -> b1.operand(RelNode.class).anyInputs(),
                b2 -> b2.operand(Uncollect.class)
                    .oneInput(b3 -> b3.operand(Project.class)
                        .oneInput(b4 -> b4.operand(LogicalValues.class)
                            .anyInputs()))))
        .as(Config.class);

    /** Configuration that only moves the outer join semantics of a LEFT
     * correlate onto the {@code Uncollect}, without eliminating the
     * correlate. */
    Config OUTER = DEFAULT
        .withDescription("CorrelateUncollectMergeRule(outer)")
        .as(Config.class)
        .withOuterOnly(true);

    /** Whether to only push the outer join semantics of a LEFT correlate
     * into the {@link Uncollect}, keeping the correlate; by default the rule
     * eliminates the correlate entirely. */
    @Value.Default default boolean outerOnly() {
      return false;
    }

    Config withOuterOnly(boolean outerOnly);

    @Override default CorrelateUncollectMergeRule toRule() {
      return new CorrelateUncollectMergeRule(this);
    }
  }
}
