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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** Planner rule that converts a {@link LogicalTableModify} to an
 * {@link EnumerableTableModify}.
 *
 * <p>For INSERT and UPDATE, the rule adds assignment checks of the following
 * form:
 *
 * <blockquote><pre>{@code
 * LogicalTableModify
 *   input
 *
 * EnumerableTableModify
 *   EnumerableCalc(
 *     condition=[
 *       AND(
 *         $THROW_UNLESS(
 *           IS_NOT_FALSE(
 *             LESS_THAN_OR_EQUAL(CHAR_LENGTH($0), targetPrecision)), ...),
 *         $THROW_UNLESS(
 *           IS_NOT_FALSE(
 *             LESS_THAN_OR_EQUAL(OCTET_LENGTH($1), targetPrecision)), ...))],
 *     projects=[
 *       $0,
 *       $1,
 *       CAST($2 AS targetType)])
 *     enumerable input
 * }</pre></blockquote>
 *
 * <p>Character and binary values are checked by length because narrowing casts
 * may truncate them. Exact numeric values use a target cast, which throws on
 * overflow.
 *
 * <p>You may provide a custom config to convert other nodes that extend
 * {@link TableModify}.
 *
 * @see EnumerableRules#ENUMERABLE_TABLE_MODIFICATION_RULE */
public class EnumerableTableModifyRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalTableModify.class, Convention.NONE,
          EnumerableConvention.INSTANCE, "EnumerableTableModificationRule")
      .withRuleFactory(EnumerableTableModifyRule::new);

  /** Creates an EnumerableTableModifyRule. */
  protected EnumerableTableModifyRule(Config config) {
    super(config);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    final TableModify modify = (TableModify) rel;
    final RelOptCluster cluster = modify.getCluster();
    final ModifiableTable modifiableTable =
        modify.getTable().unwrap(ModifiableTable.class);
    if (modifiableTable == null) {
      return null;
    }
    final RelTraitSet traitSet =
        modify.getTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode input = convert(modify.getInput(), traitSet);
    if (modify.isInsert() || modify.isUpdate()) {
      // INSERT assigns stored columns; UPDATE assigns columns in the SET list.
      RelDataType assignmentType = modify.isInsert()
          ? RelOptTableImpl.realRowType(modify.getTable())
          : modify.getCatalogReader().createTypeFromProjection(
              modify.getTable().getRowType(),
              requireNonNull(modify.getUpdateColumnList(), "updateColumnList"));
      if (modify.isFlattened()) {
        // TableModify flattens its input, so flatten the target fields too.
        assignmentType =
            SqlTypeUtil.flattenRecordType(cluster.getTypeFactory(), assignmentType, null);
      }

      final RexBuilder rexBuilder = cluster.getRexBuilder();
      final List<RexNode> projects =
          new ArrayList<>(rexBuilder.identityProjects(input.getRowType()));
      final List<RexNode> checks = new ArrayList<>();
      // UPDATE appends SET values to the old row; INSERT has only new values.
      final int assignmentOffset = projects.size() - assignmentType.getFieldCount();
      for (RelDataTypeField field : assignmentType.getFieldList()) {
        final int sourceOrdinal = assignmentOffset + field.getIndex();
        final RexNode source = projects.get(sourceOrdinal);
        final RelDataType targetType = field.getType();
        final SqlTypeName targetName = targetType.getSqlTypeName();
        if (SqlTypeUtil.inCharOrBinaryFamilies(targetType)) {
          if (targetType.getPrecision() < 0) {
            continue;
          }
          // Check character and binary lengths because their casts may truncate.
          final RexNode length =
              rexBuilder.makeCall(SqlTypeUtil.inCharFamily(targetType)
                  ? SqlStdOperatorTable.CHAR_LENGTH
                  : SqlStdOperatorTable.OCTET_LENGTH,
              source);
          final RexNode fits =
              rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, length,
              rexBuilder.makeExactLiteral(
                  BigDecimal.valueOf(targetType.getPrecision())));
          // IS_NOT_FALSE lets NULL pass this length check.
          final RexNode valid =
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_FALSE, fits);
          checks.add(
              rexBuilder.makeCall(SqlInternalOperators.THROW_UNLESS,
              valid, rexBuilder.makeLiteral("Value exceeds precision "
                  + targetType.getPrecision() + " of "
                  + targetType.getFullTypeString())));
        } else {
          if (targetName == SqlTypeName.DECIMAL) {
            // A runtime BigDecimal may exceed its declared precision.
            if (targetType.getPrecision() < 0 || targetType.getScale() < 0) {
              continue;
            }
          } else if (!SqlTypeUtil.isExactNumeric(targetType)
              || source.getType().getSqlTypeName() == targetName) {
            continue;
          }
          // Exact numeric casts reject overflow and produce the target value.
          projects.set(sourceOrdinal, rexBuilder.makeCast(targetType, source));
        }
      }
      if (!checks.isEmpty() || !RexUtil.isIdentity(projects, input.getRowType())) {
        // RexProgram has one condition, so combine all length checks.
        input =
            EnumerableCalc.create(
                input, RexProgram.create(input.getRowType(), projects,
                RexUtil.composeConjunction(rexBuilder, checks, true),
                input.getRowType().getFieldNames(), rexBuilder));
      }
    }
    return new EnumerableTableModify(
        cluster, traitSet,
        modify.getTable(),
        modify.getCatalogReader(),
        input,
        modify.getOperation(),
        modify.getUpdateColumnList(),
        modify.getSourceExpressionList(),
        modify.isFlattened());
  }
}
