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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalSort} on top of a
 * {@link org.apache.calcite.rel.logical.LogicalUnion} into a {@link EnumerableMergeUnion}.
 *
 * @see EnumerableRules#ENUMERABLE_MERGE_UNION_RULE
 */
@Value.Enclosing
public class EnumerableMergeUnionRule extends RelRule<EnumerableMergeUnionRule.Config> {

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT_CONFIG = ImmutableEnumerableMergeUnionRule.Config.of()
        .withDescription("EnumerableMergeUnionRule").withOperandSupplier(
            b0 -> b0.operand(LogicalSort.class).oneInput(
                b1 -> b1.operand(LogicalUnion.class).anyInputs()));

    @Override default EnumerableMergeUnionRule toRule() {
      return new EnumerableMergeUnionRule(this);
    }
  }

  public EnumerableMergeUnionRule(Config config) {
    super(config);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final RelCollation collation = sort.getCollation();
    if (collation == null || collation.getFieldCollations().isEmpty()) {
      return false;
    }

    final Union union = call.rel(1);
    if (union.getInputs().size() < 2) {
      return false;
    }

    return true;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final RelCollation collation = sort.getCollation();
    final Union union = call.rel(1);
    final int unionInputsSize = union.getInputs().size();

    // Push down sort limit, if possible.
    RexNode inputFetch = null;
    if (sort.fetch != null) {
      if (sort.offset == null) {
        inputFetch = sort.fetch;
      } else if (sort.fetch instanceof RexLiteral && sort.offset instanceof RexLiteral) {
        inputFetch = call.builder().literal(
            RexLiteral.intValue(sort.fetch) + RexLiteral.intValue(sort.offset));
      }
    }

    final RelBuilder builder = call.builder();
    final List<RelDataTypeField> unionFieldList = union.getRowType().getFieldList();
    final List<RelNode> inputs = new ArrayList<>(unionInputsSize);
    for (RelNode input : union.getInputs()) {
      // Check if type collations match, otherwise store it in this bitset to generate a cast later
      // to guarantee that all inputs will be sorted in the same way
      final ImmutableBitSet.Builder fieldsRequiringCastBuilder = ImmutableBitSet.builder();
      for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
        final int index = fieldCollation.getFieldIndex();
        final RelDataType unionType = unionFieldList.get(index).getType();
        final RelDataType inputType = input.getRowType().getFieldList().get(index).getType();
        if (!Objects.equals(unionType.getCollation(), inputType.getCollation())) {
          fieldsRequiringCastBuilder.set(index);
        }
      }
      final ImmutableBitSet fieldsRequiringCast = fieldsRequiringCastBuilder.build();
      final RelNode unsortedInput;
      if (fieldsRequiringCast.isEmpty()) {
        unsortedInput = input;
      } else {
        // At least one cast is required, generate the corresponding projection
        builder.push(input);
        final List<RexNode> fields = builder.fields();
        final List<RexNode> projFields = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
          RexNode node = fields.get(i);
          if (fieldsRequiringCast.get(i)) {
            final RelDataType targetType = unionFieldList.get(i).getType();
            node = builder.getRexBuilder().makeCast(targetType, node);
          }
          projFields.add(node);
        }
        builder.project(projFields);
        unsortedInput = builder.build();
      }
      final RelNode newInput =
          sort.copy(sort.getTraitSet(), unsortedInput, collation, null, inputFetch);
      inputs.add(
          convert(newInput, newInput.getTraitSet().replace(EnumerableConvention.INSTANCE)));
    }

    RelNode result = EnumerableMergeUnion.create(sort.getCollation(), inputs, union.all);

    // If Sort contained a LIMIT / OFFSET, then put it back as an EnumerableLimit.
    // The output of the MergeUnion is already sorted, so we do not need a sort anymore.
    if (sort.offset != null || sort.fetch != null) {
      result = EnumerableLimit.create(result, sort.offset, sort.fetch);
    }

    call.transformTo(result);
  }
}
