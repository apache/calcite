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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Planner rule that removes constant keys from an
 * {@link Sort}.
 *
 * <p>Constant fields are deduced using
 * {@link RelMetadataQuery#getPulledUpPredicates(RelNode)}; the input does not
 * need to be a {@link org.apache.calcite.rel.core.Project}.
 *
 * <p>Since the transformed relational expression has to match the original
 * relational expression, the constants are placed in a projection above the
 * reduced sort. If those constants are not used, another rule will remove
 * them from the project.
 */
@Value.Enclosing
public class SortProjectPullUpConstantsRule
    extends RelRule<SortProjectPullUpConstantsRule.Config>
    implements TransformationRule {

  protected SortProjectPullUpConstantsRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final RelNode input = call.rel(1);

    final RelCollation collation = sort.getCollation();
    final List<Integer> fieldCollationIndexes = collation.getFieldCollations()
        .stream()
        .map(RelFieldCollation::getFieldIndex)
        .collect(Collectors.toList());
    if (fieldCollationIndexes.isEmpty()) {
      return;
    }

    final RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelOptPredicateList predicates =
        mq.getPulledUpPredicates(input);
    if (RelOptPredicateList.isEmpty(predicates)) {
      return;
    }

    final NavigableMap<Integer, RexNode> map = new TreeMap<>();
    for (int fieldCollationIndex : fieldCollationIndexes) {
      final RexInputRef ref =
          rexBuilder.makeInputRef(input, fieldCollationIndex);
      if (predicates.constantMap.containsKey(ref)) {
        map.put(fieldCollationIndex, predicates.constantMap.get(ref));
      }
    }

    // None of the sort field expressions is constant. Nothing to do.
    if (map.isEmpty()) {
      return;
    }

    // Input of new top constant project.
    final RelNode newInputRel;
    if (map.size() == fieldCollationIndexes.size()) {
      // All field collations are constants.
      if (sort.offset == null && sort.fetch == null) {
        // No any offset or fetch in current sort.
        // Remove current sort.
        newInputRel = input;
      } else {
        // Some offset or fetch exist in current sort.
        newInputRel = sort.copy(sort.getTraitSet(), input, RelCollations.EMPTY);
      }
    } else {
      // All field collations are not all constants.
      final List<RelFieldCollation> newFieldCollations = new ArrayList<>();
      for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
        final int fieldIndex = fieldCollation.getFieldIndex();
        if (map.containsKey(fieldIndex)) {
          continue;
        }
        newFieldCollations.add(fieldCollation);
      }
      final RelCollation newRelCollation = RelCollations.of(newFieldCollations);
      newInputRel = sort.copy(sort.getTraitSet(), input, newRelCollation);
    }

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(newInputRel);

    // Create a projection back again.
    List<Pair<RexNode, String>> projects = new ArrayList<>();
    for (RelDataTypeField field : sort.getRowType().getFieldList()) {
      final int index = field.getIndex();
      final RexNode constantNode = map.get(index);
      final RexNode newExpr;
      if (constantNode == null) {
        newExpr = rexBuilder.makeInputRef(newInputRel, index);
      } else {
        // Re-generate the constant expression in the project.
        RelDataType originalType =
            sort.getRowType().getFieldList().get(projects.size()).getType();
        if (!originalType.equals(constantNode.getType())) {
          newExpr = rexBuilder.makeCast(originalType, constantNode, true);
        } else {
          newExpr = constantNode;
        }
      }
      projects.add(Pair.of(newExpr, field.getName()));
    }
    // Create a top equal project for origin sort.
    relBuilder.project(Pair.left(projects), Pair.right(projects));
    call.transformTo(relBuilder.build());
  }


  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSortProjectPullUpConstantsRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Sort.class)
                .oneInput(b1 ->
                    b1.operand(RelNode.class)
                        .anyInputs()))
        .as(Config.class);

    @Override default SortProjectPullUpConstantsRule toRule() {
      return new SortProjectPullUpConstantsRule(this);
    }
  }

}
