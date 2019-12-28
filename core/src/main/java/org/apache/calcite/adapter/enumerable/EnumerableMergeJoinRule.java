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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/** Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalJoin} relational expression
 * {@link EnumerableConvention enumerable calling convention}.
 *
 * @see EnumerableJoinRule
 */
class EnumerableMergeJoinRule extends ConverterRule {
  EnumerableMergeJoinRule() {
    // Merge join is symmetric, so we do not create instances where right.id exceeds left.id
    // EnumerableMergeJoin only supports inner join.
    // It supports non-equi join  using a post-filter
    super(LogicalJoin.class,
        (Predicate<LogicalJoin>) j ->
            j.getJoinType() == JoinRelType.INNER
                && !j.analyzeCondition().leftKeys.isEmpty()
                && keysAreNonNull(j.getLeft().getRowType(), j.analyzeCondition().leftKeys)
                && keysAreNonNull(j.getRight().getRowType(), j.analyzeCondition().rightKeys),
        Convention.NONE,
        EnumerableConvention.INSTANCE,
        RelFactories.LOGICAL_BUILDER,
        "EnumerableMergeJoinRule");
  }

  private static boolean keysAreNonNull(RelDataType rowType, ImmutableIntList keys) {
    // Workaround for CALCITE-3674
    // EnumerableMergeJoinRule fails with NPE on nullable join keys
    List<RelDataTypeField> fieldList = rowType.getFieldList();
    for (Integer key : keys) {
      if (fieldList.get(key).getType().isNullable()) {
        return false;
      }
    }
    return true;
  }

  @Override public RelNode convert(RelNode rel) {
    LogicalJoin join = (LogicalJoin) rel;
    final JoinInfo info = join.analyzeCondition();
    final List<RelNode> newInputs = new ArrayList<>();
    final List<RelCollation> collations = new ArrayList<>();
    int offset = 0;
    int addedSortCount = 0;
    for (Ord<RelNode> ord : Ord.zip(join.getInputs())) {
      RelTraitSet traits = ord.e.getTraitSet()
          .replace(EnumerableConvention.INSTANCE);
      final List<RelFieldCollation> fieldCollations = new ArrayList<>();
      for (int key : info.keys().get(ord.i)) {
        fieldCollations.add(
            new RelFieldCollation(key, RelFieldCollation.Direction.ASCENDING,
                RelFieldCollation.NullDirection.LAST));
      }
      final RelCollation collation = RelCollations.of(fieldCollations);
      collations.add(RelCollations.shift(collation, offset));
      traits = traits.replace(collation);
      // Volcano does not support creating sort node out of a collation trait
      // So LogicalSort is used here explicitly. Note: it will be removed by
      // SortRemoveRule if it was not required.
      RelNode sortedInput;
      if (ord.e.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE).satisfies(collation)) {
        // If the input is already sorted, no need to create a sort node
        sortedInput = ord.e;
      } else if (addedSortCount == 0) {
        sortedInput = LogicalSort.create(ord.e, collation, null, null);
        addedSortCount++;
      } else {
        sortedInput = ord.e;
      }
      RelNode enumerableSortedInput = convert(sortedInput, traits);
      newInputs.add(enumerableSortedInput);
      offset += ord.e.getRowType().getFieldCount();
    }
    final RelNode left = newInputs.get(0);
    final RelNode right = newInputs.get(1);
    final RelOptCluster cluster = join.getCluster();
    RelNode newRel;

    RelTraitSet traitSet = join.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    if (!collations.isEmpty()) {
      traitSet = traitSet.replace(collations);
    }
    newRel = new EnumerableMergeJoin(cluster,
        traitSet,
        left,
        right,
        info.getEquiCondition(left, right, cluster.getRexBuilder()),
        join.getVariablesSet(),
        join.getJoinType());
    if (!info.isEqui()) {
      RexNode nonEqui = RexUtil.composeConjunction(cluster.getRexBuilder(),
          info.nonEquiConditions);
      newRel = new EnumerableFilter(cluster, newRel.getTraitSet(),
          newRel, nonEqui);
    }
    return newRel;
  }
}
