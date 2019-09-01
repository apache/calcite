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
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.List;

/** Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalJoin} relational expression
 * {@link EnumerableConvention enumerable calling convention}.
 *
 * @see EnumerableJoinRule
 */
class EnumerableMergeJoinRule extends ConverterRule {
  EnumerableMergeJoinRule() {
    super(LogicalJoin.class,
        Convention.NONE,
        EnumerableConvention.INSTANCE,
        "EnumerableMergeJoinRule");
  }

  @Override public RelNode convert(RelNode rel) {
    LogicalJoin join = (LogicalJoin) rel;
    final JoinInfo info = join.analyzeCondition();
    if (join.getJoinType() != JoinRelType.INNER) {
      // EnumerableMergeJoin only supports inner join.
      // (It supports non-equi join, using a post-filter; see below.)
      return null;
    }
    if (info.pairs().size() == 0) {
      // EnumerableMergeJoin CAN support cartesian join, but disable it for now.
      return null;
    }

    final RelNode left = join.getInput(0);
    final RelNode right = join.getInput(1);

    final List<RelNode> enumerableInputs = new ArrayList<>();
    enumerableInputs.add(
        convert(left, left.getTraitSet().replace(EnumerableConvention.INSTANCE)));
    enumerableInputs.add(
        convert(right, right.getTraitSet().replace(EnumerableConvention.INSTANCE)));

    final List<RelNode> newInputs = new ArrayList<>();
    final List<RelCollation> collations = new ArrayList<>();
    int offset = 0;
    for (Ord<RelNode> ord : Ord.zip(enumerableInputs)) {
      if (info.pairs().isEmpty()) {
        newInputs.add(ord.e);
      } else {
        final List<RelFieldCollation> fieldCollations = new ArrayList<>();
        for (int key : info.keys().get(ord.i)) {
          fieldCollations.add(
              new RelFieldCollation(key, RelFieldCollation.Direction.ASCENDING,
                  RelFieldCollation.NullDirection.LAST));
        }
        final RelCollation collation =
            ord.e.getTraitSet().canonize(RelCollations.of(fieldCollations));
        if (ord.e.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE)
            .satisfies(collation)) {
          newInputs.add(ord.e);
        } else {
          // Another choice is to change the trait of collation of input
          // and tag a AbstractConverter and then expand the converter as
          // a Sort during optimization.
          // But we choose to construct the operator of EnumerableSort
          // here, thus to save another matching effort when optimization.
          // Relying on expansion of AbstractConverter could be expensive.
          // There might be matching explosion using VolcanoPlanner. Check
          // performance issue CALCITE-2970.
          RelNode enumerableSort =
              EnumerableSort.create(ord.e, collation, null, null);
          newInputs.add(enumerableSort);
        }
        collations.add(RelCollations.shift(collation, offset));
      }
      offset += ord.e.getRowType().getFieldCount();
    }
    final RelNode newLeft = newInputs.get(0);
    final RelNode newRight = newInputs.get(1);
    final RelOptCluster cluster = join.getCluster();
    RelNode newRel;

    RelTraitSet traitSet = join.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    if (!collations.isEmpty()) {
      traitSet = traitSet.replace(collations);
    }
    newRel = new EnumerableMergeJoin(cluster,
        traitSet,
        newLeft,
        newRight,
        info.getEquiCondition(newLeft, newRight, cluster.getRexBuilder()),
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

// End EnumerableMergeJoinRule.java
