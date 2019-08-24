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
    final List<RelNode> newInputs = new ArrayList<>();
    final List<RelCollation> collations = new ArrayList<>();
    int offset = 0;
    for (Ord<RelNode> ord : Ord.zip(join.getInputs())) {
      RelTraitSet traits = ord.e.getTraitSet()
          .replace(EnumerableConvention.INSTANCE);
      if (!info.pairs().isEmpty()) {
        final List<RelFieldCollation> fieldCollations = new ArrayList<>();
        for (int key : info.keys().get(ord.i)) {
          fieldCollations.add(
              new RelFieldCollation(key, RelFieldCollation.Direction.ASCENDING,
                  RelFieldCollation.NullDirection.LAST));
        }
        final RelCollation collation = RelCollations.of(fieldCollations);
        collations.add(RelCollations.shift(collation, offset));
        traits = traits.replace(collation);
      }
      newInputs.add(convert(ord.e, traits));
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

// End EnumerableMergeJoinRule.java
