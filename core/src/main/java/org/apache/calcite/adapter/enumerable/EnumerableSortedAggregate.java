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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/** Sort based physical implementation of {@link Aggregate} in
 * {@link EnumerableConvention enumerable calling convention}. */
public class EnumerableSortedAggregate extends Aggregate implements EnumerableRel {
  public EnumerableSortedAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
    assert getConvention() instanceof EnumerableConvention;
  }

  @Override public EnumerableSortedAggregate copy(RelTraitSet traitSet, RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return new EnumerableSortedAggregate(getCluster(), traitSet, input,
        groupSet, groupSets, aggCalls);
  }

  @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
      final RelTraitSet required) {
    if (!isSimple(this)) {
      return null;
    }

    RelTraitSet inputTraits = getInput().getTraitSet();
    RelCollation collation = required.getTrait(RelCollationTraitDef.INSTANCE);
    ImmutableBitSet requiredKeys = ImmutableBitSet.of(RelCollations.ordinals(collation));
    ImmutableBitSet groupKeys = ImmutableBitSet.range(groupSet.cardinality());

    Mappings.TargetMapping mapping = Mappings.source(groupSet.toList(),
        input.getRowType().getFieldCount());

    if (requiredKeys.equals(groupKeys)) {
      RelCollation inputCollation = RexUtil.apply(mapping, collation);
      return Pair.of(required, ImmutableList.of(inputTraits.replace(inputCollation)));
    } else if (groupKeys.contains(requiredKeys)) {
      // group by a,b,c order by c,b
      List<RelFieldCollation> list = new ArrayList<>(collation.getFieldCollations());
      groupKeys.except(requiredKeys).forEach(k -> list.add(new RelFieldCollation(k)));
      RelCollation aggCollation = RelCollations.of(list);
      RelCollation inputCollation = RexUtil.apply(mapping, aggCollation);
      return Pair.of(traitSet.replace(aggCollation),
          ImmutableList.of(inputTraits.replace(inputCollation)));
    }

    // Group keys doesn't contain all the required keys, e.g.
    // group by a,b order by a,b,c
    // nothing we can do to propagate traits to child nodes.
    return null;
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    throw Util.needToImplement("EnumerableSortedAggregate");
  }
}
