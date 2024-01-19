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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Arrow.
 */
class ArrowFilter extends Filter implements ArrowRel {
  private final List<String> match;

  ArrowFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition) {
    super(cluster, traitSet, input, condition);
    final ArrowTranslator translator =
        ArrowTranslator.create(cluster.getRexBuilder(), input.getRowType());
    this.match = translator.translateMatch(condition);

    assert getConvention() == ArrowRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    final RelOptCost cost = super.computeSelfCost(planner, mq);
    return requireNonNull(cost, "cost").multiplyBy(0.1);
  }

  @Override public ArrowFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new ArrowFilter(getCluster(), traitSet, input, condition);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitInput(0, getInput());
    implementor.addFilters(match);
  }
}
