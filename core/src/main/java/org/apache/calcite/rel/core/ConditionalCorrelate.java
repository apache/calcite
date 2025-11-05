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
package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * This is a extension of {@link Correlate} that contains a condition.
 * When removing SOME/IN subqueries, the condition need to be retained in the left mark type
 * Correlate (it cannot be pulled up or pushed down). This is why ConditionalCorrelate extends
 * the condition.
 *
 * @see CoreRules#FILTER_SUB_QUERY_TO_MARK_CORRELATE
 * @see CoreRules#PROJECT_SUB_QUERY_TO_MARK_CORRELATE
 */
public abstract class ConditionalCorrelate extends Correlate {

  private final RexNode condition;

  protected ConditionalCorrelate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns,
      JoinRelType joinType,
      RexNode condition) {
    super(cluster, traitSet, hints, left, right, correlationId, requiredColumns, joinType);
    this.condition = condition;
    assert joinType == JoinRelType.LEFT_MARK;
  }

  @Override public ConditionalCorrelate copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 2;
    return copy(traitSet, inputs.get(0), inputs.get(1), correlationId,
        requiredColumns, joinType, condition);
  }

  public abstract ConditionalCorrelate copy(RelTraitSet traitSet, RelNode left, RelNode right,
      CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType,
      RexNode condition);

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("condition", condition, !condition.isAlwaysTrue());
  }

  @Override public RexNode getCondition() {
    return condition;
  }
}
