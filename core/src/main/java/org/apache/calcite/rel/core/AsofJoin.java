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
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Base class for various ASOF JOIN representations.
 */
public abstract class AsofJoin extends Join {
  /** Compared to standard joins, ASOF joins have an additional condition for comparing
   * columns that usually contain timestamp values (however, the data type of these columns
   * can be any type that supports comparisons, and is not restricted to be TIMESTAMP).
   */
  protected final RexNode matchCondition;

  protected AsofJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      RexNode condition,
      RexNode matchCondition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    this.matchCondition = requireNonNull(matchCondition, "matchCondition");
  }

  public RexNode getMatchCondition() {
    return matchCondition;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("matchCondition", matchCondition);
  }
}
