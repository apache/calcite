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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

import java.util.Objects;
import java.util.Set;

/**
 * Base class for any join whose condition is based on column equality.
 */
public abstract class EquiJoin extends Join {
  public final ImmutableIntList leftKeys;
  public final ImmutableIntList rightKeys;

  /** Creates an EquiJoin. */
  public EquiJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left,
      RelNode right, RexNode condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys, Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traits, left, right, condition, variablesSet, joinType);
    this.leftKeys = Objects.requireNonNull(leftKeys);
    this.rightKeys = Objects.requireNonNull(rightKeys);
  }

  @Deprecated // to be removed before 2.0
  public EquiJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left,
      RelNode right, RexNode condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys, JoinRelType joinType,
      Set<String> variablesStopped) {
    this(cluster, traits, left, right, condition, leftKeys, rightKeys,
        CorrelationId.setOf(variablesStopped), joinType);
  }

  public ImmutableIntList getLeftKeys() {
    return leftKeys;
  }

  public ImmutableIntList getRightKeys() {
    return rightKeys;
  }

  @Override public JoinInfo analyzeCondition() {
    return JoinInfo.of(leftKeys, rightKeys);
  }
}

// End EquiJoin.java
