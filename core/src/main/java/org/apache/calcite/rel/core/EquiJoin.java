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
 *
 * <p>For most of the cases, {@link JoinInfo#isEqui()} can already decide
 * if the join condition is based on column equality.
 *
 * <p>{@code EquiJoin} is an abstract class for inheritance of Calcite enumerable
 * joins and join implementation of other system. You should inherit the {@code EquiJoin}
 * if your join implementation does not support non-equi join conditions. Calcite would
 * eliminate some optimize logic for {@code EquiJoin} in some planning rules.
 * e.g. {@link org.apache.calcite.rel.rules.FilterJoinRule} would not push non-equi
 * join conditions of the above filter into the join underneath if it is an {@code EquiJoin}.
 *
 * @deprecated This class is no longer needed; if you are writing a sub-class of
 * Join that only accepts equi conditions, it is sufficient that it extends
 * {@link Join}. It will be evident that it is an equi-join when its
 * {@link JoinInfo#nonEquiConditions} is an empty list.
 */
@Deprecated // to be removed before 2.0
public abstract class EquiJoin extends Join {
  public final ImmutableIntList leftKeys;
  public final ImmutableIntList rightKeys;

  /** Creates an EquiJoin. */
  public EquiJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left,
      RelNode right, RexNode condition, Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traits, left, right, condition, variablesSet, joinType);
    this.leftKeys = Objects.requireNonNull(joinInfo.leftKeys);
    this.rightKeys = Objects.requireNonNull(joinInfo.rightKeys);
    assert joinInfo.isEqui() : "Create EquiJoin with non-equi join condition.";
  }

  /** Creates an EquiJoin. */
  @Deprecated // to be removed before 2.0
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
}

// End EquiJoin.java
