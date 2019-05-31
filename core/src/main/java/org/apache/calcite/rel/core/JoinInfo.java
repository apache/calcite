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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.IntPair;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** An analyzed join condition.
 *
 * <p>It is useful for the many algorithms that care whether a join has an
 * equi-join condition.
 *
 * <p>You can create one using {@link #of}, or call
 * {@link Join#analyzeCondition()}; many kinds of join cache their
 * join info, especially those that are equi-joins.</p>
 *
 * @see Join#analyzeCondition() */
public class JoinInfo {
  public final ImmutableIntList leftKeys;
  public final ImmutableIntList rightKeys;
  protected final RexNode nonEquiCondition;

  /** Creates a JoinInfo. */
  protected JoinInfo(ImmutableIntList leftKeys, ImmutableIntList rightKeys,
      RexNode nonEquiCondition) {
    this.leftKeys = Objects.requireNonNull(leftKeys);
    this.rightKeys = Objects.requireNonNull(rightKeys);
    this.nonEquiCondition = nonEquiCondition;
    assert leftKeys.size() == rightKeys.size();
  }

  /** Creates a {@code JoinInfo} by analyzing a condition. */
  public static JoinInfo of(RelNode left, RelNode right, RexNode condition) {
    final List<Integer> leftKeys = new ArrayList<>();
    final List<Integer> rightKeys = new ArrayList<>();
    final List<Boolean> filterNulls = new ArrayList<>();
    RexNode remaining =
        RelOptUtil.splitJoinCondition(left, right, condition, leftKeys,
            rightKeys, filterNulls);
    return new JoinInfo(ImmutableIntList.copyOf(leftKeys),
        ImmutableIntList.copyOf(rightKeys), remaining);
  }

  /** Creates an equi-join. */
  public static JoinInfo of(ImmutableIntList leftKeys,
      ImmutableIntList rightKeys) {
    return new JoinInfo(leftKeys, rightKeys, null);
  }

  /** Returns whether this is an equi-join. */
  public boolean isEqui() {
    return nonEquiCondition == null || nonEquiCondition.isAlwaysTrue();
  }

  /** Returns a list of (left, right) key ordinals. */
  public List<IntPair> pairs() {
    return IntPair.zip(leftKeys, rightKeys);
  }

  public ImmutableBitSet leftSet() {
    return ImmutableBitSet.of(leftKeys);
  }

  public ImmutableBitSet rightSet() {
    return ImmutableBitSet.of(rightKeys);
  }

  public RexNode getRemaining(RexBuilder rexBuilder) {
    if (nonEquiCondition == null) {
      return rexBuilder.makeLiteral(true);
    }
    return nonEquiCondition;
  }

  public RexNode getEquiCondition(RelNode left, RelNode right,
      RexBuilder rexBuilder) {
    return RelOptUtil.createEquiJoinCondition(left, leftKeys, right, rightKeys,
        rexBuilder);
  }

  public List<ImmutableIntList> keys() {
    return FlatLists.of(leftKeys, rightKeys);
  }


}

// End JoinInfo.java
