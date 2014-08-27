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
package org.eigenbase.rel;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.ImmutableIntList;
import org.eigenbase.util.mapping.IntPair;

import net.hydromatic.optiq.util.BitSets;

import com.google.common.base.Preconditions;

/** An analyzed join condition.
 *
 * <p>It is useful for the many algorithms that care whether a join is an
 * equi-join.
 *
 * <p>You can create one using {@link #of}, or call {@link JoinRelBase#analyzeCondition()};
 * many kinds of join cache their join info, especially those that are
 * equi-joins and sub-class {@link org.eigenbase.rel.rules.EquiJoinRel}.</p>
 *
 * @see JoinRelBase#analyzeCondition() */
public abstract class JoinInfo {
  public final ImmutableIntList leftKeys;
  public final ImmutableIntList rightKeys;

  /** Creates a JoinInfo. */
  protected JoinInfo(ImmutableIntList leftKeys, ImmutableIntList rightKeys) {
    this.leftKeys = Preconditions.checkNotNull(leftKeys);
    this.rightKeys = Preconditions.checkNotNull(rightKeys);
    assert leftKeys.size() == rightKeys.size();
  }

  /** Creates a {@code JoinInfo} by analyzing a condition. */
  public static JoinInfo of(RelNode left, RelNode right, RexNode condition) {
    final List<Integer> leftKeys = new ArrayList<Integer>();
    final List<Integer> rightKeys = new ArrayList<Integer>();
    RexNode remaining =
        RelOptUtil.splitJoinCondition(left, right, condition, leftKeys,
            rightKeys);
    if (remaining.isAlwaysTrue()) {
      return new EquiJoinInfo(ImmutableIntList.copyOf(leftKeys),
          ImmutableIntList.copyOf(rightKeys));
    } else {
      return new NonEquiJoinInfo(ImmutableIntList.copyOf(leftKeys),
          ImmutableIntList.copyOf(rightKeys), remaining);
    }
  }

  /** Creates an equi-join. */
  public static JoinInfo of(ImmutableIntList leftKeys,
      ImmutableIntList rightKeys) {
    return new EquiJoinInfo(leftKeys, rightKeys);
  }

  /** Returns whether this is an equi-join. */
  public abstract boolean isEqui();

  /** Returns a list of (left, right) key ordinals. */
  public List<IntPair> pairs() {
    return IntPair.zip(leftKeys, rightKeys);
  }

  public BitSet leftSet() {
    return BitSets.of(leftKeys);
  }

  public BitSet rightSet() {
    return BitSets.of(rightKeys);
  }

  public abstract RexNode getRemaining(RexBuilder rexBuilder);

  public RexNode getEquiCondition(RelNode left, RelNode right,
      RexBuilder rexBuilder) {
    return RelOptUtil.createEquiJoinCondition(left, leftKeys, right, rightKeys,
        rexBuilder);
  }

  /** JoinInfo that represents an equi-join. */
  private static class EquiJoinInfo extends JoinInfo {
    protected EquiJoinInfo(ImmutableIntList leftKeys,
        ImmutableIntList rightKeys) {
      super(leftKeys, rightKeys);
    }

    @Override public boolean isEqui() {
      return true;
    }

    @Override public RexNode getRemaining(RexBuilder rexBuilder) {
      return rexBuilder.makeLiteral(true);
    }
  }

  /** JoinInfo that represents a non equi-join. */
  private static class NonEquiJoinInfo extends JoinInfo {
    public final RexNode remaining;

    protected NonEquiJoinInfo(ImmutableIntList leftKeys,
        ImmutableIntList rightKeys, RexNode remaining) {
      super(leftKeys, rightKeys);
      this.remaining = Preconditions.checkNotNull(remaining);
      assert !remaining.isAlwaysTrue();
    }

    @Override public boolean isEqui() {
      return false;
    }

    @Override public RexNode getRemaining(RexBuilder rexBuilder) {
      return remaining;
    }
  }
}

// End JoinInfo.java
