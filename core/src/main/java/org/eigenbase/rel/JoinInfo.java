/*
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.rel;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.eigenbase.relopt.RelOptUtil;
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
public class JoinInfo {
  public final ImmutableIntList leftKeys;
  public final ImmutableIntList rightKeys;
  public final RexNode remaining;

  /** Creates a JoinInfo. */
  public JoinInfo(ImmutableIntList leftKeys, ImmutableIntList rightKeys,
      RexNode remaining) {
    this.leftKeys = Preconditions.checkNotNull(leftKeys);
    this.rightKeys = Preconditions.checkNotNull(rightKeys);
    this.remaining = Preconditions.checkNotNull(remaining);
    assert leftKeys.size() == rightKeys.size();
  }

  /** Creates a {@code JoinInfo} by analyzing a condition. */
  public static JoinInfo of(RelNode left, RelNode right, RexNode condition) {
    final List<Integer> leftKeys = new ArrayList<Integer>();
    final List<Integer> rightKeys = new ArrayList<Integer>();
    RexNode remaining =
        RelOptUtil.splitJoinCondition(left, right, condition, leftKeys,
            rightKeys);
    return new JoinInfo(ImmutableIntList.copyOf(leftKeys),
        ImmutableIntList.copyOf(rightKeys), remaining);
  }

  /** Returns whether this is an equi-join. */
  public boolean isEqui() {
    return remaining.isAlwaysTrue();
  }

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
}

// End JoinInfo.java
