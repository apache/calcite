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
package org.apache.calcite.rel.mutable;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

import java.util.Objects;

/** Mutable equivalent of {@link org.apache.calcite.rel.core.SemiJoin}. */
public class MutableSemiJoin extends MutableBiRel {
  public final RexNode condition;
  public final ImmutableIntList leftKeys;
  public final ImmutableIntList rightKeys;

  private MutableSemiJoin(
      RelDataType rowType,
      MutableRel left,
      MutableRel right,
      RexNode condition,
      ImmutableIntList leftKeys,
      ImmutableIntList rightKeys) {
    super(MutableRelType.SEMIJOIN, left.cluster, rowType, left, right);
    this.condition = condition;
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
  }

  /**
   * Creates a MutableSemiJoin.
   *
   * @param rowType   Row type
   * @param left      Left input relational expression
   * @param right     Right input relational expression
   * @param condition Join condition
   * @param leftKeys  Left join keys
   * @param rightKeys Right join keys
   */
  public static MutableSemiJoin of(RelDataType rowType, MutableRel left,
      MutableRel right, RexNode condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys) {
    return new MutableSemiJoin(rowType, left, right, condition, leftKeys,
        rightKeys);
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof MutableSemiJoin
        && condition.equals(((MutableSemiJoin) obj).condition)
        && leftKeys.equals(((MutableSemiJoin) obj).leftKeys)
        && rightKeys.equals(((MutableSemiJoin) obj).rightKeys)
        && left.equals(((MutableSemiJoin) obj).left)
        && right.equals(((MutableSemiJoin) obj).right);
  }

  @Override public int hashCode() {
    return Objects.hash(left, right, condition, leftKeys, rightKeys);
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    return buf.append("SemiJoin(condition: ").append(condition)
        .append(", leftKeys: ").append(leftKeys)
        .append(", rightKeys: ").append(rightKeys)
        .append(")");
  }

  @Override public MutableRel clone() {
    return MutableSemiJoin.of(rowType, left.clone(),
        right.clone(), condition, leftKeys, rightKeys);
  }
}

// End MutableSemiJoin.java
