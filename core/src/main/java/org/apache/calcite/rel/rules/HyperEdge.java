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
package org.apache.calcite.rel.rules;

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

/**
 * Edge in HyperGraph, that represents a join predicate.
 */
@Experimental
public class HyperEdge {

  private final long leftNodeBits;

  private final long rightNodeBits;

  private final JoinRelType joinType;

  private final boolean isSimple;

  private final RexNode condition;

  public HyperEdge(long leftNodeBits, long rightNodeBits, JoinRelType joinType, RexNode condition) {
    this.leftNodeBits = leftNodeBits;
    this.rightNodeBits = rightNodeBits;
    this.joinType = joinType;
    this.condition = condition;
    boolean leftSimple = (leftNodeBits & (leftNodeBits - 1)) == 0;
    boolean rightSimple = (rightNodeBits & (rightNodeBits - 1)) == 0;
    this.isSimple = leftSimple && rightSimple;
  }

  public long getNodeBitmap() {
    return leftNodeBits | rightNodeBits;
  }

  public long getLeftNodeBitmap() {
    return leftNodeBits;
  }

  public long getRightNodeBitmap() {
    return rightNodeBits;
  }

  // hyperedge (u, v) is simple if |u| = |v| = 1
  public boolean isSimple() {
    return isSimple;
  }

  public JoinRelType getJoinType() {
    return joinType;
  }

  public RexNode getCondition() {
    return condition;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(LongBitmap.printBitmap(leftNodeBits))
        .append("——[").append(joinType).append(", ").append(condition).append("]——")
        .append(LongBitmap.printBitmap(rightNodeBits));
    return sb.toString();
  }

}
