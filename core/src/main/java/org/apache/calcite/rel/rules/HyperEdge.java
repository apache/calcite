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

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

/**
 * Edge in HyperGraph, that represents a join predicate.
 */
public class HyperEdge {

  private long leftNodeBits;

  private long rightNodeBits;

  private JoinRelType joinType;

  private RexNode condition;

  public HyperEdge(long leftNodeBits, long rightNodeBits, JoinRelType joinType, RexNode condition) {
    this.leftNodeBits = leftNodeBits;
    this.rightNodeBits = rightNodeBits;
    this.joinType = joinType;
    this.condition = condition;
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
    boolean leftSimple = (leftNodeBits & (leftNodeBits - 1)) == 0;
    boolean rightSimple = (rightNodeBits & (rightNodeBits - 1)) == 0;
    return leftSimple && rightSimple;
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
        .append("——").append(joinType).append("——")
        .append(LongBitmap.printBitmap(rightNodeBits));
    return sb.toString();
  }

  // before starting dphyp, replace RexInputRef to RexInputFieldName
  public void replaceCondition(RexNode fieldNameCond) {
    this.condition = fieldNameCond;
  }

}
