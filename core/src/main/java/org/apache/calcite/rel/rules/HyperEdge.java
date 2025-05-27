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

import java.util.HashMap;
import java.util.Map;

/**
 * Edge in HyperGraph, that represents a join predicate.
 */
@Experimental
public class HyperEdge {

  // equivalent to the l-tes in CD-C paper
  private final long leftEndpoint;

  // equivalent to the r-tes in CD-C paper
  private final long rightEndponit;

  // equivalent to the T(left(o)) ∩ F_T(o) in CD-C paper
  private final long leftNodeUsedInPredicate;

  // equivalent to the T(right(o)) ∩ F_T(o) in CD-C paper
  private final long rightNodeUsedInPredicate;

  private final Map<Long, Long> conflictRules;

  // equivalent to the T(left(o)) in CD-C paper
  private final long initialLeftNodeBits;

  // equivalent to the T(right(o)) in CD-C paper
  private final long initialRightNodeBits;

  private final JoinRelType joinType;

  private final boolean isSimple;

  // converted from join condition, using RexNodeAndFieldIndex instead of RexInputRef
  private final RexNode condition;

  public HyperEdge(
      long leftEndpoint,
      long rightEndponit,
      long leftNodeUsedInPredicate,
      long rightNodeUsedInPredicate,
      Map<Long, Long> conflictRules,
      long initialLeftNodeBits,
      long initialRightNodeBits,
      JoinRelType joinType,
      RexNode condition) {
    this.leftEndpoint = leftEndpoint;
    this.rightEndponit = rightEndponit;
    this.leftNodeUsedInPredicate = leftNodeUsedInPredicate;
    this.rightNodeUsedInPredicate = rightNodeUsedInPredicate;
    this.conflictRules = new HashMap<>(conflictRules);
    this.initialLeftNodeBits = initialLeftNodeBits;
    this.initialRightNodeBits = initialRightNodeBits;
    this.joinType = joinType;
    this.condition = condition;
    boolean leftSimple = (leftEndpoint & (leftEndpoint - 1)) == 0;
    boolean rightSimple = (rightEndponit & (rightEndponit - 1)) == 0;
    this.isSimple = leftSimple && rightSimple;
  }

  public long getEndpoint() {
    return leftEndpoint | rightEndponit;
  }

  public long getLeftEndpoint() {
    return leftEndpoint;
  }

  public long getRightEndpoint() {
    return rightEndponit;
  }

  public long getLeftNodeUsedInPredicate() {
    return leftNodeUsedInPredicate;
  }

  public long getRightNodeUsedInPredicate() {
    return rightNodeUsedInPredicate;
  }

  public Map<Long, Long> getConflictRules() {
    return conflictRules;
  }

  public long getInitialLeftNodeBits() {
    return initialLeftNodeBits;
  }

  public long getInitialRightNodeBits() {
    return initialRightNodeBits;
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
    sb.append(LongBitmap.printBitmap(leftEndpoint))
        .append("——[").append(joinType).append(", ").append(condition).append("]——")
        .append(LongBitmap.printBitmap(rightEndponit));
    return sb.toString();
  }

}
