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
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * RexNodeAndFieldIndex has the same meaning as {@link RexInputRef}, they are both reference a
 * field of an input relational expression. The difference is that, RexNodeAndFieldIndex uses the
 * input index and the relative position of field in input rowType.
 *
 * <p> For example, if the inputs to a join are
 *
 * <ul>
 * <li>Input #0: EMP(EMPNO, ENAME, DEPTNO) and</li>
 * <li>Input #1: DEPT(DEPTNO AS DEPTNO2, DNAME)</li>
 * </ul>
 *
 * <p>then the fields are:
 *
 * <ul>
 * <li>Node #0, Field #0: EMPNO</li>
 * <li>Node #0, Field #1: ENAME</li>
 * <li>Node #0, Field #2: DEPTNO (from EMP)</li>
 * <li>Node #1, Field #0: DEPTNO2 (from DEPT)</li>
 * <li>Node #1, Field #1: DNAME</li>
 * </ul>
 *
 * <p> If in some cases, the inputs order of a relation is frequently adjusted and it is difficult
 * to maintain the correct RexInputRef, you can consider temporarily replacing RexInputRef with
 * RexNodeAndFieldIndex.
 *
 * @see org.apache.calcite.rel.rules.JoinToHyperGraphRule
 * @see org.apache.calcite.rel.rules.HyperGraph#extractJoinCond
 * @see org.apache.calcite.rel.rules.HyperEdge#createHyperEdgesFromJoinConds
 */
public class RexNodeAndFieldIndex extends RexVariable {

  // the index of the node in relation inputs
  private final int nodeIndex;

  // the index of the field in the rowType of the node
  private final int fieldIndex;

  public RexNodeAndFieldIndex(int nodeIndex, int fieldIndex, String name, RelDataType type) {
    super(name, type);
    this.nodeIndex = nodeIndex;
    this.fieldIndex = fieldIndex;
  }

  public int getNodeIndex() {
    return nodeIndex;
  }

  public int getFieldIndex() {
    return fieldIndex;
  }

  @Override public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitNodeAndFieldIndex(this);
  }

  @Override public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitNodeAndFieldIndex(this, arg);
  }

  @Override public boolean equals(@Nullable Object obj) {
    return this == obj
        || obj instanceof RexNodeAndFieldIndex
        && nodeIndex == ((RexNodeAndFieldIndex) obj).nodeIndex
        && fieldIndex == ((RexNodeAndFieldIndex) obj).fieldIndex;
  }

  @Override public int hashCode() {
    return Objects.hash(nodeIndex, fieldIndex);
  }

  @Override public String toString() {
    return "node(" + nodeIndex + ")_field(" + fieldIndex + ")";
  }
}
