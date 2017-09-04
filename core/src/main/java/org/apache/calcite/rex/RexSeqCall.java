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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

/**
 * Scalar expression that represents a value retrieval from a sequence.
 */
public class RexSeqCall extends RexCall {
  public final RelNode rel;

  public RexSeqCall(RelDataType type, SqlOperator op,
                     List<RexNode> operands, RelNode rel) {
    super(type, op, operands);
    this.rel = rel;
    this.digest = computeDigest(false);
  }

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitSeqCall(this);
  }

  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitSeqCall(this, arg);
  }

  @Override protected String computeDigest(boolean withType) {
    StringBuilder sb = new StringBuilder(op.getName());
    sb.append("(");
    sb.append(RelOptUtil.toString(rel));
    sb.append(")");
    return sb.toString();
  }

  @Override public RexSeqCall clone(RelDataType type, List<RexNode> operands) {
    return new RexSeqCall(type, getOperator(),
        ImmutableList.copyOf(operands), rel);
  }

  public RexSeqCall clone(RelNode rel) {
    return new RexSeqCall(type, getOperator(), operands, rel);
  }
}

// End RexSubQuery.java
