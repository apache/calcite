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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * An expression formed by a call to an operator with zero or more expressions
 * as operands.
 *
 * <p>Operators may be binary, unary, functions, special syntactic constructs
 * like <code>CASE ... WHEN ... END</code>, or even internally generated
 * constructs like implicit type conversions. The syntax of the operator is
 * really irrelevant, because row-expressions (unlike
 * {@link org.apache.calcite.sql.SqlNode SQL expressions})
 * do not directly represent a piece of source code.
 *
 * <p>It's not often necessary to sub-class this class. The smarts should be in
 * the operator, rather than the call. Any extra information about the call can
 * often be encoded as extra arguments. (These don't need to be hidden, because
 * no one is going to be generating source code from this tree.)</p>
 */
public class RexCall extends RexNode {
  //~ Instance fields --------------------------------------------------------

  public final SqlOperator op;
  public final ImmutableList<RexNode> operands;
  public final RelDataType type;

  //~ Constructors -----------------------------------------------------------

  protected RexCall(
      RelDataType type,
      SqlOperator op,
      List<? extends RexNode> operands) {
    this.type = Objects.requireNonNull(type);
    this.op = Objects.requireNonNull(op);
    this.operands = ImmutableList.copyOf(operands);
    assert op.getKind() != null : op;
    assert op.validRexOperands(operands.size(), Litmus.THROW) : this;
  }

  //~ Methods ----------------------------------------------------------------

  protected String computeDigest(boolean withType) {
    StringBuilder sb = new StringBuilder(op.getName());
    if ((operands.size() == 0)
        && (op.getSyntax() == SqlSyntax.FUNCTION_ID)) {
      // Don't print params for empty arg list. For example, we want
      // "SYSTEM_USER", not "SYSTEM_USER()".
    } else {
      sb.append("(");
      for (int i = 0; i < operands.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        RexNode operand = operands.get(i);
        sb.append(operand.toString());
      }
      sb.append(")");
    }
    if (withType) {
      sb.append(":");

      // NOTE jvs 16-Jan-2005:  for digests, it is very important
      // to use the full type string.
      sb.append(type.getFullTypeString());
    }
    return sb.toString();
  }

  public String toString() {
    // This data race is intentional
    String localDigest = digest;
    if (localDigest == null) {
      localDigest = computeDigest(
          isA(SqlKind.CAST) || isA(SqlKind.NEW_SPECIFICATION));
      digest = localDigest;
    }
    return localDigest;
  }

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitCall(this);
  }

  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitCall(this, arg);
  }

  public RelDataType getType() {
    return type;
  }

  @Override public boolean isAlwaysTrue() {
    // "c IS NOT NULL" occurs when we expand EXISTS.
    // This reduction allows us to convert it to a semi-join.
    switch (getKind()) {
    case IS_NOT_NULL:
      return !operands.get(0).getType().isNullable();
    case IS_NOT_TRUE:
    case IS_FALSE:
    case NOT:
      return operands.get(0).isAlwaysFalse();
    case IS_NOT_FALSE:
    case IS_TRUE:
    case CAST:
      return operands.get(0).isAlwaysTrue();
    default:
      return false;
    }
  }

  @Override public boolean isAlwaysFalse() {
    switch (getKind()) {
    case IS_NULL:
      return !operands.get(0).getType().isNullable();
    case IS_NOT_TRUE:
    case IS_FALSE:
    case NOT:
      return operands.get(0).isAlwaysTrue();
    case IS_NOT_FALSE:
    case IS_TRUE:
    case CAST:
      return operands.get(0).isAlwaysFalse();
    default:
      return false;
    }
  }

  public SqlKind getKind() {
    return op.kind;
  }

  public List<RexNode> getOperands() {
    return operands;
  }

  public SqlOperator getOperator() {
    return op;
  }

  /**
   * Creates a new call to the same operator with different operands.
   *
   * @param type     Return type
   * @param operands Operands to call
   * @return New call
   */
  public RexCall clone(RelDataType type, List<RexNode> operands) {
    return new RexCall(type, op, operands);
  }
}

// End RexCall.java
