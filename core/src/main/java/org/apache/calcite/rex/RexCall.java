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
    if (type == null) {
      throw new NullPointerException("type is null for " + op + "(" + operands + ")");
    }
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
    case IS_NULL:
    case IS_UNKNOWN:
      return operands.get(0).isAlwaysNull();
    case IS_NOT_NULL:
      // IS_NOT_UNKNOWN does not exist yet
      return !operands.get(0).getType().isNullable();
    case IS_TRUE:
    case IS_NOT_FALSE:
    case MAX:
    case MIN:
      return operands.get(0).isAlwaysTrue();
    case NOT:
      return operands.get(0).isAlwaysFalse();
    case IS_NOT_TRUE:
    case IS_FALSE:
      return operands.get(0).isAlwaysFalse();
    case CAST:
      return operands.get(0).isAlwaysTrue();
    case AND:
      for (RexNode operand : operands) {
        if (!operand.isAlwaysTrue()) {
          return false;
        }
      }
      return true; // all operands are always true => AND is always true
    case OR:
      for (RexNode operand : operands) {
        // OR(true, null) => true
        if (operand.isAlwaysTrue()) {
          return true;
        }
      }
      return false;
    case EQUALS:
    case NOT_EQUALS:
    {
      RexNode o0 = operands.get(0);
      RexNode o1 = operands.get(1);
      if (!getType().isNullable() && o0.equals(o1)) {
        return getKind() == SqlKind.EQUALS;
      }
      int k0 = o0.isAlwaysTrue() ? 1 : (o0.isAlwaysFalse() ? 2 : 0);
      if (k0 != 0) {
        int k1 = o1.isAlwaysTrue() ? 1 : (o1.isAlwaysFalse() ? 2 : 0);
        return k1 != 0 && k0 == k1 ^ (getKind() != SqlKind.EQUALS);
      }
      return false;
    }
    default:
      return false;
    }
  }

  @Override public boolean isAlwaysFalse() {
    switch (getKind()) {
    case IS_NOT_NULL:
      // IS_NOT_UNKNOWN does not exist yet
      return operands.get(0).isAlwaysNull();
    case IS_NULL:
    case IS_UNKNOWN:
      return !operands.get(0).getType().isNullable();
    case IS_FALSE:
    case IS_NOT_TRUE:
    case NOT:
      return operands.get(0).isAlwaysTrue();
    case IS_NOT_FALSE:
    case IS_TRUE:
    case CAST:
    case MAX:
    case MIN:
      return operands.get(0).isAlwaysFalse();
    case OR:
      for (RexNode operand : operands) {
        if (!operand.isAlwaysFalse()) {
          return false;
        }
      }
      return true; // all operands are always FALSE => OR is always false
    case AND:
      for (RexNode operand : operands) {
        // AND(false, null) => false
        if (operand.isAlwaysFalse()) {
          return true;
        }
      }
      return false;
    case EQUALS:
    case NOT_EQUALS:
    {

      RexNode o0 = operands.get(0);
      RexNode o1 = operands.get(1);
      if (!getType().isNullable() && o0.equals(o1)) {
        return getKind() == SqlKind.NOT_EQUALS;
      }
      int k0 = o0.isAlwaysTrue() ? 1 : (o0.isAlwaysFalse() ? 2 : 0);
      if (k0 != 0) {
        int k1 = o1.isAlwaysTrue() ? 1 : (o1.isAlwaysFalse() ? 2 : 0);
        return k1 != 0 && k0 != k1 ^ (getKind() != SqlKind.EQUALS);
      }
      return false;
    }
    default:
      return false;
    }
  }

  @Override public boolean isAlwaysNull() {
    if (!getType().isNullable()) {
      return false;
    }
    boolean hasNull = false;
    switch (getKind()) {
    case NOT:
    case CAST:
      return operands.get(0).isAlwaysNull();
    case EQUALS:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case PLUS:
    case MINUS:
    case TIMES:
    case DIVIDE:
    case MAX:
    case MIN:
    case GREATEST:
    case LEAST:
      for (RexNode operand : operands) {
        // AND(false, null) => false, so the value is NOT always null
        if (operand.isAlwaysNull()) {
          return true;
        }
      }
      return false;
    case COALESCE:
      for (RexNode operand : operands) {
        if (!operand.isAlwaysNull()) {
          return false;
        }
      }
      return true; // coalesce(null, null) => null
    case AND:
      // AND(false, null) => false, so the value is NOT always null
      // So simplify AND(true, true, null) to NULL
      // AND( ..., null) can be non-null
      for (RexNode operand : operands) {
        if (!operand.isAlwaysTrue()) {
          return false;
        }
        if (!hasNull) {
          hasNull = operand.isAlwaysNull();
        }
      }
      return hasNull;
    case OR:
      // OR(true, null) => true
      for (RexNode operand : operands) {
        if (!operand.isAlwaysFalse()) {
          return false;
        }
        if (!hasNull) {
          hasNull = operand.isAlwaysNull();
        }
      }
      return hasNull;
    }
    return false;
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
