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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

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
  public final int nodeCount;
  /**
   * Cache of hash code.
   */
  protected int hash = 0;


  //~ Constructors -----------------------------------------------------------

  protected RexCall(
      RelDataType type,
      SqlOperator op,
      List<? extends RexNode> operands) {
    this.type = Objects.requireNonNull(type, "type");
    Objects.requireNonNull(op, "operator");
    final Pair<SqlOperator, ImmutableList<RexNode>> normalized =
        normalize(op, operands);
    this.op = normalized.left;
    this.operands = normalized.right;
    this.nodeCount = RexUtil.nodeCount(1, this.operands);
    assert op.getKind() != null : op;
    assert op.validRexOperands(operands.size(), Litmus.THROW) : this;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Appends call operands without parenthesis.
   * {@link RexLiteral} might omit data type depending on the context.
   * For instance, {@code null:BOOLEAN} vs {@code =(true, null)}.
   * The idea here is to omit "obvious" types for readability purposes while
   * still maintain {@link RelNode#getDigest()} contract.
   *
   * @see RexLiteral#computeDigest(RexDigestIncludeType)
   * @param sb destination
   */
  protected final void appendOperands(StringBuilder sb) {
    if (operands.isEmpty()) {
      return;
    }
    List<String> operandDigests = new ArrayList<>(operands.size());
    for (int i = 0; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      if (!(operand instanceof RexLiteral)) {
        operandDigests.add(operand.toString());
        continue;
      }
      // Type information might be omitted in certain cases to improve readability
      // For instance, AND/OR arguments should be BOOLEAN, so
      // AND(true, null) is better than AND(true, null:BOOLEAN), and we keep the same info.

      // +($0, 2) is better than +($0, 2:BIGINT). Note: if $0 is BIGINT, then 2 is expected to be
      // of BIGINT type as well.
      RexDigestIncludeType includeType = RexDigestIncludeType.OPTIONAL;
      if ((isA(SqlKind.AND) || isA(SqlKind.OR))
          && operand.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
        includeType = RexDigestIncludeType.NO_TYPE;
      }
      if (SqlKind.SIMPLE_BINARY_OPS.contains(getKind())) {
        RexNode otherArg = operands.get(1 - i);
        if ((!(otherArg instanceof RexLiteral)
            || ((RexLiteral) otherArg).digestIncludesType() == RexDigestIncludeType.NO_TYPE)
            && equalSansNullability(operand.getType(), otherArg.getType())) {
          includeType = RexDigestIncludeType.NO_TYPE;
        }
      }
      operandDigests.add(((RexLiteral) operand).computeDigest(includeType));
    }
    int totalLength = (operandDigests.size() - 1) * 2; // commas
    for (String s : operandDigests) {
      totalLength += s.length();
    }
    sb.ensureCapacity(sb.length() + totalLength);
    for (int i = 0; i < operandDigests.size(); i++) {
      String op = operandDigests.get(i);
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(op);
    }
  }

  private Pair<SqlOperator, ImmutableList<RexNode>> normalize(
      SqlOperator operator,
      List<? extends RexNode> operands) {
    final ImmutableList<RexNode> oldOperands = ImmutableList.copyOf(operands);
    final Pair<SqlOperator, ImmutableList<RexNode>> original = Pair.of(operator, oldOperands);
    if (!needNormalize()) {
      return original;
    }
    if (operands.size() != 2
        || !operands.stream().allMatch(operand -> operand.getClass() == RexInputRef.class)) {
      return original;
    }
    final RexInputRef operand0 = (RexInputRef) operands.get(0);
    final RexInputRef operand1 = (RexInputRef) operands.get(1);
    final SqlKind kind = operator.getKind();
    if (operand0.getIndex() < operand1.getIndex()) {
      return original;
    }
    // If arguments are the same, then we normalize < vs >
    // '<' == 60, '>' == 62, so we prefer <.
    if (operand0.getIndex() == operand1.getIndex()) {
      if (kind.reverse().compareTo(kind) < 0) {
        return Pair.of(SqlStdOperatorTable.reverse(operator), oldOperands);
      } else {
        return original;
      }
    }

    if (SqlKind.SYMMETRICAL_SAME_ARG_TYPE.contains(kind)) {
      final RelDataType firstType = operands.get(0).getType();
      for (int i = 1; i < operands.size(); i++) {
        if (!equalSansNullability(firstType, operands.get(i).getType())) {
          // Arguments have different type, thus they must not be sorted
          return original;
        }
      }
      // fall through: order arguments below
    } else if (!SqlKind.SYMMETRICAL.contains(kind)
        && kind == kind.reverse()) {
      // The operations have to be either symmetrical or reversible
      // Nothing matched => we skip argument sorting
      return original;
    }
    // $0=$1 is the same as $1=$0, so we make sure the digest is the same for them.

    // When $1 > $0 is normalized, the operation needs to be flipped
    // So we sort arguments first, then flip the sign.
    final SqlOperator newOp = SqlStdOperatorTable.reverse(operator);
    final ImmutableList<RexNode> newOperands = ImmutableList.of(operand1, operand0);
    return Pair.of(newOp, newOperands);
  }

  /**
   * This is a poorman's
   * {@link org.apache.calcite.sql.type.SqlTypeUtil#equalSansNullability(RelDataTypeFactory, RelDataType, RelDataType)}
   * <p>{@code SqlTypeUtil} requires {@link RelDataTypeFactory} which we haven't, so we assume that
   * "not null" is represented in the type's digest as a trailing "NOT NULL" (case sensitive)
   * @param a first type
   * @param b second type
   * @return true if the types are equal or the only difference is nullability
   */
  private static boolean equalSansNullability(RelDataType a, RelDataType b) {
    String x = a.getFullTypeString();
    String y = b.getFullTypeString();
    if (x.length() < y.length()) {
      String c = x;
      x = y;
      y = c;
    }

    return (x.length() == y.length()
        || x.length() == y.length() + 9 && x.endsWith(" NOT NULL"))
        && x.startsWith(y);
  }

  protected @Nonnull String computeDigest(boolean withType) {
    final StringBuilder sb = new StringBuilder(op.getName());
    if ((operands.size() == 0)
        && (op.getSyntax() == SqlSyntax.FUNCTION_ID)) {
      // Don't print params for empty arg list. For example, we want
      // "SYSTEM_USER", not "SYSTEM_USER()".
    } else {
      sb.append("(");
      appendOperands(sb);
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

  @Override public final @Nonnull String toString() {
    return computeDigest(digestWithType());
  }

  private boolean digestWithType() {
    return isA(SqlKind.CAST) || isA(SqlKind.NEW_SPECIFICATION);
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

  @Override public int nodeCount() {
    return nodeCount;
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

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RexCall rexCall = (RexCall) o;
    return op.equals(rexCall.op)
        && operands.equals(rexCall.operands)
        && type.equals(rexCall.type);
  }

  @Override public int hashCode() {
    if (hash == 0) {
      hash = Objects.hash(op, operands);
    }
    return hash;
  }
}
