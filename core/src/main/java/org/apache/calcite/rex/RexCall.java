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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Sarg;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

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
 * no one is going to be generating source code from this tree.)
 */
public class RexCall extends RexNode {

  //~ Instance fields --------------------------------------------------------

  /** In the calls which can produce runtime errors we carry
   * the source position, so the backend can produce runtime error messages
   * pointing to the original source position.
   * For calls that are can never generate runtime failures, this field may
   * be ZERO.  Note that some optimizations may "lose" position information. */
  public final SqlParserPos pos;
  public final SqlOperator op;
  public final ImmutableList<RexNode> operands;
  public final RelDataType type;
  public final int nodeCount;

  /**
   * Cache of hash code.
   */
  protected int hash = 0;

  /**
   * Cache of normalized variables used for #equals and #hashCode.
   */
  private @Nullable Pair<SqlOperator, List<RexNode>> normalized;

  //~ Constructors -----------------------------------------------------------

  protected RexCall(
      SqlParserPos pos,
      RelDataType type,
      SqlOperator operator,
      List<? extends RexNode> operands) {
    this.pos = pos;
    this.type = requireNonNull(type, "type");
    this.op = requireNonNull(operator, "operator");
    this.operands = ImmutableList.copyOf(operands);
    this.nodeCount = RexUtil.nodeCount(1, this.operands);
    assert operator.validRexOperands(operands.size(), Litmus.THROW) : this;
    assert operator.kind != SqlKind.IN || this instanceof RexSubQuery;
  }

  protected RexCall(
      RelDataType type,
      SqlOperator operator,
      List<? extends RexNode> operands) {
    this(SqlParserPos.ZERO, type, operator, operands);
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
            || digestSkipsType((RexLiteral) otherArg))
            && SqlTypeUtil.equalSansNullability(operand.getType(), otherArg.getType())) {
          includeType = RexDigestIncludeType.NO_TYPE;
        }
      }
      operandDigests.add(computeDigest((RexLiteral) operand, includeType));
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

  private static boolean digestSkipsType(RexLiteral literal) {
    // This seems trivial, however, this method
    // workarounds https://github.com/typetools/checker-framework/issues/3631
    return literal.digestIncludesType() == RexDigestIncludeType.NO_TYPE;
  }

  private static String computeDigest(RexLiteral literal, RexDigestIncludeType includeType) {
    // This seems trivial, however, this method
    // workarounds https://github.com/typetools/checker-framework/issues/3631
    return literal.computeDigest(includeType);
  }

  protected String computeDigest(boolean withType) {
    final StringBuilder sb = new StringBuilder(op.getName());
    if (operands.isEmpty()
        && op.getSyntax() == SqlSyntax.FUNCTION_ID) {
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

  public SqlParserPos getParserPosition() {
    return this.pos;
  }

  @Override public final String toString() {
    return computeDigest(digestWithType());
  }

  private boolean digestWithType() {
    return isA(SqlKind.CAST) || isA(SqlKind.NEW_SPECIFICATION);
  }

  @Override public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitCall(this);
  }

  @Override public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitCall(this, arg);
  }

  @Override public RelDataType getType() {
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
    case SEARCH:
      final Sarg<?> sarg = ((RexLiteral) operands.get(1)).getValueAs(Sarg.class);
      return requireNonNull(sarg, "sarg").isAll()
          && (sarg.nullAs == RexUnknownAs.TRUE
              || !operands.get(0).getType().isNullable());
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
    case SEARCH:
      final Sarg<?> sarg = ((RexLiteral) operands.get(1)).getValueAs(Sarg.class);
      return requireNonNull(sarg, "sarg").isNone()
          && (sarg.nullAs == RexUnknownAs.FALSE
              || !operands.get(0).getType().isNullable());
    default:
      return false;
    }
  }

  @Override public SqlKind getKind() {
    return op.kind;
  }

  public List<RexNode> getOperands() {
    return operands;
  }

  public int operandCount() {
    return operands.size();
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
    return new RexCall(pos, type, op, operands);
  }

  private Pair<SqlOperator, List<RexNode>> getNormalized() {
    if (this.normalized == null) {
      this.normalized = RexNormalize.normalize(this.op, this.operands);
    }
    return this.normalized;
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Pair<SqlOperator, List<RexNode>> x = getNormalized();
    RexCall rexCall = (RexCall) o;
    Pair<SqlOperator, List<RexNode>> y = rexCall.getNormalized();
    return x.left.equals(y.left)
        && x.right.equals(y.right)
        && type.equals(rexCall.type);
  }

  @Override public int hashCode() {
    if (hash == 0) {
      hash = RexNormalize.hashCode(this.op, this.operands);
    }
    return hash;
  }
}
