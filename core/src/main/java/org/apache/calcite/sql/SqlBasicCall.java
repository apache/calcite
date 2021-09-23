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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Implementation of {@link SqlCall} that keeps its operands in an array.
 */
public class SqlBasicCall extends SqlCall {
  private SqlOperator operator;

  /** Array of operands.
   *
   * @deprecated Use the methods {@link #getOperandList()} and
   * {@link #setOperand(int, SqlNode)}. To be removed before 1.29.
   */
  @Deprecated // to be removed before 1.29
  public final @Nullable SqlNode[] operands;

  private final List<SqlNode> operandList;
  private final @Nullable SqlLiteral functionQuantifier;
  private final boolean expanded;

  public SqlBasicCall(
      SqlOperator operator,
      @Nullable SqlNode[] operands,
      SqlParserPos pos) {
    this(operator, operands, pos, null, false);
  }

  /** Creates a SqlBasicCall.
   *
   * @deprecated Use
   * {@link #SqlBasicCall(SqlOperator, SqlNode[], SqlParserPos, SqlLiteral)}
   * followed by {@link #withExpanded(boolean)}. To be removed before 1.29.
   */
  @Deprecated // to be removed before 1.29
  public SqlBasicCall(
      SqlOperator operator,
      @Nullable SqlNode[] operands,
      SqlParserPos pos,
      boolean expanded,
      @Nullable SqlLiteral functionQualifier) {
    this(operator, operands, pos, functionQualifier, expanded);
  }

  /** Creates an unexpanded SqlBasicCall. */
  public SqlBasicCall(
      SqlOperator operator,
      @Nullable SqlNode[] operands,
      SqlParserPos pos,
      @Nullable SqlLiteral functionQualifier) {
    this(operator, operands, pos, functionQualifier, false);
  }

  /** Private constructor. */
  private SqlBasicCall(
      SqlOperator operator,
      @Nullable SqlNode[] operands,
      SqlParserPos pos,
      @Nullable SqlLiteral functionQualifier,
      boolean expanded) {
    super(pos);
    this.operator = Objects.requireNonNull(operator, "operator");
    this.operands = operands;
    this.operandList = Arrays.asList(operands);
    this.expanded = expanded;
    this.functionQuantifier = functionQualifier;
  }

  @Override public SqlKind getKind() {
    return operator.getKind();
  }

  @Override public boolean isExpanded() {
    return expanded;
  }

  public SqlCall withExpanded(boolean expanded) {
    return expanded == this.expanded ? this
        : new SqlBasicCall(operator, operandList.toArray(new SqlNode[0]), pos,
            functionQuantifier, expanded);
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    operandList.set(i, operand);
  }

  public void setOperator(SqlOperator operator) {
    this.operator = Objects.requireNonNull(operator, "operator");
  }

  @Override public SqlOperator getOperator() {
    return operator;
  }

  /** Returns the array of operands.
   *
   * @deprecated Use the methods {@link #getOperandList()} and
   * {@link #setOperand(int, SqlNode)}. To be removed before 1.29.
   */
  @Deprecated // to be removed before 1.29
  public @Nullable SqlNode[] getOperands() {
    return operands;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.copyOf(operandList);
  }

  @SuppressWarnings("unchecked")
  @Override public <S extends SqlNode> S operand(int i) {
    return (S) castNonNull(operandList.get(i));
  }

  @Override public int operandCount() {
    return operandList.size();
  }

  @Override public @Nullable SqlLiteral getFunctionQuantifier() {
    return functionQuantifier;
  }

  @Override public SqlNode clone(SqlParserPos pos) {
    return getOperator().createCall(getFunctionQuantifier(), pos, operandList);
  }

}
