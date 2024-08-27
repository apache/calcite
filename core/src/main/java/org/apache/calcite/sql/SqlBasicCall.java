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

import java.util.List;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link SqlCall} that keeps its operands in an array.
 */
public class SqlBasicCall extends SqlCall {
  private SqlOperator operator;
  private List<@Nullable SqlNode> operandList;
  private final @Nullable SqlLiteral functionQuantifier;

  @Deprecated // to be removed before 2.0
  public SqlBasicCall(
      SqlOperator operator,
      @Nullable SqlNode[] operands,
      SqlParserPos pos) {
    this(operator, ImmutableNullableList.copyOf(operands), pos, null);
  }

  /** Creates a SqlBasicCall.
   *
   * <p>It is not expanded; call {@link #withExpanded withExpanded(true)}
   * to expand. */
  public SqlBasicCall(
      SqlOperator operator,
      List<? extends @Nullable SqlNode> operandList,
      SqlParserPos pos) {
    this(operator, operandList, pos, null);
  }

  @Deprecated // to be removed before 2.0
  public SqlBasicCall(
      SqlOperator operator,
      @Nullable SqlNode[] operands,
      SqlParserPos pos,
      @Nullable SqlLiteral functionQualifier) {
    this(operator, ImmutableNullableList.copyOf(operands), pos,
        functionQualifier);
  }

  /** Creates a SqlBasicCall with an optional function qualifier.
   *
   * <p>It is not expanded; call {@link #withExpanded withExpanded(true)}
   * to expand. */
  public SqlBasicCall(
      SqlOperator operator,
      List<? extends @Nullable SqlNode> operandList,
      SqlParserPos pos,
      @Nullable SqlLiteral functionQualifier) {
    super(pos);
    this.operator = requireNonNull(operator, "operator");
    this.operandList = ImmutableNullableList.copyOf(operandList);
    this.functionQuantifier = functionQualifier;
  }

  @Override public SqlKind getKind() {
    return operator.getKind();
  }

  /** Sets whether this call is expanded.
   *
   * @see #isExpanded() */
  public SqlCall withExpanded(boolean expanded) {
    return !expanded
        ? this
        : new ExpandedBasicCall(operator, operandList, pos,
            functionQuantifier);
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    operandList = set(operandList, i, operand);
  }

  /** Sets the operator (or function) that is being called.
   *
   * <p>This method is used by the validator to set a more refined version of
   * the same operator (for instance, a version where overloading has been
   * resolved); use with care. */
  public void setOperator(SqlOperator operator) {
    this.operator = requireNonNull(operator, "operator");
  }

  @Override public SqlOperator getOperator() {
    return operator;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return operandList;
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

  /** Sets the {@code i}th element of {@code list} to value {@code e}, creating
   * an immutable copy of the list. */
  private static <E> List<@Nullable E> set(List<E> list, int i, @Nullable E e) {
    if (i == 0 && list.size() == 1) {
      // short-cut case where the contents of the previous list can be ignored
      return ImmutableNullableList.of(e);
    }
    //noinspection unchecked
    @Nullable E[] objects = (E[]) list.toArray();
    objects[i] = e;
    return ImmutableNullableList.copyOf(objects);
  }

  /** Sub-class of {@link org.apache.calcite.sql.SqlBasicCall}
   * for which {@link #isExpanded()} returns true. */
  private static class ExpandedBasicCall extends SqlBasicCall {
    ExpandedBasicCall(SqlOperator operator,
        List<? extends @Nullable SqlNode> operandList, SqlParserPos pos,
        @Nullable SqlLiteral functionQualifier) {
      super(operator, operandList, pos, functionQualifier);
    }

    @Override public boolean isExpanded() {
      return true;
    }

    @Override public SqlCall withExpanded(boolean expanded) {
      return expanded
          ? this
          : new SqlBasicCall(getOperator(), getOperandList(), pos,
              getFunctionQuantifier());
    }
  }
}
