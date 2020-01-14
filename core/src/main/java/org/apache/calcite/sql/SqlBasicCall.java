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
import org.apache.calcite.util.UnmodifiableArrayList;

import java.util.List;
import java.util.Objects;

/**
 * Implementation of {@link SqlCall} that keeps its operands in an array.
 */
public class SqlBasicCall extends SqlCall {
  private SqlOperator operator;
  public final SqlNode[] operands;
  private final SqlLiteral functionQuantifier;
  private final boolean expanded;

  public SqlBasicCall(
      SqlOperator operator,
      SqlNode[] operands,
      SqlParserPos pos) {
    this(operator, operands, pos, false, null);
  }

  public SqlBasicCall(
      SqlOperator operator,
      SqlNode[] operands,
      SqlParserPos pos,
      boolean expanded,
      SqlLiteral functionQualifier) {
    super(pos);
    this.operator = Objects.requireNonNull(operator);
    this.operands = operands;
    this.expanded = expanded;
    this.functionQuantifier = functionQualifier;
  }

  public SqlKind getKind() {
    return operator.getKind();
  }

  @Override public boolean isExpanded() {
    return expanded;
  }

  @Override public void setOperand(int i, SqlNode operand) {
    operands[i] = operand;
  }

  public void setOperator(SqlOperator operator) {
    this.operator = Objects.requireNonNull(operator);
  }

  public SqlOperator getOperator() {
    return operator;
  }

  public SqlNode[] getOperands() {
    return operands;
  }

  public List<SqlNode> getOperandList() {
    return UnmodifiableArrayList.of(operands); // not immutable, but quick
  }

  @SuppressWarnings("unchecked")
  @Override public <S extends SqlNode> S operand(int i) {
    return (S) operands[i];
  }

  @Override public int operandCount() {
    return operands.length;
  }

  @Override public SqlLiteral getFunctionQuantifier() {
    return functionQuantifier;
  }

  @Override public SqlNode clone(SqlParserPos pos) {
    return getOperator().createCall(getFunctionQuantifier(), pos, operands);
  }

}

// End SqlBasicCall.java
