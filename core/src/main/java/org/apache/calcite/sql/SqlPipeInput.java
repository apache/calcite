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

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Node that represents the input of a pipe operator in its
 * {@link SqlPipeOperator#surrogateSelect}.
 *
 * @see SqlKind#PIPE_INPUT
 */
public class SqlPipeInput extends SqlCall {
  /** Definition of the pipe-input placeholder. */
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("PIPE_INPUT", SqlKind.PIPE_INPUT) {
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier, SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlPipeInput(pos, requireNonNull(operands[0], "input"));
        }

        @Override public void unparse(SqlWriter writer, SqlCall call,
            int leftPrec, int rightPrec) {
          ((SqlPipeInput) call).input.unparse(writer, leftPrec, rightPrec);
        }
      };

  private SqlNode input;

  //~ Constructors -----------------------------------------------------------

  private SqlPipeInput(SqlParserPos pos, SqlNode input) {
    super(pos);
    this.input = input;
  }

  //~ Methods ----------------------------------------------------------------

  /** Creates a placeholder for the input of {@code pipe}. */
  public static SqlPipeInput of(SqlPipeOperator pipe) {
    return new SqlPipeInput(pipe.getParserPosition(), pipe.getQuery());
  }

  /** Returns the query that this node stands for. */
  public SqlNode getInput() {
    return input;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(input);
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    if (i == 0) {
      input = requireNonNull(operand, "operand");
    } else {
      super.setOperand(i, operand);
    }
  }
}
