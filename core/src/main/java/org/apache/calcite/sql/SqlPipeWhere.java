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
import org.apache.calcite.sql.validate.SqlConformance;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Parse tree node that represents the {@code WHERE} pipe operator in SQL pipe
 * syntax; for example, {@code FROM emp |> WHERE deptno = 10}.
 *
 * @see SqlConformance#isPipeSyntaxAllowed()
 */
public class SqlPipeWhere extends SqlPipeOperator {
  public static final SqlSpecialOperator OPERATOR =
      new Operator(PIPE + " WHERE") {
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier, SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlPipeWhere(pos, requireNonNull(operands[0], "query"),
              requireNonNull(operands[1], "condition"));
        }

        @Override protected void unparseTail(SqlWriter writer,
            SqlPipeOperator call) {
          SqlUtil.unparseConditionClause(writer,
              ((SqlPipeWhere) call).condition, 0, 0);
        }
      };

  private SqlNode condition;

  //~ Constructors -----------------------------------------------------------

  public SqlPipeWhere(SqlParserPos pos, SqlNode query, SqlNode condition) {
    super(pos, query);
    this.condition = condition;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlSelect surrogateSelect() {
    final SqlSelect select = surrogate(null, SqlNodeList.SINGLETON_STAR);
    select.setWhere(condition);
    return select;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(query, condition);
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    if (i == 1) {
      condition = requireNonNull(operand, "operand");
    } else {
      super.setOperand(i, operand);
    }
  }
}
