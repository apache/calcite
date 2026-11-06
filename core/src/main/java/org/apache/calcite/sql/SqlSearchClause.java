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

/** SQL-standard search ordering clause of a recursive common table expression.
 *
 * <p>Syntax: {@code SEARCH { DEPTH | BREADTH } FIRST BY columns SET orderColumn}.
 * The generated column is available to consumers of the WITH item; an explicit
 * ORDER BY is needed to return rows in search order. */
public class SqlSearchClause extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SEARCH", SqlKind.SEARCH_CLAUSE) {
        @Override public SqlCall createCall(@Nullable SqlLiteral qualifier,
            SqlParserPos pos, @Nullable SqlNode... operands) {
          return new SqlSearchClause(pos,
              (SqlLiteral) requireNonNull(operands[0]),
              (SqlNodeList) requireNonNull(operands[1]),
              (SqlIdentifier) requireNonNull(operands[2]));
        }
      };

  public SqlLiteral breadthFirst;
  public SqlNodeList columns;
  public SqlIdentifier orderColumn;

  public SqlSearchClause(SqlParserPos pos, SqlLiteral breadthFirst,
      SqlNodeList columns, SqlIdentifier orderColumn) {
    super(pos);
    this.breadthFirst = breadthFirst;
    this.columns = columns;
    this.orderColumn = orderColumn;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(breadthFirst, columns, orderColumn);
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    requireNonNull(operand, "operand");
    switch (i) {
    case 0:
      breadthFirst = (SqlLiteral) operand;
      break;
    case 1:
      columns = (SqlNodeList) operand;
      break;
    case 2:
      orderColumn = (SqlIdentifier) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SEARCH");
    writer.keyword(breadthFirst.booleanValue() ? "BREADTH" : "DEPTH");
    writer.keyword("FIRST BY");
    final SqlWriter.Frame frame = writer.startList("", "");
    for (SqlNode column : columns) {
      writer.sep(",");
      column.unparse(writer, 0, 0);
    }
    writer.endList(frame);
    writer.keyword("SET");
    orderColumn.unparse(writer, 0, 0);
  }
}
