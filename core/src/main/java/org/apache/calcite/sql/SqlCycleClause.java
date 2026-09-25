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

/** SQL-standard cycle detection clause of a recursive common table expression.
 *
 * <p>Syntax: {@code CYCLE columns SET mark TO value DEFAULT default USING path}.
 * The mark and path columns are generated columns, not members of the WITH
 * item's explicit column list. */
public class SqlCycleClause extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CYCLE", SqlKind.CYCLE) {
        @Override public SqlCall createCall(@Nullable SqlLiteral qualifier,
            SqlParserPos pos, @Nullable SqlNode... operands) {
          return new SqlCycleClause(pos,
              (SqlNodeList) requireNonNull(operands[0]),
              (SqlIdentifier) requireNonNull(operands[1]),
              requireNonNull(operands[2]), requireNonNull(operands[3]),
              (SqlIdentifier) requireNonNull(operands[4]));
        }
      };

  public SqlNodeList columns;
  public SqlIdentifier markColumn;
  public SqlNode markValue;
  public SqlNode defaultValue;
  public SqlIdentifier pathColumn;

  public SqlCycleClause(SqlParserPos pos, SqlNodeList columns,
      SqlIdentifier markColumn, SqlNode markValue, SqlNode defaultValue,
      SqlIdentifier pathColumn) {
    super(pos);
    this.columns = columns;
    this.markColumn = markColumn;
    this.markValue = markValue;
    this.defaultValue = defaultValue;
    this.pathColumn = pathColumn;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(columns, markColumn, markValue, defaultValue, pathColumn);
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    requireNonNull(operand, "operand");
    switch (i) {
    case 0:
      columns = (SqlNodeList) operand;
      break;
    case 1:
      markColumn = (SqlIdentifier) operand;
      break;
    case 2:
      markValue = operand;
      break;
    case 3:
      defaultValue = operand;
      break;
    case 4:
      pathColumn = (SqlIdentifier) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CYCLE");
    final SqlWriter.Frame frame = writer.startList("", "");
    for (SqlNode column : columns) {
      writer.sep(",");
      column.unparse(writer, 0, 0);
    }
    writer.endList(frame);
    writer.keyword("SET");
    markColumn.unparse(writer, 0, 0);
    writer.keyword("TO");
    markValue.unparse(writer, 0, 0);
    writer.keyword("DEFAULT");
    defaultValue.unparse(writer, 0, 0);
    writer.keyword("USING");
    pathColumn.unparse(writer, 0, 0);
  }
}
