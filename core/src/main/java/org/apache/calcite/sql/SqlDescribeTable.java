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

import java.util.List;

/**
 * A <code>SqlDescribeTable</code> is a node of a parse tree which represents an
 * DESCRIBE table statement.
 */
public class SqlDescribeTable extends SqlCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DESCRIBE_TABLE", SqlKind.DESCRIBE) {
        @Override public SqlCall createCall(SqlLiteral functionQualifier,
                              SqlParserPos pos,
                              SqlNode... operands) {
          return new SqlDescribeTable(pos,
                      (SqlIdentifier) operands[0],
                      (SqlIdentifier) operands[1],
                      operands[2]);
        }
      };

  SqlIdentifier table;
  SqlIdentifier column;
  SqlNode columnQualifier;

  public SqlDescribeTable(SqlParserPos pos,
                          SqlIdentifier table,
                          SqlIdentifier column,
                          SqlNode columnQualifier) {
    super(pos);
    this.table = table;
    this.column = column;
    this.columnQualifier = columnQualifier;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DESCRIBE");
    table.unparse(writer, leftPrec, rightPrec);
    if (column != null) {
      column.unparse(writer, leftPrec, rightPrec);
    }
    if (columnQualifier != null) {
      columnQualifier.unparse(writer, leftPrec, rightPrec);
    }
  }


  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      table = (SqlIdentifier) operand;
      break;
    case 1:
      column = (SqlIdentifier) operand;
      break;
    case 2:
      columnQualifier = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public SqlKind getKind() {
    return SqlKind.DESCRIBE;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(table, column, columnQualifier);
  }

  public SqlIdentifier getTable() { return table; }

  public SqlIdentifier getColumn() { return column; }

  public SqlNode getColumnQualifier() { return columnQualifier; }
}

// End SqlDescribeTable.java
