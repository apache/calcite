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

/**
 * A <code>SqlDescribeDatabase</code> is a node of a parse tree that represents a
 * {@code DESCRIBE DATABASE} statement.
 */
public class SqlDescribeDatabase extends SqlCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DESCRIBE_DATABASE", SqlKind.DESCRIBE_DATABASE) {
        @SuppressWarnings("argument.type.incompatible")
        @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
            SqlParserPos pos, @Nullable SqlNode... operands) {
          return new SqlDescribeSchema(pos, (SqlIdentifier) operands[0]);
        }
      };

  SqlIdentifier database;

  /** Creates a SqlDescribeDatabase. */
  public SqlDescribeDatabase(SqlParserPos pos, SqlIdentifier database) {
    super(pos);
    this.database = database;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DESCRIBE");
    writer.keyword("DATABASE");
    database.unparse(writer, leftPrec, rightPrec);
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      database = (SqlIdentifier) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(database);
  }

  public SqlIdentifier getDatabase() {
    return database;
  }
}
