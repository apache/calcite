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
 * A <code>SqlDescribeSchema</code> is a node of a parse tree that represents a
 * {@code DESCRIBE SCHEMA} statement.
 */
public class SqlDescribeSchema extends SqlCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DESCRIBE_SCHEMA", SqlKind.DESCRIBE_SCHEMA) {
        @Override public SqlCall createCall(SqlLiteral functionQualifier,
            SqlParserPos pos, SqlNode... operands) {
          return new SqlDescribeSchema(pos, (SqlIdentifier) operands[0]);
        }
      };

  SqlIdentifier schema;

  /** Creates a SqlDescribeSchema. */
  public SqlDescribeSchema(SqlParserPos pos, SqlIdentifier schema) {
    super(pos);
    this.schema = schema;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DESCRIBE");
    writer.keyword("SCHEMA");
    schema.unparse(writer, leftPrec, rightPrec);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      schema = (SqlIdentifier) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(schema);
  }

  public SqlIdentifier getSchema() {
    return schema;
  }
}

// End SqlDescribeSchema.java
