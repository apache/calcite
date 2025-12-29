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
package org.apache.calcite.sql.ddl;

import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Parse tree for {@code UNIQUE}, {@code PRIMARY KEY} constraints.
 *
 * <p>And {@code FOREIGN KEY}, when we support it.
 */
public class SqlColumnDeclaration extends SqlCall {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL) {
        @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
            SqlParserPos pos, @Nullable SqlNode... operands) {
          return new SqlColumnDeclaration(pos,
              (SqlIdentifier) requireNonNull(operands[0], "name"),
              (SqlDataTypeSpec) requireNonNull(operands[1], "dataType"),
              operands[2],
              operands[3] != null
                  ? ColumnStrategy.valueOf(((SqlIdentifier) operands[3]).getSimple())
                  : null);
        }
      };

  public final SqlIdentifier name;
  public final SqlDataTypeSpec dataType;
  public final @Nullable SqlNode expression;
  // The Babel parser can supply null for the strategy
  public final @Nullable ColumnStrategy strategy;

  /** Creates a SqlColumnDeclaration; use {@link SqlDdlNodes#column}. */
  SqlColumnDeclaration(SqlParserPos pos, SqlIdentifier name,
      SqlDataTypeSpec dataType, @Nullable SqlNode expression,
      @Nullable ColumnStrategy strategy) {
    super(pos);
    this.name = name;
    this.dataType = dataType;
    this.expression = expression;
    this.strategy = strategy;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, dataType, expression,
        strategy != null ? new SqlIdentifier(strategy.name(), SqlParserPos.ZERO) : null);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, 0, 0);
    dataType.unparse(writer, 0, 0);
    if (Boolean.FALSE.equals(dataType.getNullable())) {
      writer.keyword("NOT NULL");
    }
    SqlNode expression = this.expression;
    if (expression != null && strategy != null) {
      switch (strategy) {
      case VIRTUAL:
      case STORED:
        writer.keyword("AS");
        exp(writer, expression);
        writer.keyword(strategy.name());
        break;
      case DEFAULT:
        writer.keyword("DEFAULT");
        exp(writer, expression);
        break;
      default:
        throw new AssertionError("unexpected: " + strategy);
      }
    }
  }

  static void exp(SqlWriter writer, SqlNode expression) {
    if (writer.isAlwaysUseParentheses()) {
      expression.unparse(writer, 0, 0);
    } else {
      writer.sep("(");
      expression.unparse(writer, 0, 0);
      writer.sep(")");
    }
  }
}
