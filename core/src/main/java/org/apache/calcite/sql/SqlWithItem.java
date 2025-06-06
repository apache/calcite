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
 * An item in a WITH clause of a query.
 * It has a name, an optional column list, and a query.
 */
public class SqlWithItem extends SqlCall {
  public SqlIdentifier name;
  public @Nullable SqlNodeList columnList; // may be null
  public SqlLiteral recursive;
  public SqlNode query;

  // to be removed before 2.0
  public SqlWithItem(SqlParserPos pos, SqlIdentifier name,
      @Nullable SqlNodeList columnList, SqlNode query) {
    this(pos, name, columnList, query,
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO));
  }

  public SqlWithItem(SqlParserPos pos, SqlIdentifier name,
      @Nullable SqlNodeList columnList, SqlNode query,
      SqlLiteral recursive) {
    super(pos);
    this.name = name;
    this.columnList = columnList;
    this.recursive = recursive;
    this.query = query;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.WITH_ITEM;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query, recursive);
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      name = (SqlIdentifier) operand;
      break;
    case 1:
      columnList = (@Nullable SqlNodeList) operand;
      break;
    case 2:
      query = operand;
      break;
    case 3:
      recursive = (SqlLiteral) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public SqlOperator getOperator() {
    return SqlWithItemOperator.INSTANCE;
  }

  /**
   * SqlWithItemOperator is used to represent an item in a WITH clause of a
   * query. It has a name, an optional column list, and a query.
   */
  private static class SqlWithItemOperator extends SqlSpecialOperator {
    private static final SqlWithItemOperator INSTANCE =
        new SqlWithItemOperator();

    SqlWithItemOperator() {
      super("WITH_ITEM", SqlKind.WITH_ITEM, 0);
    }

    //~ Methods ----------------------------------------------------------------

    @Override public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      final SqlWithItem withItem = (SqlWithItem) call;
      withItem.name.unparse(writer, getLeftPrec(), getRightPrec());
      SqlDialect dialect = writer.getDialect();
      if (dialect.supportsColumnListForWithItem() && withItem.columnList != null && withItem.columnList.size() > 0) {
        withItem.columnList.unparse(writer, getLeftPrec(), getRightPrec());
      }
      writer.keyword("AS");
      withItem.query.unparse(writer, MDX_PRECEDENCE, MDX_PRECEDENCE);
    }

    @SuppressWarnings("argument.type.incompatible")
    @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
        SqlParserPos pos, @Nullable SqlNode... operands) {
      assert functionQualifier == null;
      assert operands.length == 4;
      return new SqlWithItem(pos, (SqlIdentifier) operands[0],
          (SqlNodeList) operands[1], operands[2], (SqlLiteral) operands[3]);
    }
  }
}
