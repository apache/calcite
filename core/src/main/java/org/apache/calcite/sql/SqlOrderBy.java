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
 * Parse tree node that represents an {@code ORDER BY} on a query other than a
 * {@code SELECT} (e.g. {@code VALUES} or {@code UNION}).
 *
 * <p>It is a purely syntactic operator, and is eliminated by
 * {@link org.apache.calcite.sql.validate.SqlValidatorImpl#performUnconditionalRewrites}
 * and replaced with the ORDER_OPERAND of SqlSelect.</p>
 */
public class SqlOrderBy extends SqlCall {
  public static final SqlSpecialOperator OPERATOR = new Operator() {
    @SuppressWarnings("argument.type.incompatible")
    @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
        SqlParserPos pos, @Nullable SqlNode... operands) {
      return new SqlOrderBy(pos, operands[0], (SqlNodeList) operands[1],
          operands[2], operands[3]);
    }
  };

  public final SqlNode query;
  public final SqlNodeList orderList;
  public final @Nullable SqlNode offset;
  public final @Nullable SqlNode fetch;

  //~ Constructors -----------------------------------------------------------

  public SqlOrderBy(SqlParserPos pos, SqlNode query, SqlNodeList orderList,
      @Nullable SqlNode offset, @Nullable SqlNode fetch) {
    super(pos);
    this.query = query;
    this.orderList = orderList;
    this.offset = offset;
    this.fetch = fetch;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.ORDER_BY;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(query, orderList, offset, fetch);
  }

  /** Definition of {@code ORDER BY} operator. */
  private static class Operator extends SqlSpecialOperator {
    private Operator() {
      // NOTE:  make precedence lower then SELECT to avoid extra parens
      super("ORDER BY", SqlKind.ORDER_BY, 0);
    }

    @Override public SqlSyntax getSyntax() {
      return SqlSyntax.POSTFIX;
    }

    @Override public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      SqlOrderBy orderBy = (SqlOrderBy) call;
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY);
      orderBy.query.unparse(writer, getLeftPrec(), getRightPrec());
      if (orderBy.orderList != SqlNodeList.EMPTY) {
        writer.sep(getName());
        writer.list(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA,
            orderBy.orderList);
      }
      if (orderBy.offset != null) {
        final SqlWriter.Frame frame2 =
            writer.startList(SqlWriter.FrameTypeEnum.OFFSET);
        writer.newlineAndIndent();
        writer.keyword("OFFSET");
        orderBy.offset.unparse(writer, -1, -1);
        writer.keyword("ROWS");
        writer.endList(frame2);
      }
      if (orderBy.fetch != null) {
        final SqlWriter.Frame frame3 =
            writer.startList(SqlWriter.FrameTypeEnum.FETCH);
        writer.newlineAndIndent();
        writer.keyword("FETCH");
        writer.keyword("NEXT");
        orderBy.fetch.unparse(writer, -1, -1);
        writer.keyword("ROWS");
        writer.keyword("ONLY");
        writer.endList(frame3);
      }
      writer.endList(frame);
    }
  }
}
