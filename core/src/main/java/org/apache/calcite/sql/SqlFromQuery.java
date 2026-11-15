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

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Parse tree node that represents a standalone {@code FROM} query in SQL pipe
 * syntax; for example, {@code FROM emp}.
 *
 * @see SqlConformance#isPipeSyntaxAllowed()
 */
public class SqlFromQuery extends SqlPipeQuery {
  public static final SqlSpecialOperator OPERATOR = new Operator() {
    @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
        SqlParserPos pos, @Nullable SqlNode... operands) {
      checkArgument(operands.length == 1);
      return new SqlFromQuery(pos, requireNonNull(operands[0], "from"));
    }
  };

  private SqlNode from;

  //~ Constructors -----------------------------------------------------------

  public SqlFromQuery(SqlParserPos pos, SqlNode from) {
    super(pos);
    this.from = from;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.FROM_QUERY;
  }

  @Override public SqlSelect surrogateSelect() {
    return surrogate(null, SqlNodeList.SINGLETON_STAR, from);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(from);
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    if (i != 0) {
      throw new AssertionError(i);
    }
    from = requireNonNull(operand, "operand");
  }

  /** Definition of the standalone {@code FROM} query operator. */
  private static class Operator extends SqlPipeQuery.Operator {
    private Operator() {
      super("FROM", SqlKind.FROM_QUERY);
    }

    @Override public SqlSyntax getSyntax() {
      return SqlSyntax.PREFIX;
    }

    @Override public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.SELECT);
      writer.sep("FROM");
      SqlUtil.unparseFromClause(writer, ((SqlFromQuery) call).from);
      writer.endList(frame);
    }
  }
}
