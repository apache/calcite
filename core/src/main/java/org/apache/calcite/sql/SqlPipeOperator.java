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

import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Base class for parse tree nodes that represent operators in SQL pipe
 * syntax; for example, {@code FROM emp |> WHERE deptno = 10}.
 *
 * <p>A pipe operator stays in the parse tree through validation. It is
 * validated and converted as its {@link #surrogateSelect}, which reads the
 * operator's input through a {@link SqlPipeInput}. For example, the
 * surrogate of {@code |> WHERE c} is {@code SELECT * FROM input WHERE c}.
 *
 * @see SqlConformance#isPipeSyntaxAllowed()
 * @see SqlKind#PIPE_OPERATOR
 */
public abstract class SqlPipeOperator extends SqlPipeQuery {
  /** The {@code |>} token, with which each operator's name starts. */
  public static final String PIPE = "|>";

  protected SqlNode query;

  //~ Constructors -----------------------------------------------------------

  protected SqlPipeOperator(SqlParserPos pos, SqlNode query) {
    super(pos);
    this.query = requireNonNull(query, "query");
  }

  //~ Methods ----------------------------------------------------------------

  /** Creates a {@code SELECT} for {@link #surrogateSelect} to return, which
   * reads this operator's input through a {@link SqlPipeInput}. */
  protected SqlSelect surrogate(@Nullable SqlNodeList keywordList,
      SqlNodeList selectList) {
    return surrogate(keywordList, selectList, SqlPipeInput.of(this));
  }

  /** Returns the query that this pipe operator applies to. */
  public SqlNode getQuery() {
    return query;
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    if (i == 0) {
      query = requireNonNull(operand, "operand");
    } else {
      super.setOperand(i, operand);
    }
  }

  @Override public final SqlKind getKind() {
    return SqlKind.PIPE_OPERATOR;
  }

  /** Definition of a pipe operator. Unparses the input query, then the
   * operator's keyword, then a tail supplied by the sub-class. */
  public abstract static class Operator extends SqlPipeQuery.Operator {
    protected Operator(String name) {
      super(name, SqlKind.PIPE_OPERATOR);
    }

    @Override public SqlSyntax getSyntax() {
      return SqlSyntax.POSTFIX;
    }

    @Override public final void unparse(SqlWriter writer, SqlCall call,
        int leftPrec, int rightPrec) {
      final SqlPipeOperator pipe = (SqlPipeOperator) call;
      // An ORDER_BY frame starts each operator on a new line, unindented.
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY);
      final SqlNode query = pipe.query;
      if (query instanceof SqlWith
          || query instanceof SqlOrderBy
              && ((SqlOrderBy) query).query instanceof SqlWith) {
        // Without parentheses, the operator would be re-parsed into the WITH.
        final SqlWriter.Frame frame1 = writer.startList("(", ")");
        query.unparse(writer, 0, 0);
        writer.endList(frame1);
      } else {
        query.unparse(writer, 0, 0);
      }
      writer.sep(getName());
      unparseTail(writer, pipe);
      writer.endList(frame);
    }

    /** Unparses whatever follows the operator's keyword. */
    protected abstract void unparseTail(SqlWriter writer,
        SqlPipeOperator call);
  }
}
