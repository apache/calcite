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
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Base class for parse tree nodes that represent a query in SQL pipe
 * syntax: {@link SqlFromQuery} or {@link SqlPipeCall}.
 *
 * @see SqlConformance#isPipeSyntaxAllowed()
 * @see SqlKind#PIPE_QUERY
 */
public abstract class SqlPipeQuery extends SqlCall {
  protected SqlPipeQuery(SqlParserPos pos) {
    super(pos);
  }

  /**
   * Creates the surrogate SELECT of this query: a {@code SELECT} that stands
   * in for the query while it is validated and converted.
   *
   * <p>For example, {@code FROM emp |> WHERE c} is a {@link SqlPipeWhere}
   * whose input is the {@link SqlFromQuery} {@code FROM emp}. The surrogate of
   * {@code FROM emp} is {@code SELECT * FROM emp}, and the surrogate of the
   * {@link SqlPipeWhere} is {@code SELECT * FROM input WHERE c}.
   */
  public abstract SqlSelect surrogateSelect();

  /** Creates a surrogate SELECT with the given select list and FROM clause,
   * and no other clauses. */
  protected SqlSelect surrogate(@Nullable SqlNodeList keywordList,
      SqlNodeList selectList, @Nullable SqlNode from) {
    return new SqlSelect(getParserPosition(), keywordList, selectList, from,
        null, null, null, null, null, null, null, null, null);
  }

  @Override public void validate(SqlValidator validator,
      SqlValidatorScope scope) {
    validator.validateQuery(this, scope, validator.getUnknownType());
  }

  /** Definition of an operator written in pipe syntax. */
  protected abstract static class Operator extends SqlSpecialOperator {
    protected Operator(String name, SqlKind kind) {
      super(name, kind, 0);
    }

    @Override public <R> void acceptCall(SqlVisitor<R> visitor, SqlCall call,
        boolean onlyExpressions, SqlBasicVisitor.ArgHandler<R> argHandler) {
      if (!onlyExpressions) {
        // None of the operands of a pipe operator are expressions.
        super.acceptCall(visitor, call, onlyExpressions, argHandler);
      }
    }
  }
}
