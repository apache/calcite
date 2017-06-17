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
package org.apache.calcite.sql2rel;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Contains the context necessary for a {@link SqlRexConvertlet} to convert a
 * {@link SqlNode} expression into a {@link RexNode}.
 */
public interface SqlRexContext {
  //~ Methods ----------------------------------------------------------------

  /**
   * Converts an expression from {@link SqlNode} to {@link RexNode} format.
   *
   * @param expr Expression to translate
   * @return Converted expression
   */
  RexNode convertExpression(SqlNode expr);

  /**
   * If the operator call occurs in an aggregate query, returns the number of
   * columns in the GROUP BY clause. For example, for "SELECT count(*) FROM emp
   * GROUP BY deptno, gender", returns 2.
   * If the operator call occurs in window aggregate query, then returns 1 if
   * the window is guaranteed to be non-empty, or 0 if the window might be
   * empty.
   *
   * <p>Returns 0 if the query is implicitly "GROUP BY ()" because of an
   * aggregate expression. For example, "SELECT sum(sal) FROM emp".</p>
   *
   * <p>Returns -1 if the query is not an aggregate query.</p>
   * @return 0 if the query is implicitly GROUP BY (), -1 if the query is not
   * and aggregate query
   * @see org.apache.calcite.sql.SqlOperatorBinding#getGroupCount()
   */
  int getGroupCount();

  /**
   * Returns the {@link RexBuilder} to use to create {@link RexNode} objects.
   */
  RexBuilder getRexBuilder();

  /**
   * Returns the expression used to access a given IN or EXISTS
   * {@link SqlSelect sub-query}.
   *
   * @param call IN or EXISTS expression
   * @return Expression used to access current row of sub-query
   */
  RexRangeRef getSubQueryExpr(SqlCall call);

  /**
   * Returns the type factory.
   */
  RelDataTypeFactory getTypeFactory();

  /**
   * Returns the factory which supplies default values for INSERT, UPDATE, and
   * NEW.
   */
  InitializerExpressionFactory getInitializerExpressionFactory();

  /**
   * Returns the validator.
   */
  SqlValidator getValidator();

  /**
   * Converts a literal.
   */
  RexNode convertLiteral(SqlLiteral literal);
}

// End SqlRexContext.java
