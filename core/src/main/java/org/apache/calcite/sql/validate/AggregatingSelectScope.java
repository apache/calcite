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
package org.eigenbase.sql.validate;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;

import static org.eigenbase.sql.SqlUtil.stripAs;

/**
 * Scope for resolving identifiers within a SELECT statement that has a
 * GROUP BY clause.
 *
 * <p>The same set of identifiers are in scope, but it won't allow access to
 * identifiers or expressions which are not group-expressions.
 */
public class AggregatingSelectScope
    extends DelegatingScope implements AggregatingScope {
  //~ Instance fields --------------------------------------------------------

  private final SqlSelect select;
  private final boolean distinct;
  private final List<SqlNode> groupExprList;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AggregatingSelectScope
   *
   * @param selectScope Parent scope
   * @param select      Enclosing SELECT node
   * @param distinct    Whether SELECT is DISTINCT
   */
  AggregatingSelectScope(
      SqlValidatorScope selectScope,
      SqlSelect select,
      boolean distinct) {
    // The select scope is the parent in the sense that all columns which
    // are available in the select scope are available. Whether they are
    // valid as aggregation expressions... now that's a different matter.
    super(selectScope);
    this.select = select;
    this.distinct = distinct;
    if (distinct) {
      groupExprList = null;
    } else if (select.getGroup() != null) {
      // We deep-copy the group-list in case subsequent validation
      // modifies it and makes it no longer equivalent. While copying,
      // we fully qualify all identifiers.
      SqlNodeList sqlNodeList =
          (SqlNodeList) this.select.getGroup().accept(
              new SqlValidatorUtil.DeepCopier(parent));
      this.groupExprList = sqlNodeList.getList();
    } else {
      groupExprList = null;
    }
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the expressions that are in the GROUP BY clause (or the SELECT
   * DISTINCT clause, if distinct) and that can therefore be referenced
   * without being wrapped in aggregate functions.
   *
   * <p>The expressions are fully-qualified, and any "*" in select clauses are
   * expanded.
   *
   * @return list of grouping expressions
   */
  private List<SqlNode> getGroupExprs() {
    if (distinct) {
      // Cannot compute this in the constructor: select list has not been
      // expanded yet.
      assert select.isDistinct();

      // Remove the AS operator so the expressions are consistent with
      // OrderExpressionExpander.
      List<SqlNode> groupExprs = new ArrayList<SqlNode>();
      for (SqlNode selectItem
          : ((SelectScope) parent).getExpandedSelectList()) {
        groupExprs.add(stripAs(selectItem));
      }
      return groupExprs;
    } else if (select.getGroup() != null) {
      return groupExprList;
    } else {
      return Collections.emptyList();
    }
  }

  public SqlNode getNode() {
    return select;
  }

  public SqlValidatorScope getOperandScope(SqlCall call) {
    if (call.getOperator().isAggregator()) {
      // If we're the 'SUM' node in 'select a + sum(b + c) from t
      // group by a', then we should validate our arguments in
      // the non-aggregating scope, where 'b' and 'c' are valid
      // column references.
      return parent;
    } else {
      // Check whether expression is constant within the group.
      //
      // If not, throws. Example, 'empno' in
      //    SELECT empno FROM emp GROUP BY deptno
      //
      // If it perfectly matches an expression in the GROUP BY
      // clause, we validate its arguments in the non-aggregating
      // scope. Example, 'empno + 1' in
      //
      //   SELECT empno + 1 FROM emp GROUP BY empno + 1

      final boolean matches = checkAggregateExpr(call, false);
      if (matches) {
        return parent;
      }
    }
    return super.getOperandScope(call);
  }

  public boolean checkAggregateExpr(SqlNode expr, boolean deep) {
    // Fully-qualify any identifiers in expr.
    if (deep) {
      expr = validator.expand(expr, this);
    }

    // Make sure expression is valid, throws if not.
    List<SqlNode> groupExprs = getGroupExprs();
    final AggChecker aggChecker =
        new AggChecker(
            validator,
            this,
            groupExprs,
            distinct);
    if (deep) {
      expr.accept(aggChecker);
    }

    // Return whether expression exactly matches one of the group
    // expressions.
    return aggChecker.isGroupExpr(expr);
  }

  public void validateExpr(SqlNode expr) {
    checkAggregateExpr(expr, true);
  }

  /**
   * Adds a GROUP BY expression.
   *
   * <p>This method is used when the GROUP BY list is validated, and
   * expressions are expanded, in which case they are not structurally
   * identical to the unexpanded form.  We leave the previous expression in
   * the list (in case there are occurrences of the expression's unexpanded
   * form in the parse tree.
   *
   * @param expr Expression
   */
  public void addGroupExpr(SqlNode expr) {
    groupExprList.add(expr);
  }
}

// End AggregatingSelectScope.java
