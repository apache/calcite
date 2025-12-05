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
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.Iterables;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * Visitor which throws an exception if any component of the expression is not a
 * group expression.
 */
class AggChecker extends SqlBasicVisitor<Void> {
  //~ Instance fields --------------------------------------------------------

  private final Deque<SqlValidatorScope> scopes = new ArrayDeque<>();
  private final List<SqlNode> extraExprs;
  private final List<SqlNode> measureExprs;
  private final List<SqlNode> groupExprs;
  private final boolean distinct;
  private final SqlValidatorImpl validator;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AggChecker.
   *
   * @param validator  Validator
   * @param scope      Scope
   * @param extraExprs Expressions in GROUP BY (or SELECT DISTINCT) clause,
   *                   that are therefore available
   * @param measureExprs Expressions that are the names of measures
   * @param groupExprs Expressions in GROUP BY (or SELECT DISTINCT) clause,
   *                   that are therefore available
   * @param distinct   Whether aggregation checking is because of a SELECT
   *                   DISTINCT clause
   */
  AggChecker(
      SqlValidatorImpl validator,
      AggregatingScope scope,
      List<SqlNode> extraExprs,
      List<SqlNode> measureExprs,
      List<SqlNode> groupExprs,
      boolean distinct) {
    this.validator = validator;
    this.extraExprs = extraExprs;
    this.measureExprs = measureExprs;
    this.groupExprs = groupExprs;
    this.distinct = distinct;
    this.scopes.push(scope);
  }

  //~ Methods ----------------------------------------------------------------

  boolean isGroupExpr(SqlNode e) {
    for (SqlNode expr : Iterables.concat(extraExprs, measureExprs, groupExprs)) {
      if (expr.equalsDeep(e, Litmus.IGNORE)) {
        return true;
      }
    }
    return false;
  }

  boolean isMeasureExp(SqlNode e) {
    for (SqlNode expr : measureExprs) {
      if (expr.equalsDeep(e, Litmus.IGNORE)) {
        return true;
      }
    }
    return false;
  }

  @Override public Void visit(SqlIdentifier id) {
    if (id.isStar()) {
      // Star may validly occur in "SELECT COUNT(*) OVER w"
      return null;
    }

    if (!validator.config().nakedMeasuresInAggregateQuery()
        && isMeasureExp(id)) {
      SqlNode originalExpr = validator.getOriginal(id);
      throw validator.newValidationError(originalExpr,
          RESOURCE.measureIllegal());
    }

    if (isGroupExpr(id)) {
      return null;
    }

    // Is it a call to a parentheses-free function?
    final SqlCall call = validator.makeNullaryCall(id);
    if (call != null) {
      return call.accept(this);
    }

    SqlValidatorScope firstScope = scopes.getFirst();
    if (firstScope instanceof SqlLambdaScope) {
      SqlLambdaScope lambdaScope = (SqlLambdaScope) firstScope;
      if (lambdaScope.isParameter(id)) {
        return null;
      }
    }

    // Didn't find the identifier in the group-by list as is, now find
    // it fully-qualified.
    // TODO: It would be better if we always compared fully-qualified
    // to fully-qualified.
    final SqlQualified fqId = firstScope.fullyQualify(id);
    if (isGroupExpr(fqId.identifier)) {
      return null;
    }
    SqlNode originalExpr = validator.getOriginal(id);
    final String exprString = originalExpr.toString();
    throw validator.newValidationError(originalExpr,
        distinct
            ? RESOURCE.notSelectDistinctExpr(exprString)
            : RESOURCE.notGroupExpr(exprString));
  }

  @Override public Void visit(SqlCall call) {
    final SqlValidatorScope scope =
        requireNonNull(scopes.peek(), () -> "scope for " + call);
    if (call.getOperator().isAggregator()) {
      if (distinct) {
        if (scope instanceof AggregatingSelectScope) {
          final SqlSelect select = (SqlSelect) scope.getNode();
          SelectScope selectScope =
              requireNonNull(validator.getRawSelectScope(select),
                  () -> "rawSelectScope for " + scope.getNode());
          List<SqlNode> selectList =
              requireNonNull(selectScope.getExpandedSelectList(),
                  () -> "expandedSelectList for " + selectScope);

          // Check if this aggregation function is just an element in the select
          for (SqlNode sqlNode : selectList) {
            if (sqlNode.getKind() == SqlKind.AS) {
              sqlNode = ((SqlCall) sqlNode).operand(0);
            }

            if (validator.expand(sqlNode, scope)
                .equalsDeep(call, Litmus.IGNORE)) {
              return null;
            }
          }
        }

        // Cannot use agg fun in ORDER BY clause if have SELECT DISTINCT.
        SqlNode originalExpr = validator.getOriginal(call);
        final String exprString = originalExpr.toString();
        throw validator.newValidationError(call,
            RESOURCE.notSelectDistinctExpr(exprString));
      }

      // For example, 'sum(sal)' in 'SELECT sum(sal) FROM emp GROUP
      // BY deptno'
      return null;
    }
    switch (call.getKind()) {
    case FILTER:
    case WITHIN_GROUP:
    case RESPECT_NULLS:
    case IGNORE_NULLS:
    case WITHIN_DISTINCT:
      call.operand(0).accept(this);
      return null;
    default:
      break;
    }
    // Visit the operand in window function
    if (call.getKind() == SqlKind.OVER) {
      for (SqlNode operand : call.<SqlCall>operand(0).getOperandList()) {
        operand.accept(this);
      }
      // Check the OVER clause
      final SqlNode over = call.operand(1);
      if (over instanceof SqlCall) {
        over.accept(this);
      } else if (over instanceof SqlIdentifier) {
        // Check the corresponding SqlWindow in WINDOW clause
        final SqlWindow window =
            scope.lookupWindow(((SqlIdentifier) over).getSimple());
        requireNonNull(window, () -> "window for " + call);
        window.getPartitionList().accept(this);
        window.getOrderList().accept(this);
      }
    }
    if (isGroupExpr(call)) {
      // This call matches an expression in the GROUP BY clause.
      return null;
    }

    final SqlCall groupCall =
        SqlStdOperatorTable.convertAuxiliaryToGroupCall(call);
    if (groupCall != null) {
      if (isGroupExpr(groupCall)) {
        // This call is an auxiliary function that matches a group call in the
        // GROUP BY clause.
        //
        // For example TUMBLE_START is an auxiliary of the TUMBLE
        // group function, and
        //   TUMBLE_START(rowtime, INTERVAL '1' HOUR)
        // matches
        //   TUMBLE(rowtime, INTERVAL '1' HOUR')
        return null;
      }
      throw validator.newValidationError(groupCall,
          RESOURCE.auxiliaryWithoutMatchingGroupCall(
              call.getOperator().getName(), groupCall.getOperator().getName()));
    }

    if (call.isA(SqlKind.QUERY)) {
      // Allow queries for now, even though they may contain
      // references to forbidden columns.
      return null;
    }

    // Switch to new scope.
    SqlValidatorScope newScope = scope.getOperandScope(call);
    scopes.push(newScope);

    // Visit the operands (only expressions).
    call.getOperator()
        .acceptCall(this, call, true, ArgHandlerImpl.instance());

    // Restore scope.
    scopes.pop();
    return null;
  }

}
