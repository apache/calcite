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

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

import static org.apache.calcite.util.Static.RESOURCE;
import static org.apache.calcite.util.Util.first;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.util.IdPair;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ScopeValidationRegister {

  Map<SqlNode, SqlValidatorScope> nodeToScope = new HashMap<>();

  SqlValidatorScope register(
      SqlNode sqlNode,
      SqlValidatorScope lateralScope) {

  }


  /**
   * Registers a query in a parent scope.
   *
   * @param parentScope Parent scope which this scope turns to in order to
   *                    resolve objects
   * @param usingScope  Scope whose child list this scope should add itself to
   * @param node        Query node
   * @param alias       Name of this query within its parent. Must be specified
   *                    if usingScope != null
   */
  protected void registerQuery(
      SqlValidatorScope parentScope,
      @Nullable SqlValidatorScope usingScope,
      SqlValidatorScope lateralScope,
      SqlNode node,
      SqlNode enclosingNode,
      @Nullable String alias,
      boolean forceNullable) {
    checkArgument(usingScope == null || alias != null);
    registerQuery(
        parentScope,
        usingScope,
        lateralScope,
        node,
        enclosingNode,
        alias,
        forceNullable,
        true);
  }

  /**
   * Registers a query in a parent scope.
   *
   * @param parentScope Parent scope which this scope turns to in order to
   *                    resolve objects
   * @param usingScope  Scope whose child list this scope should add itself to
   * @param node        Query node
   * @param alias       Name of this query within its parent. Must be specified
   *                    if usingScope != null
   * @param checkUpdate if true, validate that the update feature is supported
   *                    if validating the update statement
   */
  private void registerQuery(
      SqlValidatorScope parentScope,
      @Nullable SqlValidatorScope usingScope,
      SqlValidatorScope lateralScope,
      SqlNode node,
      SqlNode enclosingNode,
      @Nullable String alias,
      boolean forceNullable,
      boolean checkUpdate) {
    requireNonNull(node, "node");
    requireNonNull(enclosingNode, "enclosingNode");
    checkArgument(usingScope == null || alias != null);

    SqlCall call;
    List<SqlNode> operands;
    switch (node.getKind()) {
    case SELECT:
      final SqlSelect select = (SqlSelect) node;
      final SelectNamespace selectNs =
          createSelectNamespace(select, enclosingNode);
      registerNamespace(usingScope, alias, selectNs, forceNullable);
      final SqlValidatorScope windowParentScope =
          first(usingScope, parentScope);
      SelectScope selectScope =
          new SelectScope(parentScope, windowParentScope, select);
      scopes.put(select, selectScope);

      // Start by registering the WHERE clause
      clauseScopes.put(IdPair.of(select, SqlValidatorImpl.Clause.WHERE), selectScope);
      registerOperandSubQueries(
          selectScope,
          select,
          SqlSelect.WHERE_OPERAND);

      // Register subqueries in the QUALIFY clause
      registerOperandSubQueries(
          selectScope,
          select,
          SqlSelect.QUALIFY_OPERAND);

      // Register FROM with the inherited scope 'parentScope', not
      // 'selectScope', otherwise tables in the FROM clause would be
      // able to see each other.
      final SqlNode from = select.getFrom();
      if (from != null) {
        final SqlNode newFrom =
            registerFrom(
                parentScope,
                selectScope,
                lateralScope,
                true,
                from,
                from,
                null,
                null,
                false,
                false);
        if (newFrom != from) {
          select.setFrom(newFrom);
        }
      }

      // If this is an aggregate query, the SELECT list and HAVING
      // clause use a different scope, where you can only reference
      // columns which are in the GROUP BY clause.
      final SqlValidatorScope selectScope2 =
          isAggregate(select)
              ? new AggregatingSelectScope(selectScope, select, false)
              : selectScope;
      clauseScopes.put(IdPair.of(select, SqlValidatorImpl.Clause.SELECT), selectScope2);
      clauseScopes.put(IdPair.of(select, SqlValidatorImpl.Clause.MEASURE),
          new MeasureScope(selectScope, select));
      if (select.getGroup() != null) {
        GroupByScope groupByScope =
            new GroupByScope(selectScope, select.getGroup(), select);
        clauseScopes.put(IdPair.of(select, SqlValidatorImpl.Clause.GROUP_BY), groupByScope);
        registerSubQueries(groupByScope, select.getGroup());
      }
      registerOperandSubQueries(
          selectScope2,
          select,
          SqlSelect.HAVING_OPERAND);
      registerSubQueries(selectScope2,
          SqlNonNullableAccessors.getSelectList(select));
      final SqlNodeList orderList = select.getOrderList();
      if (orderList != null) {
        // If the query is 'SELECT DISTINCT', restrict the columns
        // available to the ORDER BY clause.
        final SqlValidatorScope selectScope3 =
            select.isDistinct()
                ? new AggregatingSelectScope(selectScope, select, true)
                : selectScope2;
        OrderByScope orderScope =
            new OrderByScope(selectScope3, orderList, select);
        clauseScopes.put(IdPair.of(select, SqlValidatorImpl.Clause.ORDER), orderScope);
        registerSubQueries(orderScope, orderList);

        if (!isAggregate(select)) {
          // Since this is not an aggregate query,
          // there cannot be any aggregates in the ORDER BY clause.
          SqlNode agg = aggFinder.findAgg(orderList);
          if (agg != null) {
            throw newValidationError(agg, RESOURCE.aggregateIllegalInOrderBy());
          }
        }
      }
      break;

    case INTERSECT:
      validateFeature(RESOURCE.sQLFeature_F302(), node.getParserPosition());
      registerSetop(
          parentScope,
          usingScope,
          lateralScope,
          node,
          node,
          alias,
          forceNullable);
      break;

    case EXCEPT:
      validateFeature(RESOURCE.sQLFeature_E071_03(), node.getParserPosition());
      registerSetop(
          parentScope,
          usingScope,
          lateralScope,
          node,
          node,
          alias,
          forceNullable);
      break;

    case UNION:
      registerSetop(
          parentScope,
          usingScope,
          lateralScope,
          node,
          enclosingNode,
          alias,
          forceNullable);
      break;

    case LAMBDA:
      call = (SqlCall) node;
      SqlLambdaScope lambdaScope =
          new SqlLambdaScope(parentScope, (SqlLambda) call);
      scopes.put(call, lambdaScope);
      final LambdaNamespace lambdaNamespace =
          new LambdaNamespace(this, (SqlLambda) call, node);
      registerNamespace(
          usingScope,
          alias,
          lambdaNamespace,
          forceNullable);
      operands = call.getOperandList();
      for (int i = 0; i < operands.size(); i++) {
        registerOperandSubQueries(parentScope, call, i);
      }
      break;

    case WITH:
      registerWith(parentScope, usingScope, lateralScope, (SqlWith) node, enclosingNode,
          alias, forceNullable, checkUpdate);
      break;

    case VALUES:
      call = (SqlCall) node;
      scopes.put(call, parentScope);
      final TableConstructorNamespace tableConstructorNamespace =
          new TableConstructorNamespace(
              this,
              call,
              parentScope,
              enclosingNode);
      registerNamespace(
          usingScope,
          alias,
          tableConstructorNamespace,
          forceNullable);
      operands = call.getOperandList();
      for (int i = 0; i < operands.size(); ++i) {
        assert operands.get(i).getKind() == SqlKind.ROW;

        // FIXME jvs 9-Feb-2005:  Correlation should
        // be illegal in these sub-queries.  Same goes for
        // any non-lateral SELECT in the FROM list.
        registerOperandSubQueries(parentScope, call, i);
      }
      break;

    case INSERT:
      SqlInsert insertCall = (SqlInsert) node;
      SqlValidatorImpl.InsertNamespace insertNs =
          new SqlValidatorImpl.InsertNamespace(
              this,
              insertCall,
              enclosingNode,
              parentScope);
      registerNamespace(usingScope, null, insertNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          lateralScope,
          insertCall.getSource(),
          enclosingNode,
          null,
          false);
      break;

    case DELETE:
      SqlDelete deleteCall = (SqlDelete) node;
      SqlValidatorImpl.DeleteNamespace deleteNs =
          new SqlValidatorImpl.DeleteNamespace(
              this,
              deleteCall,
              enclosingNode,
              parentScope);
      registerNamespace(usingScope, null, deleteNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          lateralScope,
          SqlNonNullableAccessors.getSourceSelect(deleteCall),
          enclosingNode,
          null,
          false);
      break;

    case UPDATE:
      if (checkUpdate) {
        validateFeature(RESOURCE.sQLFeature_E101_03(),
            node.getParserPosition());
      }
      SqlUpdate updateCall = (SqlUpdate) node;
      SqlValidatorImpl.UpdateNamespace updateNs =
          new SqlValidatorImpl.UpdateNamespace(
              this,
              updateCall,
              enclosingNode,
              parentScope);
      registerNamespace(usingScope, null, updateNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          lateralScope,
          SqlNonNullableAccessors.getSourceSelect(updateCall),
          enclosingNode,
          null,
          false);
      break;

    case MERGE:
      validateFeature(RESOURCE.sQLFeature_F312(), node.getParserPosition());
      SqlMerge mergeCall = (SqlMerge) node;
      SqlValidatorImpl.MergeNamespace mergeNs =
          new SqlValidatorImpl.MergeNamespace(
              this,
              mergeCall,
              enclosingNode,
              parentScope);
      registerNamespace(usingScope, null, mergeNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          lateralScope,
          SqlNonNullableAccessors.getSourceSelect(mergeCall),
          enclosingNode,
          null,
          false);

      // update call can reference either the source table reference
      // or the target table, so set its parent scope to the merge's
      // source select; when validating the update, skip the feature
      // validation check
      SqlUpdate mergeUpdateCall = mergeCall.getUpdateCall();
      if (mergeUpdateCall != null) {
        registerQuery(
            getScope(SqlNonNullableAccessors.getSourceSelect(mergeCall), SqlValidatorImpl.Clause.WHERE),
            null,
            lateralScope,
            mergeUpdateCall,
            enclosingNode,
            null,
            false,
            false);
      }
      SqlInsert mergeInsertCall = mergeCall.getInsertCall();
      if (mergeInsertCall != null) {
        registerQuery(
            parentScope,
            null,
            lateralScope,
            mergeInsertCall,
            enclosingNode,
            null,
            false);
      }
      break;

    case UNNEST:
      call = (SqlCall) node;
      final UnnestNamespace unnestNs =
          new UnnestNamespace(this, call, parentScope, enclosingNode);
      registerNamespace(
          usingScope,
          alias,
          unnestNs,
          forceNullable);
      registerOperandSubQueries(parentScope, call, 0);
      scopes.put(node, parentScope);
      break;
    case OTHER_FUNCTION:
      call = (SqlCall) node;
      ProcedureNamespace procNs =
          new ProcedureNamespace(
              this,
              parentScope,
              call,
              enclosingNode);
      registerNamespace(
          usingScope,
          alias,
          procNs,
          forceNullable);
      registerSubQueries(parentScope, call);
      break;

    case MULTISET_QUERY_CONSTRUCTOR:
    case MULTISET_VALUE_CONSTRUCTOR:
      validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition());
      call = (SqlCall) node;
      CollectScope cs = new CollectScope(parentScope, usingScope, call);
      final CollectNamespace tableConstructorNs =
          new CollectNamespace(call, cs, enclosingNode);
      final String alias2 = SqlValidatorUtil.alias(node, nextGeneratedId++);
      registerNamespace(
          usingScope,
          alias2,
          tableConstructorNs,
          forceNullable);
      operands = call.getOperandList();
      for (int i = 0; i < operands.size(); i++) {
        registerOperandSubQueries(parentScope, call, i);
      }
      break;

    default:
      throw Util.unexpected(node.getKind());
    }
  }

  private void registerSetop(
      SqlValidatorScope parentScope,
      @Nullable SqlValidatorScope usingScope,
      SqlValidatorScope lateralScope,
      SqlNode node,
      SqlNode enclosingNode,
      @Nullable String alias,
      boolean forceNullable) {
    SqlCall call = (SqlCall) node;
    final SetopNamespace setopNamespace =
        createSetopNamespace(call, enclosingNode);
    registerNamespace(usingScope, alias, setopNamespace, forceNullable);

    // A setop is in the same scope as its parent.
    scopes.put(call, parentScope);
    @NonNull SqlValidatorScope recursiveScope = parentScope;
    if (enclosingNode.getKind() == SqlKind.WITH_ITEM) {
      if (node.getKind() != SqlKind.UNION) {
        throw newValidationError(node, RESOURCE.recursiveWithMustHaveUnionSetOp());
      } else if (call.getOperandList().size() > 2) {
        throw newValidationError(node, RESOURCE.recursiveWithMustHaveTwoChildUnionSetOp());
      }
      final WithScope scope = (WithScope) scopes.get(enclosingNode);
      // recursive scope is only set for the recursive queries.
      recursiveScope = scope != null && scope.recursiveScope != null
          ? Objects.requireNonNull(scope.recursiveScope) : parentScope;
    }
    for (int i = 0; i < call.getOperandList().size(); i++) {
      SqlNode operand = call.getOperandList().get(i);
      @NonNull SqlValidatorScope scope = i == 0 ? parentScope : recursiveScope;
      registerQuery(
          scope,
          null,
          lateralScope,
          operand,
          operand,
          null,
          false);
    }
  }

  private void registerWith(
      SqlValidatorScope parentScope,
      @Nullable SqlValidatorScope usingScope,
      SqlValidatorScope lateralScope,
      SqlWith with,
      SqlNode enclosingNode,
      @Nullable String alias,
      boolean forceNullable,
      boolean checkUpdate) {
    final WithNamespace withNamespace =
        new WithNamespace(this, with, enclosingNode);
    registerNamespace(usingScope, alias, withNamespace, forceNullable);
    scopes.put(with, parentScope);

    SqlValidatorScope scope = parentScope;
    for (SqlNode withItem_ : with.withList) {
      final SqlWithItem withItem = (SqlWithItem) withItem_;

      final boolean isRecursiveWith = withItem.recursive.booleanValue();
      final SqlValidatorScope withScope =
          new WithScope(scope, withItem,
              isRecursiveWith ? new WithRecursiveScope(scope, withItem) : null);
      scopes.put(withItem, withScope);

      registerQuery(scope, null, lateralScope, withItem.query,
          withItem.recursive.booleanValue() ? withItem : with, withItem.name.getSimple(),
          forceNullable);
      registerNamespace(null, alias,
          new WithItemNamespace(this, withItem, enclosingNode),
          false);
      scope = withScope;
    }
    registerQuery(scope, null, lateralScope, with.body, enclosingNode, alias, forceNullable,
        checkUpdate);
  }
}
