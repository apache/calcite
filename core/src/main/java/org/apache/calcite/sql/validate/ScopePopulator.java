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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPivot;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnpivot;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.sql.SqlUtil.stripAs;
import static org.apache.calcite.util.Static.RESOURCE;
import static org.apache.calcite.util.Util.first;

import static java.util.Objects.requireNonNull;

/**
 * Populates {@link ScopeMap}.
 */
public class ScopePopulator {

  private final ScopeMapImpl scopeMap;
  private final NamespaceBuilder namespaceBuilder;
  private final SqlCluster sqlCluster;
  private final ValidatorAggStuff validatorAggStuff;
  private final boolean identifierExpansion;

  public ScopePopulator(
      ScopeMapImpl scopeMap,
      NamespaceBuilder namespaceBuilder,
      SqlCluster sqlCluster,
      ValidatorAggStuff validatorAggStuff,
      boolean identifierExpansion) {
    this.scopeMap = scopeMap;
    this.namespaceBuilder = namespaceBuilder;
    this.sqlCluster = sqlCluster;
    this.validatorAggStuff = validatorAggStuff;
    this.identifierExpansion = identifierExpansion;
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
  public void registerQuery(
      SqlValidatorScope parentScope,
      @Nullable SqlValidatorScope usingScope,
      SqlNode node,
      SqlNode enclosingNode,
      @Nullable String alias,
      boolean forceNullable) {
    checkArgument(usingScope == null || alias != null);
    registerQuery(
        parentScope,
        usingScope,
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
          namespaceBuilder.createSelectNamespace(select, enclosingNode);
      scopeMap.registerNamespace(usingScope, alias, selectNs, forceNullable);
      final SqlValidatorScope windowParentScope =
          first(usingScope, parentScope);
      SelectScope selectScope =
          new SelectScope(parentScope, windowParentScope, select);
      scopeMap.putScope(select, selectScope);

      // Start by registering the WHERE clause
      scopeMap.putWhereScope(select, selectScope);
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
          validatorAggStuff.isAggregate(select)
              ? new AggregatingSelectScope(selectScope, select, false)
              : selectScope;
      scopeMap.putSelectScope(select, selectScope2);
      scopeMap.putMeasureScope(select, new MeasureScope(selectScope, select));
      if (select.getGroup() != null) {
        GroupByScope groupByScope =
            new GroupByScope(selectScope, select.getGroup(), select);
        scopeMap.putGroupByScope(select, groupByScope);
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
        scopeMap.putOrderScope(select, orderScope);
        registerSubQueries(orderScope, orderList);

        if (!validatorAggStuff.isAggregate(select)) {
          // Since this is not an aggregate query,
          // there cannot be any aggregates in the ORDER BY clause.
          SqlNode agg = validatorAggStuff.findAgg(orderList);
          if (agg != null) {
            throw sqlCluster.newValidationError(agg, RESOURCE.aggregateIllegalInOrderBy());
          }
        }
      }
      break;

    case INTERSECT:
      sqlCluster.validateFeature(RESOURCE.sQLFeature_F302(), node.getParserPosition());
      registerSetop(
          parentScope,
          usingScope,
          node,
          node,
          alias,
          forceNullable);
      break;

    case EXCEPT:
      sqlCluster.validateFeature(RESOURCE.sQLFeature_E071_03(), node.getParserPosition());
      registerSetop(
          parentScope,
          usingScope,
          node,
          node,
          alias,
          forceNullable);
      break;

    case UNION:
      registerSetop(
          parentScope,
          usingScope,
          node,
          enclosingNode,
          alias,
          forceNullable);
      break;

    case LAMBDA:
      call = (SqlCall) node;
      SqlLambdaScope lambdaScope =
          new SqlLambdaScope(parentScope, (SqlLambda) call);
      scopeMap.putScope(call, lambdaScope);
      final LambdaNamespace lambdaNamespace =
          namespaceBuilder.createLambdaNamespace((SqlLambda) call, node);
      scopeMap.registerNamespace(
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
      registerWith(parentScope, usingScope, (SqlWith) node, enclosingNode,
          alias, forceNullable, checkUpdate);
      break;

    case VALUES:
      call = (SqlCall) node;
      scopeMap.putScope(call, parentScope);
      final TableConstructorNamespace tableConstructorNamespace =
          namespaceBuilder.createTableConstructorNamespace(
              call,
              parentScope,
              enclosingNode);
      scopeMap.registerNamespace(
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
      NamespaceBuilder.DmlNamespace insertNs =
          namespaceBuilder.createInsertNamespace(
              insertCall,
              enclosingNode,
              parentScope);
      scopeMap.registerNamespace(usingScope, null, insertNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          insertCall.getSource(),
          enclosingNode,
          null,
          false);
      break;

    case DELETE:
      SqlDelete deleteCall = (SqlDelete) node;
      NamespaceBuilder.DmlNamespace deleteNs =
          namespaceBuilder.createDeleteNamespace(
              deleteCall,
              enclosingNode,
              parentScope);
      scopeMap.registerNamespace(usingScope, null, deleteNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          SqlNonNullableAccessors.getSourceSelect(deleteCall),
          enclosingNode,
          null,
          false);
      break;

    case UPDATE:
      if (checkUpdate) {
        sqlCluster.validateFeature(RESOURCE.sQLFeature_E101_03(),
            node.getParserPosition());
      }
      SqlUpdate updateCall = (SqlUpdate) node;
      NamespaceBuilder.DmlNamespace updateNs =
          namespaceBuilder.createUpdate(
              updateCall,
              enclosingNode,
              parentScope);
      scopeMap.registerNamespace(usingScope, null, updateNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          SqlNonNullableAccessors.getSourceSelect(updateCall),
          enclosingNode,
          null,
          false);
      break;

    case MERGE:
      sqlCluster.validateFeature(RESOURCE.sQLFeature_F312(), node.getParserPosition());
      SqlMerge mergeCall = (SqlMerge) node;
      NamespaceBuilder.DmlNamespace mergeNs =
          namespaceBuilder.createMergeNamespace(
              mergeCall,
              enclosingNode,
              parentScope);
      scopeMap.registerNamespace(usingScope, null, mergeNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
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
            scopeMap.getWhereScope(SqlNonNullableAccessors.getSourceSelect(mergeCall)),
            null,
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
            mergeInsertCall,
            enclosingNode,
            null,
            false);
      }
      break;

    case UNNEST:
      call = (SqlCall) node;
      final UnnestNamespace unnestNs =
          namespaceBuilder.createUnnestNamespace(call, parentScope, enclosingNode);
      scopeMap.registerNamespace(
          usingScope,
          alias,
          unnestNs,
          forceNullable);
      registerOperandSubQueries(parentScope, call, 0);
      scopeMap.putScope(node, parentScope);
      break;
    case OTHER_FUNCTION:
      call = (SqlCall) node;
      ProcedureNamespace procNs =
          namespaceBuilder.createProcedureNamespace(
              parentScope,
              call,
              enclosingNode);
      scopeMap.registerNamespace(
          usingScope,
          alias,
          procNs,
          forceNullable);
      registerSubQueries(parentScope, call);
      break;

    case MULTISET_QUERY_CONSTRUCTOR:
    case MULTISET_VALUE_CONSTRUCTOR:
      sqlCluster.validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition());
      call = (SqlCall) node;
      CollectScope cs = new CollectScope(parentScope, usingScope, call);
      final CollectNamespace tableConstructorNs =
          new CollectNamespace(call, cs, enclosingNode);
      final String alias2 =
          SqlValidatorUtil.alias(node, sqlCluster.getAndIncermentNextGeneratedId());
      scopeMap.registerNamespace(
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
      SqlNode node,
      SqlNode enclosingNode,
      @Nullable String alias,
      boolean forceNullable) {
    SqlCall call = (SqlCall) node;
    final SetopNamespace setopNamespace =
        namespaceBuilder.createSetopNamespace(call, enclosingNode);
    scopeMap.registerNamespace(usingScope, alias, setopNamespace, forceNullable);

    // A setop is in the same scope as its parent.
    scopeMap.putScope(call, parentScope);
    @NonNull SqlValidatorScope recursiveScope = parentScope;
    if (enclosingNode.getKind() == SqlKind.WITH_ITEM) {
      if (node.getKind() != SqlKind.UNION) {
        throw sqlCluster.newValidationError(node, RESOURCE.recursiveWithMustHaveUnionSetOp());
      } else if (call.getOperandList().size() > 2) {
        throw sqlCluster.newValidationError(
            node, RESOURCE.recursiveWithMustHaveTwoChildUnionSetOp());
      }
      final WithScope scope = (WithScope) scopeMap.getScope(enclosingNode);
      // recursive scope is only set for the recursive queries.
      recursiveScope = scope != null && scope.recursiveScope != null
          ? requireNonNull(scope.recursiveScope) : parentScope;
    }
    for (int i = 0; i < call.getOperandList().size(); i++) {
      SqlNode operand = call.getOperandList().get(i);
      @NonNull SqlValidatorScope scope = i == 0 ? parentScope : recursiveScope;
      registerQuery(
          scope,
          null,
          operand,
          operand,
          null,
          false);
    }
  }

  private void registerWith(
      SqlValidatorScope parentScope,
      @Nullable SqlValidatorScope usingScope,
      SqlWith with,
      SqlNode enclosingNode,
      @Nullable String alias,
      boolean forceNullable,
      boolean checkUpdate) {
    final WithNamespace withNamespace =
        namespaceBuilder.createWithNamespace(with, enclosingNode);
    scopeMap.registerNamespace(usingScope, alias, withNamespace, forceNullable);
    scopeMap.putScope(with, parentScope);

    SqlValidatorScope scope = parentScope;
    for (SqlNode withItem_ : with.withList) {
      final SqlWithItem withItem = (SqlWithItem) withItem_;

      final boolean isRecursiveWith = withItem.recursive.booleanValue();
      final SqlValidatorScope withScope =
          new WithScope(scope, withItem,
              isRecursiveWith ? new WithRecursiveScope(scope, withItem) : null);
      scopeMap.putScope(withItem, withScope);

      registerQuery(scope, null, withItem.query,
          withItem.recursive.booleanValue() ? withItem : with, withItem.name.getSimple(),
          forceNullable);
      scopeMap.registerNamespace(null, alias,
          namespaceBuilder.createWithItemNamespace(withItem, enclosingNode),
          false);
      scope = withScope;
    }
    registerQuery(scope, null, with.body, enclosingNode, alias, forceNullable,
        checkUpdate);
  }


  /**
   * Registers scopes and namespaces implied a relational expression in the
   * FROM clause.
   *
   * <p>{@code parentScope0} and {@code usingScope} are often the same. They
   * differ when the namespace are not visible within the parent. (Example
   * needed.)
   *
   * <p>Likewise, {@code enclosingNode} and {@code node} are often the same.
   * {@code enclosingNode} is the topmost node within the FROM clause, from
   * which any decorations like an alias (<code>AS alias</code>) or a table
   * sample clause are stripped away to get {@code node}. Both are recorded in
   * the namespace.
   *
   * @param parentScope0  Parent scope that this scope turns to in order to
   *                      resolve objects
   * @param usingScope    Scope whose child list this scope should add itself to
   * @param register      Whether to register this scope as a child of
   *                      {@code usingScope}
   * @param node          Node which namespace is based on
   * @param enclosingNode Outermost node for namespace, including decorations
   *                      such as alias and sample clause
   * @param alias         Alias
   * @param extendList    Definitions of extended columns
   * @param forceNullable Whether to force the type of namespace to be
   *                      nullable because it is in an outer join
   * @param lateral       Whether LATERAL is specified, so that items to the
   *                      left of this in the JOIN tree are visible in the
   *                      scope
   * @return registered node, usually the same as {@code node}
   */
  // CHECKSTYLE: OFF
  // CheckStyle thinks this method is too long
  private SqlNode registerFrom(
      SqlValidatorScope parentScope0,
      SqlValidatorScope usingScope,
      boolean register,
      final SqlNode node,
      SqlNode enclosingNode,
      @Nullable String alias,
      @Nullable SqlNodeList extendList,
      boolean forceNullable,
      final boolean lateral) {
    final SqlKind kind = node.getKind();

    SqlNode expr;
    SqlNode newExpr;

    // Add an alias if necessary.
    SqlNode newNode = node;
    if (alias == null) {
      switch (kind) {
      case IDENTIFIER:
      case OVER:
        alias = SqlValidatorUtil.alias(node);
        if (alias == null) {
          alias = SqlValidatorUtil.alias(node, sqlCluster.getAndIncermentNextGeneratedId());
        }
        if (identifierExpansion) {
          newNode = SqlValidatorUtil.addAlias(node, alias);
        }
        break;

      case SELECT:
      case UNION:
      case INTERSECT:
      case EXCEPT:
      case VALUES:
      case UNNEST:
      case OTHER_FUNCTION:
      case COLLECTION_TABLE:
      case PIVOT:
      case UNPIVOT:
      case MATCH_RECOGNIZE:
      case WITH:
        // give this anonymous construct a name since later
        // query processing stages rely on it
        alias = SqlValidatorUtil.alias(node, sqlCluster.getAndIncermentNextGeneratedId());
        if (identifierExpansion) {
          // Since we're expanding identifiers, we should make the
          // aliases explicit too, otherwise the expanded query
          // will not be consistent if we convert back to SQL, e.g.
          // "select EXPR$1.EXPR$2 from values (1)".
          newNode = SqlValidatorUtil.addAlias(node, alias);
        }
        break;
      default:
        break;
      }
    }

    final SqlValidatorScope parentScope;
    if (lateral) {
      SqlValidatorScope s = usingScope;
      while (s instanceof JoinScope) {
        s = ((JoinScope) s).getUsingScope();
      }
      final SqlNode node2 = s != null ? s.getNode() : node;
      final TableScope tableScope = new TableScope(parentScope0, node2);
      if (usingScope instanceof ListScope) {
        for (ScopeChild child : ((ListScope) usingScope).children) {
          tableScope.addChild(child.namespace, child.name, child.nullable);
        }
      }
      parentScope = tableScope;
    } else {
      parentScope = parentScope0;
    }

    SqlCall call;
    SqlNode operand;
    SqlNode newOperand;

    switch (kind) {
    case AS:
      call = (SqlCall) node;
      if (alias == null) {
        alias = String.valueOf(call.operand(1));
      }
      expr = call.operand(0);
      final boolean needAliasNamespace = call.operandCount() > 2
          || expr.getKind() == SqlKind.VALUES || expr.getKind() == SqlKind.UNNEST
          || expr.getKind() == SqlKind.COLLECTION_TABLE;
      newExpr =
          registerFrom(
              parentScope,
              usingScope,
              !needAliasNamespace,
              expr,
              enclosingNode,
              alias,
              extendList,
              forceNullable,
              lateral);
      if (newExpr != expr) {
        call.setOperand(0, newExpr);
      }

      // If alias has a column list, introduce a namespace to translate
      // column names. We skipped registering it just now.
      if (needAliasNamespace) {
        scopeMap.registerNamespace(
            usingScope,
            alias,
            namespaceBuilder.createAliasNamespace(call, enclosingNode),
            forceNullable);
      }
      return node;

    case MATCH_RECOGNIZE:
      registerMatchRecognize(parentScope, usingScope,
          (SqlMatchRecognize) node, enclosingNode, alias, forceNullable);
      return node;

    case PIVOT:
      registerPivot(parentScope, usingScope, (SqlPivot) node, enclosingNode,
          alias, forceNullable);
      return node;

    case UNPIVOT:
      registerUnpivot(parentScope, usingScope, (SqlUnpivot) node, enclosingNode,
          alias, forceNullable);
      return node;

    case TABLESAMPLE:
      call = (SqlCall) node;
      expr = call.operand(0);
      newExpr =
          registerFrom(
              parentScope,
              usingScope,
              true,
              expr,
              enclosingNode,
              alias,
              extendList,
              forceNullable,
              lateral);
      if (newExpr != expr) {
        call.setOperand(0, newExpr);
      }
      return node;

    case JOIN:
      final SqlJoin join = (SqlJoin) node;
      final JoinScope joinScope =
          new JoinScope(parentScope, usingScope, join);
      scopeMap.putScope(join, joinScope);
      final SqlNode left = join.getLeft();
      final SqlNode right = join.getRight();
      boolean forceLeftNullable = forceNullable;
      boolean forceRightNullable = forceNullable;
      switch (join.getJoinType()) {
      case LEFT:
      case LEFT_ASOF:
        forceRightNullable = true;
        break;
      case RIGHT:
        forceLeftNullable = true;
        break;
      case FULL:
        forceLeftNullable = true;
        forceRightNullable = true;
        break;
      default:
        break;
      }
      final SqlNode newLeft =
          registerFrom(
              parentScope,
              joinScope,
              true,
              left,
              left,
              null,
              null,
              forceLeftNullable,
              lateral);
      if (newLeft != left) {
        join.setLeft(newLeft);
      }
      final SqlNode newRight =
          registerFrom(
              parentScope,
              joinScope,
              true,
              right,
              right,
              null,
              null,
              forceRightNullable,
              lateral);
      if (newRight != right) {
        join.setRight(newRight);
      }
      scopeMap.putIfAbsent(stripAs(join.getRight()), parentScope);
      scopeMap.putIfAbsent(stripAs(join.getLeft()), parentScope);
      registerSubQueries(joinScope, join.getCondition());
      final JoinNamespace joinNamespace = namespaceBuilder.createJoinNamespace(join);
      scopeMap.registerNamespace(null, null, joinNamespace, forceNullable);
      return join;

    case IDENTIFIER:
      final SqlIdentifier id = (SqlIdentifier) node;
      final IdentifierNamespace newNs =
          namespaceBuilder.createIdentifierNamespace(id, extendList, enclosingNode,
              parentScope);
      scopeMap.registerNamespace(register ? usingScope : null, alias, newNs,
          forceNullable);
      @Nullable TableScope tableScope = scopeMap.getTableScope();
      if (tableScope == null) {
        tableScope = new TableScope(parentScope, node);
        scopeMap.setTableScope(tableScope);
      }
      tableScope.addChild(newNs, requireNonNull(alias, "alias"), forceNullable);
      if (extendList != null && !extendList.isEmpty()) {
        return enclosingNode;
      }
      return newNode;

    case LATERAL:
      return registerFrom(
          parentScope,
          usingScope,
          register,
          ((SqlCall) node).operand(0),
          enclosingNode,
          alias,
          extendList,
          forceNullable,
          true);

    case COLLECTION_TABLE:
      call = (SqlCall) node;
      operand = call.operand(0);
      newOperand =
          registerFrom(
              parentScope,
              usingScope,
              register,
              operand,
              enclosingNode,
              alias,
              extendList,
              forceNullable, lateral);
      if (newOperand != operand) {
        call.setOperand(0, newOperand);
      }
      // If the operator is SqlWindowTableFunction, restricts the scope as
      // its first operand's (the table) scope.
      if (operand instanceof SqlBasicCall) {
        final SqlBasicCall call1 = (SqlBasicCall) operand;
        final SqlOperator op = call1.getOperator();
        if (op instanceof SqlWindowTableFunction
            && call1.operand(0).getKind() == SqlKind.SELECT) {
          scopeMap.putScope(node, scopeMap.getSelectScope(call1.operand(0)));
          return newNode;
        }
      }
      // Put the usingScope which can be a JoinScope
      // or a SelectScope, in order to see the left items
      // of the JOIN tree.
      scopeMap.putScope(node, usingScope);
      return newNode;

    case UNNEST:
      if (!lateral) {
        return registerFrom(parentScope, usingScope, register, node,
            enclosingNode, alias, extendList, forceNullable, true);
      }
      // fall through
    case SELECT:
    case UNION:
    case INTERSECT:
    case EXCEPT:
    case VALUES:
    case WITH:
    case OTHER_FUNCTION:
      if (alias == null) {
        alias = SqlValidatorUtil.alias(node, sqlCluster.getAndIncermentNextGeneratedId());
      }
      registerQuery(
          parentScope,
          register ? usingScope : null,
          node,
          enclosingNode,
          alias,
          forceNullable);
      return newNode;

    case OVER:
      if (!shouldAllowOverRelation()) {
        throw Util.unexpected(kind);
      }
      call = (SqlCall) node;
      final OverScope overScope = new OverScope(usingScope, call);
      scopeMap.putScope(call, overScope);
      operand = call.operand(0);
      newOperand =
          registerFrom(
              parentScope,
              overScope,
              true,
              operand,
              enclosingNode,
              alias,
              extendList,
              forceNullable,
              lateral);
      if (newOperand != operand) {
        call.setOperand(0, newOperand);
      }

      for (ScopeChild child : overScope.children) {
        scopeMap.registerNamespace(register ? usingScope : null, child.name,
            child.namespace, forceNullable);
      }

      return newNode;

    case TABLE_REF:
      call = (SqlCall) node;
      registerFrom(parentScope,
          usingScope,
          register,
          call.operand(0),
          enclosingNode,
          alias,
          extendList,
          forceNullable,
          lateral);
      if (extendList != null && !extendList.isEmpty()) {
        return enclosingNode;
      }
      return newNode;

    case EXTEND:
      final SqlCall extend = (SqlCall) node;
      return registerFrom(parentScope,
          usingScope,
          true,
          extend.getOperandList().get(0),
          extend,
          alias,
          (SqlNodeList) extend.getOperandList().get(1),
          forceNullable,
          lateral);

    case SNAPSHOT:
      call = (SqlCall) node;
      operand = call.operand(0);
      newOperand =
          registerFrom(parentScope,
              usingScope,
              register,
              operand,
              enclosingNode,
              alias,
              extendList,
              forceNullable,
              lateral);
      if (newOperand != operand) {
        call.setOperand(0, newOperand);
      }
      // Put the usingScope which can be a JoinScope
      // or a SelectScope, in order to see the left items
      // of the JOIN tree.
      scopeMap.putScope(node, usingScope);
      return newNode;

    default:
      throw Util.unexpected(kind);
    }
  }
  // CHECKSTYLE: ON

  protected boolean shouldAllowOverRelation() {
    return false;
  }



  private void validateNodeFeature(SqlNode node) {
    switch (node.getKind()) {
    case MULTISET_VALUE_CONSTRUCTOR:
      sqlCluster.validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition());
      break;
    default:
      break;
    }
  }

  private void registerSubQueries(
      SqlValidatorScope parentScope,
      @Nullable SqlNode node) {
    if (node == null) {
      return;
    }
    if (node.getKind().belongsTo(SqlKind.QUERY)
        || node.getKind() == SqlKind.LAMBDA
        || node.getKind() == SqlKind.MULTISET_QUERY_CONSTRUCTOR
        || node.getKind() == SqlKind.MULTISET_VALUE_CONSTRUCTOR) {
      registerQuery(parentScope, null, node, node, null, false);
    } else if (node instanceof SqlCall) {
      validateNodeFeature(node);
      SqlCall call = (SqlCall) node;
      for (int i = 0; i < call.operandCount(); i++) {
        registerOperandSubQueries(parentScope, call, i);
      }
    } else if (node instanceof SqlNodeList) {
      SqlNodeList list = (SqlNodeList) node;
      for (int i = 0, count = list.size(); i < count; i++) {
        SqlNode listNode = list.get(i);
        if (listNode.getKind().belongsTo(SqlKind.QUERY)) {
          listNode =
              SqlStdOperatorTable.SCALAR_QUERY.createCall(
                  listNode.getParserPosition(),
                  listNode);
          list.set(i, listNode);
        }
        registerSubQueries(parentScope, listNode);
      }
    } else {
      // atomic node -- can be ignored
    }
  }


  private void registerMatchRecognize(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlMatchRecognize call,
      SqlNode enclosingNode,
      @Nullable String alias,
      boolean forceNullable) {

    final MatchRecognizeNamespace matchRecognizeNamespace =
        namespaceBuilder.createMatchRecognizeNameSpace(call, enclosingNode);
    scopeMap.registerNamespace(usingScope, alias, matchRecognizeNamespace, forceNullable);

    final MatchRecognizeScope matchRecognizeScope =
        new MatchRecognizeScope(parentScope, call);
    scopeMap.putScope(call, matchRecognizeScope);

    // parse input query
    SqlNode expr = call.getTableRef();
    SqlNode newExpr =
        registerFrom(usingScope, matchRecognizeScope, true, expr,
            expr, null, null, forceNullable, false);
    if (expr != newExpr) {
      call.setOperand(0, newExpr);
    }
  }

  private void registerPivot(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlPivot pivot,
      SqlNode enclosingNode,
      @Nullable String alias,
      boolean forceNullable) {
    final PivotNamespace namespace = namespaceBuilder.createPivotNameSpace(pivot, enclosingNode);
    scopeMap.registerNamespace(usingScope, alias, namespace, forceNullable);

    final SqlValidatorScope scope =
        new PivotScope(parentScope, pivot);
    scopeMap.putScope(pivot, scope);

    // parse input query
    SqlNode expr = pivot.query;
    SqlNode newExpr =
        registerFrom(parentScope, scope, true, expr,
            expr, null, null, forceNullable, false);
    if (expr != newExpr) {
      pivot.setOperand(0, newExpr);
    }
  }

  private void registerUnpivot(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlUnpivot call,
      SqlNode enclosingNode,
      @Nullable String alias,
      boolean forceNullable) {
    final UnpivotNamespace namespace =
        namespaceBuilder.createUnpivotNameSpace(call, enclosingNode);
    scopeMap.registerNamespace(usingScope, alias, namespace, forceNullable);

    final SqlValidatorScope scope =
        new UnpivotScope(parentScope, call);
    scopeMap.putScope(call, scope);

    // parse input query
    SqlNode expr = call.query;
    SqlNode newExpr =
        registerFrom(parentScope, scope, true, expr,
            expr, null, null, forceNullable, false);
    if (expr != newExpr) {
      call.setOperand(0, newExpr);
    }
  }


  /**
   * Registers any sub-queries inside a given call operand, and converts the
   * operand to a scalar sub-query if the operator requires it.
   *
   * @param parentScope    Parent scope
   * @param call           Call
   * @param operandOrdinal Ordinal of operand within call
   * @see SqlOperator#argumentMustBeScalar(int)
   */
  private void registerOperandSubQueries(
      SqlValidatorScope parentScope,
      SqlCall call,
      int operandOrdinal) {
    SqlNode operand = call.operand(operandOrdinal);
    if (operand == null) {
      return;
    }
    if (operand.getKind().belongsTo(SqlKind.QUERY)
        && call.getOperator().argumentMustBeScalar(operandOrdinal)) {
      operand =
          SqlStdOperatorTable.SCALAR_QUERY.createCall(
              operand.getParserPosition(),
              operand);
      call.setOperand(operandOrdinal, operand);
    }
    registerSubQueries(parentScope, operand);
  }

}
