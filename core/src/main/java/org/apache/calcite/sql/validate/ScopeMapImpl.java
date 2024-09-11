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

import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.IdPair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import static org.apache.calcite.sql.SqlUtil.stripAs;

import static java.util.Objects.requireNonNull;

/**
 * An implementation of SqlQueryScopes with helper methods for building up the mapping.
 */
public class ScopeMapImpl implements ScopeMap {

  private final SqlValidatorCatalogReader catalogReader;
  /**
   * Maps {@link SqlNode query node} objects to the {@link SqlValidatorScope}
   * scope created from them.
   */
  protected final IdentityHashMap<SqlNode, SqlValidatorScope> scopes =
      new IdentityHashMap<>();


  /**
   * Maps a {@link SqlSelect} and a {@link Clause} to the scope used by that
   * clause.
   */
  private final Map<IdPair<SqlSelect, Clause>, SqlValidatorScope>
      clauseScopes = new HashMap<>();

  /**
   * The name-resolution scope of a LATERAL TABLE clause.
   */
  private @Nullable TableScope tableScope = null;


  /**
   * Maps a {@link SqlNode node} to the
   * {@link SqlValidatorNamespace namespace} which describes what columns they
   * contain.
   */
  protected final IdentityHashMap<SqlNode, SqlValidatorNamespace> namespaces =
      new IdentityHashMap<>();

  public ScopeMapImpl(SqlValidatorCatalogReader catalogReader) {
    this.catalogReader = catalogReader;
  }

  @Override public SqlValidatorScope getCursorScope(SqlSelect select) {
    return getScope(select, Clause.CURSOR);
  }

  public @Nullable SqlValidatorScope putCursorScope(
      SqlSelect select, SelectScope selectScope) {
    return putScope(select, Clause.CURSOR, selectScope);
  }

  @Override public SqlValidatorScope getFromScope(SqlSelect select) {
    return requireNonNull(scopes.get(select),
        () -> "no scope for " + select);
  }

  @Override public SqlValidatorScope getGroupScope(SqlSelect select) {
    // Yes, it's the same as getWhereScope
    return getScope(select, Clause.WHERE);
  }


  public @Nullable SqlValidatorScope putGroupByScope(
      SqlSelect select, SqlValidatorScope sqlValidatorScope) {
    // Why is the group by, but the get is where?
    return putScope(select, Clause.GROUP_BY, sqlValidatorScope);
  }


  @Override public SqlValidatorScope getHavingScope(SqlSelect select) {
    // Yes, it's the same as getSelectScope
    return getScope(select, Clause.SELECT);
  }

  @Override public SqlValidatorScope getJoinScope(SqlNode node) {
    return requireNonNull(scopes.get(stripAs(node)),
        () -> "scope for " + node);
  }

  @Override public SqlValidatorScope getLambdaScope(SqlLambda node) {
    return getScopeOrThrow(node);
  }

  @Override public SqlValidatorScope getMatchRecognizeScope(SqlMatchRecognize node) {
    return getScopeOrThrow(node);
  }

  @Override public SqlValidatorScope getMeasureScope(SqlSelect select) {
    return getScope(select, Clause.MEASURE);
  }

  public @Nullable SqlValidatorScope putMeasureScope(
      SqlSelect select, SqlValidatorScope sqlValidatorScope) {
    return putScope(select, Clause.MEASURE, sqlValidatorScope);
  }

  @Override public SqlValidatorScope getOrderScope(SqlSelect select) {
    return getScope(select, Clause.ORDER);
  }

  public @Nullable SqlValidatorScope putOrderScope(
      SqlSelect select, SqlValidatorScope sqlValidatorScope) {
    return putScope(select, Clause.ORDER, sqlValidatorScope);
  }

  @Override public SqlValidatorScope getOverScope(SqlNode node) {
    return getScopeOrThrow(node);
  }

  @Override public SqlValidatorScope getSelectScope(SqlSelect select) {
    return getScope(select, Clause.SELECT);
  }

  public @Nullable SqlValidatorScope putSelectScope(
      SqlSelect select, SqlValidatorScope sqlValidatorScope) {
    return putScope(select, Clause.SELECT, sqlValidatorScope);
  }

  @Override public SqlValidatorScope getWhereScope(SqlSelect select) {
    return getScope(select, Clause.WHERE);
  }

  public @Nullable SqlValidatorScope putWhereScope(
      SqlSelect sqlNode, SelectScope selectScope) {
    return putScope(sqlNode, Clause.WHERE, selectScope);
  }

  @Override public SqlValidatorScope getWithScope(SqlNode withItem) {
    assert withItem.getKind() == SqlKind.WITH_ITEM;
    return getScopeOrThrow(withItem);
  }

  @Override public SqlValidatorScope getScopeOrThrow(SqlNode node) {
    return requireNonNull(scopes.get(node), () -> "scope for " + node);
  }

  @Override public @Nullable SqlValidatorScope getScope(SqlNode sqlNode) {
    return scopes.get(sqlNode);
  }

  public @Nullable SqlValidatorScope putScope(SqlNode sqlNode,
      SqlValidatorScope sqlValidatorScope) {
    return scopes.put(sqlNode, sqlValidatorScope);
  }

  public @Nullable SqlValidatorScope putIfAbsent(SqlNode sqlNode,
      SqlValidatorScope sqlValidatorScope) {
    return scopes.putIfAbsent(sqlNode, sqlValidatorScope);
  }

  private @Nullable SqlValidatorScope putScope(SqlSelect select, Clause clause,
      SqlValidatorScope sqlValidatorScope) {
    return clauseScopes.put(IdPair.of(select, clause), sqlValidatorScope);
  }

  @Override public @Nullable SelectScope getRawSelectScope(SqlSelect select) {
    SqlValidatorScope scope = clauseScopes.get(IdPair.of(select, Clause.SELECT));
    if (scope instanceof AggregatingSelectScope) {
      scope = ((AggregatingSelectScope) scope).getParent();
    }
    return (SelectScope) scope;
  }

  @Override public SelectScope getRawSelectScopeNonNull(SqlSelect select) {
    return requireNonNull(getRawSelectScope(select),
        () -> "getRawSelectScope for " + select);
  }

  @Override public @Nullable TableScope getTableScope() {
    return tableScope;
  }

  public void setTableScope(@Nullable TableScope tableScope) {
    this.tableScope = tableScope;
  }


  /**
   * Registers a new namespace, and adds it as a child of its parent scope.
   * Derived class can override this method to tinker with namespaces as they
   * are created.
   *
   * @param usingScope    Parent scope (which will want to look for things in
   *                      this namespace)
   * @param alias         Alias by which parent will refer to this namespace
   * @param ns            Namespace
   * @param forceNullable Whether to force the type of namespace to be nullable
   */
  public void registerNamespace(
      @Nullable SqlValidatorScope usingScope,
      @Nullable String alias,
      SqlValidatorNamespace ns,
      boolean forceNullable) {
    SqlValidatorNamespace namespace =
        namespaces.get(requireNonNull(ns.getNode(), () -> "ns.getNode() for " + ns));
    if (namespace == null) {
      namespaces.put(requireNonNull(ns.getNode()), ns);
      namespace = ns;
    }
    if (usingScope != null) {
      if (alias == null) {
        throw new IllegalArgumentException("Registering namespace " + ns
            + ", into scope " + usingScope + ", so alias must not be null");
      }
      usingScope.addChild(namespace, alias, forceNullable);
    }
  }


  @Override public @Nullable SqlValidatorNamespace getNamespace(SqlNode node) {
    switch (node.getKind()) {
    case AS:

      // AS has a namespace if it has a column list 'AS t (c1, c2, ...)'
      final SqlValidatorNamespace ns = namespaces.get(node);
      if (ns != null) {
        return ns;
      }
      // fall through
    case TABLE_REF:
    case SNAPSHOT:
    case OVER:
    case COLLECTION_TABLE:
    case ORDER_BY:
    case TABLESAMPLE:
      return getNamespace(((SqlCall) node).operand(0));
    default:
      return namespaces.get(node);
    }
  }

  public @Nullable SqlValidatorNamespace getNamespace(SqlNode node,
      SqlValidatorScope scope) {
    if (node instanceof SqlIdentifier && scope instanceof DelegatingScope) {
      final SqlIdentifier id = (SqlIdentifier) node;
      final DelegatingScope idScope =
          (DelegatingScope) ((DelegatingScope) scope).getParent();
      return getNamespace(id, idScope);
    } else if (node instanceof SqlCall) {
      // Handle extended identifiers.
      final SqlCall call = (SqlCall) node;
      switch (call.getOperator().getKind()) {
      case TABLE_REF:
        return getNamespace(call.operand(0), scope);
      case EXTEND:
        final SqlNode operand0 = call.getOperandList().get(0);
        final SqlIdentifier identifier = operand0.getKind() == SqlKind.TABLE_REF
            ? ((SqlCall) operand0).operand(0)
            : (SqlIdentifier) operand0;
        final DelegatingScope idScope = (DelegatingScope) scope;
        return getNamespace(identifier, idScope);
      case AS:
        final SqlNode nested = call.getOperandList().get(0);
        switch (nested.getKind()) {
        case TABLE_REF:
        case EXTEND:
          return getNamespace(nested, scope);
        default:
          break;
        }
        break;
      default:
        break;
      }
    }
    return getNamespace(node);
  }

  public @Nullable SqlValidatorNamespace getNamespace(SqlIdentifier id,
      @Nullable DelegatingScope scope) {
    if (id.isSimple()) {
      final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
      final SqlValidatorScope.ResolvedImpl resolved =
          new SqlValidatorScope.ResolvedImpl();
      requireNonNull(scope, () -> "scope needed to lookup " + id)
          .resolve(id.names, nameMatcher, false, resolved);
      if (resolved.count() == 1) {
        return resolved.only().namespace;
      }
    }
    return getNamespace(id);
  }

  @Override public SqlValidatorNamespace getNamespaceOrThrow(SqlNode node) {
    return requireNonNull(
        getNamespace(node),
        () -> "namespace for " + node);
  }

  @Override public SqlValidatorNamespace getNamespaceOrThrow(SqlNode node,
      SqlValidatorScope scope) {
    return requireNonNull(
        getNamespace(node, scope),
        () -> "namespace for " + node + ", scope " + scope);
  }

  @Override public SqlValidatorNamespace getNamespaceOrThrow(SqlIdentifier id,
      @Nullable DelegatingScope scope) {
    return requireNonNull(
        getNamespace(id, scope),
        () -> "namespace for " + id + ", scope " + scope);
  }

  private SqlValidatorScope getScope(SqlSelect select, Clause clause) {
    return requireNonNull(
        clauseScopes.get(IdPair.of(select, clause)),
        () -> "no " + clause + " scope for " + select);
  }

  public interface Factory {
    Factory DEFAULT = ScopeMapImpl::new;
    ScopeMapImpl create(SqlValidatorCatalogReader catalogReader);
  }
}
