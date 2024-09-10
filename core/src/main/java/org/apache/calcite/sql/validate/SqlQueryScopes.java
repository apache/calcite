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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Util;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Contains mapping of SqlNodes to SqlValidatorScope.
 *
 * <p>The methods {@link #getSelectScope}, {@link #getFromScope},
 * {@link #getWhereScope}, {@link #getGroupScope}, {@link #getHavingScope},
 * {@link #getOrderScope} and {@link #getJoinScope} get the correct scope
 * to resolve names in a particular clause of a SQL statement.
 */
public interface SqlQueryScopes {
  SqlValidatorScope getCursorScope(SqlSelect select);

  /**
   * Returns a scope containing the objects visible from the FROM clause of a
   * query.
   *
   * @param select SELECT statement
   * @return naming scope for FROM clause
   */
  SqlValidatorScope getFromScope(SqlSelect select);

  /**
   * Returns a scope containing the objects visible from the GROUP BY clause
   * of a query.
   *
   * @param select SELECT statement
   * @return naming scope for GROUP BY clause
   */
  SqlValidatorScope getGroupScope(SqlSelect select);

  /**
   * Returns a scope containing the objects visible from the HAVING clause of
   * a query.
   *
   * @param select SELECT statement
   * @return naming scope for HAVING clause
   */
  SqlValidatorScope getHavingScope(SqlSelect select);

  /**
   * Returns a scope containing the objects visible from the ON and USING
   * sections of a JOIN clause.
   *
   * @param node The item in the FROM clause which contains the ON or USING
   *             expression
   * @return naming scope for JOIN clause
   * @see #getFromScope
   */
  SqlValidatorScope getJoinScope(SqlNode node);

  /**
   * Returns the lambda expression scope.
   *
   * @param node Lambda expression
   * @return naming scope for lambda expression
   */
  SqlValidatorScope getLambdaScope(SqlLambda node);

  /**
   * Returns a scope match recognize clause.
   *
   * @param node Match recognize
   * @return naming scope for Match recognize clause
   */
  SqlValidatorScope getMatchRecognizeScope(SqlMatchRecognize node);

  SqlValidatorScope getMeasureScope(SqlSelect select);

  /**
   * Returns the scope that expressions in the SELECT and HAVING clause of
   * this query should use. This scope consists of the FROM clause and the
   * enclosing scope. If the query is aggregating, only columns in the GROUP
   * BY clause may be used.
   *
   * @param select SELECT statement
   * @return naming scope for ORDER BY clause
   */
  SqlValidatorScope getOrderScope(SqlSelect select);

  /**
   * Returns the scope of an OVER or VALUES node.
   *
   * @param node Node
   * @return Scope
   */
  SqlValidatorScope getOverScope(SqlNode node);

  /**
   * Returns the appropriate scope for validating a particular clause of a
   * SELECT statement.
   *
   * <p>Consider
   *
   * <blockquote><pre><code>SELECT *
   * FROM foo
   * WHERE EXISTS (
   *    SELECT deptno AS x
   *    FROM emp
   *       JOIN dept ON emp.deptno = dept.deptno
   *    WHERE emp.deptno = 5
   *    GROUP BY deptno
   *    ORDER BY x)</code></pre></blockquote>
   *
   * <p>What objects can be seen in each part of the sub-query?
   *
   * <ul>
   * <li>In FROM ({@link #getFromScope} , you can only see 'foo'.
   *
   * <li>In WHERE ({@link #getWhereScope}), GROUP BY ({@link #getGroupScope}),
   * SELECT ({@code getSelectScope}), and the ON clause of the JOIN
   * ({@link #getJoinScope}) you can see 'emp', 'dept', and 'foo'.
   *
   * <li>In ORDER BY ({@link #getOrderScope}), you can see the column alias 'x';
   * and tables 'emp', 'dept', and 'foo'.
   *
   * </ul>
   *
   * @param select SELECT statement
   * @return naming scope for SELECT statement
   */
  SqlValidatorScope getSelectScope(SqlSelect select);
  /**
   * Returns the scope that expressions in the WHERE and GROUP BY clause of
   * this query should use. This scope consists of the tables in the FROM
   * clause, and the enclosing scope.
   *
   * @param select Query
   * @return naming scope of WHERE clause
   */
  SqlValidatorScope getWhereScope(SqlSelect select);
  SqlValidatorScope getWithScope(SqlNode withItem);

  SqlValidatorScope getScopeOrThrow(SqlNode node);

  @Nullable SqlValidatorScope getScope(SqlNode sqlNode);

  SelectScope getRawSelectScopeNonNull(SqlSelect select);

  @Nullable
  TableScope getTableScope();


  /**
   * Finds the namespace corresponding to a given node.
   *
   * <p>For example, in the query <code>SELECT * FROM (SELECT * FROM t), t1 AS
   * alias</code>, the both items in the FROM clause have a corresponding
   * namespace.
   *
   * @param node Parse tree node
   * @return namespace of node
   */
  @Nullable SqlValidatorNamespace getNamespace(SqlNode node);

  /**
   * Returns the scope for resolving the SELECT, GROUP BY and HAVING clauses.
   * Always a {@link SelectScope}; if this is an aggregation query, the
   * {@link AggregatingScope} is stripped away.
   *
   * @param select SELECT statement
   * @return naming scope for SELECT statement, sans any aggregating scope
   */
  @Nullable SelectScope getRawSelectScope(SqlSelect select);


  /**
   * Namespace for the given node.
   *
   * @param node node to compute the namespace for
   * @param scope namespace scope
   * @return namespace for the given node, never null
   * @see #getNamespace(SqlNode)
   */
  @API(since = "1.27", status = API.Status.INTERNAL)
  SqlValidatorNamespace getNamespaceOrThrow(SqlNode node, SqlValidatorScope scope);

  /**
   * Namespace for the given node.
   *
   * @param id identifier to resolve
   * @param scope namespace scope
   * @return namespace for the given node, never null
   * @see SqlQueryScopesImpl#getNamespace(SqlIdentifier, DelegatingScope)
   */
  @API(since = "1.26", status = API.Status.INTERNAL)
  SqlValidatorNamespace getNamespaceOrThrow(SqlIdentifier id, @Nullable DelegatingScope scope);

  /**
   * Namespace for the given node.
   *
   * @param node node to compute the namespace for
   * @return namespace for the given node, never null
   * @see #getNamespace(SqlNode)
   */
  @API(since = "1.27", status = API.Status.INTERNAL)
  SqlValidatorNamespace getNamespaceOrThrow(SqlNode node);


  /** Allows {@link SqlQueryScopesImpl}.clauseScopes to have multiple values per SELECT. */
  enum Clause {
    WHERE,
    GROUP_BY,
    SELECT,
    MEASURE,
    ORDER,
    CURSOR,
    HAVING,
    QUALIFY;

    /**
     * Determines if the extender should replace aliases with expanded values.
     * For example:
     *
     * <blockquote><pre>{@code
     * SELECT a + a as twoA
     * GROUP BY twoA
     * }</pre></blockquote>
     *
     * <p>turns into
     *
     * <blockquote><pre>{@code
     * SELECT a + a as twoA
     * GROUP BY a + a
     * }</pre></blockquote>
     *
     * <p>This is determined both by the clause and the config.
     *
     * @param config The configuration
     * @return Whether we should replace the alias with its expanded value
     */
    public boolean shouldReplaceAliases(SqlValidator.Config config) {
      switch (this) {
      case GROUP_BY:
        return config.conformance().isGroupByAlias();

      case HAVING:
        return config.conformance().isHavingAlias();

      case QUALIFY:
        return true;

      default:
        throw Util.unexpected(this);
      }
    }
  }
}
