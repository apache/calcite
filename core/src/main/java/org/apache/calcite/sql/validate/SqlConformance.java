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

import org.apache.calcite.sql.fun.SqlLibrary;

/**
 * Enumeration of valid SQL compatibility modes.
 *
 * <p>For most purposes, one of the built-in compatibility modes in enum
 * {@link SqlConformanceEnum} will suffice.
 *
 * <p>If you wish to implement this interface to build your own conformance,
 * we strongly recommend that you extend {@link SqlAbstractConformance},
 * or use a {@link SqlDelegatingConformance},
 * so that you won't be broken by future changes.
 *
 * @see SqlConformanceEnum
 * @see SqlAbstractConformance
 * @see SqlDelegatingConformance
 */
public interface SqlConformance {
  /** Short-cut for {@link SqlConformanceEnum#DEFAULT}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum DEFAULT = SqlConformanceEnum.DEFAULT;
  /** Short-cut for {@link SqlConformanceEnum#STRICT_92}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum STRICT_92 = SqlConformanceEnum.STRICT_92;
  /** Short-cut for {@link SqlConformanceEnum#STRICT_99}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum STRICT_99 = SqlConformanceEnum.STRICT_99;
  /** Short-cut for {@link SqlConformanceEnum#PRAGMATIC_99}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum PRAGMATIC_99 = SqlConformanceEnum.PRAGMATIC_99;
  /** Short-cut for {@link SqlConformanceEnum#ORACLE_10}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum ORACLE_10 = SqlConformanceEnum.ORACLE_10;
  /** Short-cut for {@link SqlConformanceEnum#STRICT_2003}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum STRICT_2003 = SqlConformanceEnum.STRICT_2003;
  /** Short-cut for {@link SqlConformanceEnum#PRAGMATIC_2003}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum PRAGMATIC_2003 = SqlConformanceEnum.PRAGMATIC_2003;

  /**
   * Whether this dialect supports features from a wide variety of
   * dialects. This is enabled for the Babel parser, disabled otherwise.
   */
  boolean isLiberal();

  /**
   * Whether this dialect allows character literals as column aliases.
   *
   * <p>For example,
   *
   * <blockquote><pre>
   *   SELECT empno, sal + comm AS 'remuneration'
   *   FROM Emp</pre></blockquote>
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#BIG_QUERY},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#MYSQL_5},
   * {@link SqlConformanceEnum#SQL_SERVER_2008};
   * false otherwise.
   */
  boolean allowCharLiteralAlias();

  /**
   * Whether to allow aliases from the {@code SELECT} clause to be used as
   * column names in the {@code GROUP BY} clause.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#BIG_QUERY},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#MYSQL_5};
   * false otherwise.
   */
  boolean isGroupByAlias();

  /**
   * Whether {@code GROUP BY 2} is interpreted to mean 'group by the 2nd column
   * in the select list'.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#BIG_QUERY},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#MYSQL_5},
   * {@link SqlConformanceEnum#PRESTO};
   * false otherwise.
   */
  boolean isGroupByOrdinal();

  /**
   * Whether to allow aliases from the {@code SELECT} clause to be used as
   * column names in the {@code HAVING} clause.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#BIG_QUERY},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#MYSQL_5};
   * false otherwise.
   */
  boolean isHavingAlias();

  /**
   * Whether '{@code ORDER BY 2}' is interpreted to mean 'sort by the 2nd
   * column in the select list'.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#DEFAULT},
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#MYSQL_5},
   * {@link SqlConformanceEnum#ORACLE_10},
   * {@link SqlConformanceEnum#ORACLE_12},
   * {@link SqlConformanceEnum#PRAGMATIC_99},
   * {@link SqlConformanceEnum#PRAGMATIC_2003},
   * {@link SqlConformanceEnum#PRESTO},
   * {@link SqlConformanceEnum#SQL_SERVER_2008},
   * {@link SqlConformanceEnum#STRICT_92};
   * false otherwise.
   */
  boolean isSortByOrdinal();

  /**
   * Whether '{@code ORDER BY x}' is interpreted to mean 'sort by the select
   * list item whose alias is x' even if there is a column called x.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#DEFAULT},
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#BIG_QUERY},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#MYSQL_5},
   * {@link SqlConformanceEnum#ORACLE_10},
   * {@link SqlConformanceEnum#ORACLE_12},
   * {@link SqlConformanceEnum#SQL_SERVER_2008},
   * {@link SqlConformanceEnum#STRICT_92};
   * false otherwise.
   */
  boolean isSortByAlias();

  /**
   * Whether "empno" is invalid in "select empno as x from emp order by empno"
   * because the alias "x" obscures it.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#STRICT_92};
   * false otherwise.
   */
  boolean isSortByAliasObscures();

  /**
   * Whether {@code FROM} clause is required in a {@code SELECT} statement.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#ORACLE_10},
   * {@link SqlConformanceEnum#ORACLE_12},
   * {@link SqlConformanceEnum#STRICT_92},
   * {@link SqlConformanceEnum#STRICT_99},
   * {@link SqlConformanceEnum#STRICT_2003};
   * false otherwise.
   */
  boolean isFromRequired();

  /**
   * Whether to split a quoted table name. If true, {@code `x.y.z`} is parsed as
   * if the user had written {@code `x`.`y`.`z`}.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BIG_QUERY};
   * false otherwise.
   */
  boolean splitQuotedTableName();

  /**
   * Whether to allow hyphens in an unquoted table name.
   *
   * <p>If true, {@code SELECT * FROM foo-bar.baz-buzz} is valid, and is parsed
   * as if the user had written {@code SELECT * FROM `foo-bar`.`baz-buzz`}.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BIG_QUERY};
   * false otherwise.
   */
  boolean allowHyphenInUnquotedTableName();

  /**
   * Whether the bang-equal token != is allowed as an alternative to &lt;&gt; in
   * the parser.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#MYSQL_5},
   * {@link SqlConformanceEnum#ORACLE_10},
   * {@link SqlConformanceEnum#ORACLE_12},
   * {@link SqlConformanceEnum#PRESTO};
   * false otherwise.
   */
  boolean isBangEqualAllowed();

  /**
   * Whether the "%" operator is allowed by the parser as an alternative to the
   * {@code mod} function.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#MYSQL_5},
   * {@link SqlConformanceEnum#PRESTO};
   * false otherwise.
   */
  boolean isPercentRemainderAllowed();

  /**
   * Whether {@code MINUS} is allowed as an alternative to {@code EXCEPT} in
   * the parser.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#ORACLE_10},
   * {@link SqlConformanceEnum#ORACLE_12};
   * false otherwise.
   *
   * <p>Note: MySQL does not support {@code MINUS} or {@code EXCEPT} (as of
   * version 5.5).
   */
  boolean isMinusAllowed();

  /**
   * Whether {@code CROSS APPLY} and {@code OUTER APPLY} operators are allowed
   * in the parser.
   *
   * <p>{@code APPLY} invokes a table-valued function for each row returned
   * by a table expression. It is syntactic sugar:<ul>
   *
   * <li>{@code SELECT * FROM emp CROSS APPLY TABLE(promote(empno)}<br>
   * is equivalent to<br>
   * {@code SELECT * FROM emp CROSS JOIN LATERAL TABLE(promote(empno)}
   *
   * <li>{@code SELECT * FROM emp OUTER APPLY TABLE(promote(empno)}<br>
   * is equivalent to<br>
   * {@code SELECT * FROM emp LEFT JOIN LATERAL TABLE(promote(empno)} ON true
   * </ul>
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#ORACLE_12},
   * {@link SqlConformanceEnum#SQL_SERVER_2008};
   * false otherwise.
   */
  boolean isApplyAllowed();

  /**
   * Whether to allow {@code INSERT} (or {@code UPSERT}) with no column list
   * but fewer values than the target table.
   *
   * <p>The N values provided are assumed to match the first N columns of the
   * table, and for each of the remaining columns, the default value of the
   * column is used. It is an error if any of these columns has no default
   * value.
   *
   * <p>The default value of a column is specified by the {@code DEFAULT}
   * clause in the {@code CREATE TABLE} statement, or is {@code NULL} if the
   * column is not declared {@code NOT NULL}.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#PRAGMATIC_99},
   * {@link SqlConformanceEnum#PRAGMATIC_2003};
   * false otherwise.
   */
  boolean isInsertSubsetColumnsAllowed();

  /**
   * Whether directly alias array items in UNNEST.
   *
   * <p>E.g. in UNNEST(a_array, b_array) AS T(a, b),
   * a and b will be aliases of elements in a_array and b_array
   * respectively.
   *
   * <p>Without this flag set, T will be the alias
   * of the element in a_array and a, b will be the top level
   * fields of T if T is a STRUCT type.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#PRESTO};
   * false otherwise.
   */
  boolean allowAliasUnnestItems();

  /**
   * Whether to allow parentheses to be specified in calls to niladic functions
   * and procedures (that is, functions and procedures with no parameters).
   *
   * <p>For example, {@code CURRENT_DATE} is a niladic system function. In
   * standard SQL it must be invoked without parentheses:
   *
   * <blockquote><code>VALUES CURRENT_DATE</code></blockquote>
   *
   * <p>If {@code allowNiladicParentheses}, the following syntax is also valid:
   *
   * <blockquote><code>VALUES CURRENT_DATE()</code></blockquote>
   *
   * <p>Of the popular databases, MySQL, Apache Phoenix and VoltDB allow this
   * behavior;
   * Apache Hive, HSQLDB, IBM DB2, Microsoft SQL Server, Oracle, PostgreSQL do
   * not.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#MYSQL_5};
   * false otherwise.
   */
  boolean allowNiladicParentheses();

  /**
   * Whether to allow SQL syntax "{@code ROW(expr1, expr2, expr3)}".
   * <p>The equivalent syntax in standard SQL is
   * "{@code (expr1, expr2, expr3)}".
   *
   * <p>Standard SQL does not allow this because the type is not
   * well-defined. However, PostgreSQL allows this behavior.
   *
   * <p>Standard SQL allows row expressions in other contexts, for instance
   * inside {@code VALUES} clause.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#DEFAULT},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#PRESTO};
   * false otherwise.
   */
  boolean allowExplicitRowValueConstructor();

  /**
   * Whether to allow mixing table columns with extended columns in
   * {@code INSERT} (or {@code UPSERT}).
   *
   * <p>For example, suppose that the declaration of table {@code T} has columns
   * {@code A} and {@code B}, and you want to insert data of column
   * {@code C INTEGER} not present in the table declaration as an extended
   * column. You can specify the columns in an {@code INSERT} statement as
   * follows:
   *
   * <blockquote>
   *   <code>INSERT INTO T (A, B, C INTEGER) VALUES (1, 2, 3)</code>
   * </blockquote>
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT};
   * false otherwise.
   */
  boolean allowExtend();

  /**
   * Whether to allow the SQL syntax "{@code LIMIT start, count}".
   *
   * <p>The equivalent syntax in standard SQL is
   * "{@code OFFSET start ROW FETCH FIRST count ROWS ONLY}",
   * and in PostgreSQL "{@code LIMIT count OFFSET start}".
   *
   * <p>MySQL and CUBRID allow this behavior.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#MYSQL_5};
   * false otherwise.
   */
  boolean isLimitStartCountAllowed();

  /**
   * Whether to allow the SQL syntax "{@code OFFSET start LIMIT count}"
   * (that is, {@code OFFSET} before {@code LIMIT},
   * in addition to {@code LIMIT} before {@code OFFSET}
   * and {@code OFFSET} before {@code FETCH}).
   *
   * <p>The equivalent syntax in standard SQL is
   * "{@code OFFSET start ROW FETCH FIRST count ROWS ONLY}".
   *
   * <p>Trino allows this behavior.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT};
   * false otherwise.
   */
  boolean isOffsetLimitAllowed();

  /**
   * Whether to allow geo-spatial extensions, including the GEOMETRY type.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#MYSQL_5},
   * {@link SqlConformanceEnum#PRESTO},
   * {@link SqlConformanceEnum#SQL_SERVER_2008};
   * false otherwise.
   */
  boolean allowGeometry();

  /**
   * Whether the least restrictive type of a number of CHAR types of different
   * lengths should be a VARCHAR type. And similarly BINARY to VARBINARY.
   *
   * <p>For example, consider the query
   *
   * <blockquote><pre>SELECT 'abcde' UNION SELECT 'xyz'</pre></blockquote>
   *
   * <p>The input columns have types {@code CHAR(5)} and {@code CHAR(3)}, and
   * we need a result type that is large enough for both:
   * <ul>
   * <li>Under strict SQL:2003 behavior, its column has type {@code CHAR(5)},
   *     and the value in the second row will have trailing spaces.
   * <li>With lenient behavior, its column has type {@code VARCHAR(5)}, and the
   *     values have no trailing spaces.
   * </ul>
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#PRAGMATIC_99},
   * {@link SqlConformanceEnum#PRAGMATIC_2003},
   * {@link SqlConformanceEnum#MYSQL_5},
   * {@link SqlConformanceEnum#ORACLE_10},
   * {@link SqlConformanceEnum#ORACLE_12},
   * {@link SqlConformanceEnum#PRESTO},
   * {@link SqlConformanceEnum#SQL_SERVER_2008};
   * false otherwise.
   */
  boolean shouldConvertRaggedUnionTypesToVarying();

  /**
   * Whether TRIM should support more than one trim character.
   *
   * <p>For example, consider the query
   *
   * <blockquote><pre>SELECT TRIM('eh' FROM 'hehe__hehe')</pre></blockquote>
   *
   * <p>Under strict behavior, if the length of trim character is not 1,
   * TRIM throws an exception, and the query fails.
   * However many implementations (in databases such as MySQL and SQL Server)
   * trim all the characters, resulting in a return value of '__'.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT},
   * {@link SqlConformanceEnum#MYSQL_5},
   * {@link SqlConformanceEnum#SQL_SERVER_2008};
   * false otherwise.
   */
  boolean allowExtendedTrim();

  /**
   * Whether interval literals should allow plural time units
   * such as "YEARS" and "DAYS" in interval literals.
   *
   * <p>Under strict behavior, {@code INTERVAL '2' DAY} is valid
   * and {@code INTERVAL '2' DAYS} is invalid;
   * PostgreSQL allows both; Oracle only allows singular time units.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#BABEL},
   * {@link SqlConformanceEnum#LENIENT};
   * false otherwise.
   */
  boolean allowPluralTimeUnits();

  /**
   * Whether to allow a qualified common column in a query that has a
   * NATURAL join or a join with a USING clause.
   *
   * <p>For example, in the query
   *
   * <blockquote><pre>SELECT emp.deptno
   * FROM emp
   * JOIN dept USING (deptno)</pre></blockquote>
   *
   * <p>{@code deptno} is the common column. A qualified common column
   * such as {@code emp.deptno} is not allowed in Oracle, but is allowed
   * in PostgreSQL.
   *
   * <p>Among the built-in conformance levels, false in
   * {@link SqlConformanceEnum#ORACLE_10},
   * {@link SqlConformanceEnum#ORACLE_12},
   * {@link SqlConformanceEnum#PRESTO},
   * {@link SqlConformanceEnum#STRICT_92},
   * {@link SqlConformanceEnum#STRICT_99},
   * {@link SqlConformanceEnum#STRICT_2003};
   * true otherwise.
   */
  boolean allowQualifyingCommonColumn();

  /**
   * Controls the behavior of operators that are part of Standard SQL but
   * nevertheless have different behavior in different databases.
   *
   * <p>Consider the {@code SUBSTRING} operator. In ISO standard SQL, negative
   * start indexes are converted to 1; in Google BigQuery, negative start
   * indexes are treated as offsets from the end of the string. For example,
   * {@code SUBSTRING('abcde' FROM -3 FOR 2)} returns {@code 'ab'} in standard
   * SQL and 'cd' in BigQuery.
   *
   * <p>If you specify {@code conformance=BIG_QUERY} in your connection
   * parameters, {@code SUBSTRING} will give the BigQuery behavior. Similarly
   * MySQL and Oracle.
   *
   * <p>Among the built-in conformance levels:
   * <ul>
   * <li>{@link SqlConformanceEnum#BIG_QUERY} returns
   *     {@link SqlLibrary#BIG_QUERY};
   * <li>{@link SqlConformanceEnum#MYSQL_5} returns {@link SqlLibrary#MYSQL};
   * <li>{@link SqlConformanceEnum#ORACLE_10} and
   *     {@link SqlConformanceEnum#ORACLE_12} return {@link SqlLibrary#ORACLE};
   * <li>otherwise returns {@link SqlLibrary#STANDARD}.
   * </ul>
   */
  SqlLibrary semantics();
}
