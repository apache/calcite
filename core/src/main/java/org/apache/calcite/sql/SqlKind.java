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

import com.google.common.collect.Sets;

import org.apiguardian.api.API;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

/**
 * Enumerates the possible types of {@link SqlNode}.
 *
 * <p>The values are immutable, canonical constants, so you can use Kinds to
 * find particular types of expressions quickly. To identity a call to a common
 * operator such as '=', use {@link org.apache.calcite.sql.SqlNode#isA}:</p>
 *
 * <blockquote>
 * exp.{@link org.apache.calcite.sql.SqlNode#isA isA}({@link #EQUALS})
 * </blockquote>
 *
 * <p>Only commonly-used nodes have their own type; other nodes are of type
 * {@link #OTHER}. Some of the values, such as {@link #SET_QUERY}, represent
 * aggregates.</p>
 *
 * <p>To quickly choose between a number of options, use a switch statement:</p>
 *
 * <blockquote>
 * <pre>switch (exp.getKind()) {
 * case {@link #EQUALS}:
 *     ...;
 * case {@link #NOT_EQUALS}:
 *     ...;
 * default:
 *     throw new AssertionError("unexpected");
 * }</pre>
 * </blockquote>
 *
 * <p>Note that we do not even have to check that a {@code SqlNode} is a
 * {@link SqlCall}.</p>
 *
 * <p>To identify a category of expressions, use {@code SqlNode.isA} with
 * an aggregate SqlKind. The following expression will return <code>true</code>
 * for calls to '=' and '&gt;=', but <code>false</code> for the constant '5', or
 * a call to '+':</p>
 *
 * <blockquote>
 * <pre>exp.isA({@link #COMPARISON SqlKind.COMPARISON})</pre>
 * </blockquote>
 *
 * <p>RexNode also has a {@code getKind} method; {@code SqlKind} values are
 * preserved during translation from {@code SqlNode} to {@code RexNode}, where
 * applicable.</p>
 *
 * <p>There is no water-tight definition of "common", but that's OK. There will
 * always be operators that don't have their own kind, and for these we use the
 * {@code SqlOperator}. But for really the common ones, e.g. the many places
 * where we are looking for {@code AND}, {@code OR} and {@code EQUALS}, the enum
 * helps.</p>
 *
 * <p>(If we were using Scala, {@link SqlOperator} would be a case
 * class, and we wouldn't need {@code SqlKind}. But we're not.)</p>
 */
public enum SqlKind {
  //~ Static fields/initializers ---------------------------------------------

  // the basics

  /**
   * Expression not covered by any other {@link SqlKind} value.
   *
   * @see #OTHER_FUNCTION
   */
  OTHER,

  /**
   * SELECT statement or sub-query.
   */
  SELECT,

  /**
   * Sql Hint statement.
   */
  HINT,

  /**
   * Table reference.
   */
  TABLE_REF,

  /**
   * JOIN operator or compound FROM clause.
   *
   * <p>A FROM clause with more than one table is represented as if it were a
   * join. For example, "FROM x, y, z" is represented as
   * "JOIN(x, JOIN(x, y))".</p>
   */
  JOIN,

  /** An identifier. */
  IDENTIFIER,

  /** A literal. */
  LITERAL,

  /** Interval qualifier. */
  INTERVAL_QUALIFIER,

  /**
   * Function that is not a special function.
   *
   * @see #FUNCTION
   */
  OTHER_FUNCTION,

  /**
   * Input tables have either row semantics or set semantics.
   * <ul>
   * <li>Row semantics means that the result of the table function is
   * decided on a row-by-row basis.
   * <li>Set semantics means that the outcome of the function depends on how
   * the data is partitioned.
   * When the table function is called from a query, the table parameter can
   * optionally be extended with either a PARTITION BY clause or
   * an ORDER BY clause or both.
   * </ul>
   */
  SET_SEMANTICS_TABLE,

  /** POSITION function. */
  POSITION,

  /** EXPLAIN statement. */
  EXPLAIN,

  /** DESCRIBE SCHEMA statement. */
  DESCRIBE_SCHEMA,

  /** DESCRIBE TABLE statement. */
  DESCRIBE_TABLE,

  /** INSERT statement. */
  INSERT,

  /** DELETE statement. */
  DELETE,

  /** UPDATE statement. */
  UPDATE,

  /** "{@code ALTER scope SET option = value}" statement. */
  SET_OPTION,

  /** A dynamic parameter. */
  DYNAMIC_PARAM,

  /** The DISTINCT keyword of the GROUP BY clause. */
  GROUP_BY_DISTINCT,

  /**
   * ORDER BY clause.
   *
   * @see #DESCENDING
   * @see #NULLS_FIRST
   * @see #NULLS_LAST
   */
  ORDER_BY,

  /** WITH clause. */
  WITH,

  /** Item in WITH clause. */
  WITH_ITEM,

  /** Item expression. */
  ITEM,

  /** {@code UNION} relational operator. */
  UNION,

  /** {@code EXCEPT} relational operator (known as {@code MINUS} in some SQL
   * dialects). */
  EXCEPT,

  /** {@code INTERSECT} relational operator. */
  INTERSECT,

  /** {@code AS} operator. */
  AS,

  /** Argument assignment operator, {@code =>}. */
  ARGUMENT_ASSIGNMENT,

  /** {@code DEFAULT} operator. */
  DEFAULT,

  /** {@code OVER} operator. */
  OVER,

  /** {@code RESPECT NULLS} operator. */
  RESPECT_NULLS("RESPECT NULLS"),

  /** {@code IGNORE NULLS} operator. */
  IGNORE_NULLS("IGNORE NULLS"),

  /** {@code FILTER} operator. */
  FILTER,

  /** {@code WITHIN GROUP} operator. */
  WITHIN_GROUP,

  /** {@code WITHIN DISTINCT} operator. */
  WITHIN_DISTINCT,

  /** Window specification. */
  WINDOW,

  /** MERGE statement. */
  MERGE,

  /** TABLESAMPLE relational operator. */
  TABLESAMPLE,

  /** PIVOT clause. */
  PIVOT,

  /** UNPIVOT clause. */
  UNPIVOT,

  /** MATCH_RECOGNIZE clause. */
  MATCH_RECOGNIZE,

  /** SNAPSHOT operator. */
  SNAPSHOT,

  // binary operators

  /** Arithmetic multiplication operator, "*". */
  TIMES,

  /** Arithmetic division operator, "/". */
  DIVIDE,

  /** Arithmetic remainder operator, "MOD" (and "%" in some dialects). */
  MOD,

  /**
   * Arithmetic plus operator, "+".
   *
   * @see #PLUS_PREFIX
   */
  PLUS,

  /**
   * Arithmetic minus operator, "-".
   *
   * @see #MINUS_PREFIX
   */
  MINUS,

  /**
   * Alternation operator in a pattern expression within a
   * {@code MATCH_RECOGNIZE} clause.
   */
  PATTERN_ALTER,

  /**
   * Concatenation operator in a pattern expression within a
   * {@code MATCH_RECOGNIZE} clause.
   */
  PATTERN_CONCAT,

  // comparison operators

  /** {@code IN} operator. */
  IN,

  /**
   * {@code NOT IN} operator.
   *
   * <p>Only occurs in SqlNode trees. Is expanded to NOT(IN ...) before
   * entering RelNode land.
   */
  NOT_IN("NOT IN"),

  /** Variant of {@code IN} for the Druid adapter. */
  DRUID_IN,

  /** Variant of {@code NOT_IN} for the Druid adapter. */
  DRUID_NOT_IN,

  /** Less-than operator, "&lt;". */
  LESS_THAN("<"),

  /** Greater-than operator, "&gt;". */
  GREATER_THAN(">"),

  /** Less-than-or-equal operator, "&lt;=". */
  LESS_THAN_OR_EQUAL("<="),

  /** Greater-than-or-equal operator, "&gt;=". */
  GREATER_THAN_OR_EQUAL(">="),

  /** Equals operator, "=". */
  EQUALS("="),

  /**
   * Not-equals operator, "&#33;=" or "&lt;&gt;".
   * The latter is standard, and preferred.
   */
  NOT_EQUALS("<>"),

  /** {@code IS DISTINCT FROM} operator. */
  IS_DISTINCT_FROM,

  /** {@code IS NOT DISTINCT FROM} operator. */
  IS_NOT_DISTINCT_FROM,

  /** {@code SEARCH} operator. (Analogous to scalar {@code IN}, used only in
   * RexNode, not SqlNode.) */
  SEARCH,

  /** Logical "OR" operator. */
  OR,

  /** Logical "AND" operator. */
  AND,

  // other infix

  /** Dot. */
  DOT,

  /** {@code OVERLAPS} operator for periods. */
  OVERLAPS,

  /** {@code CONTAINS} operator for periods. */
  CONTAINS,

  /** {@code PRECEDES} operator for periods. */
  PRECEDES,

  /** {@code IMMEDIATELY PRECEDES} operator for periods. */
  IMMEDIATELY_PRECEDES("IMMEDIATELY PRECEDES"),

  /** {@code SUCCEEDS} operator for periods. */
  SUCCEEDS,

  /** {@code IMMEDIATELY SUCCEEDS} operator for periods. */
  IMMEDIATELY_SUCCEEDS("IMMEDIATELY SUCCEEDS"),

  /** {@code EQUALS} operator for periods. */
  PERIOD_EQUALS("EQUALS"),

  /** {@code LIKE} operator. */
  LIKE,

  /** {@code RLIKE} operator. */
  RLIKE,

  /** {@code SIMILAR} operator. */
  SIMILAR,

  /** {@code ~} operator (for POSIX-style regular expressions). */
  POSIX_REGEX_CASE_SENSITIVE,

  /** {@code ~*} operator (for case-insensitive POSIX-style regular
   * expressions). */
  POSIX_REGEX_CASE_INSENSITIVE,

  /** {@code BETWEEN} operator. */
  BETWEEN,

  /** Variant of {@code BETWEEN} for the Druid adapter. */
  DRUID_BETWEEN,

  /** {@code CASE} expression. */
  CASE,

  /** {@code INTERVAL} expression. */
  INTERVAL,

  /** {@code SEPARATOR} expression. */
  SEPARATOR,

  /** {@code NULLIF} operator. */
  NULLIF,

  /** {@code COALESCE} operator. */
  COALESCE,

  /** {@code DECODE} function (Oracle). */
  DECODE,

  /** {@code NVL} function (Oracle). */
  NVL,

  /** {@code GREATEST} function (Oracle). */
  GREATEST,

  /** The two-argument {@code CONCAT} function (Oracle). */
  CONCAT2,

  /** The "IF" function (BigQuery, Hive, Spark). */
  IF,

  /** {@code LEAST} function (Oracle). */
  LEAST,

  /** {@code TIMESTAMP_ADD} function (ODBC, SQL Server, MySQL). */
  TIMESTAMP_ADD,

  /** {@code TIMESTAMP_DIFF} function (ODBC, SQL Server, MySQL). */
  TIMESTAMP_DIFF,

  // prefix operators

  /** Logical {@code NOT} operator. */
  NOT,

  /**
   * Unary plus operator, as in "+1".
   *
   * @see #PLUS
   */
  PLUS_PREFIX,

  /**
   * Unary minus operator, as in "-1".
   *
   * @see #MINUS
   */
  MINUS_PREFIX,

  /** {@code EXISTS} operator. */
  EXISTS,

  /** {@code SOME} quantification operator (also called {@code ANY}). */
  SOME,

  /** {@code ALL} quantification operator. */
  ALL,

  /** {@code VALUES} relational operator. */
  VALUES,

  /**
   * Explicit table, e.g. <code>select * from (TABLE t)</code> or <code>TABLE
   * t</code>. See also {@link #COLLECTION_TABLE}.
   */
  EXPLICIT_TABLE,

  /**
   * Scalar query; that is, a sub-query used in an expression context, and
   * returning one row and one column.
   */
  SCALAR_QUERY,

  /** Procedure call. */
  PROCEDURE_CALL,

  /** New specification. */
  NEW_SPECIFICATION,

  // special functions in MATCH_RECOGNIZE

  /** {@code FINAL} operator in {@code MATCH_RECOGNIZE}. */
  FINAL,

  /** {@code FINAL} operator in {@code MATCH_RECOGNIZE}. */
  RUNNING,

  /** {@code PREV} operator in {@code MATCH_RECOGNIZE}. */
  PREV,

  /** {@code NEXT} operator in {@code MATCH_RECOGNIZE}. */
  NEXT,

  /** {@code FIRST} operator in {@code MATCH_RECOGNIZE}. */
  FIRST,

  /** {@code LAST} operator in {@code MATCH_RECOGNIZE}. */
  LAST,

  /** {@code CLASSIFIER} operator in {@code MATCH_RECOGNIZE}. */
  CLASSIFIER,

  /** {@code MATCH_NUMBER} operator in {@code MATCH_RECOGNIZE}. */
  MATCH_NUMBER,

  /** {@code SKIP TO FIRST} qualifier of restarting point in a
   * {@code MATCH_RECOGNIZE} clause. */
  SKIP_TO_FIRST,

  /** {@code SKIP TO LAST} qualifier of restarting point in a
   * {@code MATCH_RECOGNIZE} clause. */
  SKIP_TO_LAST,

  // postfix operators

  /** {@code DESC} operator in {@code ORDER BY}. A parse tree, not a true
   * expression. */
  DESCENDING,

  /** {@code NULLS FIRST} clause in {@code ORDER BY}. A parse tree, not a true
   * expression. */
  NULLS_FIRST,

  /** {@code NULLS LAST} clause in {@code ORDER BY}. A parse tree, not a true
   * expression. */
  NULLS_LAST,

  /** {@code IS TRUE} operator. */
  IS_TRUE,

  /** {@code IS FALSE} operator. */
  IS_FALSE,

  /** {@code IS NOT TRUE} operator. */
  IS_NOT_TRUE,

  /** {@code IS NOT FALSE} operator. */
  IS_NOT_FALSE,

  /** {@code IS UNKNOWN} operator. */
  IS_UNKNOWN,

  /** {@code IS NULL} operator. */
  IS_NULL,

  /** {@code IS NOT NULL} operator. */
  IS_NOT_NULL,

  /** {@code PRECEDING} qualifier of an interval end-point in a window
   * specification. */
  PRECEDING,

  /** {@code FOLLOWING} qualifier of an interval end-point in a window
   * specification. */
  FOLLOWING,

  /**
   * The field access operator, ".".
   *
   * <p>(Only used at the RexNode level; at
   * SqlNode level, a field-access is part of an identifier.)</p>
   */
  FIELD_ACCESS,

  /**
   * Reference to an input field.
   *
   * <p>(Only used at the RexNode level.)</p>
   */
  INPUT_REF,

  /**
   * Reference to an input field, with a qualified name and an identifier.
   *
   * <p>(Only used at the RexNode level.)</p>
   */
  TABLE_INPUT_REF,

  /**
   * Reference to an input field, with pattern var as modifier.
   *
   * <p>(Only used at the RexNode level.)</p>
   */
  PATTERN_INPUT_REF,
  /**
   * Reference to a sub-expression computed within the current relational
   * operator.
   *
   * <p>(Only used at the RexNode level.)</p>
   */
  LOCAL_REF,

  /**
   * Reference to correlation variable.
   *
   * <p>(Only used at the RexNode level.)</p>
   */
  CORREL_VARIABLE,

  /**
   * the repetition quantifier of a pattern factor in a match_recognize clause.
   */
  PATTERN_QUANTIFIER,

  // functions

  /**
   * The row-constructor function. May be explicit or implicit:
   * {@code VALUES 1, ROW (2)}.
   */
  ROW,

  /**
   * The non-standard constructor used to pass a
   * COLUMN_LIST parameter to a user-defined transform.
   */
  COLUMN_LIST,

  /**
   * The "CAST" operator, and also the PostgreSQL-style infix cast operator
   * "::".
   */
  CAST,

  /**
   * The "NEXT VALUE OF sequence" operator.
   */
  NEXT_VALUE,

  /**
   * The "CURRENT VALUE OF sequence" operator.
   */
  CURRENT_VALUE,

  /** {@code FLOOR} function. */
  FLOOR,

  /** {@code CEIL} function. */
  CEIL,

  /** {@code TRIM} function. */
  TRIM,

  /** {@code LTRIM} function (Oracle). */
  LTRIM,

  /** {@code RTRIM} function (Oracle). */
  RTRIM,

  /** {@code EXTRACT} function. */
  EXTRACT,

  /** {@code ARRAY_CONCAT} function (BigQuery semantics). */
  ARRAY_CONCAT,

  /** {@code ARRAY_REVERSE} function (BigQuery semantics). */
  ARRAY_REVERSE,

  /** {@code REVERSE} function (SQL Server, MySQL). */
  REVERSE,

  /** {@code SUBSTR} function (BigQuery semantics). */
  SUBSTR_BIG_QUERY,

  /** {@code SUBSTR} function (MySQL semantics). */
  SUBSTR_MYSQL,

  /** {@code SUBSTR} function (Oracle semantics). */
  SUBSTR_ORACLE,

  /** {@code SUBSTR} function (PostgreSQL semantics). */
  SUBSTR_POSTGRESQL,

  /** Call to a function using JDBC function syntax. */
  JDBC_FN,

  /** {@code MULTISET} value constructor. */
  MULTISET_VALUE_CONSTRUCTOR,

  /** {@code MULTISET} query constructor. */
  MULTISET_QUERY_CONSTRUCTOR,

  /** {@code JSON} value expression. */
  JSON_VALUE_EXPRESSION,

  /** {@code JSON_ARRAYAGG} aggregate function. */
  JSON_ARRAYAGG,

  /** {@code JSON_OBJECTAGG} aggregate function. */
  JSON_OBJECTAGG,

  /** {@code JSON} type function. */
  JSON_TYPE,

  /** {@code UNNEST} operator. */
  UNNEST,

  /**
   * The "LATERAL" qualifier to relations in the FROM clause.
   */
  LATERAL,

  /**
   * Table operator which converts user-defined transform into a relation, for
   * example, <code>select * from TABLE(udx(x, y, z))</code>. See also the
   * {@link #EXPLICIT_TABLE} prefix operator.
   */
  COLLECTION_TABLE,

  /**
   * Array Value Constructor, e.g. {@code Array[1, 2, 3]}.
   */
  ARRAY_VALUE_CONSTRUCTOR,

  /**
   * Array Query Constructor, e.g. {@code Array(select deptno from dept)}.
   */
  ARRAY_QUERY_CONSTRUCTOR,

  /** MAP value constructor, e.g. {@code MAP ['washington', 1, 'obama', 44]}. */
  MAP_VALUE_CONSTRUCTOR,

  /** MAP query constructor,
   * e.g. {@code MAP (SELECT empno, deptno FROM emp)}. */
  MAP_QUERY_CONSTRUCTOR,

  /** {@code CURSOR} constructor, for example, <code>SELECT * FROM
   * TABLE(udx(CURSOR(SELECT ...), x, y, z))</code>. */
  CURSOR,

  // internal operators (evaluated in validator) 200-299

  /**
   * Literal chain operator (for composite string literals).
   * An internal operator that does not appear in SQL syntax.
   */
  LITERAL_CHAIN,

  /**
   * Escape operator (always part of LIKE or SIMILAR TO expression).
   * An internal operator that does not appear in SQL syntax.
   */
  ESCAPE,

  /**
   * The internal REINTERPRET operator (meaning a reinterpret cast).
   * An internal operator that does not appear in SQL syntax.
   */
  REINTERPRET,

  /** The internal {@code EXTEND} operator that qualifies a table name in the
   * {@code FROM} clause. */
  EXTEND,

  /** The internal {@code CUBE} operator that occurs within a {@code GROUP BY}
   * clause. */
  CUBE,

  /** The internal {@code ROLLUP} operator that occurs within a {@code GROUP BY}
   * clause. */
  ROLLUP,

  /** The internal {@code GROUPING SETS} operator that occurs within a
   * {@code GROUP BY} clause. */
  GROUPING_SETS,

  /** The {@code GROUPING(e, ...)} function. */
  GROUPING,

  // CHECKSTYLE: IGNORE 1
  /** @deprecated Use {@link #GROUPING}. */
  @Deprecated // to be removed before 2.0
  GROUPING_ID,

  /** The {@code GROUP_ID()} function. */
  GROUP_ID,

  /** The internal "permute" function in a MATCH_RECOGNIZE clause. */
  PATTERN_PERMUTE,

  /** The special patterns to exclude enclosing pattern from output in a
   * MATCH_RECOGNIZE clause. */
  PATTERN_EXCLUDED,

  // Aggregate functions

  /** The {@code COUNT} aggregate function. */
  COUNT,

  /** The {@code SUM} aggregate function. */
  SUM,

  /** The {@code SUM0} aggregate function. */
  SUM0,

  /** The {@code MIN} aggregate function. */
  MIN,

  /** The {@code MAX} aggregate function. */
  MAX,

  /** The {@code LEAD} aggregate function. */
  LEAD,

  /** The {@code LAG} aggregate function. */
  LAG,

  /** The {@code FIRST_VALUE} aggregate function. */
  FIRST_VALUE,

  /** The {@code LAST_VALUE} aggregate function. */
  LAST_VALUE,

  /** The {@code ANY_VALUE} aggregate function. */
  ANY_VALUE,

  /** The {@code COVAR_POP} aggregate function. */
  COVAR_POP,

  /** The {@code COVAR_SAMP} aggregate function. */
  COVAR_SAMP,

  /** The {@code REGR_COUNT} aggregate function. */
  REGR_COUNT,

  /** The {@code REGR_SXX} aggregate function. */
  REGR_SXX,

  /** The {@code REGR_SYY} aggregate function. */
  REGR_SYY,

  /** The {@code AVG} aggregate function. */
  AVG,

  /** The {@code STDDEV_POP} aggregate function. */
  STDDEV_POP,

  /** The {@code STDDEV_SAMP} aggregate function. */
  STDDEV_SAMP,

  /** The {@code VAR_POP} aggregate function. */
  VAR_POP,

  /** The {@code VAR_SAMP} aggregate function. */
  VAR_SAMP,

  /** The {@code NTILE} aggregate function. */
  NTILE,

  /** The {@code NTH_VALUE} aggregate function. */
  NTH_VALUE,

  /** The {@code LISTAGG} aggregate function. */
  LISTAGG,

  /** The {@code STRING_AGG} aggregate function. */
  STRING_AGG,

  /** The {@code COUNTIF} aggregate function. */
  COUNTIF,

  /** The {@code ARRAY_AGG} aggregate function. */
  ARRAY_AGG,

  /** The {@code ARRAY_CONCAT_AGG} aggregate function. */
  ARRAY_CONCAT_AGG,

  /** The {@code GROUP_CONCAT} aggregate function. */
  GROUP_CONCAT,

  /** The {@code COLLECT} aggregate function. */
  COLLECT,

  /** The {@code MODE} aggregate function. */
  MODE,

  /** The {@code PERCENTILE_CONT} aggregate function. */
  PERCENTILE_CONT,

  /** The {@code PERCENTILE_DISC} aggregate function. */
  PERCENTILE_DISC,

  /** The {@code FUSION} aggregate function. */
  FUSION,

  /** The {@code INTERSECTION} aggregate function. */
  INTERSECTION,

  /** The {@code SINGLE_VALUE} aggregate function. */
  SINGLE_VALUE,

  /** The {@code AGGREGATE} aggregate function. */
  AGGREGATE_FN,

  /** The {@code BIT_AND} aggregate function. */
  BIT_AND,

  /** The {@code BIT_OR} aggregate function. */
  BIT_OR,

  /** The {@code BIT_XOR} aggregate function. */
  BIT_XOR,

  /** The {@code ROW_NUMBER} window function. */
  ROW_NUMBER,

  /** The {@code RANK} window function. */
  RANK,

  /** The {@code PERCENT_RANK} window function. */
  PERCENT_RANK,

  /** The {@code DENSE_RANK} window function. */
  DENSE_RANK,

  /** The {@code ROW_NUMBER} window function. */
  CUME_DIST,

  /** The {@code DESCRIPTOR(column_name, ...)}. */
  DESCRIPTOR,

  /** The {@code TUMBLE} group function. */
  TUMBLE,

  // Group functions
  /** The {@code TUMBLE_START} auxiliary function of
   * the {@link #TUMBLE} group function. */
  // TODO: deprecate TUMBLE_START.
  TUMBLE_START,

  /** The {@code TUMBLE_END} auxiliary function of
   * the {@link #TUMBLE} group function. */
  // TODO: deprecate TUMBLE_END.
  TUMBLE_END,

  /** The {@code HOP} group function. */
  HOP,

  /** The {@code HOP_START} auxiliary function of
   * the {@link #HOP} group function. */
  HOP_START,

  /** The {@code HOP_END} auxiliary function of
   * the {@link #HOP} group function. */
  HOP_END,

  /** The {@code SESSION} group function. */
  SESSION,

  /** The {@code SESSION_START} auxiliary function of
   * the {@link #SESSION} group function. */
  SESSION_START,

  /** The {@code SESSION_END} auxiliary function of
   * the {@link #SESSION} group function. */
  SESSION_END,

  /** Column declaration. */
  COLUMN_DECL,

  /** Attribute definition. */
  ATTRIBUTE_DEF,

  /** {@code CHECK} constraint. */
  CHECK,

  /** {@code UNIQUE} constraint. */
  UNIQUE,

  /** {@code PRIMARY KEY} constraint. */
  PRIMARY_KEY,

  /** {@code FOREIGN KEY} constraint. */
  FOREIGN_KEY,

  // Spatial functions. They are registered as "user-defined functions" but it
  // is convenient to have a "kind" so that we can quickly match them in planner
  // rules.

  /** The {@code ST_DWithin} geo-spatial function. */
  ST_DWITHIN,

  /** The {@code ST_Point} function. */
  ST_POINT,

  /** The {@code ST_Point} function that makes a 3D point. */
  ST_POINT3,

  /** The {@code ST_MakeLine} function that makes a line. */
  ST_MAKE_LINE,

  /** The {@code ST_Contains} function that tests whether one geometry contains
   * another. */
  ST_CONTAINS,

  /** The {@code Hilbert} function that converts (x, y) to a position on a
   * Hilbert space-filling curve. */
  HILBERT,

  // DDL and session control statements follow. The list is not exhaustive: feel
  // free to add more.

  /** {@code COMMIT} session control statement. */
  COMMIT,

  /** {@code ROLLBACK} session control statement. */
  ROLLBACK,

  /** {@code ALTER SESSION} DDL statement. */
  ALTER_SESSION,

  /** {@code CREATE SCHEMA} DDL statement. */
  CREATE_SCHEMA,

  /** {@code CREATE FOREIGN SCHEMA} DDL statement. */
  CREATE_FOREIGN_SCHEMA,

  /** {@code DROP SCHEMA} DDL statement. */
  DROP_SCHEMA,

  /** {@code CREATE TABLE} DDL statement. */
  CREATE_TABLE,

  /** {@code ALTER TABLE} DDL statement. */
  ALTER_TABLE,

  /** {@code DROP TABLE} DDL statement. */
  DROP_TABLE,

  /** {@code CREATE VIEW} DDL statement. */
  CREATE_VIEW,

  /** {@code ALTER VIEW} DDL statement. */
  ALTER_VIEW,

  /** {@code DROP VIEW} DDL statement. */
  DROP_VIEW,

  /** {@code CREATE MATERIALIZED VIEW} DDL statement. */
  CREATE_MATERIALIZED_VIEW,

  /** {@code ALTER MATERIALIZED VIEW} DDL statement. */
  ALTER_MATERIALIZED_VIEW,

  /** {@code DROP MATERIALIZED VIEW} DDL statement. */
  DROP_MATERIALIZED_VIEW,

  /** {@code CREATE SEQUENCE} DDL statement. */
  CREATE_SEQUENCE,

  /** {@code ALTER SEQUENCE} DDL statement. */
  ALTER_SEQUENCE,

  /** {@code DROP SEQUENCE} DDL statement. */
  DROP_SEQUENCE,

  /** {@code CREATE INDEX} DDL statement. */
  CREATE_INDEX,

  /** {@code ALTER INDEX} DDL statement. */
  ALTER_INDEX,

  /** {@code DROP INDEX} DDL statement. */
  DROP_INDEX,

  /** {@code CREATE TYPE} DDL statement. */
  CREATE_TYPE,

  /** {@code DROP TYPE} DDL statement. */
  DROP_TYPE,

  /** {@code CREATE FUNCTION} DDL statement. */
  CREATE_FUNCTION,

  /** {@code DROP FUNCTION} DDL statement. */
  DROP_FUNCTION,

  /** DDL statement not handled above.
   *
   * <p><b>Note to other projects</b>: If you are extending Calcite's SQL parser
   * and have your own object types you no doubt want to define CREATE and DROP
   * commands for them. Use OTHER_DDL in the short term, but we are happy to add
   * new enum values for your object types. Just ask!
   */
  OTHER_DDL;

  //~ Static fields/initializers ---------------------------------------------

  // Most of the static fields are categories, aggregating several kinds into
  // a set.

  /**
   * Category consisting of set-query node types.
   *
   * <p>Consists of:
   * {@link #EXCEPT},
   * {@link #INTERSECT},
   * {@link #UNION}.
   */
  public static final EnumSet<SqlKind> SET_QUERY =
      EnumSet.of(UNION, INTERSECT, EXCEPT);

  /**
   * Category consisting of all built-in aggregate functions.
   */
  public static final EnumSet<SqlKind> AGGREGATE =
      EnumSet.of(COUNT, SUM, SUM0, MIN, MAX, LEAD, LAG, FIRST_VALUE,
          LAST_VALUE, COVAR_POP, COVAR_SAMP, REGR_COUNT, REGR_SXX, REGR_SYY,
          AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP, NTILE, COLLECT,
          MODE, FUSION, SINGLE_VALUE, ROW_NUMBER, RANK, PERCENT_RANK, DENSE_RANK,
          CUME_DIST, JSON_ARRAYAGG, JSON_OBJECTAGG, BIT_AND, BIT_OR, BIT_XOR,
          LISTAGG, STRING_AGG, ARRAY_AGG, ARRAY_CONCAT_AGG, GROUP_CONCAT, COUNTIF,
          PERCENTILE_CONT, PERCENTILE_DISC,
          INTERSECTION, ANY_VALUE);

  /**
   * Category consisting of all DML operators.
   *
   * <p>Consists of:
   * {@link #INSERT},
   * {@link #UPDATE},
   * {@link #DELETE},
   * {@link #MERGE},
   * {@link #PROCEDURE_CALL}.
   *
   * <p>NOTE jvs 1-June-2006: For now we treat procedure calls as DML;
   * this makes it easy for JDBC clients to call execute or
   * executeUpdate and not have to process dummy cursor results.  If
   * in the future we support procedures which return results sets,
   * we'll need to refine this.
   */
  public static final EnumSet<SqlKind> DML =
      EnumSet.of(INSERT, DELETE, UPDATE, MERGE, PROCEDURE_CALL);

  /**
   * Category consisting of all DDL operators.
   */
  public static final EnumSet<SqlKind> DDL =
      EnumSet.of(COMMIT, ROLLBACK, ALTER_SESSION,
          CREATE_SCHEMA, CREATE_FOREIGN_SCHEMA, DROP_SCHEMA,
          CREATE_TABLE, ALTER_TABLE, DROP_TABLE,
          CREATE_FUNCTION, DROP_FUNCTION,
          CREATE_VIEW, ALTER_VIEW, DROP_VIEW,
          CREATE_MATERIALIZED_VIEW, ALTER_MATERIALIZED_VIEW,
          DROP_MATERIALIZED_VIEW,
          CREATE_SEQUENCE, ALTER_SEQUENCE, DROP_SEQUENCE,
          CREATE_INDEX, ALTER_INDEX, DROP_INDEX,
          CREATE_TYPE, DROP_TYPE,
          SET_OPTION, OTHER_DDL);

  /**
   * Category consisting of query node types.
   *
   * <p>Consists of:
   * {@link #SELECT},
   * {@link #EXCEPT},
   * {@link #INTERSECT},
   * {@link #UNION},
   * {@link #VALUES},
   * {@link #ORDER_BY},
   * {@link #EXPLICIT_TABLE}.
   */
  public static final EnumSet<SqlKind> QUERY =
      EnumSet.of(SELECT, UNION, INTERSECT, EXCEPT, VALUES, WITH, ORDER_BY,
          EXPLICIT_TABLE);

  /**
   * Category consisting of all expression operators.
   *
   * <p>A node is an expression if it is NOT one of the following:
   * {@link #AS},
   * {@link #ARGUMENT_ASSIGNMENT},
   * {@link #DEFAULT},
   * {@link #DESCENDING},
   * {@link #SELECT},
   * {@link #JOIN},
   * {@link #OTHER_FUNCTION},
   * {@link #CAST},
   * {@link #TRIM},
   * {@link #LITERAL_CHAIN},
   * {@link #JDBC_FN},
   * {@link #PRECEDING},
   * {@link #FOLLOWING},
   * {@link #ORDER_BY},
   * {@link #COLLECTION_TABLE},
   * {@link #TABLESAMPLE},
   * {@link #UNNEST}
   * or an aggregate function, DML or DDL.
   */
  public static final Set<SqlKind> EXPRESSION =
      EnumSet.complementOf(
          concat(
              EnumSet.of(AS, ARGUMENT_ASSIGNMENT, DEFAULT,
                  RUNNING, FINAL, LAST, FIRST, PREV, NEXT,
                  FILTER, WITHIN_GROUP, IGNORE_NULLS, RESPECT_NULLS, SEPARATOR,
                  DESCENDING, CUBE, ROLLUP, GROUPING_SETS, EXTEND, LATERAL,
                  SELECT, JOIN, OTHER_FUNCTION, POSITION, CAST, TRIM, FLOOR, CEIL,
                  TIMESTAMP_ADD, TIMESTAMP_DIFF, EXTRACT, INTERVAL,
                  LITERAL_CHAIN, JDBC_FN, PRECEDING, FOLLOWING, ORDER_BY,
                  NULLS_FIRST, NULLS_LAST, COLLECTION_TABLE, TABLESAMPLE,
                  VALUES, WITH, WITH_ITEM, ITEM, SKIP_TO_FIRST, SKIP_TO_LAST,
                  JSON_VALUE_EXPRESSION, UNNEST),
              SET_QUERY, AGGREGATE, DML, DDL));

  /**
   * Category of all SQL statement types.
   *
   * <p>Consists of all types in {@link #QUERY}, {@link #DML} and {@link #DDL}.
   */
  public static final EnumSet<SqlKind> TOP_LEVEL = concat(QUERY, DML, DDL);

  /**
   * Category consisting of regular and special functions.
   *
   * <p>Consists of regular functions {@link #OTHER_FUNCTION} and special
   * functions {@link #ROW}, {@link #TRIM}, {@link #CAST}, {@link #REVERSE}, {@link #JDBC_FN}.
   */
  public static final Set<SqlKind> FUNCTION =
      EnumSet.of(OTHER_FUNCTION, ROW, TRIM, LTRIM, RTRIM, CAST, REVERSE, JDBC_FN, POSITION);

  /**
   * Category of SqlAvgAggFunction.
   *
   * <p>Consists of {@link #AVG}, {@link #STDDEV_POP}, {@link #STDDEV_SAMP},
   * {@link #VAR_POP}, {@link #VAR_SAMP}.
   */
  public static final Set<SqlKind> AVG_AGG_FUNCTIONS =
      EnumSet.of(AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP);

  /**
   * Category of SqlCovarAggFunction.
   *
   * <p>Consists of {@link #COVAR_POP}, {@link #COVAR_SAMP}, {@link #REGR_SXX},
   * {@link #REGR_SYY}.
   */
  public static final Set<SqlKind> COVAR_AVG_AGG_FUNCTIONS =
      EnumSet.of(COVAR_POP, COVAR_SAMP, REGR_COUNT, REGR_SXX, REGR_SYY);

  /**
   * Category of comparison operators.
   *
   * <p>Consists of:
   * {@link #IN},
   * {@link #EQUALS},
   * {@link #NOT_EQUALS},
   * {@link #LESS_THAN},
   * {@link #GREATER_THAN},
   * {@link #LESS_THAN_OR_EQUAL},
   * {@link #GREATER_THAN_OR_EQUAL}.
   */
  public static final Set<SqlKind> COMPARISON =
      EnumSet.of(
          IN, EQUALS, NOT_EQUALS,
          LESS_THAN, GREATER_THAN,
          GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL);

  /**
   * Category of binary arithmetic.
   *
   * <p>Consists of:
   * {@link #PLUS}
   * {@link #MINUS}
   * {@link #TIMES}
   * {@link #DIVIDE}
   * {@link #MOD}.
   */
  public static final Set<SqlKind> BINARY_ARITHMETIC =
      EnumSet.of(PLUS, MINUS, TIMES, DIVIDE, MOD);

  /**
   * Category of binary equality.
   *
   * <p>Consists of:
   * {@link #EQUALS}
   * {@link #NOT_EQUALS}
   */
  public static final Set<SqlKind> BINARY_EQUALITY =
      EnumSet.of(EQUALS, NOT_EQUALS);

  /**
   * Category of binary comparison.
   *
   * <p>Consists of:
   * {@link #EQUALS}
   * {@link #NOT_EQUALS}
   * {@link #GREATER_THAN}
   * {@link #GREATER_THAN_OR_EQUAL}
   * {@link #LESS_THAN}
   * {@link #LESS_THAN_OR_EQUAL}
   * {@link #IS_DISTINCT_FROM}
   * {@link #IS_NOT_DISTINCT_FROM}
   */
  public static final Set<SqlKind> BINARY_COMPARISON =
      EnumSet.of(
          EQUALS, NOT_EQUALS,
          GREATER_THAN, GREATER_THAN_OR_EQUAL,
          LESS_THAN, LESS_THAN_OR_EQUAL,
          IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM);

  /**
   * Category of operators that do not depend on the argument order.
   *
   * <p>For instance: {@link #AND}, {@link #OR}, {@link #EQUALS}, {@link #LEAST}</p>
   * <p>Note: {@link #PLUS} does depend on the argument oder if argument types are different</p>
   */
  @API(since = "1.22", status = API.Status.EXPERIMENTAL)
  public static final Set<SqlKind> SYMMETRICAL =
      EnumSet.of(
          AND, OR, EQUALS, NOT_EQUALS,
          IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM,
          GREATEST, LEAST);

  /**
   * Category of operators that do not depend on the argument order if argument types are equal.
   *
   * <p>For instance: {@link #PLUS}, {@link #TIMES}</p>
   */
  @API(since = "1.22", status = API.Status.EXPERIMENTAL)
  public static final Set<SqlKind> SYMMETRICAL_SAME_ARG_TYPE =
      EnumSet.of(
          PLUS, TIMES);

  /**
   * Simple binary operators are those operators which expects operands from the same Domain.
   *
   * <p>Example: simple comparisons ({@code =}, {@code <}).
   *
   * <p>Note: it does not contain {@code IN} because that is defined on D x D^n.
   */
  @API(since = "1.24", status = API.Status.EXPERIMENTAL)
  public static final Set<SqlKind> SIMPLE_BINARY_OPS;

  static {
    EnumSet<SqlKind> kinds = EnumSet.copyOf(SqlKind.BINARY_ARITHMETIC);
    kinds.remove(SqlKind.MOD);
    kinds.addAll(SqlKind.BINARY_COMPARISON);
    SIMPLE_BINARY_OPS = Sets.immutableEnumSet(kinds);
  }

  /** Lower-case name. */
  public final String lowerName = name().toLowerCase(Locale.ROOT);
  public final String sql;

  SqlKind() {
    sql = name();
  }

  SqlKind(String sql) {
    this.sql = sql;
  }

  /** Returns the kind that corresponds to this operator but in the opposite
   * direction. Or returns this, if this kind is not reversible.
   *
   * <p>For example, {@code GREATER_THAN.reverse()} returns {@link #LESS_THAN}.
   */
  public SqlKind reverse() {
    switch (this) {
    case GREATER_THAN:
      return LESS_THAN;
    case GREATER_THAN_OR_EQUAL:
      return LESS_THAN_OR_EQUAL;
    case LESS_THAN:
      return GREATER_THAN;
    case LESS_THAN_OR_EQUAL:
      return GREATER_THAN_OR_EQUAL;
    default:
      return this;
    }
  }

  /** Returns the kind that you get if you apply NOT to this kind.
   *
   * <p>For example, {@code IS_NOT_NULL.negate()} returns {@link #IS_NULL}.
   *
   * <p>For {@link #IS_TRUE}, {@link #IS_FALSE}, {@link #IS_NOT_TRUE},
   * {@link #IS_NOT_FALSE}, nullable inputs need to be treated carefully.
   *
   * <p>{@code NOT(IS_TRUE(null))} = {@code NOT(false)} = {@code true},
   * while {@code IS_FALSE(null)} = {@code false},
   * so {@code NOT(IS_TRUE(X))} should be {@code IS_NOT_TRUE(X)}.
   * On the other hand,
   * {@code IS_TRUE(NOT(null))} = {@code IS_TRUE(null)} = {@code false}.
   *
   * <p>This is why negate() != negateNullSafe() for these operators.
   */
  public SqlKind negate() {
    switch (this) {
    case IS_TRUE:
      return IS_NOT_TRUE;
    case IS_FALSE:
      return IS_NOT_FALSE;
    case IS_NULL:
      return IS_NOT_NULL;
    case IS_NOT_TRUE:
      return IS_TRUE;
    case IS_NOT_FALSE:
      return IS_FALSE;
    case IS_NOT_NULL:
      return IS_NULL;
    case IS_DISTINCT_FROM:
      return IS_NOT_DISTINCT_FROM;
    case IS_NOT_DISTINCT_FROM:
      return IS_DISTINCT_FROM;
    default:
      return this;
    }
  }

  /** Returns the kind that you get if you negate this kind.
   * To conform to null semantics, null value should not be compared.
   *
   * <p>For {@link #IS_TRUE}, {@link #IS_FALSE}, {@link #IS_NOT_TRUE} and
   * {@link #IS_NOT_FALSE}, nullable inputs need to be treated carefully:
   *
   * <ul>
   * <li>NOT(IS_TRUE(null)) = NOT(false) = true
   * <li>IS_TRUE(NOT(null)) = IS_TRUE(null) = false
   * <li>IS_FALSE(null) = false
   * <li>IS_NOT_TRUE(null) = true
   * </ul>
   */
  public SqlKind negateNullSafe() {
    switch (this) {
    case EQUALS:
      return NOT_EQUALS;
    case NOT_EQUALS:
      return EQUALS;
    case LESS_THAN:
      return GREATER_THAN_OR_EQUAL;
    case GREATER_THAN:
      return LESS_THAN_OR_EQUAL;
    case LESS_THAN_OR_EQUAL:
      return GREATER_THAN;
    case GREATER_THAN_OR_EQUAL:
      return LESS_THAN;
    case IN:
      return NOT_IN;
    case NOT_IN:
      return IN;
    case DRUID_IN:
      return DRUID_NOT_IN;
    case DRUID_NOT_IN:
      return DRUID_IN;
    case IS_TRUE:
      return IS_FALSE;
    case IS_FALSE:
      return IS_TRUE;
    case IS_NOT_TRUE:
      return IS_NOT_FALSE;
    case IS_NOT_FALSE:
      return IS_NOT_TRUE;
     // (NOT x) IS NULL => x IS NULL
     // Similarly (NOT x) IS NOT NULL => x IS NOT NULL
    case IS_NOT_NULL:
    case IS_NULL:
      return this;
    default:
      return this.negate();
    }
  }

  /**
   * Returns whether this {@code SqlKind} belongs to a given category.
   *
   * <p>A category is a collection of kinds, not necessarily disjoint. For
   * example, QUERY is { SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY,
   * EXPLICIT_TABLE }.
   *
   * @param category Category
   * @return Whether this kind belongs to the given category
   */
  public final boolean belongsTo(Collection<SqlKind> category) {
    return category.contains(this);
  }

  @SafeVarargs
  private static <E extends Enum<E>> EnumSet<E> concat(EnumSet<E> set0,
      EnumSet<E>... sets) {
    EnumSet<E> set = set0.clone();
    for (EnumSet<E> s : sets) {
      set.addAll(s);
    }
    return set;
  }
}
