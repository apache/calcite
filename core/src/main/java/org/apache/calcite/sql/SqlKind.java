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

import java.util.Collection;
import java.util.EnumSet;
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
   * JOIN operator or compound FROM clause.
   *
   * <p>A FROM clause with more than one table is represented as if it were a
   * join. For example, "FROM x, y, z" is represented as
   * "JOIN(x, JOIN(x, y))".</p>
   */
  JOIN,

  /**
   * Identifier
   */
  IDENTIFIER,

  /**
   * A literal.
   */
  LITERAL,

  /**
   * Function that is not a special function.
   *
   * @see #FUNCTION
   */
  OTHER_FUNCTION,

  /**
   * EXPLAIN statement
   */
  EXPLAIN,

  /**
   * DESCRIBE SCHEMA statement
   */
  DESCRIBE_SCHEMA,

  /**
   * DESCRIBE TABLE statement
   */
  DESCRIBE_TABLE,

  /**
   * INSERT statement
   */
  INSERT,

  /**
   * DELETE statement
   */
  DELETE,

  /**
   * UPDATE statement
   */
  UPDATE,

  /**
   * "ALTER scope SET option = value" statement.
   */
  SET_OPTION,

  /**
   * A dynamic parameter.
   */
  DYNAMIC_PARAM,

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

  /**
   * Union
   */
  UNION,

  /**
   * Except
   */
  EXCEPT,

  /**
   * Intersect
   */
  INTERSECT,

  /**
   * AS operator
   */
  AS,

  /**
   * ARGUMENT_ASSIGNMENT operator, {@code =>}
   */
  ARGUMENT_ASSIGNMENT,

  /**
   * DEFAULT operator
   */
  DEFAULT,

  /**
   * OVER operator
   */
  OVER,

  /**
   * FILTER operator
   */
  FILTER,

  /**
   * Window specification
   */
  WINDOW,

  /**
   * MERGE statement
   */
  MERGE,

  /**
   * TABLESAMPLE operator
   */
  TABLESAMPLE,

  // binary operators

  /**
   * The arithmetic multiplication operator, "*".
   */
  TIMES,

  /**
   * The arithmetic division operator, "/".
   */
  DIVIDE,

  /**
   * The arithmetic plus operator, "+".
   *
   * @see #PLUS_PREFIX
   */
  PLUS,

  /**
   * The arithmetic minus operator, "-".
   *
   * @see #MINUS_PREFIX
   */
  MINUS,

  // comparison operators

  /**
   * The "IN" operator.
   */
  IN,

  /**
   * The less-than operator, "&lt;".
   */
  LESS_THAN,

  /**
   * The greater-than operator, "&gt;".
   */
  GREATER_THAN,

  /**
   * The less-than-or-equal operator, "&lt;=".
   */
  LESS_THAN_OR_EQUAL,

  /**
   * The greater-than-or-equal operator, "&gt;=".
   */
  GREATER_THAN_OR_EQUAL,

  /**
   * The equals operator, "=".
   */
  EQUALS,

  /**
   * The not-equals operator, "&#33;=" or "&lt;&gt;".
   */
  NOT_EQUALS,

  /**
   * The is-distinct-from operator.
   */
  IS_DISTINCT_FROM,

  /**
   * The is-not-distinct-from operator.
   */
  IS_NOT_DISTINCT_FROM,

  /**
   * The logical "OR" operator.
   */
  OR,

  /**
   * The logical "AND" operator.
   */
  AND,

  // other infix

  /**
   * Dot
   */
  DOT,

  /**
   * The "OVERLAPS" operator.
   */
  OVERLAPS,

  /**
   * The "LIKE" operator.
   */
  LIKE,

  /**
   * The "SIMILAR" operator.
   */
  SIMILAR,

  /**
   * The "BETWEEN" operator.
   */
  BETWEEN,

  /**
   * A "CASE" expression.
   */
  CASE,

  /**
   * The "NULLIF" operator.
   */
  NULLIF,

  /**
   * The "COALESCE" operator.
   */
  COALESCE,

  /**
   * The "DECODE" function (Oracle).
   */
  DECODE,

  /**
   * The "NVL" function (Oracle).
   */
  NVL,

  /**
   * The "GREATEST" function (Oracle).
   */
  GREATEST,

  /**
   * The "LEAST" function (Oracle).
   */
  LEAST,

  /**
   * The "TIMESTAMP_ADD" function (ODBC, SQL Server, MySQL).
   */
  TIMESTAMP_ADD,

  /**
   * The "TIMESTAMP_DIFF" function (ODBC, SQL Server, MySQL).
   */
  TIMESTAMP_DIFF,

  // prefix operators

  /**
   * The logical "NOT" operator.
   */
  NOT,

  /**
   * The unary plus operator, as in "+1".
   *
   * @see #PLUS
   */
  PLUS_PREFIX,

  /**
   * The unary minus operator, as in "-1".
   *
   * @see #MINUS
   */
  MINUS_PREFIX,

  /**
   * The "EXISTS" operator.
   */
  EXISTS,

  /**
   * The "VALUES" operator.
   */
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

  /**
   * ProcedureCall
   */
  PROCEDURE_CALL,

  /**
   * NewSpecification
   */
  NEW_SPECIFICATION,

  // postfix operators

  /**
   * DESC in ORDER BY. A parse tree, not a true expression.
   */
  DESCENDING,

  /**
   * NULLS FIRST clause in ORDER BY. A parse tree, not a true expression.
   */
  NULLS_FIRST,

  /**
   * NULLS LAST clause in ORDER BY. A parse tree, not a true expression.
   */
  NULLS_LAST,

  /**
   * The "IS TRUE" operator.
   */
  IS_TRUE,

  /**
   * The "IS FALSE" operator.
   */
  IS_FALSE,

  /**
   * The "IS NOT TRUE" operator.
   */
  IS_NOT_TRUE,

  /**
   * The "IS NOT FALSE" operator.
   */
  IS_NOT_FALSE,

  /**
   * The "IS UNKNOWN" operator.
   */
  IS_UNKNOWN,

  /**
   * The "IS NULL" operator.
   */
  IS_NULL,

  /**
   * The "IS NOT NULL" operator.
   */
  IS_NOT_NULL,

  /**
   * The "PRECEDING" qualifier of an interval end-point in a window
   * specification.
   */
  PRECEDING,

  /**
   * The "FOLLOWING" qualifier of an interval end-point in a window
   * specification.
   */
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
   * The "CAST" operator.
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

  /**
   * The "FLOOR" function
   */
  FLOOR,

  /**
   * The "CEIL" function
   */
  CEIL,

  /**
   * The "TRIM" function.
   */
  TRIM,

  /**
   * The "LTRIM" function (Oracle).
   */
  LTRIM,

  /**
   * The "RTRIM" function (Oracle).
   */
  RTRIM,

  /**
   * The "EXTRACT" function.
   */
  EXTRACT,

  /**
   * Call to a function using JDBC function syntax.
   */
  JDBC_FN,

  /**
   * The MULTISET value constructor.
   */
  MULTISET_VALUE_CONSTRUCTOR,

  /**
   * The MULTISET query constructor.
   */
  MULTISET_QUERY_CONSTRUCTOR,

  /**
   * The "UNNEST" operator.
   */
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

  /**
   * Map Value Constructor, e.g. {@code Map['washington', 1, 'obama', 44]}.
   */
  MAP_VALUE_CONSTRUCTOR,

  /**
   * Map Query Constructor, e.g. {@code MAP (SELECT empno, deptno FROM emp)}.
   */
  MAP_QUERY_CONSTRUCTOR,

  /**
   * CURSOR constructor, for example, <code>select * from
   * TABLE(udx(CURSOR(select ...), x, y, z))</code>
   */
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

  /** The internal {@code GROUPING(e)} function. */
  GROUPING,

  /** The internal {@code GROUPING_ID(e, ...)} function. */
  GROUPING_ID,

  /** The internal {@code GROUP_ID()} function. */
  GROUP_ID,

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

  /** The {@code COVAR_POP} aggregate function. */
  COVAR_POP,

  /** The {@code COVAR_SAMP} aggregate function. */
  COVAR_SAMP,

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

  /** The {@code COLLECT} aggregate function. */
  COLLECT,

  /** The {@code FUSION} aggregate function. */
  FUSION,

  /** The {@code SINGLE_VALUE} aggregate function. */
  SINGLE_VALUE,

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

  // Group functions

  /** The {@code TUMBLE} group function. */
  TUMBLE,

  /** The {@code TUMBLE_START} auxiliary function of
   * the {@link #TUMBLE} function. */
  TUMBLE_START,

  /** The {@code TUMBLE_END} auxiliary function of
   * the {@link #TUMBLE} function. */
  TUMBLE_END,

  // DDL and session control statements follow. The list is not exhaustive: feel
  // free to add more.

  /** {@code COMMIT} session control statement. */
  COMMIT,

  /** {@code ROLLBACK} session control statement. */
  ROLLBACK,

  /** {@code ALTER SESSION} DDL statement. */
  ALTER_SESSION,

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
          LAST_VALUE, COVAR_POP, COVAR_SAMP, REGR_SXX, REGR_SYY,
          AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP, NTILE, COLLECT,
          FUSION, SINGLE_VALUE, ROW_NUMBER, RANK, PERCENT_RANK, DENSE_RANK,
          CUME_DIST);

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
          CREATE_TABLE, ALTER_TABLE, DROP_TABLE,
          CREATE_VIEW, ALTER_VIEW, DROP_VIEW,
          CREATE_SEQUENCE, ALTER_SEQUENCE, DROP_SEQUENCE,
          CREATE_INDEX, ALTER_INDEX, DROP_INDEX,
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
   * or an aggregate function, DML or DDL.
   */
  public static final Set<SqlKind> EXPRESSION =
      EnumSet.complementOf(
          concat(
              EnumSet.of(AS, ARGUMENT_ASSIGNMENT, DEFAULT,
                  DESCENDING, CUBE, ROLLUP, GROUPING_SETS, EXTEND, LATERAL,
                  SELECT, JOIN, OTHER_FUNCTION, CAST, TRIM, FLOOR, CEIL,
                  TIMESTAMP_ADD, TIMESTAMP_DIFF, EXTRACT,
                  LITERAL_CHAIN, JDBC_FN, PRECEDING, FOLLOWING, ORDER_BY,
                  NULLS_FIRST, NULLS_LAST, COLLECTION_TABLE, TABLESAMPLE,
                  VALUES, WITH, WITH_ITEM),
              AGGREGATE, DML, DDL));

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
   * functions {@link #ROW}, {@link #TRIM}, {@link #CAST}, {@link #JDBC_FN}.
   */
  public static final Set<SqlKind> FUNCTION =
      EnumSet.of(OTHER_FUNCTION, ROW, TRIM, LTRIM, RTRIM, CAST, JDBC_FN);

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
   * <p>For example, {@code IS_NOT_NULL.negate()} returns {@link #IS_NULL}. */
  public SqlKind negate() {
    switch (this) {
    case IS_TRUE:
      return IS_FALSE;
    case IS_FALSE:
      return IS_TRUE;
    case IS_NULL:
      return IS_NOT_NULL;
    case IS_NOT_TRUE:
      return IS_NOT_FALSE;
    case IS_NOT_FALSE:
      return IS_NOT_TRUE;
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
   * To conform to null semantics, null value should not be compared. */
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

// End SqlKind.java
