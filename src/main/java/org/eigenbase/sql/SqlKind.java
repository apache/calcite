/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql;

import java.util.EnumSet;
import java.util.Set;


/**
 * Enumerates the possible types of {@link SqlNode}.
 *
 * <p>Only commonly-used nodes have their own type; other nodes are of type
 * {@link #OTHER}. Some of the values, such as {@link #SET_QUERY}, represent
 * aggregates.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 12, 2003
 */
public enum SqlKind
{
    //~ Static fields/initializers ---------------------------------------------

    // the basics

    /**
     * Other
     */
    OTHER,

    /**
     * SELECT statement or sub-query
     */
    SELECT,


    /**
     * JOIN operator or compound FROM clause.
     *
     * <p>A FROM clause with more than one table is represented as if it were a
     * join. For example, "FROM x, y, z" is represented as "JOIN(x, JOIN(x,
     * y))".</p>
     */
    JOIN,

    /**
     * Identifier
     */
    IDENTIFIER,

    /**
     * Literal
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
     * Dynamic Param
     */
    DYNAMIC_PARAM,

    /**
     * ORDER BY clause
     */
    ORDER_BY,

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
     * OVER operator
     */
    OVER,

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
     * Times
     */
    TIMES,

    /**
     * Divide
     */
    DIVIDE,

    /**
     * Plus
     */
    PLUS,

    /**
     * Minus
     */
    MINUS,

    // comparison operators

    /**
     * In
     */
    IN,

    /**
     * LessThan
     */
    LESS_THAN,

    /**
     * Greater Than
     */
    GREATER_THAN,

    /**
     * Less Than Or Equal
     */
    LESS_THAN_OR_EQUAL,

    /**
     * Greater Than Or Equal
     */
    GREATER_THAN_OR_EQUAL,

    /**
     * Equals
     */
    EQUALS,

    /**
     * Not Equals
     */
    NOT_EQUALS,

    /**
     * Or
     */
    OR,

    /**
     * And
     */
    AND,

    // other infix

    /**
     * Dot
     */
    DOT,

    /**
     * Overlaps
     */
    OVERLAPS,

    /**
     * Like
     */
    LIKE,

    /**
     * Similar
     */
    SIMILAR,

    /**
     * Between
     */
    BETWEEN,

    /**
     * CASE
     */
    CASE,

    // prefix operators

    /**
     * Not
     */
    NOT,

    /**
     * PlusPrefix
     */
    PLUS_PREFIX,

    /**
     * MinusPrefix
     */
    MINUS_PREFIX,

    /**
     * Exists
     */
    EXISTS,

    /**
     * Values
     */
    VALUES,

    /**
     * Explicit table, e.g. <code>select * from (TABLE t)</code> or <code>TABLE
     * t</code>. See also {@link #COLLECTION_TABLE}.
     */
    EXPLICIT_TABLE,

    /**
     * Scalar query; that is, a subquery used in an expression context, and
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
     * IS TRUE operator.
     */
    IS_TRUE,

    /**
     * IS FALSE operator.
     */
    IS_FALSE,

    /**
     * IS UNKNOWN operator.
     */
    IS_UNKNOWN,

    /**
     * IS NULL operator.
     */
    IS_NULL,

    /**
     * PRECEDING
     */
    PRECEDING,

    /**
     * FOLLOWING
     */
    FOLLOWING,

    // functions

    /**
     * ROW function.
     */
    ROW,

    /**
     * The non-standard constructor used to pass a
     * COLUMN_LIST parameter to a UDX.
     */
    COLUMN_LIST,

    /**
     * CAST operator.
     */
    CAST,

    /**
     * TRIM function.
     */
    TRIM,

    /**
     * Call to a function using JDBC function syntax.
     */
    JDBC_FN,

    /**
     * Multiset Value Constructor.
     */
    MULTISET_VALUE_CONSTRUCTOR,

    /**
     * Multiset Query Constructor.
     */
    MULTISET_QUERY_CONSTRUCTOR,

    /**
     * Unnest
     */
    UNNEST,

    /**
     * Lateral
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
     * LiteralChain operator (for composite string literals)
     */
    LITERAL_CHAIN,

    /**
     * Escape operator (always part of LIKE or SIMILAR TO expression)
     */
    ESCAPE,

    /**
     * Reinterpret operator (a reinterpret cast)
     */
    REINTERPRET;

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
     * Category consisting of all expression operators.
     *
     * <p>A node is an expression if it is NOT one of the following:
     * {@link #AS},
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
     * {@link #TABLESAMPLE}.
     */
    public static final Set<SqlKind> EXPRESSION =
        EnumSet.complementOf(
            EnumSet.of(
                AS, DESCENDING, SELECT, JOIN, OTHER_FUNCTION, CAST, TRIM,
                LITERAL_CHAIN, JDBC_FN, PRECEDING, FOLLOWING, ORDER_BY,
                NULLS_FIRST, NULLS_LAST, COLLECTION_TABLE, TABLESAMPLE));

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
        EnumSet.of(
            SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY, EXPLICIT_TABLE);

    /**
     * Category of all SQL statement types.
     *
     * <p>Consists of all types in {@link #QUERY} and {@link #DML}.
     */
    public static final Set<SqlKind> TOP_LEVEL;

    static {
        TOP_LEVEL = EnumSet.copyOf(QUERY);
        TOP_LEVEL.addAll(DML);
    }

    /**
     * Category consisting of regular and special functions.
     *
     * <p>Consists of regular functions {@link #OTHER_FUNCTION} and specical
     * functions {@link #ROW}, {@link #TRIM}, {@link #CAST}, {@link #JDBC_FN}.
     */
    public static final Set<SqlKind> FUNCTION =
        EnumSet.of(OTHER_FUNCTION, ROW, TRIM, CAST, JDBC_FN);

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
     * Returns whether this {@code SqlKind} belongs to a given category.
     *
     * <p>A category is a collection of kinds, not necessarily disjoint. For
     * example, QUERY is { SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY,
     * EXPLICIT_TABLE }.
     *
     * @param category Category
     * @return Whether this kind belongs to the given cateogry
     */
    public final boolean belongsTo(Set<SqlKind> category)
    {
        return category.contains(this);
    }
}

// End SqlKind.java
