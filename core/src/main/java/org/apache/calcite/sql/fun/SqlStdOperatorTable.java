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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFilterOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlFunctionalOperator;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOverOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlProcedureCallOperator;
import org.apache.calcite.sql.SqlRankFunction;
import org.apache.calcite.sql.SqlSampleSpec;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUnnestOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlValuesOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlModality;

import com.google.common.collect.ImmutableList;

/**
 * Implementation of {@link org.apache.calcite.sql.SqlOperatorTable} containing
 * the standard operators and functions.
 */
public class SqlStdOperatorTable extends ReflectiveSqlOperatorTable {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * The standard operator table.
   */
  private static SqlStdOperatorTable instance;

  //-------------------------------------------------------------
  //                   SET OPERATORS
  //-------------------------------------------------------------
  // The set operators can be compared to the arithmetic operators
  // UNION -> +
  // EXCEPT -> -
  // INTERSECT -> *
  // which explains the different precedence values
  public static final SqlSetOperator UNION =
      new SqlSetOperator("UNION", SqlKind.UNION, 14, false);

  public static final SqlSetOperator UNION_ALL =
      new SqlSetOperator("UNION ALL", SqlKind.UNION, 14, true);

  public static final SqlSetOperator EXCEPT =
      new SqlSetOperator("EXCEPT", SqlKind.EXCEPT, 14, false);

  public static final SqlSetOperator EXCEPT_ALL =
      new SqlSetOperator("EXCEPT ALL", SqlKind.EXCEPT, 14, true);

  public static final SqlSetOperator INTERSECT =
      new SqlSetOperator("INTERSECT", SqlKind.INTERSECT, 18, false);

  public static final SqlSetOperator INTERSECT_ALL =
      new SqlSetOperator("INTERSECT ALL", SqlKind.INTERSECT, 18, true);

  /**
   * The "MULTISET UNION" operator.
   */
  public static final SqlMultisetSetOperator MULTISET_UNION =
      new SqlMultisetSetOperator("MULTISET UNION", 14, false);

  /**
   * The "MULTISET UNION ALL" operator.
   */
  public static final SqlMultisetSetOperator MULTISET_UNION_ALL =
      new SqlMultisetSetOperator("MULTISET UNION ALL", 14, true);

  /**
   * The "MULTISET EXCEPT" operator.
   */
  public static final SqlMultisetSetOperator MULTISET_EXCEPT =
      new SqlMultisetSetOperator("MULTISET EXCEPT", 14, false);

  /**
   * The "MULTISET EXCEPT ALL" operator.
   */
  public static final SqlMultisetSetOperator MULTISET_EXCEPT_ALL =
      new SqlMultisetSetOperator("MULTISET EXCEPT ALL", 14, true);

  /**
   * The "MULTISET INTERSECT" operator.
   */
  public static final SqlMultisetSetOperator MULTISET_INTERSECT =
      new SqlMultisetSetOperator("MULTISET INTERSECT", 18, false);

  /**
   * The "MULTISET INTERSECT ALL" operator.
   */
  public static final SqlMultisetSetOperator MULTISET_INTERSECT_ALL =
      new SqlMultisetSetOperator("MULTISET INTERSECT ALL", 18, true);

  //-------------------------------------------------------------
  //                   BINARY OPERATORS
  //-------------------------------------------------------------

  /**
   * Logical <code>AND</code> operator.
   */
  public static final SqlBinaryOperator AND =
      new SqlBinaryOperator(
          "AND",
          SqlKind.AND,
          28,
          true,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN_BOOLEAN);

  /**
   * <code>AS</code> operator associates an expression in the SELECT clause
   * with an alias.
   */
  public static final SqlAsOperator AS = new SqlAsOperator();

  /** <code>FILTER</code> operator filters which rows are included in an
   *  aggregate function. */
  public static final SqlFilterOperator FILTER = new SqlFilterOperator();

  /** {@code CUBE} operator, occurs within {@code GROUP BY} clause
   * or nested within a {@code GROUPING SETS}. */
  public static final SqlInternalOperator CUBE =
      new SqlRollupOperator("CUBE", SqlKind.CUBE);

  /** {@code ROLLUP} operator, occurs within {@code GROUP BY} clause
   * or nested within a {@code GROUPING SETS}. */
  public static final SqlInternalOperator ROLLUP =
      new SqlRollupOperator("ROLLUP", SqlKind.ROLLUP);

  /** {@code GROUPING SETS} operator, occurs within {@code GROUP BY} clause
   * or nested within a {@code GROUPING SETS}. */
  public static final SqlInternalOperator GROUPING_SETS =
      new SqlRollupOperator("GROUPING SETS", SqlKind.GROUPING_SETS);

  /** {@code GROUPING} function. Occurs in similar places to an aggregate
   * function ({@code SELECT}, {@code HAVING} clause, etc. of an aggregate
   * query), but not technically an aggregate function. */
  public static final SqlGroupingFunction GROUPING =
      new SqlGroupingFunction();

  /** {@code GROUP_ID} function. */
  public static final SqlGroupIdFunction GROUP_ID =
      new SqlGroupIdFunction();

  /** {@code GROUPING_ID} function. */
  public static final SqlGroupingIdFunction GROUPING_ID =
      new SqlGroupingIdFunction();

  /** {@code EXTEND} operator. */
  public static final SqlInternalOperator EXTEND = new SqlExtendOperator();

  /**
   * String concatenation operator, '<code>||</code>'.
   */
  public static final SqlBinaryOperator CONCAT =
      new SqlBinaryOperator(
          "||",
          SqlKind.OTHER,
          60,
          true,
          ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE,
          null,
          OperandTypes.STRING_SAME_SAME);

  /**
   * Arithmetic division operator, '<code>/</code>'.
   */
  public static final SqlBinaryOperator DIVIDE =
      new SqlBinaryOperator(
          "/",
          SqlKind.DIVIDE,
          60,
          true,
          ReturnTypes.QUOTIENT_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.DIVISION_OPERATOR);

  /**
   * Internal integer arithmetic division operator, '<code>/INT</code>'. This
   * is only used to adjust scale for numerics. We distinguish it from
   * user-requested division since some personalities want a floating-point
   * computation, whereas for the internal scaling use of division, we always
   * want integer division.
   */
  public static final SqlBinaryOperator DIVIDE_INTEGER =
      new SqlBinaryOperator(
          "/INT",
          SqlKind.DIVIDE,
          60,
          true,
          ReturnTypes.INTEGER_QUOTIENT_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.DIVISION_OPERATOR);

  /**
   * Dot operator, '<code>.</code>', used for referencing fields of records.
   */
  public static final SqlBinaryOperator DOT =
      new SqlBinaryOperator(
          ".",
          SqlKind.DOT,
          80,
          true,
          null,
          null,
          OperandTypes.ANY_ANY);

  /**
   * Logical equals operator, '<code>=</code>'.
   */
  public static final SqlBinaryOperator EQUALS =
      new SqlBinaryOperator(
          "=",
          SqlKind.EQUALS,
          30,
          true,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

  /**
   * Logical greater-than operator, '<code>&gt;</code>'.
   */
  public static final SqlBinaryOperator GREATER_THAN =
      new SqlBinaryOperator(
          ">",
          SqlKind.GREATER_THAN,
          30,
          true,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

  /**
   * <code>IS DISTINCT FROM</code> operator.
   */
  public static final SqlBinaryOperator IS_DISTINCT_FROM =
      new SqlBinaryOperator(
          "IS DISTINCT FROM",
          SqlKind.IS_DISTINCT_FROM,
          30,
          true,
          ReturnTypes.BOOLEAN,
          InferTypes.FIRST_KNOWN,
          OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

  /**
   * <code>IS NOT DISTINCT FROM</code> operator. Is equivalent to <code>NOT(x
   * IS DISTINCT FROM y)</code>
   */
  public static final SqlBinaryOperator IS_NOT_DISTINCT_FROM =
      new SqlBinaryOperator(
          "IS NOT DISTINCT FROM",
          SqlKind.IS_NOT_DISTINCT_FROM,
          30,
          true,
          ReturnTypes.BOOLEAN,
          InferTypes.FIRST_KNOWN,
          OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

  /**
   * The internal <code>$IS_DIFFERENT_FROM</code> operator is the same as the
   * user-level {@link #IS_DISTINCT_FROM} in all respects except that
   * the test for equality on character datatypes treats trailing spaces as
   * significant.
   */
  public static final SqlBinaryOperator IS_DIFFERENT_FROM =
      new SqlBinaryOperator(
          "$IS_DIFFERENT_FROM",
          SqlKind.OTHER,
          30,
          true,
          ReturnTypes.BOOLEAN,
          InferTypes.FIRST_KNOWN,
          OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

  /**
   * Logical greater-than-or-equal operator, '<code>&gt;=</code>'.
   */
  public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL =
      new SqlBinaryOperator(
          ">=",
          SqlKind.GREATER_THAN_OR_EQUAL,
          30,
          true,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

  /**
   * <code>IN</code> operator tests for a value's membership in a subquery or
   * a list of values.
   */
  public static final SqlBinaryOperator IN = new SqlInOperator(false);

  /**
   * <code>NOT IN</code> operator tests for a value's membership in a subquery
   * or a list of values.
   */
  public static final SqlBinaryOperator NOT_IN =
      new SqlInOperator(true);

  /**
   * Logical less-than operator, '<code>&lt;</code>'.
   */
  public static final SqlBinaryOperator LESS_THAN =
      new SqlBinaryOperator(
          "<",
          SqlKind.LESS_THAN,
          30,
          true,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

  /**
   * Logical less-than-or-equal operator, '<code>&lt;=</code>'.
   */
  public static final SqlBinaryOperator LESS_THAN_OR_EQUAL =
      new SqlBinaryOperator(
          "<=",
          SqlKind.LESS_THAN_OR_EQUAL,
          30,
          true,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

  /**
   * Infix arithmetic minus operator, '<code>-</code>'.
   *
   * <p>Its precedence is less than the prefix {@link #UNARY_PLUS +}
   * and {@link #UNARY_MINUS -} operators.
   */
  public static final SqlBinaryOperator MINUS =
      new SqlMonotonicBinaryOperator(
          "-",
          SqlKind.MINUS,
          40,
          true,

          // Same type inference strategy as sum
          ReturnTypes.NULLABLE_SUM,
          InferTypes.FIRST_KNOWN,
          OperandTypes.MINUS_OPERATOR);

  /**
   * Arithmetic multiplication operator, '<code>*</code>'.
   */
  public static final SqlBinaryOperator MULTIPLY =
      new SqlMonotonicBinaryOperator(
          "*",
          SqlKind.TIMES,
          60,
          true,
          ReturnTypes.PRODUCT_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.MULTIPLY_OPERATOR);

  /**
   * Logical not-equals operator, '<code>&lt;&gt;</code>'.
   */
  public static final SqlBinaryOperator NOT_EQUALS =
      new SqlBinaryOperator(
          "<>",
          SqlKind.NOT_EQUALS,
          30,
          true,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

  /**
   * Logical <code>OR</code> operator.
   */
  public static final SqlBinaryOperator OR =
      new SqlBinaryOperator(
          "OR",
          SqlKind.OR,
          26,
          true,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN_BOOLEAN);

  /**
   * Infix arithmetic plus operator, '<code>+</code>'.
   */
  public static final SqlBinaryOperator PLUS =
      new SqlMonotonicBinaryOperator(
          "+",
          SqlKind.PLUS,
          40,
          true,
          ReturnTypes.NULLABLE_SUM,
          InferTypes.FIRST_KNOWN,
          OperandTypes.PLUS_OPERATOR);

  /**
   * Infix datetime plus operator, '<code>DATETIME + INTERVAL</code>'.
   */
  public static final SqlSpecialOperator DATETIME_PLUS =
      new SqlSpecialOperator(
          "DATETIME_PLUS",
          SqlKind.PLUS,
          40,
          true,
          ReturnTypes.NULLABLE_SUM,
          InferTypes.FIRST_KNOWN,
          OperandTypes.PLUS_OPERATOR);

  /**
   * Multiset MEMBER OF. Checks to see if a element belongs to a multiset.<br>
   * Example:<br>
   * <code>'green' MEMBER OF MULTISET['red','almost green','blue']</code>
   * returns <code>false</code>.
   */
  public static final SqlBinaryOperator MEMBER_OF =
      new SqlMultisetMemberOfOperator();

  /**
   * Submultiset. Checks to see if an multiset is a sub-set of another
   * multiset.<br>
   * Example:<br>
   * <code>MULTISET['green'] SUBMULTISET OF MULTISET['red','almost
   * green','blue']</code> returns <code>false</code>.
   *
   * <p>But <code>MULTISET['blue', 'red'] SUBMULTISET OF
   * MULTISET['red','almost green','blue']</code> returns <code>true</code>
   * (<b>NB</b> multisets is order independant)
   */
  public static final SqlBinaryOperator SUBMULTISET_OF =

      // TODO: check if precedence is correct
      new SqlBinaryOperator(
          "SUBMULTISET OF",
          SqlKind.OTHER,
          30,
          true,
          ReturnTypes.BOOLEAN_NULLABLE,
          null,
          OperandTypes.MULTISET_MULTISET);

  //-------------------------------------------------------------
  //                   POSTFIX OPERATORS
  //-------------------------------------------------------------
  public static final SqlPostfixOperator DESC =
      new SqlPostfixOperator(
          "DESC",
          SqlKind.DESCENDING,
          20,
          ReturnTypes.ARG0,
          InferTypes.RETURN_TYPE,
          OperandTypes.ANY);

  public static final SqlPostfixOperator NULLS_FIRST =
      new SqlPostfixOperator(
          "NULLS FIRST",
          SqlKind.NULLS_FIRST,
          18,
          ReturnTypes.ARG0,
          InferTypes.RETURN_TYPE,
          OperandTypes.ANY);

  public static final SqlPostfixOperator NULLS_LAST =
      new SqlPostfixOperator(
          "NULLS LAST",
          SqlKind.NULLS_LAST,
          18,
          ReturnTypes.ARG0,
          InferTypes.RETURN_TYPE,
          OperandTypes.ANY);

  public static final SqlPostfixOperator IS_NOT_NULL =
      new SqlPostfixOperator(
          "IS NOT NULL",
          SqlKind.IS_NOT_NULL,
          30,
          ReturnTypes.BOOLEAN_NOT_NULL,
          InferTypes.VARCHAR_1024,
          OperandTypes.ANY);

  public static final SqlPostfixOperator IS_NULL =
      new SqlPostfixOperator(
          "IS NULL",
          SqlKind.IS_NULL,
          30,
          ReturnTypes.BOOLEAN_NOT_NULL,
          InferTypes.VARCHAR_1024,
          OperandTypes.ANY);

  public static final SqlPostfixOperator IS_NOT_TRUE =
      new SqlPostfixOperator(
          "IS NOT TRUE",
          SqlKind.IS_NOT_TRUE,
          30,
          ReturnTypes.BOOLEAN_NOT_NULL,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN);

  public static final SqlPostfixOperator IS_TRUE =
      new SqlPostfixOperator(
          "IS TRUE",
          SqlKind.IS_TRUE,
          30,
          ReturnTypes.BOOLEAN_NOT_NULL,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN);

  public static final SqlPostfixOperator IS_NOT_FALSE =
      new SqlPostfixOperator(
          "IS NOT FALSE",
          SqlKind.IS_NOT_FALSE,
          30,
          ReturnTypes.BOOLEAN_NOT_NULL,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN);

  public static final SqlPostfixOperator IS_FALSE =
      new SqlPostfixOperator(
          "IS FALSE",
          SqlKind.IS_FALSE,
          30,
          ReturnTypes.BOOLEAN_NOT_NULL,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN);

  public static final SqlPostfixOperator IS_NOT_UNKNOWN =
      new SqlPostfixOperator(
          "IS NOT UNKNOWN",
          SqlKind.IS_NOT_NULL,
          30,
          ReturnTypes.BOOLEAN_NOT_NULL,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN);

  public static final SqlPostfixOperator IS_UNKNOWN =
      new SqlPostfixOperator(
          "IS UNKNOWN",
          SqlKind.IS_NULL,
          30,
          ReturnTypes.BOOLEAN_NOT_NULL,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN);

  public static final SqlPostfixOperator IS_A_SET =
      new SqlPostfixOperator(
          "IS A SET",
          SqlKind.OTHER,
          30,
          ReturnTypes.BOOLEAN,
          null,
          OperandTypes.MULTISET);

  //-------------------------------------------------------------
  //                   PREFIX OPERATORS
  //-------------------------------------------------------------
  public static final SqlPrefixOperator EXISTS =
      new SqlPrefixOperator(
          "EXISTS",
          SqlKind.EXISTS,
          40,
          ReturnTypes.BOOLEAN,
          null,
          OperandTypes.ANY) {
        public boolean argumentMustBeScalar(int ordinal) {
          return false;
        }
      };

  public static final SqlPrefixOperator NOT =
      new SqlPrefixOperator(
          "NOT",
          SqlKind.NOT,
          30,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN);

  /**
   * Prefix arithmetic minus operator, '<code>-</code>'.
   *
   * <p>Its precedence is greater than the infix '{@link #PLUS +}' and
   * '{@link #MINUS -}' operators.
   */
  public static final SqlPrefixOperator UNARY_MINUS =
      new SqlPrefixOperator(
          "-",
          SqlKind.MINUS_PREFIX,
          80,
          ReturnTypes.ARG0,
          InferTypes.RETURN_TYPE,
          OperandTypes.NUMERIC_OR_INTERVAL);

  /**
   * Prefix arithmetic plus operator, '<code>+</code>'.
   *
   * <p>Its precedence is greater than the infix '{@link #PLUS +}' and
   * '{@link #MINUS -}' operators.
   */
  public static final SqlPrefixOperator UNARY_PLUS =
      new SqlPrefixOperator(
          "+",
          SqlKind.PLUS_PREFIX,
          80,
          ReturnTypes.ARG0,
          InferTypes.RETURN_TYPE,
          OperandTypes.NUMERIC_OR_INTERVAL);

  /**
   * Keyword which allows an identifier to be explicitly flagged as a table.
   * For example, <code>select * from (TABLE t)</code> or <code>TABLE
   * t</code>. See also {@link #COLLECTION_TABLE}.
   */
  public static final SqlPrefixOperator EXPLICIT_TABLE =
      new SqlPrefixOperator(
          "TABLE",
          SqlKind.EXPLICIT_TABLE,
          2,
          null,
          null,
          null);

  //-------------------------------------------------------------
  // AGGREGATE OPERATORS
  //-------------------------------------------------------------
  /**
   * <code>SUM</code> aggregate function.
   */
  public static final SqlAggFunction SUM = new SqlSumAggFunction(null);

  /**
   * <code>COUNT</code> aggregate function.
   */
  public static final SqlAggFunction COUNT = new SqlCountAggFunction();

  /**
   * <code>MIN</code> aggregate function.
   */
  public static final SqlAggFunction MIN =
      new SqlMinMaxAggFunction(
          ImmutableList.<RelDataType>of(),
          true,
          SqlMinMaxAggFunction.MINMAX_COMPARABLE);

  /**
   * <code>MAX</code> aggregate function.
   */
  public static final SqlAggFunction MAX =
      new SqlMinMaxAggFunction(
          ImmutableList.<RelDataType>of(),
          false,
          SqlMinMaxAggFunction.MINMAX_COMPARABLE);

  /**
   * <code>LAST_VALUE</code> aggregate function.
   */
  public static final SqlAggFunction LAST_VALUE =
      new SqlFirstLastValueAggFunction(false);

  /**
   * <code>FIRST_VALUE</code> aggregate function.
   */
  public static final SqlAggFunction FIRST_VALUE =
      new SqlFirstLastValueAggFunction(true);

  /**
   * <code>LEAD</code> aggregate function.
   */
  public static final SqlAggFunction LEAD =
      new SqlLeadLagAggFunction(true);

  /**
   * <code>LAG</code> aggregate function.
   */
  public static final SqlAggFunction LAG =
      new SqlLeadLagAggFunction(false);

  /**
   * <code>NTILE</code> aggregate function.
   */
  public static final SqlAggFunction NTILE =
      new SqlNtileAggFunction();

  /**
   * <code>SINGLE_VALUE</code> aggregate function.
   */
  public static final SqlAggFunction SINGLE_VALUE =
      new SqlSingleValueAggFunction(null);

  /**
   * <code>AVG</code> aggregate function.
   */
  public static final SqlAggFunction AVG =
      new SqlAvgAggFunction(null, SqlAvgAggFunction.Subtype.AVG);

  /**
   * <code>STDDEV_POP</code> aggregate function.
   */
  public static final SqlAggFunction STDDEV_POP =
      new SqlAvgAggFunction(null, SqlAvgAggFunction.Subtype.STDDEV_POP);

  /**
   * <code>REGR_SXX</code> aggregate function.
   */
  public static final SqlAggFunction REGR_SXX =
      new SqlCovarAggFunction(null, SqlCovarAggFunction.Subtype.REGR_SXX);

  /**
   * <code>REGR_SYY</code> aggregate function.
   */
  public static final SqlAggFunction REGR_SYY =
      new SqlCovarAggFunction(null, SqlCovarAggFunction.Subtype.REGR_SYY);

  /**
   * <code>COVAR_POP</code> aggregate function.
   */
  public static final SqlAggFunction COVAR_POP =
      new SqlCovarAggFunction(null, SqlCovarAggFunction.Subtype.COVAR_POP);

  /**
   * <code>COVAR_SAMP</code> aggregate function.
   */
  public static final SqlAggFunction COVAR_SAMP =
      new SqlCovarAggFunction(null, SqlCovarAggFunction.Subtype.COVAR_SAMP);

  /**
   * <code>STDDEV_SAMP</code> aggregate function.
   */
  public static final SqlAggFunction STDDEV_SAMP =
      new SqlAvgAggFunction(null, SqlAvgAggFunction.Subtype.STDDEV_SAMP);

  /**
   * <code>VAR_POP</code> aggregate function.
   */
  public static final SqlAggFunction VAR_POP =
      new SqlAvgAggFunction(null, SqlAvgAggFunction.Subtype.VAR_POP);

  /**
   * <code>VAR_SAMP</code> aggregate function.
   */
  public static final SqlAggFunction VAR_SAMP =
      new SqlAvgAggFunction(null, SqlAvgAggFunction.Subtype.VAR_SAMP);

  //-------------------------------------------------------------
  // WINDOW Aggregate Functions
  //-------------------------------------------------------------
  /**
   * <code>HISTOGRAM</code> aggregate function support. Used by window
   * aggregate versions of MIN/MAX
   */
  public static final SqlAggFunction HISTOGRAM_AGG =
      new SqlHistogramAggFunction(null);

  /**
   * <code>HISTOGRAM_MIN</code> window aggregate function.
   */
  public static final SqlFunction HISTOGRAM_MIN =
      new SqlFunction(
          "$HISTOGRAM_MIN",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE,
          null,
          OperandTypes.NUMERIC_OR_STRING,
          SqlFunctionCategory.NUMERIC);

  /**
   * <code>HISTOGRAM_MAX</code> window aggregate function.
   */
  public static final SqlFunction HISTOGRAM_MAX =
      new SqlFunction(
          "$HISTOGRAM_MAX",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE,
          null,
          OperandTypes.NUMERIC_OR_STRING,
          SqlFunctionCategory.NUMERIC);

  /**
   * <code>HISTOGRAM_FIRST_VALUE</code> window aggregate function.
   */
  public static final SqlFunction HISTOGRAM_FIRST_VALUE =
      new SqlFunction(
          "$HISTOGRAM_FIRST_VALUE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE,
          null,
          OperandTypes.NUMERIC_OR_STRING,
          SqlFunctionCategory.NUMERIC);

  /**
   * <code>HISTOGRAM_LAST_VALUE</code> window aggregate function.
   */
  public static final SqlFunction HISTOGRAM_LAST_VALUE =
      new SqlFunction(
          "$HISTOGRAM_LAST_VALUE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE,
          null,
          OperandTypes.NUMERIC_OR_STRING,
          SqlFunctionCategory.NUMERIC);

  /**
   * <code>SUM0</code> aggregate function.
   */
  public static final SqlAggFunction SUM0 =
      new SqlSumEmptyIsZeroAggFunction();

  //-------------------------------------------------------------
  // WINDOW Rank Functions
  //-------------------------------------------------------------
  /**
   * <code>CUME_DIST</code> Window function.
   */
  public static final SqlRankFunction CUME_DIST =
      new SqlRankFunction("CUME_DIST");

  /**
   * <code>DENSE_RANK</code> Window function.
   */
  public static final SqlRankFunction DENSE_RANK =
      new SqlRankFunction("DENSE_RANK");

  /**
   * <code>PERCENT_RANK</code> Window function.
   */
  public static final SqlRankFunction PERCENT_RANK =
      new SqlRankFunction("PERCENT_RANK");

  /**
   * <code>RANK</code> Window function.
   */
  public static final SqlRankFunction RANK = new SqlRankFunction("RANK");

  /**
   * <code>ROW_NUMBER</code> Window function.
   */
  public static final SqlRankFunction ROW_NUMBER =
      new SqlRankFunction("ROW_NUMBER");

  //-------------------------------------------------------------
  //                   SPECIAL OPERATORS
  //-------------------------------------------------------------
  public static final SqlRowOperator ROW = new SqlRowOperator();

  /**
   * A special operator for the subtraction of two DATETIMEs. The format of
   * DATETIME substraction is:
   *
   * <blockquote><code>"(" &lt;datetime&gt; "-" &lt;datetime&gt; ")"
   * &lt;interval qualifier&gt;</code></blockquote>
   *
   * <p>This operator is special since it needs to hold the
   * additional interval qualifier specification.</p>
   */
  public static final SqlOperator MINUS_DATE =
      new SqlDatetimeSubtractionOperator();

  /**
   * The MULTISET Value Constructor. e.g. "<code>MULTISET[1,2,3]</code>".
   */
  public static final SqlMultisetValueConstructor MULTISET_VALUE =
      new SqlMultisetValueConstructor();

  /**
   * The MULTISET Query Constructor. e.g. "<code>SELECT dname, MULTISET(SELECT
   * FROM emp WHERE deptno = dept.deptno) FROM dept</code>".
   */
  public static final SqlMultisetQueryConstructor MULTISET_QUERY =
      new SqlMultisetQueryConstructor();

  /**
   * The ARRAY Query Constructor. e.g. "<code>SELECT dname, ARRAY(SELECT
   * FROM emp WHERE deptno = dept.deptno) FROM dept</code>".
   */
  public static final SqlMultisetQueryConstructor ARRAY_QUERY =
      new SqlArrayQueryConstructor();

  /**
   * The MAP Query Constructor. e.g. "<code>MAP(SELECT empno, deptno
   * FROM emp)</code>".
   */
  public static final SqlMultisetQueryConstructor MAP_QUERY =
      new SqlMapQueryConstructor();

  /**
   * The CURSOR constructor. e.g. "<code>SELECT * FROM
   * TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), 'name'))</code>".
   */
  public static final SqlCursorConstructor CURSOR =
      new SqlCursorConstructor();

  /**
   * The COLUMN_LIST constructor. e.g. the ROW() call in "<code>SELECT * FROM
   * TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), ROW(name, empno)))</code>".
   */
  public static final SqlColumnListConstructor COLUMN_LIST =
      new SqlColumnListConstructor();

  /**
   * The <code>UNNEST</code> operator.
   */
  public static final SqlSpecialOperator UNNEST =
      new SqlUnnestOperator();

  /**
   * The <code>LATERAL</code> operator.
   */
  public static final SqlSpecialOperator LATERAL =
      new SqlFunctionalOperator(
          "LATERAL",
          SqlKind.LATERAL,
          200,
          true,
          ReturnTypes.ARG0,
          null,
          OperandTypes.ANY);

  /**
   * The "table function derived table" operator, which a table-valued
   * function into a relation, e.g. "<code>SELECT * FROM
   * TABLE(ramp(5))</code>".
   *
   * <p>This operator has function syntax (with one argument), whereas
   * {@link #EXPLICIT_TABLE} is a prefix operator.
   */
  public static final SqlSpecialOperator COLLECTION_TABLE =
      new SqlCollectionTableOperator("TABLE", SqlModality.RELATION);

  public static final SqlOverlapsOperator OVERLAPS =
      new SqlOverlapsOperator();

  public static final SqlSpecialOperator VALUES =
      new SqlValuesOperator();

  public static final SqlLiteralChainOperator LITERAL_CHAIN =
      new SqlLiteralChainOperator();

  public static final SqlThrowOperator THROW = new SqlThrowOperator();

  public static final SqlBetweenOperator BETWEEN =
      new SqlBetweenOperator(
          SqlBetweenOperator.Flag.ASYMMETRIC,
          false);

  public static final SqlBetweenOperator SYMMETRIC_BETWEEN =
      new SqlBetweenOperator(
          SqlBetweenOperator.Flag.SYMMETRIC,
          false);

  public static final SqlBetweenOperator NOT_BETWEEN =
      new SqlBetweenOperator(
          SqlBetweenOperator.Flag.ASYMMETRIC,
          true);

  public static final SqlBetweenOperator SYMMETRIC_NOT_BETWEEN =
      new SqlBetweenOperator(
          SqlBetweenOperator.Flag.SYMMETRIC,
          true);

  public static final SqlSpecialOperator NOT_LIKE =
      new SqlLikeOperator("NOT LIKE", SqlKind.LIKE, true);

  public static final SqlSpecialOperator LIKE =
      new SqlLikeOperator("LIKE", SqlKind.LIKE, false);

  public static final SqlSpecialOperator NOT_SIMILAR_TO =
      new SqlLikeOperator("NOT SIMILAR TO", SqlKind.SIMILAR, true);

  public static final SqlSpecialOperator SIMILAR_TO =
      new SqlLikeOperator("SIMILAR TO", SqlKind.SIMILAR, false);

  /**
   * Internal operator used to represent the ESCAPE clause of a LIKE or
   * SIMILAR TO expression.
   */
  public static final SqlSpecialOperator ESCAPE =
      new SqlSpecialOperator("Escape", SqlKind.ESCAPE, 30);

  public static final SqlCaseOperator CASE = SqlCaseOperator.INSTANCE;

  public static final SqlOperator PROCEDURE_CALL =
      new SqlProcedureCallOperator();

  public static final SqlOperator NEW = new SqlNewOperator();

  /**
   * The <code>OVER</code> operator, which applies an aggregate functions to a
   * {@link SqlWindow window}.
   *
   * <p>Operands are as follows:
   *
   * <ol>
   * <li>name of window function ({@link org.apache.calcite.sql.SqlCall})</li>
   * <li>window name ({@link org.apache.calcite.sql.SqlLiteral}) or window
   * in-line specification (@link SqlWindowOperator})</li>
   * </ol>
   */
  public static final SqlBinaryOperator OVER = new SqlOverOperator();

  /**
   * An <code>REINTERPRET</code> operator is internal to the planner. When the
   * physical storage of two types is the same, this operator may be used to
   * reinterpret values of one type as the other. This operator is similar to
   * a cast, except that it does not alter the data value. Like a regular cast
   * it accepts one operand and stores the target type as the return type. It
   * performs an overflow check if it has <i>any</i> second operand, whether
   * true or not.
   */
  public static final SqlSpecialOperator REINTERPRET =
      new SqlSpecialOperator("Reinterpret", SqlKind.REINTERPRET) {
        public SqlOperandCountRange getOperandCountRange() {
          return SqlOperandCountRanges.between(1, 2);
        }
      };

  /** Internal operator that extracts time periods (year, month, date) from a
   * date in internal format (number of days since epoch). */
  public static final SqlSpecialOperator EXTRACT_DATE =
      new SqlSpecialOperator("EXTRACT_DATE", SqlKind.OTHER);

  //-------------------------------------------------------------
  //                   FUNCTIONS
  //-------------------------------------------------------------

  /**
   * The character substring function: <code>SUBSTRING(string FROM start [FOR
   * length])</code>.
   *
   * <p>If the length parameter is a constant, the length of the result is the
   * minimum of the length of the input and that length. Otherwise it is the
   * length of the input.
   */
  public static final SqlFunction SUBSTRING = new SqlSubstringFunction();

  public static final SqlFunction CONVERT =
      new SqlConvertFunction("CONVERT");

  public static final SqlFunction TRANSLATE =
      new SqlConvertFunction("TRANSLATE");

  public static final SqlFunction OVERLAY = new SqlOverlayFunction();

  /** The "TRIM" function. */
  public static final SqlFunction TRIM = new SqlTrimFunction();

  public static final SqlFunction POSITION = new SqlPositionFunction();

  public static final SqlFunction CHAR_LENGTH =
      new SqlFunction(
          "CHAR_LENGTH",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.NUMERIC);

  public static final SqlFunction CHARACTER_LENGTH =
      new SqlFunction(
          "CHARACTER_LENGTH",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.NUMERIC);

  public static final SqlFunction UPPER =
      new SqlFunction(
          "UPPER",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE,
          null,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  public static final SqlFunction LOWER =
      new SqlFunction(
          "LOWER",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE,
          null,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  public static final SqlFunction INITCAP =
      new SqlFunction(
          "INITCAP",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE,
          null,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  /**
   * Uses SqlOperatorTable.useDouble for its return type since we don't know
   * what the result type will be by just looking at the operand types. For
   * example POW(int, int) can return a non integer if the second operand is
   * negative.
   */
  public static final SqlFunction POWER =
      new SqlFunction(
          "POWER",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);

  public static final SqlFunction SQRT =
      new SqlFunction(
          "SQRT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  public static final SqlFunction MOD =

      // Return type is same as divisor (2nd operand)
      // SQL2003 Part2 Section 6.27, Syntax Rules 9
      new SqlFunction(
          "MOD",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG1_NULLABLE,
          null,
          OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC,
          SqlFunctionCategory.NUMERIC);

  public static final SqlFunction LN =
      new SqlFunction(
          "LN",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  public static final SqlFunction LOG10 =
      new SqlFunction(
          "LOG10",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  public static final SqlFunction ABS =
      new SqlFunction(
          "ABS",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0,
          null,
          OperandTypes.NUMERIC_OR_INTERVAL,
          SqlFunctionCategory.NUMERIC);

  public static final SqlFunction EXP =
      new SqlFunction(
          "EXP",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  public static final SqlFunction NULLIF = new SqlNullifFunction();

  /**
   * The COALESCE builtin function.
   */
  public static final SqlFunction COALESCE = new SqlCoalesceFunction();

  /**
   * The <code>FLOOR</code> function.
   */
  public static final SqlFunction FLOOR = new SqlFloorFunction(SqlKind.FLOOR);

  /**
   * The <code>CEIL</code> function.
   */
  public static final SqlFunction CEIL = new SqlFloorFunction(SqlKind.CEIL);

  /**
   * The <code>USER</code> function.
   */
  public static final SqlFunction USER =
      new SqlStringContextVariable("USER");

  /**
   * The <code>CURRENT_USER</code> function.
   */
  public static final SqlFunction CURRENT_USER =
      new SqlStringContextVariable("CURRENT_USER");

  /**
   * The <code>SESSION_USER</code> function.
   */
  public static final SqlFunction SESSION_USER =
      new SqlStringContextVariable("SESSION_USER");

  /**
   * The <code>SYSTEM_USER</code> function.
   */
  public static final SqlFunction SYSTEM_USER =
      new SqlStringContextVariable("SYSTEM_USER");

  /**
   * The <code>CURRENT_PATH</code> function.
   */
  public static final SqlFunction CURRENT_PATH =
      new SqlStringContextVariable("CURRENT_PATH");

  /**
   * The <code>CURRENT_ROLE</code> function.
   */
  public static final SqlFunction CURRENT_ROLE =
      new SqlStringContextVariable("CURRENT_ROLE");

  /**
   * The <code>CURRENT_CATALOG</code> function.
   */
  public static final SqlFunction CURRENT_CATALOG =
      new SqlStringContextVariable("CURRENT_CATALOG");

  /**
   * The <code>CURRENT_SCHEMA</code> function.
   */
  public static final SqlFunction CURRENT_SCHEMA =
      new SqlStringContextVariable("CURRENT_SCHEMA");

  /**
   * The <code>LOCALTIME [(<i>precision</i>)]</code> function.
   */
  public static final SqlFunction LOCALTIME =
      new SqlAbstractTimeFunction("LOCALTIME", SqlTypeName.TIME);

  /**
   * The <code>LOCALTIMESTAMP [(<i>precision</i>)]</code> function.
   */
  public static final SqlFunction LOCALTIMESTAMP =
      new SqlAbstractTimeFunction("LOCALTIMESTAMP", SqlTypeName.TIMESTAMP);

  /**
   * The <code>CURRENT_TIME [(<i>precision</i>)]</code> function.
   */
  public static final SqlFunction CURRENT_TIME =
      new SqlAbstractTimeFunction("CURRENT_TIME", SqlTypeName.TIME);

  /**
   * The <code>CURRENT_TIMESTAMP [(<i>precision</i>)]</code> function.
   */
  public static final SqlFunction CURRENT_TIMESTAMP =
      new SqlAbstractTimeFunction("CURRENT_TIMESTAMP", SqlTypeName.TIMESTAMP);

  /**
   * The <code>CURRENT_DATE</code> function.
   */
  public static final SqlFunction CURRENT_DATE =
      new SqlCurrentDateFunction();

  /**
   * Use of the <code>IN_FENNEL</code> operator forces the argument to be
   * evaluated in Fennel. Otherwise acts as identity function.
   */
  public static final SqlFunction IN_FENNEL =
      new SqlMonotonicUnaryFunction(
          "IN_FENNEL",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0,
          null,
          OperandTypes.ANY,
          SqlFunctionCategory.SYSTEM);

  /**
   * The SQL <code>CAST</code> operator.
   *
   * <p>The SQL syntax is
   *
   * <blockquote><code>CAST(<i>expression</i> AS <i>type</i>)</code>
   * </blockquote>
   *
   * <p>When the CAST operator is applies as a {@link SqlCall}, it has two
   * arguments: the expression and the type. The type must not include a
   * constraint, so <code>CAST(x AS INTEGER NOT NULL)</code>, for instance, is
   * invalid.</p>
   *
   * <p>When the CAST operator is applied as a <code>RexCall</code>, the
   * target type is simply stored as the return type, not an explicit operand.
   * For example, the expression <code>CAST(1 + 2 AS DOUBLE)</code> will
   * become a call to <code>CAST</code> with the expression <code>1 + 2</code>
   * as its only operand.</p>
   *
   * <p>The <code>RexCall</code> form can also have a type which contains a
   * <code>NOT NULL</code> constraint. When this expression is implemented, if
   * the value is NULL, an exception will be thrown.</p>
   */
  public static final SqlFunction CAST = new SqlCastFunction();

  /**
   * The SQL <code>EXTRACT</code> operator. Extracts a specified field value
   * from a DATETIME or an INTERVAL. E.g.<br>
   * <code>EXTRACT(HOUR FROM INTERVAL '364 23:59:59')</code> returns <code>
   * 23</code>
   */
  public static final SqlFunction EXTRACT = new SqlExtractFunction();

  /**
   * The SQL <code>QUARTER</code> operator. Returns the Quarter
   * from a DATETIME  E.g.<br>
   * <code>QUARTER(date '2008-9-23')</code> returns <code>
   * 3</code>
   */
  public static final SqlQuarterFunction QUARTER = new SqlQuarterFunction();

  /**
   * The ELEMENT operator, used to convert a multiset with only one item to a
   * "regular" type. Example ... log(ELEMENT(MULTISET[1])) ...
   */
  public static final SqlFunction ELEMENT =
      new SqlFunction(
          "ELEMENT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.MULTISET_ELEMENT_NULLABLE,
          null,
          OperandTypes.COLLECTION,
          SqlFunctionCategory.SYSTEM);

  /**
   * The item operator {@code [ ... ]}, used to access a given element of an
   * array or map. For example, {@code myArray[3]} or {@code "myMap['foo']"}.
   *
   * <p>The SQL standard calls the ARRAY variant a
   * &lt;array element reference&gt;. Index is 1-based. The standard says
   * to raise "data exception - array element error" but we currently return
   * null.</p>
   *
   * <p>MAP is not standard SQL.</p>
   */
  public static final SqlOperator ITEM = new SqlItemOperator();

  /**
   * The ARRAY Value Constructor. e.g. "<code>ARRAY[1, 2, 3]</code>".
   */
  public static final SqlArrayValueConstructor ARRAY_VALUE_CONSTRUCTOR =
      new SqlArrayValueConstructor();

  /**
   * The MAP Value Constructor,
   * e.g. "<code>MAP['washington', 1, 'obama', 44]</code>".
   */
  public static final SqlMapValueConstructor MAP_VALUE_CONSTRUCTOR =
      new SqlMapValueConstructor();

  /**
   * The internal "$SLICE" operator takes a multiset of records and returns a
   * multiset of the first column of those records.
   *
   * <p>It is introduced when multisets of scalar types are created, in order
   * to keep types consistent. For example, <code>MULTISET [5]</code> has type
   * <code>INTEGER MULTISET</code> but is translated to an expression of type
   * <code>RECORD(INTEGER EXPR$0) MULTISET</code> because in our internal
   * representation of multisets, every element must be a record. Applying the
   * "$SLICE" operator to this result converts the type back to an <code>
   * INTEGER MULTISET</code> multiset value.
   *
   * <p><code>$SLICE</code> is often translated away when the multiset type is
   * converted back to scalar values.
   */
  public static final SqlInternalOperator SLICE =
      new SqlInternalOperator(
          "$SLICE",
          SqlKind.OTHER,
          0,
          false,
          ReturnTypes.MULTISET_PROJECT0,
          null,
          OperandTypes.RECORD_COLLECTION) {
      };

  /**
   * The internal "$ELEMENT_SLICE" operator returns the first field of the
   * only element of a multiset.
   *
   * <p>It is introduced when multisets of scalar types are created, in order
   * to keep types consistent. For example, <code>ELEMENT(MULTISET [5])</code>
   * is translated to <code>$ELEMENT_SLICE(MULTISET (VALUES ROW (5
   * EXPR$0))</code> It is translated away when the multiset type is converted
   * back to scalar values.</p>
   *
   * <p>NOTE: jhyde, 2006/1/9: Usages of this operator are commented out, but
   * I'm not deleting the operator, because some multiset tests are disabled,
   * and we may need this operator to get them working!</p>
   */
  public static final SqlInternalOperator ELEMENT_SLICE =
      new SqlInternalOperator(
          "$ELEMENT_SLICE",
          SqlKind.OTHER,
          0,
          false,
          ReturnTypes.MULTISET_RECORD,
          null,
          OperandTypes.MULTISET) {
        public void unparse(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {
          SqlUtil.unparseFunctionSyntax(
              this,
              writer, call);
        }
      };

  /**
   * The internal "$SCALAR_QUERY" operator returns a scalar value from a
   * record type. It assumes the record type only has one field, and returns
   * that field as the output.
   */
  public static final SqlInternalOperator SCALAR_QUERY =
      new SqlInternalOperator(
          "$SCALAR_QUERY",
          SqlKind.SCALAR_QUERY,
          0,
          false,
          ReturnTypes.RECORD_TO_SCALAR,
          null,
          OperandTypes.RECORD_TO_SCALAR) {
        public void unparse(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {
          final SqlWriter.Frame frame = writer.startList("(", ")");
          call.operand(0).unparse(writer, 0, 0);
          writer.endList(frame);
        }

        public boolean argumentMustBeScalar(int ordinal) {
          // Obvious, really.
          return false;
        }
      };

  /**
   * The CARDINALITY operator, used to retrieve the number of elements in a
   * MULTISET, ARRAY or MAP.
   */
  public static final SqlFunction CARDINALITY =
      new SqlFunction(
          "CARDINALITY",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.COLLECTION_OR_MAP,
          SqlFunctionCategory.SYSTEM);

  /**
   * The COLLECT operator. Multiset aggregator function.
   */
  public static final SqlFunction COLLECT =
      new SqlFunction(
          "COLLECT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0,
          null,
          OperandTypes.ANY,
          SqlFunctionCategory.SYSTEM);

  /**
   * The FUSION operator. Multiset aggregator function.
   */
  public static final SqlFunction FUSION =
      new SqlFunction(
          "FUSION",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0,
          null,
          OperandTypes.MULTISET,
          SqlFunctionCategory.SYSTEM);

  /**
   * The sequence next value function: <code>NEXT VALUE FOR sequence</code>
   */
  public static final SqlOperator NEXT_VALUE =
      new SqlSequenceValueOperator(SqlKind.NEXT_VALUE);

  /**
   * The sequence current value function: <code>CURRENT VALUE FOR
   * sequence</code>
   */
  public static final SqlOperator CURRENT_VALUE =
      new SqlSequenceValueOperator(SqlKind.CURRENT_VALUE);

  /**
   * The <code>TABLESAMPLE</code> operator.
   *
   * <p>Examples:
   *
   * <ul>
   * <li><code>&lt;query&gt; TABLESAMPLE SUBSTITUTE('sampleName')</code>
   * (non-standard)
   * <li><code>&lt;query&gt; TABLESAMPLE BERNOULLI(&lt;percent&gt;)
   * [REPEATABLE(&lt;seed&gt;)]</code> (standard, but not implemented for FTRS
   * yet)
   * <li><code>&lt;query&gt; TABLESAMPLE SYSTEM(&lt;percent&gt;)
   * [REPEATABLE(&lt;seed&gt;)]</code> (standard, but not implemented for FTRS
   * yet)
   * </ul>
   *
   * <p>Operand #0 is a query or table; Operand #1 is a {@link SqlSampleSpec}
   * wrapped in a {@link SqlLiteral}.
   */
  public static final SqlSpecialOperator TABLESAMPLE =
      new SqlSpecialOperator(
          "TABLESAMPLE",
          SqlKind.TABLESAMPLE,
          20,
          true,
          ReturnTypes.ARG0,
          null,
          OperandTypes.VARIADIC) {
        public void unparse(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {
          call.operand(0).unparse(writer, leftPrec, 0);
          writer.keyword("TABLESAMPLE");
          call.operand(1).unparse(writer, 0, rightPrec);
        }
      };

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the standard operator table, creating it if necessary.
   */
  public static synchronized SqlStdOperatorTable instance() {
    if (instance == null) {
      // Creates and initializes the standard operator table.
      // Uses two-phase construction, because we can't initialize the
      // table until the constructor of the sub-class has completed.
      instance = new SqlStdOperatorTable();
      instance.init();
    }
    return instance;
  }
}

// End SqlStdOperatorTable.java
