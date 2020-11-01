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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlLibrary.BIG_QUERY;
import static org.apache.calcite.sql.fun.SqlLibrary.HIVE;
import static org.apache.calcite.sql.fun.SqlLibrary.MYSQL;
import static org.apache.calcite.sql.fun.SqlLibrary.ORACLE;
import static org.apache.calcite.sql.fun.SqlLibrary.POSTGRESQL;
import static org.apache.calcite.sql.fun.SqlLibrary.SPARK;

/**
 * Defines functions and operators that are not part of standard SQL but
 * belong to one or more other dialects of SQL.
 *
 * <p>They are read by {@link SqlLibraryOperatorTableFactory} into instances
 * of {@link SqlOperatorTable} that contain functions and operators for
 * particular libraries.
 */
public abstract class SqlLibraryOperators {
  private SqlLibraryOperators() {
  }

  /** The "CONVERT_TIMEZONE(tz1, tz2, datetime)" function;
   * converts the timezone of {@code datetime} from {@code tz1} to {@code tz2}.
   * This function is only on Redshift, but we list it in PostgreSQL
   * because Redshift does not have its own library. */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlFunction CONVERT_TIMEZONE =
      new SqlFunction("CONVERT_TIMEZONE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DATE_NULLABLE,
          null,
          OperandTypes.CHARACTER_CHARACTER_DATETIME,
          SqlFunctionCategory.TIMEDATE);

  /** Return type inference for {@code DECODE}. */
  private static final SqlReturnTypeInference DECODE_RETURN_TYPE =
      opBinding -> {
        final List<RelDataType> list = new ArrayList<>();
        for (int i = 1, n = opBinding.getOperandCount(); i < n; i++) {
          if (i < n - 1) {
            ++i;
          }
          list.add(opBinding.getOperandType(i));
        }
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        RelDataType type = typeFactory.leastRestrictive(list);
        if (opBinding.getOperandCount() % 2 == 1) {
          type = typeFactory.createTypeWithNullability(type, true);
        }
        return type;
      };

  /** The "DECODE(v, v1, result1, [v2, result2, ...], resultN)" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction DECODE =
      new SqlFunction("DECODE", SqlKind.DECODE, DECODE_RETURN_TYPE, null,
          OperandTypes.VARIADIC, SqlFunctionCategory.SYSTEM);

  /** The "IF(condition, thenValue, elseValue)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, HIVE, SPARK})
  public static final SqlFunction IF =
      new SqlFunction("IF", SqlKind.IF, SqlLibraryOperators::inferIfReturnType,
          null,
          OperandTypes.and(
              OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY,
                  SqlTypeFamily.ANY),
              // Arguments 1 and 2 must have same type
              new SameOperandTypeChecker(3) {
                @Override protected List<Integer>
                getOperandList(int operandCount) {
                  return ImmutableList.of(1, 2);
                }
              }),
          SqlFunctionCategory.SYSTEM) {
        @Override public boolean validRexOperands(int count, Litmus litmus) {
          // IF is translated to RexNode by expanding to CASE.
          return litmus.fail("not a rex operator");
        }
      };

  /** Infers the return type of {@code IF(b, x, y)},
   * namely the least restrictive of the types of x and y.
   * Similar to {@link ReturnTypes#LEAST_RESTRICTIVE}. */
  private static RelDataType inferIfReturnType(SqlOperatorBinding opBinding) {
    return opBinding.getTypeFactory()
        .leastRestrictive(opBinding.collectOperandTypes().subList(1, 3));
  }

  /** The "NVL(value, value)" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction NVL =
      new SqlFunction("NVL", SqlKind.NVL,
          ReturnTypes.LEAST_RESTRICTIVE
              .andThen(SqlTypeTransforms.TO_NULLABLE_ALL),
          null, OperandTypes.SAME_SAME, SqlFunctionCategory.SYSTEM);

  /** The "LTRIM(string)" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction LTRIM =
      new SqlFunction("LTRIM", SqlKind.LTRIM,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_NULLABLE)
              .andThen(SqlTypeTransforms.TO_VARYING), null,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  /** The "RTRIM(string)" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction RTRIM =
      new SqlFunction("RTRIM", SqlKind.RTRIM,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_NULLABLE)
              .andThen(SqlTypeTransforms.TO_VARYING), null,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  /** Oracle's "SUBSTR(string, position [, substringLength ])" function.
   *
   * <p>It has similar semantics to standard SQL's
   * {@link SqlStdOperatorTable#SUBSTRING} function but different syntax. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction SUBSTR =
      new SqlFunction("SUBSTR", SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING, null, null,
          SqlFunctionCategory.STRING);

  /** The "GREATEST(value, value)" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction GREATEST =
      new SqlFunction("GREATEST", SqlKind.GREATEST,
          ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.SAME_VARIADIC, SqlFunctionCategory.SYSTEM);

  /** The "LEAST(value, value)" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction LEAST =
      new SqlFunction("LEAST", SqlKind.LEAST,
          ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.SAME_VARIADIC, SqlFunctionCategory.SYSTEM);

  /**
   * The <code>TRANSLATE(<i>string_expr</i>, <i>search_chars</i>,
   * <i>replacement_chars</i>)</code> function returns <i>string_expr</i> with
   * all occurrences of each character in <i>search_chars</i> replaced by its
   * corresponding character in <i>replacement_chars</i>.
   *
   * <p>It is not defined in the SQL standard, but occurs in Oracle and
   * PostgreSQL.
   */
  @LibraryOperator(libraries = {ORACLE, POSTGRESQL})
  public static final SqlFunction TRANSLATE3 = new SqlTranslate3Function();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_TYPE = new SqlJsonTypeFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_DEPTH = new SqlJsonDepthFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_LENGTH = new SqlJsonLengthFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_KEYS = new SqlJsonKeysFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_PRETTY = new SqlJsonPrettyFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_REMOVE = new SqlJsonRemoveFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_STORAGE_SIZE = new SqlJsonStorageSizeFunction();

  @LibraryOperator(libraries = {MYSQL, ORACLE})
  public static final SqlFunction REGEXP_REPLACE = new SqlRegexpReplaceFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction COMPRESS =
      new SqlFunction("COMPRESS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARBINARY)
              .andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.STRING, SqlFunctionCategory.STRING);


  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction EXTRACT_VALUE =
      new SqlFunction("EXTRACTVALUE", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000.andThen(SqlTypeTransforms.FORCE_NULLABLE),
          null, OperandTypes.STRING_STRING, SqlFunctionCategory.SYSTEM);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction XML_TRANSFORM =
      new SqlFunction("XMLTRANSFORM", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000.andThen(SqlTypeTransforms.FORCE_NULLABLE),
          null, OperandTypes.STRING_STRING, SqlFunctionCategory.SYSTEM);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction EXTRACT_XML =
      new SqlFunction("EXTRACT", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000.andThen(SqlTypeTransforms.FORCE_NULLABLE),
          null, OperandTypes.STRING_STRING_OPTIONAL_STRING,
          SqlFunctionCategory.SYSTEM);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction EXISTS_NODE =
      new SqlFunction("EXISTSNODE", SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE
              .andThen(SqlTypeTransforms.FORCE_NULLABLE), null,
          OperandTypes.STRING_STRING_OPTIONAL_STRING, SqlFunctionCategory.SYSTEM);

  /** The "BOOL_AND(condition)" aggregate function, PostgreSQL and Redshift's
   * equivalent to {@link SqlStdOperatorTable#EVERY}. */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlAggFunction BOOL_AND =
      new SqlMinMaxAggFunction("BOOL_AND", SqlKind.MIN, OperandTypes.BOOLEAN);

  /** The "BOOL_OR(condition)" aggregate function, PostgreSQL and Redshift's
   * equivalent to {@link SqlStdOperatorTable#SOME}. */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlAggFunction BOOL_OR =
      new SqlMinMaxAggFunction("BOOL_OR", SqlKind.MAX, OperandTypes.BOOLEAN);

  /** The "LOGICAL_AND(condition)" aggregate function, BigQuery's
   * equivalent to {@link SqlStdOperatorTable#EVERY}. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlAggFunction LOGICAL_AND =
      new SqlMinMaxAggFunction("LOGICAL_AND", SqlKind.MIN, OperandTypes.BOOLEAN);

  /** The "LOGICAL_OR(condition)" aggregate function, BigQuery's
   * equivalent to {@link SqlStdOperatorTable#SOME}. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlAggFunction LOGICAL_OR =
      new SqlMinMaxAggFunction("LOGICAL_OR", SqlKind.MAX, OperandTypes.BOOLEAN);

  /** The "ARRAY_AGG(value [ ORDER BY ...])" aggregate function,
   * in BigQuery and PostgreSQL, gathers values into arrays. */
  @LibraryOperator(libraries = {POSTGRESQL, BIG_QUERY})
  public static final SqlAggFunction ARRAY_AGG =
      SqlBasicAggFunction
          .create(SqlKind.ARRAY_AGG,
              ReturnTypes.andThen(ReturnTypes::stripOrderBy,
                  ReturnTypes.TO_ARRAY), OperandTypes.ANY)
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withSyntax(SqlSyntax.ORDERED_FUNCTION)
          .withAllowsNullTreatment(true);

  /** The "ARRAY_CONCAT_AGG(value [ ORDER BY ...])" aggregate function,
   * in BigQuery and PostgreSQL, concatenates array values into arrays. */
  @LibraryOperator(libraries = {POSTGRESQL, BIG_QUERY})
  public static final SqlAggFunction ARRAY_CONCAT_AGG =
      SqlBasicAggFunction
          .create(SqlKind.ARRAY_CONCAT_AGG, ReturnTypes.ARG0,
              OperandTypes.ARRAY)
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withSyntax(SqlSyntax.ORDERED_FUNCTION);

  /** The "STRING_AGG(value [, separator ] [ ORDER BY ...])" aggregate function,
   * BigQuery and PostgreSQL's equivalent of
   * {@link SqlStdOperatorTable#LISTAGG}.
   *
   * <p>{@code STRING_AGG(v, sep ORDER BY x, y)} is implemented by
   * rewriting to {@code LISTAGG(v, sep) WITHIN GROUP (ORDER BY x, y)}. */
  @LibraryOperator(libraries = {POSTGRESQL, BIG_QUERY})
  public static final SqlAggFunction STRING_AGG =
      SqlBasicAggFunction
          .create(SqlKind.STRING_AGG, ReturnTypes.ARG0_NULLABLE,
              OperandTypes.or(OperandTypes.STRING, OperandTypes.STRING_STRING))
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withSyntax(SqlSyntax.ORDERED_FUNCTION);

  /** The "DATE(string)" function, equivalent to "CAST(string AS DATE). */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATE =
      new SqlFunction("DATE", SqlKind.OTHER_FUNCTION,
          ReturnTypes.DATE_NULLABLE, null, OperandTypes.STRING,
          SqlFunctionCategory.TIMEDATE);

  /** The "CURRENT_DATETIME([timezone])" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction CURRENT_DATETIME =
      new SqlFunction("CURRENT_DATETIME", SqlKind.OTHER_FUNCTION,
          ReturnTypes.TIMESTAMP.andThen(SqlTypeTransforms.TO_NULLABLE), null,
          OperandTypes.or(OperandTypes.NILADIC, OperandTypes.STRING),
          SqlFunctionCategory.TIMEDATE);

  /** The "DATE_FROM_UNIX_DATE(integer)" function; returns a DATE value
   * a given number of seconds after 1970-01-01. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATE_FROM_UNIX_DATE =
      new SqlFunction("DATE_FROM_UNIX_DATE", SqlKind.OTHER_FUNCTION,
          ReturnTypes.DATE_NULLABLE, null, OperandTypes.INTEGER,
          SqlFunctionCategory.TIMEDATE);

  /** The "UNIX_DATE(date)" function; returns the number of days since
   * 1970-01-01. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction UNIX_DATE =
      new SqlFunction("UNIX_DATE", SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE, null, OperandTypes.DATE,
          SqlFunctionCategory.TIMEDATE);

  /** The "MONTHNAME(datetime)" function; returns the name of the month,
   * in the current locale, of a TIMESTAMP or DATE argument. */
  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction MONTHNAME =
      new SqlFunction("MONTHNAME", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000, null, OperandTypes.DATETIME,
          SqlFunctionCategory.TIMEDATE);

  /** The "DAYNAME(datetime)" function; returns the name of the day of the week,
   * in the current locale, of a TIMESTAMP or DATE argument. */
  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction DAYNAME =
      new SqlFunction("DAYNAME", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000, null, OperandTypes.DATETIME,
          SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {MYSQL, POSTGRESQL})
  public static final SqlFunction LEFT =
      new SqlFunction("LEFT", SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING, null,
          OperandTypes.CBSTRING_INTEGER, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL, POSTGRESQL})
  public static final SqlFunction REPEAT =
      new SqlFunction(
          "REPEAT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING,
          null,
          OperandTypes.STRING_INTEGER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL, POSTGRESQL})
  public static final SqlFunction RIGHT =
      new SqlFunction("RIGHT", SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING, null,
          OperandTypes.CBSTRING_INTEGER, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction SPACE =
      new SqlFunction("SPACE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000_NULLABLE,
          null,
          OperandTypes.INTEGER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction STRCMP =
      new SqlFunction("STRCMP",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL, POSTGRESQL, ORACLE})
  public static final SqlFunction SOUNDEX =
      new SqlFunction("SOUNDEX",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_4_NULLABLE,
          null,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlFunction DIFFERENCE =
      new SqlFunction("DIFFERENCE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  /** The "CONCAT(arg, ...)" function that concatenates strings.
   * For example, "CONCAT('a', 'bc', 'd')" returns "abcd". */
  @LibraryOperator(libraries = {MYSQL, POSTGRESQL})
  public static final SqlFunction CONCAT_FUNCTION =
      new SqlFunction("CONCAT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION_NULLABLE,
          null,
          OperandTypes.repeat(SqlOperandCountRanges.from(2),
              OperandTypes.STRING),
          SqlFunctionCategory.STRING);

  /** The "CONCAT(arg0, arg1)" function that concatenates strings.
   * For example, "CONCAT('a', 'bc')" returns "abc". */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction CONCAT2 =
      new SqlFunction("CONCAT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION_NULLABLE,
          null,
          OperandTypes.STRING_SAME_SAME,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction REVERSE =
      new SqlFunction("REVERSE",
          SqlKind.REVERSE,
          ReturnTypes.ARG0_NULLABLE_VARYING,
          null,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction FROM_BASE64 =
      new SqlFunction("FROM_BASE64", SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARBINARY)
              .andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.STRING, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction TO_BASE64 =
      new SqlFunction("TO_BASE64", SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARCHAR)
              .andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.or(OperandTypes.STRING, OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  /** The "TO_DATE(string1, string2)" function; casts string1
   * to a DATE using the format specified in string2. */
  @LibraryOperator(libraries = {POSTGRESQL, ORACLE})
  public static final SqlFunction TO_DATE =
      new SqlFunction("TO_DATE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DATE_NULLABLE,
          null,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.TIMEDATE);

  /** The "TO_TIMESTAMP(string1, string2)" function; casts string1
   * to a TIMESTAMP using the format specified in string2. */
  @LibraryOperator(libraries = {POSTGRESQL, ORACLE})
  public static final SqlFunction TO_TIMESTAMP =
      new SqlFunction("TO_TIMESTAMP",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DATE_NULLABLE,
          null,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.TIMEDATE);

  /** The "TIMESTAMP_SECONDS(bigint)" function; returns a TIMESTAMP value
   * a given number of seconds after 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIMESTAMP_SECONDS =
      new SqlFunction("TIMESTAMP_SECONDS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypes.INTEGER,
          SqlFunctionCategory.TIMEDATE);

  /** The "TIMESTAMP_MILLIS(bigint)" function; returns a TIMESTAMP value
   * a given number of milliseconds after 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIMESTAMP_MILLIS =
      new SqlFunction("TIMESTAMP_MILLIS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypes.INTEGER,
          SqlFunctionCategory.TIMEDATE);

  /** The "TIMESTAMP_MICROS(bigint)" function; returns a TIMESTAMP value
   * a given number of micro-seconds after 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIMESTAMP_MICROS =
      new SqlFunction("TIMESTAMP_MICROS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypes.INTEGER,
          SqlFunctionCategory.TIMEDATE);

  /** The "UNIX_SECONDS(bigint)" function; returns the number of seconds
   * since 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction UNIX_SECONDS =
      new SqlFunction("UNIX_SECONDS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.BIGINT_NULLABLE, null, OperandTypes.TIMESTAMP,
          SqlFunctionCategory.TIMEDATE);

  /** The "UNIX_MILLIS(bigint)" function; returns the number of milliseconds
   * since 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction UNIX_MILLIS =
      new SqlFunction("UNIX_MILLIS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.BIGINT_NULLABLE, null, OperandTypes.TIMESTAMP,
          SqlFunctionCategory.TIMEDATE);

  /** The "UNIX_MICROS(bigint)" function; returns the number of microseconds
   * since 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction UNIX_MICROS =
      new SqlFunction("UNIX_MICROS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.BIGINT_NULLABLE, null, OperandTypes.TIMESTAMP,
          SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction CHR =
      new SqlFunction("CHR",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.CHAR,
          null,
          OperandTypes.INTEGER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction TANH =
      new SqlFunction("TANH",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction COSH =
      new SqlFunction("COSH",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction SINH =
      new SqlFunction("SINH",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {MYSQL, POSTGRESQL})
  public static final SqlFunction MD5 =
      new SqlFunction("MD5", SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARCHAR)
              .andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.or(OperandTypes.STRING, OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL, POSTGRESQL})
  public static final SqlFunction SHA1 =
      new SqlFunction("SHA1", SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARCHAR)
              .andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.or(OperandTypes.STRING, OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  /** Infix "::" cast operator used by PostgreSQL, for example
   * {@code '100'::INTEGER}. */
  @LibraryOperator(libraries = { POSTGRESQL })
  public static final SqlOperator INFIX_CAST =
      new SqlCastOperator();
}
