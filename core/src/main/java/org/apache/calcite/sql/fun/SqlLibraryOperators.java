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
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandHandlers;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.sql.fun.SqlLibrary.ALL;
import static org.apache.calcite.sql.fun.SqlLibrary.BIG_QUERY;
import static org.apache.calcite.sql.fun.SqlLibrary.CALCITE;
import static org.apache.calcite.sql.fun.SqlLibrary.HIVE;
import static org.apache.calcite.sql.fun.SqlLibrary.MSSQL;
import static org.apache.calcite.sql.fun.SqlLibrary.MYSQL;
import static org.apache.calcite.sql.fun.SqlLibrary.ORACLE;
import static org.apache.calcite.sql.fun.SqlLibrary.POSTGRESQL;
import static org.apache.calcite.sql.fun.SqlLibrary.REDSHIFT;
import static org.apache.calcite.sql.fun.SqlLibrary.SNOWFLAKE;
import static org.apache.calcite.sql.fun.SqlLibrary.SPARK;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.BITCOUNT;
import static org.apache.calcite.sql.type.OperandTypes.STRING_FIRST_OBJECT_REPEAT;
import static org.apache.calcite.sql.type.OperandTypes.STRING_FIRST_STRING_ARRAY_REPEAT;
import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

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

  /** The "AGGREGATE(m)" aggregate function;
   * aggregates a measure column according to the measure's rollup strategy.
   * This is a Calcite-specific extension.
   *
   * <p>This operator is for SQL (and AST); for internal use (RexNode and
   * Aggregate) use {@code AGG_M2M}. */
  @LibraryOperator(libraries = {CALCITE})
  public static final SqlFunction AGGREGATE =
      SqlBasicAggFunction.create("AGGREGATE", SqlKind.AGGREGATE_FN,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.FROM_MEASURE),
          OperandTypes.MEASURE);

  /** The "CONVERT_TIMEZONE(tz1, tz2, datetime)" function;
   * converts the timezone of {@code datetime} from {@code tz1} to {@code tz2}. */
  @LibraryOperator(libraries = {REDSHIFT})
  public static final SqlFunction CONVERT_TIMEZONE =
      SqlBasicFunction.create("CONVERT_TIMEZONE",
          ReturnTypes.DATE_NULLABLE, OperandTypes.CHARACTER_CHARACTER_DATETIME,
          SqlFunctionCategory.TIMEDATE);

  /** THE "DATE_ADD(date, interval)" function
   * (BigQuery) adds the interval to the date. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATE_ADD =
      SqlBasicFunction.create(SqlKind.DATE_ADD, ReturnTypes.ARG0_NULLABLE,
              OperandTypes.DATE_INTERVAL)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** THE "DATE_DIFF(date, date2, timeUnit)" function
   * (BigQuery) returns the number of timeUnit in (date - date2). */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATE_DIFF =
      new SqlTimestampDiffFunction("DATE_DIFF",
          OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.DATE, SqlTypeFamily.ANY));

  /** The "DATEADD(timeUnit, numeric, datetime)" function
   * (Microsoft SQL Server, Redshift, Snowflake). */
  @LibraryOperator(libraries = {MSSQL, REDSHIFT, SNOWFLAKE})
  public static final SqlFunction DATEADD =
      new SqlTimestampAddFunction("DATEADD");

  /** The "DATE_ADD(date, numDays)" function
   * (Spark) Returns the date that is num_days after start_date. */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction DATE_ADD_SPARK =
      SqlBasicFunction.create(SqlKind.DATE_ADD, ReturnTypes.DATE_NULLABLE,
              OperandTypes.DATE_ANY)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** The "DATE_SUB(date, numDays)" function
   * (Spark) Returns the date that is num_days before start_date.*/
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction DATE_SUB_SPARK =
      SqlBasicFunction.create(SqlKind.DATE_SUB, ReturnTypes.DATE_NULLABLE,
              OperandTypes.DATE_ANY)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** The "ADD_MONTHS(start_date, num_months)" function
   * (SPARK) Returns the date that is num_months after start_date. */
  @LibraryOperator(libraries = {ORACLE, SPARK})
  public static final SqlFunction ADD_MONTHS =
      SqlBasicFunction.create(SqlKind.ADD_MONTHS, ReturnTypes.ARG0_NULLABLE,
              OperandTypes.DATE_ANY)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** The "DATEDIFF(timeUnit, datetime, datetime2)" function
   * (Microsoft SQL Server, Redshift, Snowflake).
   *
   * <p>MySQL has "DATEDIFF(date, date2)" and "TIMEDIFF(time, time2)" functions
   * but Calcite does not implement these because they have no "timeUnit"
   * argument. */
  @LibraryOperator(libraries = {MSSQL, REDSHIFT, SNOWFLAKE})
  public static final SqlFunction DATEDIFF =
      new SqlTimestampDiffFunction("DATEDIFF",
          OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.DATE,
              SqlTypeFamily.DATE));

  /** The "CONVERT(type, expr [,style])" function (Microsoft SQL Server).
   *
   * <p>Syntax:
   * <blockquote>{@code
   * CONVERT( data_type [ ( length ) ], expression [, style ] )
   * }</blockquote>
   *
   * <p>The optional "style" argument specifies how the value is going to be
   * converted; this implementation ignores the {@code style} parameter.
   *
   * <p>{@code CONVERT(type, expr, style)} is equivalent to CAST(expr AS type),
   * and the implementation delegates most of its logic to actual CAST operator.
   *
   * <p>Not to be confused with standard {@link SqlStdOperatorTable#CONVERT},
   * which converts a string from one character set to another. */
  @LibraryOperator(libraries = {MSSQL})
  public static final SqlFunction MSSQL_CONVERT =
      SqlBasicFunction.create(SqlKind.CAST,
              ReturnTypes.andThen(SqlLibraryOperators::transformConvert,
                  SqlCastFunction.returnTypeInference(false)),
              OperandTypes.repeat(SqlOperandCountRanges.between(2, 3),
                  OperandTypes.ANY))
          .withName("CONVERT")
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withOperandTypeInference(InferTypes.FIRST_KNOWN)
          .withOperandHandler(
              OperandHandlers.of(SqlLibraryOperators::transformConvert));

  /** Transforms a call binding of {@code CONVERT} to an equivalent binding for
   * {@code CAST}. */
  private static SqlCallBinding transformConvert(SqlOperatorBinding opBinding) {
    // Guaranteed to be a SqlCallBinding, with 2 or 3 arguments
    final SqlCallBinding binding = (SqlCallBinding) opBinding;
    return new SqlCallBinding(binding.getValidator(), binding.getScope(),
        transformConvert(binding.getValidator(), binding.getCall()));
  }

  /** Transforms a call to {@code CONVERT} to an equivalent call to
   * {@code CAST}. */
  private static SqlCall transformConvert(SqlValidator validator, SqlCall call) {
    return SqlStdOperatorTable.CAST.createCall(call.getParserPosition(),
        call.operand(1), call.operand(0));
  }

  /** The "DATE_PART(timeUnit, datetime)" function
   * (Databricks, Postgres, Redshift, Snowflake). */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlFunction DATE_PART =
      new SqlExtractFunction("DATE_PART", true) {
        @Override public void unparse(SqlWriter writer, SqlCall call,
            int leftPrec, int rightPrec) {
          getSyntax().unparse(writer, this, call, leftPrec, rightPrec);
        }
      };

  /** The "DATE_SUB(date, interval)" function (BigQuery);
   * subtracts interval from the date, independent of any time zone. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATE_SUB =
      SqlBasicFunction.create(SqlKind.DATE_SUB, ReturnTypes.ARG0_NULLABLE,
           OperandTypes.DATE_INTERVAL)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** The "DATEPART(timeUnit, datetime)" function
   * (Microsoft SQL Server). */
  @LibraryOperator(libraries = {MSSQL})
  public static final SqlFunction DATEPART =
      new SqlExtractFunction("DATEPART", false) {
        @Override public void unparse(SqlWriter writer, SqlCall call,
            int leftPrec, int rightPrec) {
          getSyntax().unparse(writer, this, call, leftPrec, rightPrec);
        }
      };

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
        if (type != null && opBinding.getOperandCount() % 2 == 1) {
          type = typeFactory.createTypeWithNullability(type, true);
        }
        return type;
      };

  /** The "DECODE(v, v1, result1, [v2, result2, ...], resultN)" function. */
  @LibraryOperator(libraries = {ORACLE, REDSHIFT, SPARK, HIVE})
  public static final SqlFunction DECODE =
      SqlBasicFunction.create(SqlKind.DECODE, DECODE_RETURN_TYPE,
          OperandTypes.VARIADIC);

  /** The "IF(condition, thenValue, elseValue)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, HIVE, SPARK})
  public static final SqlFunction IF =
      new SqlFunction("IF", SqlKind.IF, SqlLibraryOperators::inferIfReturnType,
          null,
          OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY,
              SqlTypeFamily.ANY)
              .and(
                  // Arguments 1 and 2 must have same type
                  OperandTypes.same(3, 1, 2)),
          SqlFunctionCategory.SYSTEM) {
        @Override public boolean validRexOperands(int count, Litmus litmus) {
          // IF is translated to RexNode by expanding to CASE.
          return litmus.fail("not a rex operator");
        }
      };

  /** Infers the return type of {@code IF(b, x, y)},
   * namely the least restrictive of the types of x and y.
   * Similar to {@link ReturnTypes#LEAST_RESTRICTIVE}. */
  private static @Nullable RelDataType inferIfReturnType(SqlOperatorBinding opBinding) {
    return opBinding.getTypeFactory()
        .leastRestrictive(opBinding.collectOperandTypes().subList(1, 3));
  }

  /** The "NVL(value, value)" function. */
  @LibraryOperator(libraries = {ORACLE, REDSHIFT, SPARK})
  public static final SqlBasicFunction NVL =
      SqlBasicFunction.create(SqlKind.NVL,
          ReturnTypes.LEAST_RESTRICTIVE
              .andThen(SqlTypeTransforms.TO_NULLABLE_ALL),
          OperandTypes.SAME_SAME);

  /** The "NVL2(value, value, value)" function. */
  @LibraryOperator(libraries = {ORACLE, REDSHIFT, SPARK})
  public static final SqlBasicFunction NVL2 =
      SqlBasicFunction.create(SqlKind.NVL2,
          ReturnTypes.NVL2_RESTRICTIVE,
          OperandTypes.SECOND_THIRD_SAME);

  /** The "IFNULL(value, value)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction IFNULL = NVL.withName("IFNULL");

  /** The "LEN(string)" function. */
  @LibraryOperator(libraries = {REDSHIFT, SNOWFLAKE, SPARK})
  public static final SqlFunction LEN =
      SqlStdOperatorTable.CHAR_LENGTH.withName("LEN");

  /** The "LENGTH(string)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, HIVE, POSTGRESQL, SNOWFLAKE, SPARK})
  public static final SqlFunction LENGTH =
      SqlStdOperatorTable.CHAR_LENGTH.withName("LENGTH");

  // Helper function for deriving types for the *PAD functions
  private static RelDataType deriveTypePad(SqlOperatorBinding binding, RelDataType type) {
    SqlTypeName result = SqlTypeUtil.isBinary(type) ? SqlTypeName.VARBINARY : SqlTypeName.VARCHAR;
    return binding.getTypeFactory().createSqlType(result);
  }

  /** The "LPAD(original_value, return_length[, pattern])" function. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction LPAD =
      SqlBasicFunction.create(
          "LPAD",
          ReturnTypes.ARG0.andThen(SqlLibraryOperators::deriveTypePad),
          OperandTypes.STRING_NUMERIC_OPTIONAL_STRING,
          SqlFunctionCategory.STRING);

  /** The "RPAD(original_value, return_length[, pattern])" function. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction RPAD =
      SqlBasicFunction.create(
          "RPAD",
          ReturnTypes.ARG0.andThen(SqlLibraryOperators::deriveTypePad),
          OperandTypes.STRING_NUMERIC_OPTIONAL_STRING,
          SqlFunctionCategory.STRING);

  /** The "LTRIM(string)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction LTRIM =
      SqlBasicFunction.create(SqlKind.LTRIM,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_NULLABLE)
              .andThen(SqlTypeTransforms.TO_VARYING),
          OperandTypes.STRING)
          .withFunctionType(SqlFunctionCategory.STRING);

  /** The "RTRIM(string)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction RTRIM =
      SqlBasicFunction.create(SqlKind.RTRIM,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_NULLABLE)
              .andThen(SqlTypeTransforms.TO_VARYING),
          OperandTypes.STRING)
          .withFunctionType(SqlFunctionCategory.STRING);

  /** The "SPLIT(string [, delimiter])" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction SPLIT =
      SqlBasicFunction.create("SPLIT",
          ReturnTypes.ARG0
              .andThen(SqlLibraryOperators::deriveTypeSplit)
              .andThen(SqlTypeTransforms.TO_ARRAY_NULLABLE),
          OperandTypes.or(OperandTypes.CHARACTER_CHARACTER,
              OperandTypes.CHARACTER,
              OperandTypes.BINARY_BINARY,
              OperandTypes.BINARY),
          SqlFunctionCategory.STRING)
          .withValidation(3);

  static RelDataType deriveTypeSplit(SqlOperatorBinding operatorBinding,
      RelDataType type) {
    if (SqlTypeUtil.isBinary(type) && operatorBinding.getOperandCount() == 1) {
      throw operatorBinding.newError(
          Static.RESOURCE.delimiterIsRequired(
              operatorBinding.getOperator().getName(), type.toString()));
    }

    SqlTypeName typeName = SqlTypeUtil.isBinary(type) ? SqlTypeName.VARBINARY : SqlTypeName.VARCHAR;
    return operatorBinding.getTypeFactory().createSqlType(typeName);
  }

  /** The "SPLIT_PART(string, delimiter, n)" function. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction SPLIT_PART =
      SqlBasicFunction.create("SPLIT_PART",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_STRING_INTEGER,
          SqlFunctionCategory.STRING);

  /** The "STRPOS(string, substring)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, POSTGRESQL})
  public static final SqlFunction STRPOS = new SqlPositionFunction("STRPOS");

  /** The "INSTR(string, substring [, position [, occurrence]])" function. */
  @LibraryOperator(libraries = {BIG_QUERY, HIVE, MYSQL, ORACLE})
  public static final SqlFunction INSTR = new SqlPositionFunction("INSTR");

  /** Generic "SUBSTR(string, position [, substringLength ])" function. */
  private static final SqlBasicFunction SUBSTR =
      SqlBasicFunction.create("SUBSTR", ReturnTypes.ARG0_NULLABLE_VARYING,
          OperandTypes.STRING_INTEGER_OPTIONAL_INTEGER,
          SqlFunctionCategory.STRING);

  /** The "STRING_TO_ARRAY(string, delimiter [, null_string ])" function (PostgreSQL). */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlFunction STRING_TO_ARRAY =
      SqlBasicFunction.create(SqlKind.STRING_TO_ARRAY,
          ReturnTypes.TO_ARRAY_NULLABLE,
          OperandTypes.STRING_STRING_OPTIONAL_STRING);

  /** The "ENDS_WITH(value1, value2)" function (BigQuery). */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlBasicFunction ENDS_WITH =
      SqlBasicFunction.create(SqlKind.ENDS_WITH, ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.STRING_SAME_SAME);

  /** The "ENDSWITH(value1, value2)" function (Snowflake). */
  @LibraryOperator(libraries = {SNOWFLAKE, SPARK})
  public static final SqlFunction ENDSWITH = ENDS_WITH.withName("ENDSWITH");

  /** The "STARTS_WITH(value1, value2)" function (BigQuery, PostgreSQL). */
  @LibraryOperator(libraries = {BIG_QUERY, POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlBasicFunction STARTS_WITH =
      SqlBasicFunction.create(SqlKind.STARTS_WITH, ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.STRING_SAME_SAME);

  /** The "STARTSWITH(value1, value2)" function (Snowflake). */
  @LibraryOperator(libraries = {SNOWFLAKE, SPARK})
  public static final SqlFunction STARTSWITH = STARTS_WITH.withName("STARTSWITH");

  /** BigQuery's "SUBSTR(string, position [, substringLength ])" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction SUBSTR_BIG_QUERY =
      SUBSTR.withKind(SqlKind.SUBSTR_BIG_QUERY);

  /** MySQL's "SUBSTR(string, position [, substringLength ])" function. */
  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction SUBSTR_MYSQL =
      SUBSTR.withKind(SqlKind.SUBSTR_MYSQL);

  /** Oracle's "SUBSTR(string, position [, substringLength ])" function.
   *
   * <p>It has different semantics to standard SQL's
   * {@link SqlStdOperatorTable#SUBSTRING} function:
   *
   * <ul>
   *   <li>If {@code substringLength} &le; 0, result is the empty string
   *   (Oracle would return null, because it treats the empty string as null,
   *   but Calcite does not have these semantics);
   *   <li>If {@code position} = 0, treat {@code position} as 1;
   *   <li>If {@code position} &lt; 0, treat {@code position} as
   *       "length(string) + position + 1".
   * </ul>
   */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction SUBSTR_ORACLE =
      SUBSTR.withKind(SqlKind.SUBSTR_ORACLE);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction CONVERT_ORACLE =
      new SqlOracleConvertFunction("CONVERT");

  /** PostgreSQL's "SUBSTR(string, position [, substringLength ])" function. */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlFunction SUBSTR_POSTGRESQL =
      SUBSTR.withKind(SqlKind.SUBSTR_POSTGRESQL);

  /** The "PARSE_URL(urlString, partToExtract [, keyToExtract] )" function. */
  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction PARSE_URL =
      SqlBasicFunction.create("PARSE_URL",
              ReturnTypes.VARCHAR_FORCE_NULLABLE,
              OperandTypes.STRING_STRING_OPTIONAL_STRING,
              SqlFunctionCategory.STRING);

  /** The "FIND_IN_SET(matchStr, textStr)" function. */
  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction FIND_IN_SET =
      SqlBasicFunction.create("FIND_IN_SET",
          ReturnTypes.INTEGER_NULLABLE,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  /** The "GREATEST(value, value)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, HIVE})
  public static final SqlFunction GREATEST =
      SqlBasicFunction.create(SqlKind.GREATEST,
          ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
          OperandTypes.SAME_VARIADIC);

  /** The "GREATEST(value, value)" function. Identical to the standard <code>GREATEST</code>
   * function except it skips null values and only returns null if all parameters are nulls. */
  @LibraryOperator(libraries = {POSTGRESQL, SPARK})
  public static final SqlFunction GREATEST_PG =
      SqlBasicFunction.create("GREATEST", SqlKind.GREATEST_PG,
          ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
          OperandTypes.SAME_VARIADIC);

  /** The "LEAST(value, value)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, HIVE})
  public static final SqlFunction LEAST =
      SqlBasicFunction.create(SqlKind.LEAST,
          ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
          OperandTypes.SAME_VARIADIC);

  /** The "LEAST(value, value)" function. Identical to the standard <code>LEAST</code>
   * function except it skips null values and only returns null if all parameters are nulls. */
  @LibraryOperator(libraries = {POSTGRESQL, SPARK})
  public static final SqlFunction LEAST_PG =
      SqlBasicFunction.create("LEAST", SqlKind.LEAST_PG,
          ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
          OperandTypes.SAME_VARIADIC);

  /** The "CEIL(value)" function. Identical to the standard <code>CEIL</code> function
   * except the return type should be a double if the operand is an integer. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction CEIL_BIG_QUERY = new SqlFloorFunction(SqlKind.CEIL)
      .withName("CEIL_BIG_QUERY")
      .withReturnTypeInference(ReturnTypes.ARG0_EXCEPT_INTEGER_NULLABLE);

  /** The "FLOOR(value)" function. Identical to the standard <code>FLOOR</code> function
   * except the return type should be a double if the operand is an integer. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction FLOOR_BIG_QUERY = new SqlFloorFunction(SqlKind.FLOOR)
      .withName("FLOOR_BIG_QUERY")
      .withReturnTypeInference(ReturnTypes.ARG0_EXCEPT_INTEGER_NULLABLE);

  /**
   * The <code>TRANSLATE(<i>string_expr</i>, <i>search_chars</i>,
   * <i>replacement_chars</i>)</code> function returns <i>string_expr</i> with
   * all occurrences of each character in <i>search_chars</i> replaced by its
   * corresponding character in <i>replacement_chars</i>.
   *
   * <p>It is not defined in the SQL standard, but occurs in Oracle and
   * PostgreSQL.
   */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, POSTGRESQL, SPARK})
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

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_INSERT = new SqlJsonModifyFunction("JSON_INSERT");

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_REPLACE = new SqlJsonModifyFunction("JSON_REPLACE");

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_SET = new SqlJsonModifyFunction("JSON_SET");

  /** The "REGEXP_CONTAINS(value, regexp)" function.
   * Returns TRUE if value is a partial match for the regular expression, regexp. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction REGEXP_CONTAINS =
      SqlBasicFunction.create("REGEXP_CONTAINS", ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  /** The "REGEXP_EXTRACT(value, regexp[, position[, occurrence]])" function.
   * Returns the substring in value that matches the regexp. Returns NULL if there is no match. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlBasicFunction REGEXP_EXTRACT =
      SqlBasicFunction.create("REGEXP_EXTRACT", ReturnTypes.VARCHAR_FORCE_NULLABLE,
          OperandTypes.STRING_STRING_OPTIONAL_INTEGER_OPTIONAL_INTEGER,
          SqlFunctionCategory.STRING);

  /** The "REGEXP_EXTRACT_ALL(value, regexp)" function.
   * Returns the substring in value that matches the regexp. Returns NULL if there is no match. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlBasicFunction REGEXP_EXTRACT_ALL =
      SqlBasicFunction.create("REGEXP_EXTRACT_ALL", ReturnTypes.ARG0
              .andThen(SqlTypeTransforms.TO_ARRAY).andThen(SqlTypeTransforms.TO_NULLABLE),
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  /** The "REGEXP_INSTR(value, regexp [, position[, occurrence, [occurrence_position]]])" function.
   * Returns the lowest 1-based position of a regexp in value. Returns NULL if there is no match. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlBasicFunction REGEXP_INSTR =
      SqlBasicFunction.create("REGEXP_INSTR", ReturnTypes.INTEGER_NULLABLE,
          OperandTypes.STRING_STRING_OPTIONAL_INTEGER_OPTIONAL_INTEGER_OPTIONAL_INTEGER,
          SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. */
  @LibraryOperator(libraries = {REDSHIFT})
  public static final SqlFunction REGEXP_REPLACE_2 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_STRING, SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. */
  @LibraryOperator(libraries = {MYSQL, ORACLE, REDSHIFT, HIVE})
  public static final SqlFunction REGEXP_REPLACE_3 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_STRING_STRING, SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep, pos)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. Start searching value from character position
   * pos. */
  @LibraryOperator(libraries = {MYSQL, ORACLE, REDSHIFT})
  public static final SqlFunction REGEXP_REPLACE_4 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING,
              SqlTypeFamily.INTEGER),
          SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep, pos, [ occurrence | matchType ])"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. Start searching value from character position
   * pos. Replace only the occurrence match or all matches if occurrence is 0. matchType
   * is a string of flags to apply to the search. */
  @LibraryOperator(libraries = {MYSQL, REDSHIFT})
  public static final SqlFunction REGEXP_REPLACE_5 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.or(
              OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING,
                  SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
              OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING,
                  SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.STRING)),
          SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep, pos, matchType)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. Start searching value from character position
   * pos. Replace only the occurrence match or all matches if occurrence is 0. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction REGEXP_REPLACE_5_ORACLE =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING,
              SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep, pos, occurrence, matchType)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. Start searching value from character position
   * pos. Replace only the occurrence match or all matches if occurrence is 0. matchType
   * is a string of flags to apply to the search. */
  @LibraryOperator(libraries = {MYSQL, ORACLE, REDSHIFT})
  public static final SqlFunction REGEXP_REPLACE_6 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING,
              SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.STRING),
          SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction REGEXP_REPLACE_BIG_QUERY_3 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_STRING_STRING, SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = REDSHIFT)
  public static final SqlFunction REGEXP_REPLACE_PG_3 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_STRING_STRING, SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep, flags)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. flags are applied to the search. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = REDSHIFT)
  public static final SqlFunction REGEXP_REPLACE_PG_4 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_STRING_STRING_STRING, SqlFunctionCategory.STRING);

  /** The "REGEXP_SUBSTR(value, regexp[, position[, occurrence]])" function.
   * Returns the substring in value that matches the regexp. Returns NULL if there is no match. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction REGEXP_SUBSTR = REGEXP_EXTRACT.withName("REGEXP_SUBSTR");

  /** The "REGEXP(value, regexp)" function, equivalent to {@link #RLIKE}. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction REGEXP =
      SqlBasicFunction.create("REGEXP", ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  /** The "REGEXP_LIKE(value, regexp)" function, equivalent to {@link #RLIKE}. */
  @LibraryOperator(libraries = {SPARK, MYSQL, POSTGRESQL, ORACLE})
  public static final SqlFunction REGEXP_LIKE =
      SqlBasicFunction.create("REGEXP_LIKE", ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.STRING_STRING_OPTIONAL_STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction COMPRESS =
      SqlBasicFunction.create("COMPRESS",
          ReturnTypes.VARBINARY_NULLABLE,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  /** The "URL_DECODE(string)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction URL_DECODE =
      SqlBasicFunction.create("URL_DECODE",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING,
          SqlFunctionCategory.STRING);

  /** The "URL_ENCODE(string)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction URL_ENCODE =
      SqlBasicFunction.create("URL_ENCODE",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction EXTRACT_VALUE =
      SqlBasicFunction.create("EXTRACTVALUE",
          ReturnTypes.VARCHAR_2000.andThen(SqlTypeTransforms.FORCE_NULLABLE),
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction XML_TRANSFORM =
      SqlBasicFunction.create("XMLTRANSFORM",
          ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE),
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction EXTRACT_XML =
      SqlBasicFunction.create("EXTRACT",
          ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE),
          OperandTypes.STRING_STRING_OPTIONAL_STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction EXISTS_NODE =
      SqlBasicFunction.create("EXISTSNODE",
          ReturnTypes.INTEGER_NULLABLE
              .andThen(SqlTypeTransforms.FORCE_NULLABLE),
          OperandTypes.STRING_STRING_OPTIONAL_STRING,
          SqlFunctionCategory.STRING);

  /** The "BOOL_AND(condition)" aggregate function, PostgreSQL and Redshift's
   * equivalent to {@link SqlStdOperatorTable#EVERY}. */
  @LibraryOperator(libraries = {POSTGRESQL, SPARK})
  public static final SqlAggFunction BOOL_AND =
      new SqlMinMaxAggFunction("BOOL_AND", SqlKind.MIN, OperandTypes.BOOLEAN);

  /** The "BOOL_OR(condition)" aggregate function, PostgreSQL and Redshift's
   * equivalent to {@link SqlStdOperatorTable#SOME}. */
  @LibraryOperator(libraries = {POSTGRESQL, SPARK})
  public static final SqlAggFunction BOOL_OR =
      new SqlMinMaxAggFunction("BOOL_OR", SqlKind.MAX, OperandTypes.BOOLEAN);

  /** The "BOOLAND_AGG(condition)" aggregate function, Snowflake's
   * equivalent to {@link SqlStdOperatorTable#EVERY}. */
  @LibraryOperator(libraries = {SNOWFLAKE})
  public static final SqlAggFunction BOOLAND_AGG =
      new SqlMinMaxAggFunction("BOOLAND_AGG", SqlKind.MIN, OperandTypes.BOOLEAN);

  /** The "BOOLOR_AGG(condition)" aggregate function, Snowflake's
   * equivalent to {@link SqlStdOperatorTable#SOME}. */
  @LibraryOperator(libraries = {SNOWFLAKE})
  public static final SqlAggFunction BOOLOR_AGG =
      new SqlMinMaxAggFunction("BOOLOR_AGG", SqlKind.MAX, OperandTypes.BOOLEAN);

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

  /** The "COUNTIF(condition) [OVER (...)]" function, in BigQuery,
   * returns the count of TRUE values for expression.
   *
   * <p>{@code COUNTIF(b)} is equivalent to
   * {@code COUNT(*) FILTER (WHERE b)}. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlAggFunction COUNTIF =
      SqlBasicAggFunction
          .create(SqlKind.COUNTIF, ReturnTypes.BIGINT, OperandTypes.BOOLEAN)
          .withDistinct(Optionality.FORBIDDEN);

  /** The "ARRAY_AGG(value [ ORDER BY ...])" aggregate function,
   * in BigQuery and PostgreSQL, gathers values into arrays. */
  @LibraryOperator(libraries = {BIG_QUERY, POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlAggFunction ARRAY_AGG =
      SqlBasicAggFunction
          .create(SqlKind.ARRAY_AGG,
              ReturnTypes.andThen(ReturnTypes::stripOrderBy,
                  ReturnTypes.TO_ARRAY_NULLABLE), OperandTypes.ANY)
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withSyntax(SqlSyntax.ORDERED_FUNCTION)
          .withAllowsNullTreatment(true);

  /** The "ARRAY_CONCAT_AGG(value [ ORDER BY ...])" aggregate function,
   * in BigQuery and PostgreSQL, concatenates array values into arrays. */
  @LibraryOperator(libraries = {BIG_QUERY, POSTGRESQL}, exceptLibraries = {REDSHIFT})
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
  @LibraryOperator(libraries = {BIG_QUERY, POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlAggFunction STRING_AGG =
      SqlBasicAggFunction
          .create(SqlKind.STRING_AGG, ReturnTypes.ARG0_NULLABLE,
              OperandTypes.STRING.or(OperandTypes.STRING_STRING))
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withSyntax(SqlSyntax.ORDERED_FUNCTION);

  /** The "GROUP_CONCAT([DISTINCT] expr [, ...] [ORDER BY ...] [SEPARATOR sep])"
   * aggregate function, MySQL's equivalent of
   * {@link SqlStdOperatorTable#LISTAGG}.
   *
   * <p>{@code GROUP_CONCAT(v ORDER BY x, y SEPARATOR s)} is implemented by
   * rewriting to {@code LISTAGG(v, s) WITHIN GROUP (ORDER BY x, y)}. */
  @LibraryOperator(libraries = {MYSQL})
  public static final SqlAggFunction GROUP_CONCAT =
      SqlBasicAggFunction
          .create(SqlKind.GROUP_CONCAT,
              ReturnTypes.andThen(ReturnTypes::stripOrderBy,
                  ReturnTypes.ARG0_NULLABLE),
              OperandTypes.STRING.or(OperandTypes.STRING_STRING))
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withAllowsNullTreatment(false)
          .withAllowsSeparator(true)
          .withSyntax(SqlSyntax.ORDERED_FUNCTION);

  /** The "MAX_BY(value, comp)" aggregate function, Spark's
   * equivalent to {@link SqlStdOperatorTable#ARG_MAX}. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlAggFunction MAX_BY =
      SqlStdOperatorTable.ARG_MAX.withName("MAX_BY");

  /** The "MIN_BY(condition)" aggregate function, Spark's
   * equivalent to {@link SqlStdOperatorTable#ARG_MIN}. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlAggFunction MIN_BY =
      SqlStdOperatorTable.ARG_MIN.withName("MIN_BY");

  /** The {@code PERCENTILE_CONT} function, BigQuery's
   * equivalent to {@link SqlStdOperatorTable#PERCENTILE_CONT},
   * but uses an {@code OVER} clause rather than {@code WITHIN GROUP}. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlAggFunction PERCENTILE_CONT2 =
      SqlBasicAggFunction
          .create("PERCENTILE_CONT", SqlKind.PERCENTILE_CONT,
              ReturnTypes.DOUBLE,
              OperandTypes.NUMERIC_UNIT_INTERVAL_NUMERIC_LITERAL)
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withOver(true)
          .withPercentile(true)
          .withAllowsNullTreatment(true)
          .withAllowsFraming(false);

  /** The {@code PERCENTILE_DISC} function, BigQuery's
   * equivalent to {@link SqlStdOperatorTable#PERCENTILE_DISC},
   * but uses an {@code OVER} clause rather than {@code WITHIN GROUP}. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlAggFunction PERCENTILE_DISC2 =
      SqlBasicAggFunction
          .create("PERCENTILE_DISC", SqlKind.PERCENTILE_DISC,
              ReturnTypes.ARG0,
              OperandTypes.NUMERIC_UNIT_INTERVAL_NUMERIC_LITERAL)
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withOver(true)
          .withPercentile(true)
          .withAllowsNullTreatment(true)
          .withAllowsFraming(false);

  /** The "DATE" function. It has the following overloads:
   *
   * <ul>
   *   <li>{@code DATE(string)} is syntactic sugar for
   *       {@code CAST(string AS DATE)}
   *   <li>{@code DATE(year, month, day)}
   *   <li>{@code DATE(timestampLtz [, timeZone])}
   *   <li>{@code DATE(timestamp)}
   * </ul>
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATE =
      SqlBasicFunction.create("DATE", ReturnTypes.DATE_NULLABLE,
          OperandTypes.or(
              // DATE(string)
              OperandTypes.STRING,
              // DATE(year, month, day)
              OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER,
                  SqlTypeFamily.INTEGER),
              // DATE(timestamp)
              OperandTypes.TIMESTAMP_NTZ,
              // DATE(timestampLtz)
              OperandTypes.TIMESTAMP_LTZ,
              // DATE(timestampLtz, timeZone)
              OperandTypes.sequence(
                  "DATE(TIMESTAMP WITH LOCAL TIME ZONE, VARCHAR)",
                  OperandTypes.TIMESTAMP_LTZ, OperandTypes.CHARACTER)),
          SqlFunctionCategory.TIMEDATE);

  /** The "DATETIME" function returns a Calcite
   * {@code TIMESTAMP} (which BigQuery calls a {@code DATETIME}).
   *
   * <p>It has the following overloads:
   *
   * <ul>
   *   <li>{@code DATETIME(year, month, day, hour, minute, second)}
   *   <li>{@code DATETIME(date[, time])}
   *   <li>{@code DATETIME(timestampLtz[, timeZone])}
   * </ul>
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATETIME =
      SqlBasicFunction.create("DATETIME", ReturnTypes.TIMESTAMP_NULLABLE,
          OperandTypes.or(
              // DATETIME(year, month, day, hour, minute, second)
              OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER,
                  SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER,
                  SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
              // DATETIME(date)
              OperandTypes.DATE,
              // DATETIME(date, time)
              OperandTypes.DATE_TIME,
              // DATETIME(timestampLtz)
              OperandTypes.TIMESTAMP_LTZ,
              // DATETIME(timestampLtz, timeZone)
              OperandTypes.sequence(
                  "DATETIME(TIMESTAMP WITH LOCAL TIME ZONE, VARCHAR)",
                  OperandTypes.TIMESTAMP_LTZ, OperandTypes.CHARACTER)),
          SqlFunctionCategory.TIMEDATE);

  /** The "TIME" function. It has the following overloads:
   *
   * <ul>
   *   <li>{@code TIME(hour, minute, second)}
   *   <li>{@code TIME(timestampLtz [, timeZone])}
   *   <li>{@code TIME(timestamp)}
   * </ul>
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIME =
      SqlBasicFunction.create("TIME", ReturnTypes.TIME_NULLABLE,
          OperandTypes.or(
              // TIME(hour, minute, second)
              OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER,
                  SqlTypeFamily.INTEGER),
              // TIME(timestamp)
              OperandTypes.TIMESTAMP_NTZ,
              // TIME(timestampLtz)
              OperandTypes.TIMESTAMP_LTZ,
              // TIME(timestampLtz, timeZone)
              OperandTypes.sequence(
                  "TIME(TIMESTAMP WITH LOCAL TIME ZONE, VARCHAR)",
                  OperandTypes.TIMESTAMP_LTZ, OperandTypes.CHARACTER)),
          SqlFunctionCategory.TIMEDATE);

  /** The "TIMESTAMP" function returns a Calcite
   * {@code TIMESTAMP WITH LOCAL TIME ZONE}
   * (which BigQuery calls a {@code TIMESTAMP}). It has the following overloads:
   *
   * <ul>
   *   <li>{@code TIMESTAMP(string[, timeZone])}
   *   <li>{@code TIMESTAMP(date[, timeZone])}
   *   <li>{@code TIMESTAMP(timestamp[, timeZone])}
   * </ul>
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIMESTAMP =
      SqlBasicFunction.create("TIMESTAMP",
          ReturnTypes.TIMESTAMP_LTZ.andThen(SqlTypeTransforms.TO_NULLABLE),
          OperandTypes.or(
              // TIMESTAMP(string)
              OperandTypes.CHARACTER,
              // TIMESTAMP(string, timeZone)
              OperandTypes.CHARACTER_CHARACTER,
              // TIMESTAMP(date)
              OperandTypes.DATE,
              // TIMESTAMP(date, timeZone)
              OperandTypes.DATE_CHARACTER,
              // TIMESTAMP(timestamp)
              OperandTypes.TIMESTAMP_NTZ,
              // TIMESTAMP(timestamp, timeZone)
              OperandTypes.sequence("TIMESTAMP(TIMESTAMP, VARCHAR)",
                  OperandTypes.TIMESTAMP_NTZ, OperandTypes.CHARACTER)),
          SqlFunctionCategory.TIMEDATE);

  /** The "CURRENT_DATETIME([timezone])" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction CURRENT_DATETIME =
      SqlBasicFunction.create("CURRENT_DATETIME",
          ReturnTypes.TIMESTAMP_NULLABLE,
          OperandTypes.NILADIC.or(OperandTypes.STRING),
          SqlFunctionCategory.TIMEDATE)
          .withSyntax(SqlSyntax.FUNCTION_ID);

  /** The "SYSDATE" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction SYSDATE =
      SqlBasicFunction.create("SYSDATE",
              ReturnTypes.DATE,
              OperandTypes.NILADIC,
              SqlFunctionCategory.TIMEDATE)
          .withSyntax(SqlSyntax.FUNCTION_ID);

  /** The "SYSTIMESTAMP" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction SYSTIMESTAMP =
      SqlBasicFunction.create("SYSTIMESTAMP",
              ReturnTypes.TIMESTAMP_TZ,
              OperandTypes.NILADIC,
              SqlFunctionCategory.TIMEDATE)
          .withSyntax(SqlSyntax.FUNCTION_ID);

  /** The "DATE_FROM_UNIX_DATE(integer)" function; returns a DATE value
   * a given number of seconds after 1970-01-01. */
  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction DATE_FROM_UNIX_DATE =
      SqlBasicFunction.create("DATE_FROM_UNIX_DATE",
          ReturnTypes.DATE_NULLABLE, OperandTypes.INTEGER,
          SqlFunctionCategory.TIMEDATE);

  /** The "UNIX_DATE(date)" function; returns the number of days since
   * 1970-01-01. */
  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction UNIX_DATE =
      SqlBasicFunction.create("UNIX_DATE",
          ReturnTypes.INTEGER_NULLABLE, OperandTypes.DATE,
          SqlFunctionCategory.TIMEDATE);

  /** "CONTAINS_SUBSTR(expression, string[, json_scope &#61;&#62; json_scope_value ])"
   * function; returns whether string exists as substring in expression, with optional
   * json_scope argument. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction CONTAINS_SUBSTR =
      SqlBasicFunction.create("CONTAINS_SUBSTR",
          ReturnTypes.BOOLEAN_NULLABLE, OperandTypes.ANY_STRING_OPTIONAL_STRING,
          SqlFunctionCategory.STRING);

  /** The "MONTHNAME(datetime)" function; returns the name of the month,
   * in the current locale, of a TIMESTAMP or DATE argument. */
  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction MONTHNAME =
      SqlBasicFunction.create("MONTHNAME",
          ReturnTypes.VARCHAR_2000, OperandTypes.DATETIME,
          SqlFunctionCategory.TIMEDATE);

  /** The "DAYNAME(datetime)" function; returns the name of the day of the week,
   * in the current locale, of a TIMESTAMP or DATE argument. */
  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction DAYNAME =
      SqlBasicFunction.create("DAYNAME",
          ReturnTypes.VARCHAR_2000, OperandTypes.DATETIME,
          SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL, POSTGRESQL, SPARK})
  public static final SqlFunction LEFT =
      SqlBasicFunction.create("LEFT",
          ReturnTypes.ARG0_NULLABLE_VARYING,
          OperandTypes.CBSTRING_INTEGER, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction REPEAT =
      SqlBasicFunction.create("REPEAT",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_INTEGER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL, POSTGRESQL, SPARK})
  public static final SqlFunction RIGHT =
      SqlBasicFunction.create("RIGHT", ReturnTypes.ARG0_NULLABLE_VARYING,
          OperandTypes.CBSTRING_INTEGER, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL, SPARK, HIVE})
  public static final SqlFunction SPACE =
      SqlBasicFunction.create("SPACE",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.INTEGER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction STRCMP =
      SqlBasicFunction.create("STRCMP",
          ReturnTypes.INTEGER_NULLABLE,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL, POSTGRESQL, ORACLE, HIVE})
  public static final SqlFunction SOUNDEX =
      SqlBasicFunction.create("SOUNDEX",
          ReturnTypes.VARCHAR_4_NULLABLE,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  /** The variant of the SOUNDEX operator. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction SOUNDEX_SPARK =
      ((SqlBasicFunction) SOUNDEX).withKind(SqlKind.SOUNDEX_SPARK)
          .withReturnTypeInference(ReturnTypes.VARCHAR_NULLABLE);

  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlFunction DIFFERENCE =
      SqlBasicFunction.create("DIFFERENCE",
          ReturnTypes.INTEGER_NULLABLE,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  /** The case-insensitive variant of the LIKE operator. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlSpecialOperator ILIKE =
      new SqlLikeOperator("ILIKE", SqlKind.LIKE, false, false);

  /** The case-insensitive variant of the NOT LIKE operator. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlSpecialOperator NOT_ILIKE =
      new SqlLikeOperator("NOT ILIKE", SqlKind.LIKE, true, false);

  /** The regex variant of the LIKE operator. */
  @LibraryOperator(libraries = {SPARK, HIVE, MYSQL})
  public static final SqlSpecialOperator RLIKE =
      new SqlLikeOperator("RLIKE", SqlKind.RLIKE, false, true);

  /** The regex variant of the NOT LIKE operator. */
  @LibraryOperator(libraries = {SPARK, HIVE, MYSQL})
  public static final SqlSpecialOperator NOT_RLIKE =
      new SqlLikeOperator("NOT RLIKE", SqlKind.RLIKE, true, true);

  /** Alias for {@link SqlStdOperatorTable#BITCOUNT}. */
  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction BIT_COUNT_BIG_QUERY =
      BITCOUNT.withName("BIT_COUNT");

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction BIT_COUNT_MYSQL =
      new SqlFunction(
          "BIT_COUNT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BIGINT_NULLABLE,
          null,
          OperandTypes.or(OperandTypes.NUMERIC,
              OperandTypes.BINARY,
              OperandTypes.BOOLEAN,
              OperandTypes.CHARACTER,
              OperandTypes.DATETIME,
              OperandTypes.DATE,
              OperandTypes.TIME,
              OperandTypes.TIMESTAMP),
          SqlFunctionCategory.NUMERIC);

  /** The "CONCAT(arg, ...)" function that concatenates strings.
   * For example, "CONCAT('a', 'bc', 'd')" returns "abcd".
   *
   * <p>It accepts at least 1 argument and returns null if any of
   * the arguments is null. */
  @LibraryOperator(libraries = {MYSQL, BIG_QUERY})
  public static final SqlFunction CONCAT_FUNCTION =
      SqlBasicFunction.create("CONCAT",
          ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION_NULLABLE,
          OperandTypes.repeat(SqlOperandCountRanges.from(1),
              OperandTypes.STRING),
          SqlFunctionCategory.STRING)
          .withOperandTypeInference(InferTypes.RETURN_TYPE);

  /** The "CONCAT(arg, ...)" function that concatenates strings,
   * which never returns null.
   * For example, "CONCAT('a', 'bc', 'd')" returns "abcd".
   *
   * <p>If one of the arguments is null, it will be treated as empty string.
   * "CONCAT('a', null)" returns "a".
   * "CONCAT('a', null, 'b')" returns "ab".
   *
   * <p>Returns empty string only when all arguments are null
   * or the empty string.
   * "CONCAT(null)" returns "".
   * "CONCAT(null, '')" returns "".
   * "CONCAT(null, null, null)" returns "".
   *
   * <p>It differs from {@link #CONCAT_FUNCTION} when processing
   * null values. */
  @LibraryOperator(libraries = {MSSQL, POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction CONCAT_FUNCTION_WITH_NULL =
      SqlBasicFunction.create("CONCAT",
          ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION_NOT_NULLABLE,
          OperandTypes.repeat(SqlOperandCountRanges.from(1),
              OperandTypes.STRING),
          SqlFunctionCategory.STRING)
          .withOperandTypeInference(InferTypes.RETURN_TYPE)
          .withKind(SqlKind.CONCAT_WITH_NULL);

  /** The "CONCAT(arg0, arg1)" function that concatenates strings.
   * For example, "CONCAT('a', 'bc')" returns "abc".
   *
   * <p>If one of the arguments is null, it will be treated as empty string.
   * "CONCAT('a', null)" returns "a".
   *
   * <p>Returns null only when both arguments are null.
   * "CONCAT(null, null)" returns null.
   *
   * <p>It is assigned {@link SqlKind#CONCAT2} to make it not equal to
   * {@link #CONCAT_FUNCTION}. */
  @LibraryOperator(libraries = {ORACLE, REDSHIFT})
  public static final SqlFunction CONCAT2 =
      SqlBasicFunction.create("CONCAT",
          ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION_NULLABLE_ALL,
          OperandTypes.STRING_SAME_SAME,
          SqlFunctionCategory.STRING)
          .withOperandTypeInference(InferTypes.RETURN_TYPE)
          .withKind(SqlKind.CONCAT2);

  /** The "CONCAT_WS(separator, arg1, ...)" function (MySQL);
   * concatenates strings with separator, and treats null arguments as empty
   * strings. For example:
   *
   * <ul>
   * <li>{@code CONCAT_WS(',', 'a')} returns "{@code a}";
   * <li>{@code CONCAT_WS(',', 'a', 'b')} returns "{@code a,b}".
   * </ul>
   *
   * <p>Returns null if the separator arg is null.
   * For example, {@code CONCAT_WS(null, 'a', 'b')} returns null.
   *
   * <p>If all the arguments except the separator are null,
   * it also returns the empty string.
   * For example, {@code CONCAT_WS(',', null, null)} returns "". */
  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction CONCAT_WS =
      SqlBasicFunction.create("CONCAT_WS",
          ReturnTypes.MULTIVALENT_STRING_WITH_SEP_SUM_PRECISION_ARG0_NULLABLE,
          OperandTypes.repeat(SqlOperandCountRanges.from(2),
              OperandTypes.STRING),
          SqlFunctionCategory.STRING)
          .withOperandTypeInference(InferTypes.RETURN_TYPE);

  /** The "CONCAT_WS(separator, arg1, ...)" function (Postgres).
   *
   * <p>Differs from {@link #CONCAT_WS} (MySQL) in that its arg1 can be of any type,
   * not limited to string. For example:
   *
   * <ul>
   * <li>{@code CONCAT_WS(',', 'a')} returns "{@code a}";
   * <li>{@code CONCAT_WS(',', 'a', DATE '1945-02-24')} returns "{@code a,1945-02-24}";
   * <li>{@code CONCAT_WS(',', 'a', ARRAY['b', 'c'])} returns "{@code a,[b, c]}".
   * </ul> */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction CONCAT_WS_POSTGRESQL =
      SqlBasicFunction.create("CONCAT_WS",
              ReturnTypes.MULTIVALENT_STRING_WITH_SEP_SUM_PRECISION_ARG0_NULLABLE,
              STRING_FIRST_OBJECT_REPEAT,
              SqlFunctionCategory.STRING)
          .withOperandTypeInference(InferTypes.RETURN_TYPE)
          .withKind(SqlKind.CONCAT_WS_POSTGRESQL);

  /** The "CONCAT_WS(separator, arg1, arg2, ...)" function in (MSSQL).
   *
   * <p>Differs from {@link #CONCAT_WS} (MySQL, Postgres) in that it accepts
   * between 3 and 254 arguments, and never returns null (even if the separator
   * is null). For example:
   *
   * <ul>
   * <li>{@code CONCAT_WS(',', 'a', 'b')} returns "{@code a,b}";
   * <li>{@code CONCAT_WS(null, 'a', 'b')} returns "{@code ab}";
   * <li>{@code CONCAT_WS(',', null, null)} returns "";
   * <li>{@code CONCAT_WS(null, null, null)} returns "".
   * </ul> */
  @LibraryOperator(libraries = {MSSQL})
  public static final SqlFunction CONCAT_WS_MSSQL =
      SqlBasicFunction.create("CONCAT_WS",
          ReturnTypes.MULTIVALENT_STRING_WITH_SEP_SUM_PRECISION_NOT_NULLABLE,
          OperandTypes.repeat(SqlOperandCountRanges.between(3, 254),
              OperandTypes.STRING),
          SqlFunctionCategory.STRING)
          .withOperandTypeInference(InferTypes.RETURN_TYPE)
          .withKind(SqlKind.CONCAT_WS_MSSQL);

  /** The "CONCAT_WS(separator[, str | array(str)]+)" function in (SPARK).
   *
   * <p>For example:
   * <ul>
   * <li>{@code CONCAT_WS(',', 'a', 'b')} returns "{@code a,b}";
   * <li>{@code CONCAT_WS(null, 'a', 'b')} returns NULL";
   * <li>{@code CONCAT_WS('s')} returns "";
   * <li>{@code CONCAT_WS('/', 'a', null, 'b')} returns "{@code a/b}";
   * <li>{@code CONCAT_WS('/', array('a', 'b'))} returns "{@code a/b}";
   * <li>{@code CONCAT_WS('/', 'c', array('a', 'b'))} returns "{@code c/a/b}".
   * </ul> */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction CONCAT_WS_SPARK =
      SqlBasicFunction.create("CONCAT_WS",
          ReturnTypes.MULTIVALENT_STRING_WITH_SEP_SUM_PRECISION_ARG0_NULLABLE,
              STRING_FIRST_STRING_ARRAY_REPEAT,
              SqlFunctionCategory.STRING)
          .withOperandTypeInference(InferTypes.RETURN_TYPE)
          .withKind(SqlKind.CONCAT_WS_SPARK);

  private static RelDataType arrayReturnType(SqlOperatorBinding opBinding) {
    final List<RelDataType> operandTypes = opBinding.collectOperandTypes();

    // only numeric & character types check, this is a special spark array case
    // the form like ARRAY(1, 2, '3') will return ["1", "2", "3"]
    boolean hasNumeric = false;
    boolean hasCharacter = false;
    boolean hasOthers = false;
    for (RelDataType type : operandTypes) {
      SqlTypeFamily family = type.getSqlTypeName().getFamily();
      // some types such as Row, the family is null, fallback to normal inferred type logic
      if (family == null) {
        hasOthers = true;
        break;
      }
      // skip it because we allow NULL literal
      if (SqlTypeUtil.isNull(type)) {
        continue;
      }
      switch (family) {
      case NUMERIC:
        hasNumeric = true;
        break;
      case CHARACTER:
        hasCharacter = true;
        break;
      default:
        hasOthers = true;
        break;
      }
    }

    RelDataType type;
    boolean useCharacterTypes = hasNumeric && hasCharacter && !hasOthers;
    if (useCharacterTypes) {
      List<RelDataType> characterTypes =
          // may include NULL literal
          operandTypes.stream().filter(
              t -> t.getSqlTypeName().getFamily() != SqlTypeFamily.NUMERIC)
              .collect(Collectors.toList());
      type = opBinding.getTypeFactory().leastRestrictive(characterTypes);
    } else {
      type =
          opBinding.getOperandCount() > 0
              ? ReturnTypes.LEAST_RESTRICTIVE.inferReturnType(opBinding)
              : opBinding.getTypeFactory().createUnknownType();
    }
    if (type == null) {
      throw opBinding.newError(
          RESOURCE.cannotInferReturnType(
              opBinding.getOperator().toString(),
              opBinding.collectOperandTypes().toString()));
    }

    // explicit cast elements to component type if they are not same
    SqlValidatorUtil.adjustTypeForArrayConstructor(type, opBinding);

    return SqlTypeUtil.createArrayType(opBinding.getTypeFactory(), type, false);
  }

  /** The "ARRAY(exp, ...)" function (Spark);
   * compare with the standard array value constructor, "ARRAY [exp, ...]". */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction ARRAY =
      SqlBasicFunction.create("ARRAY",
          SqlLibraryOperators::arrayReturnType,
          OperandTypes.ARRAY_FUNCTION,
          SqlFunctionCategory.SYSTEM);

  private static RelDataType mapReturnType(SqlOperatorBinding opBinding) {
    Pair<@Nullable RelDataType, @Nullable RelDataType> type =
        getComponentTypes(
            opBinding.getTypeFactory(), opBinding.collectOperandTypes());
    return SqlTypeUtil.createMapType(
        opBinding.getTypeFactory(),
        requireNonNull(type.left, "inferred key type"),
        requireNonNull(type.right, "inferred value type"),
        false);
  }

  private static Pair<@Nullable RelDataType, @Nullable RelDataType> getComponentTypes(
      RelDataTypeFactory typeFactory,
      List<RelDataType> argTypes) {
    // special case, allows empty map
    if (argTypes.isEmpty()) {
      return Pair.of(typeFactory.createUnknownType(), typeFactory.createUnknownType());
    }
    // Util.quotientList(argTypes, 2, 0):
    // This extracts all elements at even indices from argTypes.
    // It represents the types of keys in the map as they are placed at even positions
    // e.g. 0, 2, 4, etc.
    // Symmetrically, Util.quotientList(argTypes, 2, 1) represents odd-indexed elements.
    // details please see Util.quotientList.
    return Pair.of(
        typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 0)),
        typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 1)));
  }

  /** The "MAP(key, value, ...)" function (Spark);
   * compare with the standard map value constructor, "MAP[key, value, ...]". */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP =
      SqlBasicFunction.create("MAP",
          SqlLibraryOperators::mapReturnType,
          OperandTypes.MAP_FUNCTION,
          SqlFunctionCategory.SYSTEM);

  @SuppressWarnings("argument.type.incompatible")
  private static RelDataType arrayAppendPrependReturnType(SqlOperatorBinding opBinding) {
    final RelDataType arrayType = opBinding.collectOperandTypes().get(0);
    final RelDataType componentType = arrayType.getComponentType();
    if (componentType == null) {
      // NULL used for array.
      return arrayType;
    }
    final RelDataType elementType = opBinding.collectOperandTypes().get(1);
    requireNonNull(componentType, () -> "componentType of " + arrayType);

    RelDataType type =
        opBinding.getTypeFactory().leastRestrictive(
            ImmutableList.of(componentType, elementType));
    requireNonNull(type, "inferred array element type");

    if (elementType.isNullable()) {
      type = opBinding.getTypeFactory().createTypeWithNullability(type, true);
    }

    // make explicit CAST for array elements and inserted element to the biggest type
    // if array component type not equals to inserted element type
    if (!componentType.equalsSansFieldNames(elementType)) {
      // 0, 1 is the operand index to be CAST
      // For array_append/array_prepend, 0 is the array arg and 1 is the inserted element
      if (componentType.equalsSansFieldNames(type)) {
        SqlValidatorUtil.
            adjustTypeForArrayFunctions(type, opBinding, 1);
      } else {
        SqlValidatorUtil.
            adjustTypeForArrayFunctions(type, opBinding, 0);
      }
    }

    return SqlTypeUtil.createArrayType(opBinding.getTypeFactory(), type, arrayType.isNullable());
  }

  /** The "ARRAY_APPEND(array, element)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_APPEND =
      SqlBasicFunction.create(SqlKind.ARRAY_APPEND,
          SqlLibraryOperators::arrayAppendPrependReturnType,
          OperandTypes.ARRAY_ELEMENT_NONNULL);

  /** The "EXISTS(array, lambda)" function (Spark); returns whether a predicate holds
   * for one or more elements in the array. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction EXISTS =
      SqlBasicFunction.create("EXISTS",
          ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.EXISTS);

  @SuppressWarnings("argument.type.incompatible")
  private static RelDataType arrayCompactReturnType(SqlOperatorBinding opBinding) {
    final RelDataType arrayType = opBinding.collectOperandTypes().get(0);
    if (arrayType.getSqlTypeName() == SqlTypeName.NULL) {
      return arrayType;
    }
    RelDataType type = arrayType.getComponentType();
    // force set nullable=false, and there are no side effects for 'NULL' type
    if (type != null && type.isNullable()) {
      type = opBinding.getTypeFactory().createTypeWithNullability(type, false);
    }
    requireNonNull(type, "inferred array element type");
    return SqlTypeUtil.createArrayType(opBinding.getTypeFactory(), type, arrayType.isNullable());
  }

  /** The "ARRAY_COMPACT(array)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_COMPACT =
      SqlBasicFunction.create(SqlKind.ARRAY_COMPACT,
          SqlLibraryOperators::arrayCompactReturnType,
          OperandTypes.ARRAY);

  /** The "ARRAY_CONCAT(array [, array]*)" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction ARRAY_CONCAT =
      SqlBasicFunction.create(SqlKind.ARRAY_CONCAT,
          ReturnTypes.LEAST_RESTRICTIVE,
          new SameOperandTypeChecker(-1) {
            @Override public boolean checkOperandTypes(
                SqlCallBinding callBinding,
                boolean throwOnFailure) {
              boolean result = super.checkOperandTypes(callBinding, throwOnFailure);
              if (!result) {
                return false;
              }
              for (int i = 0; i < callBinding.getOperandCount(); i++) {
                RelDataType operandType = callBinding.getOperandType(i);
                if (operandType.getSqlTypeName() != SqlTypeName.ARRAY) {
                  if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                  }
                  return false;
                }
              }
              return true;
            }

              @Override public SqlOperandCountRange getOperandCountRange() {
                return SqlOperandCountRanges.from(1);
              }

              @Override public String getAllowedSignatures(SqlOperator op, String opName) {
                return opName + "(...)";
              }
          });

  /** The "ARRAY_CONTAINS(array, element)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_CONTAINS =
      SqlBasicFunction.create(SqlKind.ARRAY_CONTAINS,
          ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.ARRAY_ELEMENT_NONNULL);

  /** The "ARRAY_DISTINCT(array)" function. */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction ARRAY_DISTINCT =
      SqlBasicFunction.create(SqlKind.ARRAY_DISTINCT,
          ReturnTypes.ARG0_NULLABLE,
          OperandTypes.ARRAY);

  /** The "ARRAY_EXCEPT(array1, array2)" function. */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction ARRAY_EXCEPT =
      SqlBasicFunction.create(SqlKind.ARRAY_EXCEPT,
          ReturnTypes.LEAST_RESTRICTIVE,
          OperandTypes.and(
              OperandTypes.NONNULL_NONNULL,
              OperandTypes.SAME_SAME,
              OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)));

  @SuppressWarnings("argument.type.incompatible")
  private static RelDataType arrayInsertReturnType(SqlOperatorBinding opBinding) {
    final List<RelDataType> operandTypes = opBinding.collectOperandTypes();
    final RelDataType arrayType = operandTypes.get(0);
    final RelDataType componentType = arrayType.getComponentType();
    final RelDataType elementType1 = operandTypes.get(1);
    final RelDataType elementType2 = operandTypes.get(2);
    requireNonNull(componentType, () -> "componentType of " + arrayType);

    // we don't need to do leastRestrictive on componentType and elementType,
    // because in operand checker we limit the elementType such that it equals the array component
    // type. So we use componentType directly.
    RelDataType type =
        opBinding.getTypeFactory().leastRestrictive(
            ImmutableList.of(componentType, elementType2));
    requireNonNull(type, "inferred array element type");

    // The spec says that "ARRAY_INSERT may pad the array with NULL values if the
    // position is large", it implies that in the result the element type is always nullable.
    type = opBinding.getTypeFactory().createTypeWithNullability(type, true);
    // make explicit CAST for array elements and inserted element to the biggest type
    // if array component type is not equal to the inserted element type
    if (!componentType.equalsSansFieldNamesAndNullability(elementType2)) {
      // For array_insert, 0 is the array arg and 2 is the inserted element
      SqlValidatorUtil.
          adjustTypeForArrayFunctions(type, opBinding, 2);
      SqlValidatorUtil.
          adjustTypeForArrayFunctions(type, opBinding, 0);
    }
    boolean nullable = arrayType.isNullable() || elementType1.isNullable();
    return SqlTypeUtil.createArrayType(opBinding.getTypeFactory(), type, nullable);
  }

  /** The "ARRAY_INSERT(array, pos, val)" function (Spark). */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_INSERT =
      SqlBasicFunction.create(SqlKind.ARRAY_INSERT,
          SqlLibraryOperators::arrayInsertReturnType,
          OperandTypes.ARRAY_INSERT);

  /** The "ARRAY_INTERSECT(array1, array2)" function. */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction ARRAY_INTERSECT =
      SqlBasicFunction.create(SqlKind.ARRAY_INTERSECT,
          ReturnTypes.LEAST_RESTRICTIVE,
          OperandTypes.and(
              OperandTypes.NONNULL_NONNULL,
              OperandTypes.SAME_SAME,
              OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)));

  /** The "ARRAY_JOIN(array, delimiter [, nullText ])" function. */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction ARRAY_JOIN =
      SqlBasicFunction.create(SqlKind.ARRAY_JOIN,
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_ARRAY_CHARACTER_OPTIONAL_CHARACTER);

  /** The "ARRAY_LENGTH(array)" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction ARRAY_LENGTH =
      SqlBasicFunction.create(SqlKind.ARRAY_LENGTH,
          ReturnTypes.INTEGER_NULLABLE,
          OperandTypes.ARRAY);

  /** The "ARRAY_MAX(array)" function. */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction ARRAY_MAX =
      SqlBasicFunction.create(SqlKind.ARRAY_MAX,
          ReturnTypes.TO_COLLECTION_ELEMENT_FORCE_NULLABLE,
          OperandTypes.ARRAY_NONNULL);

  /** The "ARRAY_MIN(array)" function. */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction ARRAY_MIN =
      SqlBasicFunction.create(SqlKind.ARRAY_MIN,
          ReturnTypes.TO_COLLECTION_ELEMENT_FORCE_NULLABLE,
          OperandTypes.ARRAY_NONNULL);

  /** The "ARRAY_POSITION(array, element)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_POSITION =
      SqlBasicFunction.create(SqlKind.ARRAY_POSITION,
          ReturnTypes.BIGINT_NULLABLE,
          OperandTypes.ARRAY_ELEMENT_NONNULL);

  /** The "ARRAY_PREPEND(array, element)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_PREPEND =
      SqlBasicFunction.create(SqlKind.ARRAY_PREPEND,
          SqlLibraryOperators::arrayAppendPrependReturnType,
          OperandTypes.ARRAY_ELEMENT_NONNULL);

  /** The "ARRAY_REMOVE(array, element)" function. */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction ARRAY_REMOVE =
      SqlBasicFunction.create(SqlKind.ARRAY_REMOVE,
          ReturnTypes.ARG0_NULLABLE,
          OperandTypes.ARRAY_ELEMENT_NONNULL);

  /** The "ARRAY_REPEAT(element, count)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_REPEAT =
      SqlBasicFunction.create(SqlKind.ARRAY_REPEAT,
          ReturnTypes.TO_ARRAY.andThen(SqlTypeTransforms.TO_NULLABLE),
          OperandTypes.sequence(
              "ARRAY_REPEAT(ANY, INTEGER)",
              OperandTypes.ANY, OperandTypes.typeName(SqlTypeName.INTEGER)));

  /** The "ARRAY_REVERSE(array)" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction ARRAY_REVERSE =
      SqlBasicFunction.create(SqlKind.ARRAY_REVERSE,
          ReturnTypes.ARG0_NULLABLE,
          OperandTypes.ARRAY);

  /** The "ARRAY_SIZE(array)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_SIZE =
      SqlBasicFunction.create(SqlKind.ARRAY_SIZE,
          ReturnTypes.INTEGER_NULLABLE,
          OperandTypes.ARRAY);

  /** The "ARRAY_SLICE(array, start, length)" function. */
  @LibraryOperator(libraries = {HIVE})
  public static final SqlFunction ARRAY_SLICE =
      SqlBasicFunction.create(SqlKind.ARRAY_SLICE,
          ReturnTypes.ARG0_NULLABLE,
          OperandTypes.sequence(
              "ARRAY_SLICE(ARRAY, INTEGER, INTEGER)",
              OperandTypes.ARRAY, OperandTypes.INTEGER, OperandTypes.INTEGER));

  /** The "ARRAY_UNION(array1, array2)" function. */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction ARRAY_UNION =
      SqlBasicFunction.create(SqlKind.ARRAY_UNION,
          ReturnTypes.LEAST_RESTRICTIVE,
          OperandTypes.and(
              OperandTypes.SAME_SAME,
              OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)));

  /** The "ARRAY_TO_STRING(array, delimiter [, nullText ])" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction ARRAY_TO_STRING =
      SqlBasicFunction.create(SqlKind.ARRAY_TO_STRING,
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_ARRAY_CHARACTER_OPTIONAL_CHARACTER);

  /** The "ARRAYS_OVERLAP(array1, array2)" function (Spark). */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAYS_OVERLAP =
      SqlBasicFunction.create(SqlKind.ARRAYS_OVERLAP,
          ReturnTypes.BOOLEAN_NULLABLE.andThen(SqlTypeTransforms.COLLECTION_ELEMENT_TYPE_NULLABLE),
          OperandTypes.and(
              OperandTypes.SAME_SAME,
              OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY),
              OperandTypes.NONNULL_NONNULL));

  private static RelDataType deriveTypeArraysZip(SqlOperatorBinding opBinding) {
    final List<RelDataType> argComponentTypes = new ArrayList<>();
    for (RelDataType arrayType : opBinding.collectOperandTypes()) {
      final RelDataType componentType = requireNonNull(arrayType.getComponentType());
      argComponentTypes.add(componentType);
    }

    final List<String> indexes = IntStream.range(0, argComponentTypes.size())
        .mapToObj(i -> String.valueOf(i))
        .collect(Collectors.toList());
    final RelDataType structType =
        opBinding.getTypeFactory().createStructType(argComponentTypes, indexes);
    return SqlTypeUtil.createArrayType(
        opBinding.getTypeFactory(),
        requireNonNull(structType, "inferred value type"),
        false);
  }

  /** The "ARRAYS_ZIP(array, ...)" function (Spark). */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAYS_ZIP =
      SqlBasicFunction.create(SqlKind.ARRAYS_ZIP,
          ((SqlReturnTypeInference) SqlLibraryOperators::deriveTypeArraysZip)
              .andThen(SqlTypeTransforms.TO_NULLABLE),
          OperandTypes.SAME_VARIADIC);

  /** The "SORT_ARRAY(array)" function (Spark). */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction SORT_ARRAY =
      SqlBasicFunction.create(SqlKind.SORT_ARRAY,
          ReturnTypes.ARG0_NULLABLE,
          OperandTypes.ARRAY.or(OperandTypes.ARRAY_BOOLEAN_LITERAL));

  private static RelDataType deriveTypeMapConcat(SqlOperatorBinding opBinding) {
    if (opBinding.getOperandCount() == 0) {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      requireNonNull(type, "type");
      return SqlTypeUtil.createMapType(typeFactory, type, type, true);
    } else {
      final List<RelDataType> operandTypes = opBinding.collectOperandTypes();
      for (RelDataType operandType : operandTypes) {
        if (!SqlTypeUtil.isMap(operandType)) {
          throw opBinding.newError(
              RESOURCE.typesShouldAllBeMap(
                  opBinding.getOperator().getName(),
                  operandType.getFullTypeString()));
        }
      }
      return requireNonNull(opBinding.getTypeFactory().leastRestrictive(operandTypes));
    }
  }

  /** The "MAP_CONCAT(map [, map]*)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_CONCAT =
      SqlBasicFunction.create(SqlKind.MAP_CONCAT,
          SqlLibraryOperators::deriveTypeMapConcat,
          OperandTypes.SAME_VARIADIC);

  /** The "MAP_ENTRIES(map)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_ENTRIES =
      SqlBasicFunction.create(SqlKind.MAP_ENTRIES,
          ReturnTypes.TO_MAP_ENTRIES_NULLABLE,
          OperandTypes.MAP);

  /** The "MAP_KEYS(map)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_KEYS =
      SqlBasicFunction.create(SqlKind.MAP_KEYS,
          ReturnTypes.TO_MAP_KEYS_NULLABLE,
          OperandTypes.MAP);

  /** The "MAP_VALUES(map)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_VALUES =
      SqlBasicFunction.create(SqlKind.MAP_VALUES,
          ReturnTypes.TO_MAP_VALUES_NULLABLE,
          OperandTypes.MAP);

  /** The "MAP_CONTAINS_KEY(map, key)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_CONTAINS_KEY =
      SqlBasicFunction.create(SqlKind.MAP_CONTAINS_KEY,
          ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.MAP_KEY);

  private static RelDataType deriveTypeMapFromArrays(SqlOperatorBinding opBinding) {
    final RelDataType keysArrayType = opBinding.getOperandType(0);
    final RelDataType valuesArrayType = opBinding.getOperandType(1);
    final boolean nullable = keysArrayType.isNullable() || valuesArrayType.isNullable();
    return SqlTypeUtil.createMapType(
        opBinding.getTypeFactory(),
        requireNonNull(keysArrayType.getComponentType(), "inferred key type"),
        requireNonNull(valuesArrayType.getComponentType(), "inferred value type"),
        nullable);
  }

  /** The "MAP_FROM_ARRAYS(keysArray, valuesArray)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_FROM_ARRAYS =
      SqlBasicFunction.create(SqlKind.MAP_FROM_ARRAYS,
          SqlLibraryOperators::deriveTypeMapFromArrays,
          OperandTypes.ARRAY_ARRAY);

  private static RelDataType deriveTypeMapFromEntries(SqlOperatorBinding opBinding) {
    final RelDataType entriesType = opBinding.collectOperandTypes().get(0);
    final RelDataType entryType = entriesType.getComponentType();
    requireNonNull(entryType, () -> "componentType of " + entriesType);
    return SqlTypeUtil.createMapType(
        opBinding.getTypeFactory(),
        requireNonNull(entryType.getFieldList().get(0).getType(), "inferred key type"),
        requireNonNull(entryType.getFieldList().get(1).getType(), "inferred value type"),
        entriesType.isNullable() || entryType.isNullable());
  }

  /** The "MAP_FROM_ENTRIES(arrayOfEntries)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_FROM_ENTRIES =
      SqlBasicFunction.create(SqlKind.MAP_FROM_ENTRIES,
          SqlLibraryOperators::deriveTypeMapFromEntries,
          OperandTypes.MAP_FROM_ENTRIES);

  /** The "STR_TO_MAP(string[, stringDelimiter[, keyValueDelimiter]])" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction STR_TO_MAP =
      SqlBasicFunction.create(SqlKind.STR_TO_MAP,
          ReturnTypes.IDENTITY_TO_MAP_NULLABLE,
          OperandTypes.STRING_OPTIONAL_STRING_OPTIONAL_STRING);

  /** The "SUBSTRING_INDEX(string, delimiter, count)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction SUBSTRING_INDEX =
      SqlBasicFunction.create(SqlKind.SUBSTRING_INDEX,
          ReturnTypes.ARG0_NULLABLE_VARYING,
          OperandTypes.STRING_STRING_INTEGER)
          .withFunctionType(SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL})
  public static final SqlFunction REVERSE =
      SqlBasicFunction.create(SqlKind.REVERSE,
          ReturnTypes.ARG0_NULLABLE_VARYING,
          OperandTypes.CHARACTER)
          .withFunctionType(SqlFunctionCategory.STRING);

  /** The "REVERSE(string|array)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction REVERSE_SPARK =
      SqlBasicFunction.create(SqlKind.REVERSE,
              ReturnTypes.ARG0_ARRAY_NULLABLE_VARYING,
              OperandTypes.CHARACTER.or(OperandTypes.ARRAY))
          .withFunctionType(SqlFunctionCategory.STRING)
          .withKind(SqlKind.REVERSE_SPARK);

  /** The "LEVENSHTEIN(string1, string2)" function. */
  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction LEVENSHTEIN =
      SqlBasicFunction.create("LEVENSHTEIN",
          ReturnTypes.INTEGER_NULLABLE,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL})
  public static final SqlFunction FROM_BASE64 =
      SqlBasicFunction.create("FROM_BASE64",
          ReturnTypes.VARBINARY_FORCE_NULLABLE,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction TO_BASE64 =
      SqlBasicFunction.create("TO_BASE64",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING.or(OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {HIVE})
  public static final SqlFunction BASE64 =
      SqlBasicFunction.create("BASE64",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING.or(OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {HIVE})
  public static final SqlFunction UN_BASE64 =
      SqlBasicFunction.create("UNBASE64",
          ReturnTypes.VARBINARY_FORCE_NULLABLE,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction FROM_BASE32 =
      SqlBasicFunction.create("FROM_BASE32",
          ReturnTypes.VARBINARY_NULLABLE,
          OperandTypes.CHARACTER, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TO_BASE32 =
      SqlBasicFunction.create("TO_BASE32",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING,
          SqlFunctionCategory.STRING);

  /**
   * The "FROM_HEX(varchar)" function; converts a hexadecimal-encoded {@code varchar} into bytes.
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction FROM_HEX =
      SqlBasicFunction.create("FROM_HEX",
          ReturnTypes.VARBINARY_NULLABLE,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  /**
   * The "TO_HEX(binary)" function; converts {@code binary} into a hexadecimal varchar.
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TO_HEX =
      SqlBasicFunction.create("TO_HEX",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.BINARY,
          SqlFunctionCategory.STRING);

  /**
   * The "HEX(string)" function; converts {@code string} into a hexadecimal varchar.
   */
  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction HEX =
      SqlBasicFunction.create("HEX",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  /** The "FORMAT_NUMBER(value, decimalOrFormat)" function. */
  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction FORMAT_NUMBER =
      SqlBasicFunction.create("FORMAT_NUMBER",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.or(
              OperandTypes.NUMERIC_NUMERIC,
              OperandTypes.NUMERIC_CHARACTER),
          SqlFunctionCategory.STRING);

  /** The "TO_CHAR(timestamp, format)" function;
   * converts {@code timestamp} to string according to the given {@code format}.
   *
   * <p>({@code TO_CHAR} is not supported in MySQL, but it is supported in
   * MariaDB, a variant of MySQL covered by {@link SqlLibrary#MYSQL}.) */
  @LibraryOperator(libraries = {MYSQL, ORACLE, REDSHIFT})
  public static final SqlFunction TO_CHAR =
      SqlBasicFunction.create("TO_CHAR",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.TIMESTAMP_STRING,
          SqlFunctionCategory.TIMEDATE);

  /** The "TO_CHAR(timestamp, format)" function;
   * converts {@code timestamp} to string according to the given {@code format}. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction TO_CHAR_PG =
      SqlBasicFunction.create("TO_CHAR", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.TIMESTAMP_STRING, SqlFunctionCategory.TIMEDATE);

  /** The "TO_DATE(string1, string2)" function; casts string1
   * to a DATE using the format specified in string2. */
  @LibraryOperator(libraries = {ORACLE, REDSHIFT, HIVE})
  public static final SqlFunction TO_DATE =
      SqlBasicFunction.create("TO_DATE",
          ReturnTypes.DATE_NULLABLE,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.TIMEDATE);

  /** The "TO_DATE(string1, string2)" function for PostgreSQL; casts string1
   * to a DATE using the format specified in string2. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction TO_DATE_PG =
      SqlBasicFunction.create("TO_DATE", ReturnTypes.DATE_NULLABLE,
          OperandTypes.STRING_STRING, SqlFunctionCategory.TIMEDATE);

  /** The "TO_TIMESTAMP(string1, string2)" function; casts string1
   * to a TIMESTAMP using the format specified in string2. */
  @LibraryOperator(libraries = {ORACLE, REDSHIFT})
  public static final SqlFunction TO_TIMESTAMP =
      SqlBasicFunction.create("TO_TIMESTAMP",
          ReturnTypes.TIMESTAMP_NULLABLE,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.TIMEDATE);

  /** The "TO_TIMESTAMP(string1, string2)" function for PostgreSQL; casts string1
   * to a TIMESTAMP using the format specified in string2. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction TO_TIMESTAMP_PG =
      SqlBasicFunction.create("TO_TIMESTAMP", ReturnTypes.TIMESTAMP_TZ_NULLABLE,
          OperandTypes.STRING_STRING, SqlFunctionCategory.TIMEDATE);

  /**
   * The "PARSE_TIME(string, string)" function (BigQuery);
   * converts a string representation of time to a TIME value.
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction PARSE_TIME =
      SqlBasicFunction.create("PARSE_TIME", ReturnTypes.TIME_NULLABLE,
          OperandTypes.STRING_STRING, SqlFunctionCategory.TIMEDATE);

  /**
   * The "PARSE_DATE(string, string)" function (BigQuery); Converts a string representation of date
   * to a DATE object.
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction PARSE_DATE =
      SqlBasicFunction.create("PARSE_DATE",
          ReturnTypes.DATE_NULLABLE, OperandTypes.STRING_STRING, SqlFunctionCategory.TIMEDATE);

  /**
   * The "PARSE_TIMESTAMP(string, string [, timezone])" function (BigQuery); Formats a timestamp
   * object according to the specified string.
   *
   * <p>In BigQuery, the "TIMESTAMP" datatype maps to Calcite's
   * TIMESTAMP_WITH_LOCAL_TIME_ZONE
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction PARSE_TIMESTAMP =
      SqlBasicFunction.create("PARSE_TIMESTAMP",
          ReturnTypes.TIMESTAMP_LTZ_NULLABLE, OperandTypes.STRING_STRING_OPTIONAL_STRING,
          SqlFunctionCategory.TIMEDATE);

  /**
   * The "PARSE_DATETIME(string, string [, timezone])" function (BigQuery); Formats a timestamp
   * object according to the specified string.
   *
   * <p>Note that the {@code TIMESTAMP} type of Calcite and Standard SQL
   * is called {@code DATETIME} in BigQuery.
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction PARSE_DATETIME =
      SqlBasicFunction.create("PARSE_DATETIME", ReturnTypes.TIMESTAMP_NULLABLE,
          OperandTypes.STRING_STRING, SqlFunctionCategory.TIMEDATE);

  /** The "FORMAT_TIME(string, time)" function (BigQuery);
   * Formats a time object according to the specified string. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction FORMAT_TIME =
      SqlBasicFunction.create("FORMAT_TIME", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.CHARACTER_TIME, SqlFunctionCategory.STRING);

  /** The "FORMAT_DATE(string, date)" function (BigQuery);
   * Formats a date object according to the specified string. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction FORMAT_DATE =
      SqlBasicFunction.create("FORMAT_DATE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.CHARACTER_DATE, SqlFunctionCategory.STRING);

  /** The "FORMAT_TIMESTAMP(string, timestamp)" function (BigQuery);
   * Formats a timestamp object according to the specified string.
   *
   * <p>In BigQuery, the "TIMESTAMP" datatype maps to Calcite's
   * TIMESTAMP_WITH_LOCAL_TIME_ZONE */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction FORMAT_TIMESTAMP =
      SqlBasicFunction.create("FORMAT_TIMESTAMP",
          ReturnTypes.VARCHAR_2000_NULLABLE,
          OperandTypes.sequence("FORMAT_TIMESTAMP(<CHARACTER>, "
                  + "<TIMESTAMP WITH LOCAL TIME ZONE>)",
              OperandTypes.CHARACTER, OperandTypes.TIMESTAMP_LTZ)
              .or(
                  OperandTypes.sequence("FORMAT_TIMESTAMP(<CHARACTER>, "
                          + "<TIMESTAMP WITH LOCAL TIME ZONE>, <CHARACTER>)",
                      OperandTypes.CHARACTER, OperandTypes.TIMESTAMP_LTZ,
                      OperandTypes.CHARACTER)),
          SqlFunctionCategory.STRING);

  /** The "FORMAT_DATETIME(string, timestamp)" function (BigQuery);
   * formats a timestamp object according to the specified string.
   *
   * <p>Note that the {@code TIMESTAMP} type of Calcite and Standard SQL
   * is called {@code DATETIME} in BigQuery. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction FORMAT_DATETIME =
      SqlBasicFunction.create("FORMAT_DATETIME",
          ReturnTypes.VARCHAR_2000_NULLABLE,
          OperandTypes.sequence("FORMAT_DATETIME(<CHARACTER>, <TIMESTAMP>)",
                  OperandTypes.CHARACTER, OperandTypes.TIMESTAMP_NTZ)
              .or(
                  OperandTypes.sequence("FORMAT_DATETIME(<CHARACTER>, "
                          + "<TIMESTAMP>, <CHARACTER>)",
                      OperandTypes.CHARACTER, OperandTypes.TIMESTAMP_NTZ,
                      OperandTypes.CHARACTER)),
          SqlFunctionCategory.STRING);

  /** The "TIMESTAMP_ADD(timestamp, interval)" function (BigQuery), the
   * two-argument variant of the built-in
   * {@link SqlStdOperatorTable#TIMESTAMP_ADD TIMESTAMPADD} function, which has
   * three arguments.
   *
   * <p>In BigQuery, the syntax is "TIMESTAMP_ADD(timestamp, INTERVAL
   * int64_expression date_part)" but in Calcite the second argument can be any
   * interval expression, not just an interval literal. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlBasicFunction TIMESTAMP_ADD2 =
      SqlBasicFunction.create(SqlKind.TIMESTAMP_ADD, ReturnTypes.ARG0_NULLABLE,
          OperandTypes.TIMESTAMP_INTERVAL)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** The "TIMESTAMP_DIFF(timestamp, timestamp, timeUnit)" function (BigQuery);
   * returns the number of timeUnit between the two timestamp expressions.
   *
   * <p>{@code TIMESTAMP_DIFF(t1, t2, unit)} is equivalent to
   * {@code TIMESTAMPDIFF(unit, t2, t1)} and {@code (t1 - t2) unit}. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIMESTAMP_DIFF3 =
      new SqlTimestampDiffFunction("TIMESTAMP_DIFF",
          OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIMESTAMP,
              SqlTypeFamily.ANY));

  /** The "TIME_ADD(time, interval)" function (BigQuery);
   * adds interval expression to the specified time expression. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIME_ADD =
      SqlBasicFunction.create(SqlKind.TIME_ADD, ReturnTypes.ARG0_NULLABLE,
              OperandTypes.TIME_INTERVAL)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** The "TIME_DIFF(time, time, timeUnit)" function (BigQuery);
   * returns the number of timeUnit between the two time expressions. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIME_DIFF =
      new SqlTimestampDiffFunction("TIME_DIFF",
          OperandTypes.family(SqlTypeFamily.TIME, SqlTypeFamily.TIME,
              SqlTypeFamily.ANY));

  /** The "DATE_TRUNC(date, timeUnit)" function (BigQuery);
   * truncates a DATE value to the beginning of a timeUnit. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATE_TRUNC =
      SqlBasicFunction.create("DATE_TRUNC",
          ReturnTypes.ARG0_NULLABLE,
          OperandTypes.sequence("'DATE_TRUNC(<DATE>, <DATETIME_INTERVAL>)'",
              OperandTypes.DATE_OR_TIMESTAMP, OperandTypes.dateInterval()),
          SqlFunctionCategory.TIMEDATE)
          .withOperandHandler(OperandHandlers.OPERAND_1_MIGHT_BE_TIME_FRAME)
          .withKind(SqlKind.DATE_TRUNC);

  /** The "TIME_SUB(time, interval)" function (BigQuery);
   * subtracts an interval from a time, independent of any time zone.
   *
   * <p>In BigQuery, the syntax is "TIME_SUB(time, INTERVAL int64 date_part)"
   * but in Calcite the second argument can be any interval expression, not just
   * an interval literal. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIME_SUB =
      SqlBasicFunction.create(SqlKind.TIME_SUB, ReturnTypes.ARG0_NULLABLE,
              OperandTypes.TIME_INTERVAL)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** The "TIME_TRUNC(time, timeUnit)" function (BigQuery);
   * truncates a TIME value to the beginning of a timeUnit. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIME_TRUNC =
      SqlBasicFunction.create("TIME_TRUNC",
          ReturnTypes.TIME_NULLABLE,
          OperandTypes.sequence("'TIME_TRUNC(<TIME>, <DATETIME_INTERVAL>)'",
              OperandTypes.TIME, OperandTypes.timeInterval()),
          SqlFunctionCategory.TIMEDATE);

  /** The "TIMESTAMP_SUB(timestamp, interval)" function (BigQuery);
   * subtracts an interval from a timestamp, independent of any time zone.
   *
   * <p>In BigQuery, the syntax is "TIMESTAMP_SUB(timestamp,
   * INTERVAL int64 date_part)" but in Calcite the second argument can be any
   * interval expression, not just an interval literal. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlBasicFunction TIMESTAMP_SUB =
      SqlBasicFunction.create(SqlKind.TIMESTAMP_SUB, ReturnTypes.ARG0_NULLABLE,
          OperandTypes.TIMESTAMP_INTERVAL)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** The "DATETIME_SUB(timestamp, interval)" function (BigQuery).
   *
   * <p>Note that the {@code TIMESTAMP} type of Calcite and Standard SQL
   * is called {@code DATETIME} in BigQuery.
   *
   * <p>A synonym for {@link #TIMESTAMP_SUB}, which supports both
   * {@code TIMESTAMP} and {@code TIMESTAMP WITH LOCAL TIME ZONE} operands. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATETIME_SUB =
      TIMESTAMP_SUB.withName("DATETIME_SUB");

  /** The "TIMESTAMP_TRUNC(timestamp, timeUnit[, timeZone])" function (BigQuery);
   * truncates a {@code TIMESTAMP WITH LOCAL TIME ZONE} value to the beginning
   * of a timeUnit.
   *
   * <p>Note that the {@code TIMESTAMP WITH LOCAL TIME ZONE} type of Calcite
   * is called {@code TIMESTAMP} in BigQuery. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIMESTAMP_TRUNC =
      SqlBasicFunction.create("TIMESTAMP_TRUNC",
          ReturnTypes.ARG0_EXCEPT_DATE_NULLABLE,
          OperandTypes.sequence(
              "'TIMESTAMP_TRUNC(<TIMESTAMP>, <DATETIME_INTERVAL>)'",
              OperandTypes.DATE_OR_TIMESTAMP, OperandTypes.timestampInterval()),
          SqlFunctionCategory.TIMEDATE);

  /** The "DATETIME_TRUNC(timestamp, timeUnit)" function (BigQuery);
   * truncates a TIMESTAMP value to the beginning of a timeUnit.
   *
   * <p>Note that the {@code TIMESTAMP} type of Calcite and Standard SQL
   * is called {@code DATETIME} in BigQuery. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATETIME_TRUNC =
      SqlBasicFunction.create("DATETIME_TRUNC",
          ReturnTypes.ARG0_EXCEPT_DATE_NULLABLE,
          OperandTypes.sequence(
              "'DATETIME_TRUNC(<TIMESTAMP>, <DATETIME_INTERVAL>)'",
              OperandTypes.DATE_OR_TIMESTAMP, OperandTypes.timestampInterval()),
          SqlFunctionCategory.TIMEDATE);

  /** The "TIMESTAMP_SECONDS(bigint)" function; returns a TIMESTAMP value
   * a given number of seconds after 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction TIMESTAMP_SECONDS =
      SqlBasicFunction.create("TIMESTAMP_SECONDS",
          ReturnTypes.TIMESTAMP_NULLABLE, OperandTypes.INTEGER,
          SqlFunctionCategory.TIMEDATE);

  /** The "TIMESTAMP_MILLIS(bigint)" function; returns a TIMESTAMP value
   * a given number of milliseconds after 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction TIMESTAMP_MILLIS =
      SqlBasicFunction.create("TIMESTAMP_MILLIS",
          ReturnTypes.TIMESTAMP_NULLABLE, OperandTypes.INTEGER,
          SqlFunctionCategory.TIMEDATE);

  /** The "TIMESTAMP_MICROS(bigint)" function; returns a TIMESTAMP value
   * a given number of micro-seconds after 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction TIMESTAMP_MICROS =
      SqlBasicFunction.create("TIMESTAMP_MICROS",
          ReturnTypes.TIMESTAMP_NULLABLE, OperandTypes.INTEGER,
          SqlFunctionCategory.TIMEDATE);

  /** The "UNIX_SECONDS(bigint)" function; returns the number of seconds
   * since 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction UNIX_SECONDS =
      SqlBasicFunction.create("UNIX_SECONDS", ReturnTypes.BIGINT_NULLABLE,
          OperandTypes.TIMESTAMP, SqlFunctionCategory.TIMEDATE);

  /** The "UNIX_MILLIS(bigint)" function; returns the number of milliseconds
   * since 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction UNIX_MILLIS =
      SqlBasicFunction.create("UNIX_MILLIS",
          ReturnTypes.BIGINT_NULLABLE, OperandTypes.TIMESTAMP,
          SqlFunctionCategory.TIMEDATE);

  /** The "UNIX_MICROS(bigint)" function; returns the number of microseconds
   * since 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction UNIX_MICROS =
      SqlBasicFunction.create("UNIX_MICROS",
          ReturnTypes.BIGINT_NULLABLE, OperandTypes.TIMESTAMP,
          SqlFunctionCategory.TIMEDATE);

  /** The "DATETIME_ADD(timestamp, interval)" function (BigQuery).
   * As {@code TIMESTAMP_ADD}, returns a Calcite {@code TIMESTAMP}
   * (which BigQuery calls a {@code DATETIME}). */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATETIME_ADD =
      TIMESTAMP_ADD2.withName("DATETIME_ADD");

  /** The "DATETIME_DIFF(timestamp, timestamp2, timeUnit)" function (BigQuery).
   *
   * <p>Note that the {@code TIMESTAMP} type of Calcite and Standard SQL
   * is called {@code DATETIME} in BigQuery. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATETIME_DIFF =
      new SqlTimestampDiffFunction("DATETIME_DIFF",
          OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIMESTAMP,
              SqlTypeFamily.ANY));

  /** The "SAFE_ADD(numeric1, numeric2)" function; equivalent to the {@code +} operator but
   * returns null if overflow occurs. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction SAFE_ADD =
      SqlBasicFunction.create("SAFE_ADD",
          ReturnTypes.SUM_FORCE_NULLABLE,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "SAFE_DIVIDE(numeric1, numeric2)" function; equivalent to the {@code /} operator but
   * returns null if an error occurs, such as overflow or division by zero. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction SAFE_DIVIDE =
      SqlBasicFunction.create("SAFE_DIVIDE",
          ReturnTypes.DOUBLE_IF_INTEGERS.orElse(ReturnTypes.QUOTIENT_FORCE_NULLABLE),
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "SAFE_MULTIPLY(numeric1, numeric2)" function; equivalent to the {@code *} operator but
   * returns null if overflow occurs. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction SAFE_MULTIPLY =
      SqlBasicFunction.create("SAFE_MULTIPLY",
          ReturnTypes.PRODUCT_FORCE_NULLABLE,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "SAFE_NEGATE(numeric)" function; negates {@code numeric} and returns null if overflow
   * occurs. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction SAFE_NEGATE =
      SqlBasicFunction.create("SAFE_NEGATE",
          ReturnTypes.ARG0_FORCE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "SAFE_SUBTRACT(numeric1, numeric2)" function; equivalent to the {@code -} operator but
   * returns null if overflow occurs. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction SAFE_SUBTRACT =
      SqlBasicFunction.create("SAFE_SUBTRACT",
          ReturnTypes.SUM_FORCE_NULLABLE,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "CHAR(n)" function; returns the character whose ASCII code is
   * {@code n} % 256, or null if {@code n} &lt; 0. */
  @LibraryOperator(libraries = {MYSQL, SPARK})
  public static final SqlFunction CHAR =
      SqlBasicFunction.create("CHAR",
          ReturnTypes.CHAR_FORCE_NULLABLE,
          OperandTypes.INTEGER,
          SqlFunctionCategory.STRING);

  /** The "CHR(n)" function; returns the character whose UTF-8 code is
   * {@code n}. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, POSTGRESQL})
  public static final SqlFunction CHR =
      SqlBasicFunction.create("CHR",
          ReturnTypes.CHAR_NULLABLE_IF_ARGS_NULLABLE,
          OperandTypes.INTEGER,
          SqlFunctionCategory.STRING);

  /** The "CODE_POINTS_TO_BYTES(integers)" function (BigQuery); Converts an array of extended ASCII
   * code points to bytes. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction CODE_POINTS_TO_BYTES =
      SqlBasicFunction.create("CODE_POINTS_TO_BYTES",
          ReturnTypes.VARBINARY
              .andThen(SqlTypeTransforms.TO_NULLABLE)
              .andThen(SqlTypeTransforms.TO_NULLABLE_IF_ARRAY_CONTAINS_NULLABLE),
          OperandTypes.ARRAY_OF_INTEGER,
          SqlFunctionCategory.STRING);

  /** The "CODE_POINTS_TO_STRING(integers)" function (BigQuery); Converts an array of Unicode code
   * points to string. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction CODE_POINTS_TO_STRING =
      SqlBasicFunction.create("CODE_POINTS_TO_STRING",
          ReturnTypes.VARCHAR
              .andThen(SqlTypeTransforms.TO_NULLABLE)
              .andThen(SqlTypeTransforms.TO_NULLABLE_IF_ARRAY_CONTAINS_NULLABLE),
          OperandTypes.ARRAY_OF_INTEGER,
          SqlFunctionCategory.STRING);

  /** The "TO_CODE_POINTS(string or binary)" function (BigQuery); Converts a {@code string} or
   * {@code binary} value to an array of integers that represent code points or extended ASCII
   * character values. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TO_CODE_POINTS =
      SqlBasicFunction.create("TO_CODE_POINTS",
          ReturnTypes.INTEGER.andThen(SqlTypeTransforms.TO_ARRAY_FORCE_NULLABLE),
          OperandTypes.STRING.or(OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {ALL})
  public static final SqlFunction TANH =
      SqlBasicFunction.create("TANH",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "COTH(value)" function; returns the hyperbolic cotangent
   * of {@code value}. */
  @LibraryOperator(libraries = {ALL})
  public static final SqlFunction COTH =
      SqlBasicFunction.create("COTH",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {ALL})
  public static final SqlFunction COSH =
      SqlBasicFunction.create("COSH",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code ACOSH(numeric)} function; returns the inverse hyperbolic cosine
   * of {@code value}. */
  @LibraryOperator(libraries = {ALL})
  public static final SqlFunction ACOSH =
      SqlBasicFunction.create("ACOSH",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code ASINH(numeric)} function; returns the inverse hyperbolic sine of {@code value}. */
  @LibraryOperator(libraries = {ALL})
  public static final SqlFunction ASINH =
      SqlBasicFunction.create("ASINH",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code ATANH(numeric)} function; returns the inverse hyperbolic tangent
   * of {@code value}. */
  @LibraryOperator(libraries = {ALL})
  public static final SqlFunction ATANH =
      SqlBasicFunction.create("ATANH",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code COSD(numeric)} function; returns the cosine
   * of {@code value}. {@code value} is treated as degrees. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction COSD =
      SqlBasicFunction.create("COSD",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code SIND(numeric)} function; returns the sine
   * of {@code value}. {@code value} is treated as degrees. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction SIND =
      SqlBasicFunction.create("SIND",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code TAND(numeric)} function; returns the tangent
   * of {@code value}. {@code value} is treated as degrees. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction TAND =
      SqlBasicFunction.create("TAND",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code ACOSD(numeric)} function; returns the inverse cosine
   * of {@code value} in degrees. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction ACOSD =
      SqlBasicFunction.create("ACOSD",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code ACOSD(numeric)} function; returns the inverse sine
   * of {@code value} in degrees. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction ASIND =
      SqlBasicFunction.create("ASIND",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code ACOSD(numeric)} function; returns the inverse tangent
   * of {@code value} in degrees. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction ATAND =
      SqlBasicFunction.create("ATAND",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "COTH(value)" function; returns the hyperbolic secant
   * of {@code value}. */
  @LibraryOperator(libraries = {ALL})
  public static final SqlFunction SECH =
      SqlBasicFunction.create("SECH",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "COTH(value)" function; returns the hyperbolic cosecant
   * of {@code value}. */
  @LibraryOperator(libraries = {ALL})
  public static final SqlFunction CSCH =
      SqlBasicFunction.create("CSCH",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {ALL})
  public static final SqlFunction SINH =
      SqlBasicFunction.create("SINH",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {ALL})
  public static final SqlFunction CSC =
      SqlBasicFunction.create("CSC",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {ALL})
  public static final SqlFunction SEC =
      SqlBasicFunction.create("SEC",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code FACTORIAL(integer)} function.
   * Returns the factorial of integer, the range of integer is [0, 20].
   * Otherwise, returns NULL. */
  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction FACTORIAL =
      SqlBasicFunction.create("FACTORIAL",
          ReturnTypes.BIGINT_FORCE_NULLABLE,
          OperandTypes.INTEGER,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction BIN =
      SqlBasicFunction.create("BIN",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction MD5 =
      SqlBasicFunction.create("MD5",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING.or(OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction CRC32 =
      SqlBasicFunction.create("CRC32",
          ReturnTypes.BIGINT_NULLABLE,
          OperandTypes.STRING.or(OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction SHA1 =
      SqlBasicFunction.create("SHA1",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING.or(OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction SHA256 =
      SqlBasicFunction.create("SHA256",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING.or(OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction SHA512 =
      SqlBasicFunction.create("SHA512",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING.or(OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  /** The "IS_INF(value)" function. Returns whether value is infinite. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction IS_INF =
      SqlBasicFunction.create("IS_INF",
          ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "IS_NAN(value)" function. Returns whether value is NaN. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction IS_NAN =
      SqlBasicFunction.create("IS_NAN",
          ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "LOG(value [, value2])" function.
   *
   * @see SqlStdOperatorTable#LN
   * @see SqlStdOperatorTable#LOG10
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction LOG =
      SqlBasicFunction.create("LOG",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC_OPTIONAL_NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "LOG(numeric1 [, numeric2 ]) " function. Returns the logarithm of numeric2
   * to base numeric1.*/
  @LibraryOperator(libraries = {MYSQL, SPARK, HIVE})
  public static final SqlFunction LOG_MYSQL =
      SqlBasicFunction.create(SqlKind.LOG,
          ReturnTypes.DOUBLE_FORCE_NULLABLE,
          OperandTypes.NUMERIC_OPTIONAL_NUMERIC);

  /** The "LOG(numeric1 [, numeric2 ]) " function. Returns the logarithm of numeric2
   * to base numeric1.*/
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction LOG_POSTGRES =
      SqlBasicFunction.create("LOG", ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC_OPTIONAL_NUMERIC, SqlFunctionCategory.NUMERIC);

  /** The "LOG2(numeric)" function. Returns the base 2 logarithm of numeric. */
  @LibraryOperator(libraries = {MYSQL, SPARK})
  public static final SqlFunction LOG2 =
      SqlBasicFunction.create("LOG2",
          ReturnTypes.DOUBLE_FORCE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "LOG1p(numeric)" function. Returns log(1 + numeric). */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction LOG1P =
      SqlBasicFunction.create("LOG1P",
          ReturnTypes.DOUBLE_FORCE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction POW =
      SqlBasicFunction.create("POW",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code POWER(numeric, numeric)} function.
   *
   * <p>The return type is {@code DECIMAL} if either argument is a
   * {@code DECIMAL}. In all other cases, the return type is a double.
   */
  @LibraryOperator(libraries = { POSTGRESQL })
  public static final SqlFunction POWER_PG =
      SqlBasicFunction.create("POWER",
          ReturnTypes.DECIMAL_OR_DOUBLE_NULLABLE,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "TRUNC(numeric1 [, integer2])" function. Identical to the standard <code>TRUNCATE</code>
  * function except the return type should be a double if numeric1 is an integer. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TRUNC_BIG_QUERY = SqlStdOperatorTable.TRUNCATE
          .withName("TRUNC")
          .withReturnTypeInference(ReturnTypes.ARG0_EXCEPT_INTEGER_NULLABLE);

  /** Infix "::" cast operator used by PostgreSQL, for example
   * {@code '100'::INTEGER}. */
  @LibraryOperator(libraries = { POSTGRESQL })
  public static final SqlOperator INFIX_CAST =
      new SqlCastOperator();

  /** The "SAFE_CAST(expr AS type)" function; identical to CAST(),
   * except that if conversion fails, it returns NULL instead of raising an
   * error. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction SAFE_CAST =
      new SqlCastFunction("SAFE_CAST", SqlKind.SAFE_CAST);

  /** The "TRY_CAST(expr AS type)" function, equivalent to SAFE_CAST. */
  @LibraryOperator(libraries = {MSSQL})
  public static final SqlFunction TRY_CAST =
      new SqlCastFunction("TRY_CAST", SqlKind.SAFE_CAST);

  /** The "OFFSET(index)" array subscript operator used by BigQuery. The index
   * starts at 0 and produces an error if the index is out of range. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlOperator OFFSET =
      new SqlItemOperator("OFFSET", OperandTypes.ARRAY, 0, false);

  /** The "ORDINAL(index)" array subscript operator used by BigQuery. The index
   * starts at 1 and produces an error if the index is out of range. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlOperator ORDINAL =
      new SqlItemOperator("ORDINAL", OperandTypes.ARRAY, 1, false);

  /** The "SAFE_OFFSET(index)" array subscript operator used by BigQuery. The index
   * starts at 0 and returns null if the index is out of range. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlOperator SAFE_OFFSET =
      new SqlItemOperator("SAFE_OFFSET", OperandTypes.ARRAY, 0, true);

  /** The "SAFE_ORDINAL(index)" array subscript operator used by BigQuery. The index
   * starts at 1 and returns null if the index is out of range. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlOperator SAFE_ORDINAL =
      new SqlItemOperator("SAFE_ORDINAL", OperandTypes.ARRAY, 1, true);

  /** NULL-safe "&lt;=&gt;" equal operator used by MySQL, for example
   * {@code 1<=>NULL}. */
  @LibraryOperator(libraries = { MYSQL })
  public static final SqlOperator NULL_SAFE_EQUAL =
      new SqlBinaryOperator(
          "<=>",
          SqlKind.IS_NOT_DISTINCT_FROM,
          30,
          true,
          ReturnTypes.BOOLEAN,
          InferTypes.FIRST_KNOWN,
          OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

  /** The "BITAND_AGG(expression)" function. Equivalent to
  * the standard "BIT_AND(expression)". */
  @LibraryOperator(libraries = {SNOWFLAKE})
  public static final SqlAggFunction BITAND_AGG =
      new SqlBitOpAggFunction("BITAND_AGG", SqlKind.BIT_AND);

  /** The "BITOR_AGG(expression)" function. Equivalent to
  * the standard "BIT_OR(expression)". */
  @LibraryOperator(libraries = {SNOWFLAKE})
  public static final SqlAggFunction BITOR_AGG =
      new SqlBitOpAggFunction("BITOR_AGG", SqlKind.BIT_OR);

  /** The "BIT_LENGTH(string or binary)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction BIT_LENGTH =
      SqlBasicFunction.create("BIT_LENGTH",
          ReturnTypes.INTEGER_NULLABLE,
          OperandTypes.or(OperandTypes.CHARACTER, OperandTypes.BINARY),
          SqlFunctionCategory.NUMERIC);

  /** The "BIT_GET(value, position)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlBasicFunction BIT_GET =
      SqlBasicFunction.create("BIT_GET",
          ReturnTypes.TINYINT_NULLABLE,
          OperandTypes.NUMERIC_INTEGER,
          SqlFunctionCategory.NUMERIC);

  /** Alias for {@link #BIT_GET}. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction GETBIT =
      BIT_GET.withName("GETBIT");

  /** The RANDOM() function. Equivalent to RAND(). */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlFunction RANDOM = SqlStdOperatorTable.RAND
      .withName("RANDOM")
      .withOperandTypeChecker(OperandTypes.NILADIC);
}
