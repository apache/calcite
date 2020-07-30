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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlLibrary.BIGQUERY;
import static org.apache.calcite.sql.fun.SqlLibrary.HIVE;
import static org.apache.calcite.sql.fun.SqlLibrary.MYSQL;
import static org.apache.calcite.sql.fun.SqlLibrary.ORACLE;
import static org.apache.calcite.sql.fun.SqlLibrary.POSTGRESQL;
import static org.apache.calcite.sql.fun.SqlLibrary.SNOWFLAKE;
import static org.apache.calcite.sql.fun.SqlLibrary.SPARK;
import static org.apache.calcite.sql.fun.SqlLibrary.STANDARD;
import static org.apache.calcite.sql.fun.SqlLibrary.TERADATA;
import static org.apache.calcite.sql.type.OperandTypes.DATETIME_INTEGER;
import static org.apache.calcite.sql.type.OperandTypes.DATETIME_INTERVAL;


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

  /** The "NVL(value, value)" function. */
  @LibraryOperator(libraries = {ORACLE, HIVE, SPARK})
  public static final SqlFunction NVL =
      new SqlFunction("NVL", SqlKind.NVL,
          ReturnTypes.cascade(ReturnTypes.LEAST_RESTRICTIVE,
              SqlTypeTransforms.TO_NULLABLE_ALL),
          null, OperandTypes.SAME_SAME, SqlFunctionCategory.SYSTEM);

  /** The "IFNULL(value, value)" function. */
  @LibraryOperator(libraries = {BIGQUERY})
  public static final SqlFunction IFNULL =
      new SqlFunction("IFNULL", SqlKind.OTHER_FUNCTION,
          ReturnTypes.cascade(ReturnTypes.LEAST_RESTRICTIVE,
              SqlTypeTransforms.TO_NULLABLE_ALL),
          null, OperandTypes.SAME_SAME, SqlFunctionCategory.SYSTEM);

  /** The "LTRIM(string)" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction LTRIM =
      new SqlFunction("LTRIM", SqlKind.LTRIM,
          ReturnTypes.cascade(ReturnTypes.ARG0, SqlTypeTransforms.TO_NULLABLE,
              SqlTypeTransforms.TO_VARYING), null,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  /** The "RTRIM(string)" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction RTRIM =
      new SqlFunction("RTRIM", SqlKind.RTRIM,
          ReturnTypes.cascade(ReturnTypes.ARG0, SqlTypeTransforms.TO_NULLABLE,
              SqlTypeTransforms.TO_VARYING), null,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  /** Oracle's "SUBSTR(string, position [, substringLength ])" function.
   *
   * <p>It has similar semantics to standard SQL's
   * {@link SqlStdOperatorTable#SUBSTRING} function but different syntax. */
  @LibraryOperator(libraries = {ORACLE, BIGQUERY})
  public static final SqlFunction SUBSTR =
      new SqlFunction("SUBSTR", SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING, null, null,
          SqlFunctionCategory.STRING);

  /** The "GREATEST(value, value)" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction GREATEST =
      new SqlFunction("GREATEST", SqlKind.GREATEST,
          ReturnTypes.cascade(ReturnTypes.LEAST_RESTRICTIVE,
              SqlTypeTransforms.TO_NULLABLE), null,
          OperandTypes.SAME_VARIADIC, SqlFunctionCategory.SYSTEM);

  /** The "LEAST(value, value)" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction LEAST =
      new SqlFunction("LEAST", SqlKind.LEAST,
          ReturnTypes.cascade(ReturnTypes.LEAST_RESTRICTIVE,
              SqlTypeTransforms.TO_NULLABLE), null,
          OperandTypes.SAME_VARIADIC, SqlFunctionCategory.SYSTEM);

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

  @LibraryOperator(libraries = {BIGQUERY, HIVE, SPARK})
  public static final SqlFunction CURRENT_TIMESTAMP = new SqlCurrentTimestampFunction(
          "CURRENT_TIMESTAMP", SqlTypeName.TIMESTAMP);

  /**
   * The REGEXP_EXTRACT(source_string, regex_pattern) returns the first substring in source_string
   * that matches the regex_pattern. Returns NULL if there is no match.
   *
   * The REGEXP_EXTRACT_ALL(source_string, regex_pattern) returns an array of all substrings of
   * source_string that match the regex_pattern.
   */
  @LibraryOperator(libraries = {BIGQUERY})
  public static final SqlFunction REGEXP_EXTRACT = new SqlFunction("REGEXP_EXTRACT",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR),
          SqlTypeTransforms.TO_NULLABLE),
      null, OperandTypes.STRING_STRING,
      SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIGQUERY})
  public static final SqlFunction REGEXP_EXTRACT_ALL = new SqlFunction("REGEXP_EXTRACT_ALL",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR),
          SqlTypeTransforms.TO_NULLABLE),
      null, OperandTypes.STRING_STRING,
      SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {STANDARD})
  public static final SqlFunction FORMAT_TIMESTAMP = new SqlFunction("FORMAT_TIMESTAMP",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.VARCHAR_2000_NULLABLE, null,
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.TIMESTAMP),
      SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction DATE_FORMAT = new SqlFunction("DATE_FORMAT",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.VARCHAR_2000_NULLABLE, null,
      OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.STRING),
      SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {STANDARD})
  public static final SqlFunction FORMAT_DATE = new SqlFunction("FORMAT_DATE",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.VARCHAR_2000_NULLABLE, null,
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.DATE),
      SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {STANDARD})
  public static final SqlFunction FORMAT_TIME = new SqlFunction("FORMAT_TIME",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.VARCHAR_2000_NULLABLE, null,
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.TIME),
      SqlFunctionCategory.TIMEDATE);

  /** The "MONTHNAME(datetime)" function; returns the name of the month,
   * in the current locale, of a TIMESTAMP or DATE argument. */
  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction MONTHNAME =
      new SqlFunction("MONTHNAME", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000, null, OperandTypes.DATETIME,
          SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {BIGQUERY, HIVE, SPARK})
  public static final SqlFunction DATE_ADD =
      new SqlFunction(
        "DATE_ADD",
        SqlKind.PLUS,
        ReturnTypes.DATE,
        null,
        OperandTypes.or(DATETIME_INTERVAL, DATETIME_INTEGER),
        SqlFunctionCategory.TIMEDATE) {

        @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
          writer.getDialect().unparseIntervalOperandsBasedFunctions(
                writer, call, leftPrec, rightPrec);
        }
      };

  @LibraryOperator(libraries = {BIGQUERY, HIVE, SPARK})
  public static final SqlFunction DATE_SUB =
      new SqlFunction(
          "DATE_SUB",
          SqlKind.MINUS,
          ReturnTypes.DATE,
          null,
          OperandTypes.or(DATETIME_INTERVAL, DATETIME_INTEGER),
          SqlFunctionCategory.TIMEDATE) {

    @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      writer.getDialect().unparseIntervalOperandsBasedFunctions(
          writer, call, leftPrec, rightPrec);
    }
  };


  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction ADD_MONTHS =
      new SqlFunction(
        "ADD_MONTHS",
        SqlKind.PLUS,
        ReturnTypes.DATE,
        null,
        OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER),
        SqlFunctionCategory.TIMEDATE) {

        @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
          writer.getDialect().unparseIntervalOperandsBasedFunctions(
                writer, call, leftPrec, rightPrec);
        }
      };

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
  @LibraryOperator(libraries = {MYSQL, POSTGRESQL, ORACLE})
  public static final SqlFunction CONCAT_FUNCTION =
      new SqlFunction("CONCAT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.cascade(
              opBinding -> {
                int precision = opBinding.collectOperandTypes().stream()
                    .mapToInt(RelDataType::getPrecision).sum();
                return opBinding.getTypeFactory()
                    .createSqlType(SqlTypeName.VARCHAR, precision);
              },
              SqlTypeTransforms.TO_NULLABLE),
          null,
          OperandTypes.repeat(SqlOperandCountRanges.from(2),
              OperandTypes.STRING),
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
      new SqlFunction("FROM_BASE64",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARBINARY),
              SqlTypeTransforms.TO_NULLABLE),
          null,
          OperandTypes.STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction TO_BASE64 =
      new SqlFunction("TO_BASE64",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR),
              SqlTypeTransforms.TO_NULLABLE),
          null,
          OperandTypes.or(OperandTypes.STRING, OperandTypes.BINARY),
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

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction CHR =
      new SqlFunction("CHR",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.CHAR,
          null,
          OperandTypes.INTEGER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL, POSTGRESQL})
  public static final SqlFunction MD5 =
      new SqlFunction("MD5",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR),
              SqlTypeTransforms.TO_NULLABLE),
          null,
          OperandTypes.or(OperandTypes.STRING, OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL, POSTGRESQL})
  public static final SqlFunction SHA1 =
      new SqlFunction("SHA1",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR),
              SqlTypeTransforms.TO_NULLABLE),
          null,
          OperandTypes.or(OperandTypes.STRING, OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  /** Infix "::" cast operator used by PostgreSQL, for example
   * {@code '100'::INTEGER}. */
  @LibraryOperator(libraries = { POSTGRESQL })
  public static final SqlOperator INFIX_CAST =
      new SqlCastOperator();

  @LibraryOperator(libraries = {STANDARD})
  public static final SqlFunction FORMAT =
      new SqlFunction(
          "FORMAT",
          SqlKind.FORMAT,
          ReturnTypes.VARCHAR_2000_NULLABLE, null,
          OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC),
          SqlFunctionCategory.STRING);

  /** The "TO_NUMBER(string1, string2)" function; casts string1
   * as hexadecimal to a NUMBER using the format specified in string2. */
  @LibraryOperator(libraries = {TERADATA, POSTGRESQL, ORACLE})
  public static final SqlFunction TO_NUMBER =
      new SqlFunction(
          "TO_NUMBER",
          SqlKind.TO_NUMBER,
          ReturnTypes.BIGINT_FORCE_NULLABLE,
          null, OperandTypes.or(OperandTypes.STRING, OperandTypes.STRING_STRING,
          OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.NULL),
          OperandTypes.family(SqlTypeFamily.NULL, SqlTypeFamily.STRING),
          OperandTypes.STRING_STRING_STRING,
          OperandTypes.family(SqlTypeFamily.NULL)),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction CONV =
          new SqlFunction(
                  "CONV",
                  SqlKind.OTHER_FUNCTION,
                  ReturnTypes.VARCHAR_4_NULLABLE, null,
                  OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC,
                          SqlTypeFamily.NUMERIC),
                  SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIGQUERY, HIVE, SPARK})
  public static final SqlFunction IF =
      new SqlFunction(
          "IF",
          SqlKind.IF,
          ReturnTypes.ARG2_NULLABLE,
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
          SqlFunctionCategory.SYSTEM);

  @LibraryOperator(libraries = {BIGQUERY, HIVE, SPARK})
  public static final SqlFunction RPAD =
      new SqlFunction("RPAD", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000_NULLABLE, null,
          OperandTypes.STRING_INTEGER_OPTIONAL_STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIGQUERY, HIVE, SPARK})
  public static final SqlFunction LPAD =
      new SqlFunction("LPAD", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000_NULLABLE, null,
          OperandTypes.STRING_INTEGER_OPTIONAL_STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {STANDARD})
  public static final SqlFunction STR_TO_DATE = new SqlFunction(
      "STR_TO_DATE",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.DATE_NULLABLE,
      null,
      OperandTypes.STRING_STRING,
      SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {BIGQUERY})
  public static final SqlFunction PARSE_DATE =
      new SqlFunction(
          "PARSE_DATE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DATE_NULLABLE, null,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction UNIX_TIMESTAMP =
      new SqlFunction(
      "UNIX_TIMESTAMP",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BIGINT_NULLABLE, null,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.STRING, SqlTypeFamily.STRING),
          // both the operands are optional
          number -> number == 0 || number == 1),
      SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction FROM_UNIXTIME =
      new SqlFunction(
          "FROM_UNIXTIME",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000_NULLABLE, null,
          OperandTypes.family(ImmutableList.of(SqlTypeFamily.INTEGER, SqlTypeFamily.STRING),
              // Second operand is optional
              number -> number == 1),
          SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {STANDARD})
  public static final SqlFunction STRING_SPLIT = new SqlFunction(
      "STRING_SPLIT",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.MULTISET_NULLABLE,
      null,
      OperandTypes.STRING_STRING,
      SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction SPLIT = new SqlFunction(
      "SPLIT",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.MULTISET_NULLABLE,
      null,
      OperandTypes.STRING_STRING,
      SqlFunctionCategory.STRING);

  /** The "TO_VARCHAR(numeric, string)" function; casts string
   * Format first_operand to specified in second operand. */
  @LibraryOperator(libraries = {SNOWFLAKE})
  public static final SqlFunction TO_VARCHAR =
      new SqlFunction(
          "TO_VARCHAR",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000_NULLABLE, null,
          OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIGQUERY})
  public static final SqlFunction TIMESTAMP_TO_DATE =
    new SqlFunction(
      "DATE",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.ARG0_NULLABLE,
      null,
      OperandTypes.DATETIME,
      SqlFunctionCategory.TIMEDATE);
}
// End SqlLibraryOperators.java
