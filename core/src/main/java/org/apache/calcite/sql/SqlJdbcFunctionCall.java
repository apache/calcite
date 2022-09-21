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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlJdbcFunctionCall</code> is a node of a parse tree which represents
 * a JDBC function call. A JDBC call is of the form <code>{fn NAME(arg0, arg1,
 * ...)}</code>.
 *
 * <p>See <a href="http://java.sun.com/products/jdbc/driverdevs.html">Sun's
 * documentation for writers of JDBC drivers</a>.*
 *
 * <table>
 * <caption>Supported JDBC functions</caption>
 * <tr>
 * <th>Function Name</th>
 * <th>Function Returns</th>
 * </tr>
 * <tr>
 * <td colspan="2"><br>
 *
 * <h2>NUMERIC FUNCTIONS</h2>
 * </td>
 * </tr>
 * <tr>
 * <td>ABS(number)</td>
 * <td>Absolute value of number</td>
 * </tr>
 * <tr>
 * <td>ACOS(float)</td>
 * <td>Arccosine, in radians, of float</td>
 * </tr>
 * <tr>
 * <td>ASIN(float)</td>
 * <td>Arcsine, in radians, of float</td>
 * </tr>
 * <tr>
 * <td>ATAN(float)</td>
 * <td>Arctangent, in radians, of float</td>
 * </tr>
 * <tr>
 * <td>ATAN2(float1, float2)</td>
 * <td>Arctangent, in radians, of float2 / float1</td>
 * </tr>
 * <tr>
 * <td>CBRT(number)</td>
 * <td>The cube root of number</td>
 * </tr>
 * <tr>
 * <td>CEILING(number)</td>
 * <td>Smallest integer &gt;= number</td>
 * </tr>
 * <tr>
 * <td>COS(float)</td>
 * <td>Cosine of float radians</td>
 * </tr>
 * <tr>
 * <td>COT(float)</td>
 * <td>Cotangent of float radians</td>
 * </tr>
 * <tr>
 * <td>DEGREES(number)</td>
 * <td>Degrees in number radians</td>
 * </tr>
 * <tr>
 * <td>EXP(float)</td>
 * <td>Exponential function of float</td>
 * </tr>
 * <tr>
 * <td>FLOOR(number)</td>
 * <td>Largest integer &lt;= number</td>
 * </tr>
 * <tr>
 * <td>LOG(float)</td>
 * <td>Base e logarithm of float</td>
 * </tr>
 * <tr>
 * <td>LOG10(float)</td>
 * <td>Base 10 logarithm of float</td>
 * </tr>
 * <tr>
 * <td>MOD(integer1, integer2)</td>
 * <td>Remainder for integer1 / integer2</td>
 * </tr>
 * <tr>
 * <td>PI()</td>
 * <td>The constant pi</td>
 * </tr>
 * <tr>
 * <td>POWER(number, power)</td>
 * <td>number raised to (integer) power</td>
 * </tr>
 * <tr>
 * <td>RADIANS(number)</td>
 * <td>Radians in number degrees</td>
 * </tr>
 * <tr>
 * <td>RAND(integer)</td>
 * <td>Random floating point for seed integer</td>
 * </tr>
 * <tr>
 * <td>ROUND(number, places)</td>
 * <td>number rounded to places places</td>
 * </tr>
 * <tr>
 * <td>SIGN(number)</td>
 * <td>-1 to indicate number is &lt; 0; 0 to indicate number is = 0; 1 to
 * indicate number is &gt; 0</td>
 * </tr>
 * <tr>
 * <td>SIN(float)</td>
 * <td>Sine of float radians</td>
 * </tr>
 * <tr>
 * <td>SQRT(float)</td>
 * <td>Square root of float</td>
 * </tr>
 * <tr>
 * <td>TAN(float)</td>
 * <td>Tangent of float radians</td>
 * </tr>
 * <tr>
 * <td>TRUNCATE(number, places)</td>
 * <td>number truncated to places places</td>
 * </tr>
 * <tr>
 * <td colspan="2"><br>
 *
 * <h2>STRING FUNCTIONS</h2>
 * </td>
 * </tr>
 * <tr>
 * <td>ASCII(string)</td>
 * <td>Integer representing the ASCII code value of the leftmost character in
 * string</td>
 * </tr>
 * <tr>
 * <td>CHAR(code)</td>
 * <td>Character with ASCII code value code, where code is between 0 and
 * 255</td>
 * </tr>
 * <tr>
 * <td>CONCAT(string1, string2)</td>
 * <td>Character string formed by appending string2 to string1; if a string is
 * null, the result is DBMS-dependent</td>
 * </tr>
 * <tr>
 * <td>DIFFERENCE(string1, string2)</td>
 * <td>Integer indicating the difference between the values returned by the
 * function SOUNDEX for string1 and string2</td>
 * </tr>
 * <tr>
 * <td>INSERT(string1, start, length, string2)</td>
 * <td>A character string formed by deleting length characters from string1
 * beginning at start, and inserting string2 into string1 at start</td>
 * </tr>
 * <tr>
 * <td>LCASE(string)</td>
 * <td>Converts all uppercase characters in string to lowercase</td>
 * </tr>
 * <tr>
 * <td>LEFT(string, count)</td>
 * <td>The count leftmost characters from string</td>
 * </tr>
 * <tr>
 * <td>LENGTH(string)</td>
 * <td>Number of characters in string, excluding trailing blanks</td>
 * </tr>
 * <tr>
 * <td>LOCATE(string1, string2[, start])</td>
 * <td>Position in string2 of the first occurrence of string1, searching from
 * the beginning of string2; if start is specified, the search begins from
 * position start. 0 is returned if string2 does not contain string1. Position 1
 * is the first character in string2.</td>
 * </tr>
 * <tr>
 * <td>LTRIM(string)</td>
 * <td>Characters of string with leading blank spaces removed</td>
 * </tr>
 * <tr>
 * <td>REPEAT(string, count)</td>
 * <td>A character string formed by repeating string count times</td>
 * </tr>
 * <tr>
 * <td>REPLACE(string1, string2, string3)</td>
 * <td>Replaces all occurrences of string2 in string1 with string3</td>
 * </tr>
 * <tr>
 * <td>RIGHT(string, count)</td>
 * <td>The count rightmost characters in string</td>
 * </tr>
 * <tr>
 * <td>RTRIM(string)</td>
 * <td>The characters of string with no trailing blanks</td>
 * </tr>
 * <tr>
 * <td>REVERSE(string)</td>
 * <td>The string with the order of the characters reversed</td>
 * </tr>
 * <tr>
 * <td>SOUNDEX(string)</td>
 * <td>A character string, which is data source-dependent, representing the
 * sound of the words in string; this could be a four-digit SOUNDEX code, a
 * phonetic representation of each word, etc.</td>
 * </tr>
 * <tr>
 * <td>SPACE(count)</td>
 * <td>A character string consisting of count spaces</td>
 * </tr>
 * <tr>
 * <td>SUBSTRING(string, start, length)</td>
 * <td>A character string formed by extracting length characters from string
 * beginning at start</td>
 * </tr>
 * <tr>
 * <td>UCASE(string)</td>
 * <td>Converts all lowercase characters in string to uppercase</td>
 * </tr>
 * <tr>
 * <td colspan="2"><br>
 *
 * <h2>TIME and DATE FUNCTIONS</h2>
 * </td>
 * </tr>
 * <tr>
 * <td>CURDATE()</td>
 * <td>The current date as a date value</td>
 * </tr>
 * <tr>
 * <td>CURTIME()</td>
 * <td>The current local time as a time value</td>
 * </tr>
 * <tr>
 * <td>DAYNAME(date)</td>
 * <td>A character string representing the day component of date; the name for
 * the day is specific to the data source</td>
 * </tr>
 * <tr>
 * <td>DAYOFMONTH(date)</td>
 * <td>An integer from 1 to 31 representing the day of the month in date</td>
 * </tr>
 * <tr>
 * <td>DAYOFWEEK(date)</td>
 * <td>An integer from 1 to 7 representing the day of the week in date; 1
 * represents Sunday</td>
 * </tr>
 * <tr>
 * <td>DAYOFYEAR(date)</td>
 * <td>An integer from 1 to 366 representing the day of the year in date</td>
 * </tr>
 * <tr>
 * <td>HOUR(time)</td>
 * <td>An integer from 0 to 23 representing the hour component of time</td>
 * </tr>
 * <tr>
 * <td>MINUTE(time)</td>
 * <td>An integer from 0 to 59 representing the minute component of time</td>
 * </tr>
 * <tr>
 * <td>MONTH(date)</td>
 * <td>An integer from 1 to 12 representing the month component of date</td>
 * </tr>
 * <tr>
 * <td>MONTHNAME(date)</td>
 * <td>A character string representing the month component of date; the name for
 * the month is specific to the data source</td>
 * </tr>
 * <tr>
 * <td>NOW()</td>
 * <td>A timestamp value representing the current date and time</td>
 * </tr>
 * <tr>
 * <td>QUARTER(date)</td>
 * <td>An integer from 1 to 4 representing the quarter in date; 1 represents
 * January 1 through March 31</td>
 * </tr>
 * <tr>
 * <td>SECOND(time)</td>
 * <td>An integer from 0 to 59 representing the second component of time</td>
 * </tr>
 * <tr>
 * <td>TIMESTAMPADD(interval,count, timestamp)</td>
 * <td>A timestamp calculated by adding count number of interval(s) to
 * timestamp; interval may be one of the following: SQL_TSI_FRAC_SECOND,
 * SQL_TSI_SECOND, SQL_TSI_MINUTE, SQL_TSI_HOUR, SQL_TSI_DAY, SQL_TSI_WEEK,
 * SQL_TSI_MONTH, SQL_TSI_QUARTER, or SQL_TSI_YEAR</td>
 * </tr>
 * <tr>
 * <td>TIMESTAMPDIFF(interval,timestamp1, timestamp2)</td>
 * <td>An integer representing the number of interval(s) by which timestamp2 is
 * greater than timestamp1; interval may be one of the following:
 * SQL_TSI_FRAC_SECOND, SQL_TSI_SECOND, SQL_TSI_MINUTE, SQL_TSI_HOUR,
 * SQL_TSI_DAY, SQL_TSI_WEEK, SQL_TSI_MONTH, SQL_TSI_QUARTER, or
 * SQL_TSI_YEAR</td>
 * </tr>
 * <tr>
 * <td>WEEK(date)</td>
 * <td>An integer from 1 to 53 representing the week of the year in date</td>
 * </tr>
 * <tr>
 * <td>YEAR(date)</td>
 * <td>An integer representing the year component of date</td>
 * </tr>
 * <tr>
 * <td colspan="2"><br>
 *
 * <h2>SYSTEM FUNCTIONS</h2>
 * </td>
 * </tr>
 * <tr>
 * <td>DATABASE()</td>
 * <td>Name of the database</td>
 * </tr>
 * <tr>
 * <td>IFNULL(expression, value)</td>
 * <td>value if expression is null; expression if expression is not null</td>
 * </tr>
 * <tr>
 * <td>USER()</td>
 * <td>User name in the DBMS
 *
 * <tr>
 * <td colspan="2"><br>
 *
 * <h2>CONVERSION FUNCTIONS</h2>
 * </td>
 * </tr>
 * <tr>
 * <td>CONVERT(value, SQLtype)</td>
 * <td>value converted to SQLtype where SQLtype may be one of the following SQL
 * types: BIGINT, BINARY, BIT, CHAR, DATE, DECIMAL, DOUBLE, FLOAT, INTEGER,
 * LONGVARBINARY, LONGVARCHAR, REAL, SMALLINT, TIME, TIMESTAMP, TINYINT,
 * VARBINARY, or VARCHAR</td>
 * </tr>
 * </table>
 */
public class SqlJdbcFunctionCall extends SqlFunction {
  //~ Static fields/initializers ---------------------------------------------

  /** List of all numeric function names defined by JDBC. */
  private static final String NUMERIC_FUNCTIONS = constructFuncList(
      "ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CBRT", "CEILING", "COS", "COT",
      "DEGREES", "EXP", "FLOOR", "LOG", "LOG10", "MOD", "PI",
      "POWER", "RADIANS", "RAND", "ROUND", "SIGN", "SIN", "SQRT",
      "TAN", "TRUNCATE");

  /** List of all string function names defined by JDBC. */
  private static final String STRING_FUNCTIONS = constructFuncList(
      "ASCII", "CHAR", "CONCAT", "DIFFERENCE", "INSERT", "LCASE",
      "LEFT", "LENGTH", "LOCATE", "LTRIM", "REPEAT", "REPLACE",
      "RIGHT", "RTRIM", "SOUNDEX", "SPACE", "SUBSTRING", "UCASE");
      // "ASCII", "CHAR", "DIFFERENCE", "LOWER",
      // "LEFT", "TRIM", "REPEAT", "REPLACE",
      // "RIGHT", "SPACE", "SUBSTRING", "UPPER", "INITCAP", "OVERLAY"

  /** List of all time/date function names defined by JDBC. */
  private static final String TIME_DATE_FUNCTIONS = constructFuncList(
      "CONVERT_TIMEZONE", "CURDATE", "CURTIME", "DAYNAME", "DAYOFMONTH", "DAYOFWEEK",
      "DAYOFYEAR", "HOUR", "MINUTE", "MONTH", "MONTHNAME", "NOW",
      "QUARTER", "SECOND", "TIMESTAMPADD", "TIMESTAMPDIFF", "TO_DATE", "TO_TIMESTAMP",
      "WEEK", "YEAR");

  /** List of all system function names defined by JDBC. */
  private static final String SYSTEM_FUNCTIONS = constructFuncList(
      "CONVERT", "DATABASE", "IFNULL", "USER");

  //~ Instance fields --------------------------------------------------------

  private final String jdbcName;
  private final @Nullable MakeCall lookupMakeCallObj;
  private @Nullable SqlCall lookupCall;

  private @Nullable SqlNode @Nullable [] thisOperands;

  //~ Constructors -----------------------------------------------------------

  public SqlJdbcFunctionCall(String name) {
    super(
        "{fn " + name + "}",
        SqlKind.JDBC_FN,
        null,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM);
    jdbcName = name;
    lookupMakeCallObj = JdbcToInternalLookupTable.INSTANCE.lookup(name);
    lookupCall = null;
  }

  //~ Methods ----------------------------------------------------------------

  private static String constructFuncList(String... functionNames) {
    StringBuilder sb = new StringBuilder();
    int n = 0;
    for (String funcName : functionNames) {
      if (JdbcToInternalLookupTable.INSTANCE.lookup(funcName) == null) {
        continue;
      }
      if (n++ > 0) {
        sb.append(",");
      }
      sb.append(funcName);
    }
    return sb.toString();
  }

  @Override public SqlCall createCall(
      @Nullable SqlLiteral functionQualifier,
      SqlParserPos pos,
      @Nullable SqlNode... operands) {
    thisOperands = operands;
    return super.createCall(functionQualifier, pos, operands);
  }

  @Override public SqlNode rewriteCall(SqlValidator validator,
      SqlCall call) {
    if (null == lookupMakeCallObj) {
      throw validator.newValidationError(call,
          RESOURCE.functionUndefined(getName()));
    }
    return lookupMakeCallObj.getOperator().rewriteCall(validator, call);
  }

  public SqlCall getLookupCall() {
    if (null == lookupCall) {
      lookupCall =
          requireNonNull(lookupMakeCallObj, "lookupMakeCallObj")
              .createCall(SqlParserPos.ZERO, requireNonNull(thisOperands, "thisOperands"));
    }
    return lookupCall;
  }

  @Override public String getAllowedSignatures(String name) {
    return requireNonNull(lookupMakeCallObj, "lookupMakeCallObj")
        .getOperator().getAllowedSignatures(name);
  }

  @Override public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    // Override SqlFunction.deriveType, because function-resolution is
    // not relevant to a JDBC function call.
    // REVIEW: jhyde, 2006/4/18: Should SqlJdbcFunctionCall even be a
    // subclass of SqlFunction?

    for (SqlNode operand : call.getOperandList()) {
      RelDataType nodeType = validator.deriveType(scope, operand);
      validator.setValidatedNodeType(operand, nodeType);
    }
    return validateOperands(validator, scope, call);
  }

  @Override public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    // only expected to come here if validator called this method
    SqlCallBinding callBinding = (SqlCallBinding) opBinding;

    if (null == lookupMakeCallObj) {
      throw callBinding.newValidationError(
          RESOURCE.functionUndefined(getName()));
    }

    final String message = lookupMakeCallObj.isValidArgCount(callBinding);
    if (message != null) {
      throw callBinding.newValidationError(
          RESOURCE.wrongNumberOfParam(getName(),
              requireNonNull(thisOperands, "thisOperands").length,
              message));
    }

    final SqlCall newCall = getLookupCall();
    final SqlCallBinding newBinding =
        new SqlCallBinding(callBinding.getValidator(), callBinding.getScope(),
            newCall);

    final SqlOperator operator = lookupMakeCallObj.getOperator();
    if (!operator.checkOperandTypes(newBinding, false)) {
      throw callBinding.newValidationSignatureError();
    }

    return operator.validateOperands(callBinding.getValidator(),
        callBinding.getScope(), newCall);
  }

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    writer.print("{fn ");
    writer.print(jdbcName);
    final SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlNode operand : call.getOperandList()) {
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
    writer.print("}");
  }

  /**
   * As {@link java.sql.DatabaseMetaData#getNumericFunctions}.
   */
  public static String getNumericFunctions() {
    return NUMERIC_FUNCTIONS;
  }

  /**
   * As {@link java.sql.DatabaseMetaData#getStringFunctions}.
   */
  public static String getStringFunctions() {
    return STRING_FUNCTIONS;
  }

  /**
   * As {@link java.sql.DatabaseMetaData#getTimeDateFunctions}.
   */
  public static String getTimeDateFunctions() {
    return TIME_DATE_FUNCTIONS;
  }

  /**
   * As {@link java.sql.DatabaseMetaData#getSystemFunctions}.
   */
  public static String getSystemFunctions() {
    return SYSTEM_FUNCTIONS;
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Converts a call to a JDBC function to a call to a regular function. */
  private interface MakeCall {
    /**
     * Creates and return a {@link SqlCall}. If the MakeCall strategy object
     * was created with a reordering specified the call will be created with
     * the operands reordered, otherwise no change of ordering is applied
     *
     * @param operands Operands
     */
    SqlCall createCall(SqlParserPos pos, @Nullable SqlNode... operands);

    SqlOperator getOperator();

    @Nullable String isValidArgCount(SqlCallBinding binding);
  }

  /** Converter that calls a built-in function with the same arguments. */
  public static class SimpleMakeCall implements SqlJdbcFunctionCall.MakeCall {
    final SqlOperator operator;

    public SimpleMakeCall(SqlOperator operator) {
      this.operator = operator;
    }

    @Override public SqlOperator getOperator() {
      return operator;
    }

    @Override public SqlCall createCall(SqlParserPos pos, @Nullable SqlNode... operands) {
      return operator.createCall(pos, operands);
    }

    @Override public @Nullable String isValidArgCount(SqlCallBinding binding) {
      return null; // any number of arguments is valid
    }
  }

  /** Implementation of {@link MakeCall} that can re-order or ignore operands. */
  private static class PermutingMakeCall extends SimpleMakeCall {
    final int[] order;

    /**
     * Creates a MakeCall strategy object with reordering of operands.
     *
     * <p>The reordering is specified by an int array where the value of
     * element at position <code>i</code> indicates to which element in a
     * new SqlNode[] array the operand goes.
     *
     * @param operator Operator
     * @param order Order
     */
    PermutingMakeCall(SqlOperator operator, int[] order) {
      super(operator);
      this.order = requireNonNull(order, "order");
    }

    @Override public SqlCall createCall(SqlParserPos pos,
        @Nullable SqlNode... operands) {
      return super.createCall(pos, reorder(operands));
    }

    @Override public @Nullable String isValidArgCount(SqlCallBinding binding) {
      if (order.length == binding.getOperandCount()) {
        return null; // operand count is valid
      } else {
        return getArgCountMismatchMsg(order.length);
      }
    }

    private static String getArgCountMismatchMsg(int... possible) {
      StringBuilder ret = new StringBuilder();
      for (int i = 0; i < possible.length; i++) {
        if (i > 0) {
          ret.append(" or ");
        }
        ret.append(possible[i]);
      }
      ret.append(" parameter(s)");
      return ret.toString();
    }

    /**
     * Uses the data in {@link #order} to reorder a SqlNode[] array.
     *
     * @param operands Operands
     */
    protected @Nullable SqlNode[] reorder(@Nullable SqlNode[] operands) {
      assert operands.length == order.length;
      @Nullable SqlNode[] newOrder = new SqlNode[operands.length];
      for (int i = 0; i < operands.length; i++) {
        assert operands[i] != null;
        int joyDivision = order[i];
        assert newOrder[joyDivision] == null : "mapping is not 1:1";
        newOrder[joyDivision] = operands[i];
      }
      return newOrder;
    }
  }

  /**
   * Lookup table between JDBC functions and internal representation.
   */
  private static class JdbcToInternalLookupTable {
    /**
     * The {@link org.apache.calcite.util.Glossary#SINGLETON_PATTERN singleton}
     * instance.
     */
    static final JdbcToInternalLookupTable INSTANCE =
        new JdbcToInternalLookupTable();

    private final Map<String, MakeCall> map;

    @SuppressWarnings("method.invocation.invalid")
    private JdbcToInternalLookupTable() {
      // A table of all functions can be found at
      // http://java.sun.com/products/jdbc/driverdevs.html
      // which is also provided in the javadoc for this class.
      // See also SqlOperatorTests.testJdbcFn, which contains the list.
      ImmutableMap.Builder<String, MakeCall> map = ImmutableMap.builder();
      map.put("ABS", simple(SqlStdOperatorTable.ABS));
      map.put("ACOS", simple(SqlStdOperatorTable.ACOS));
      map.put("ASIN", simple(SqlStdOperatorTable.ASIN));
      map.put("ATAN", simple(SqlStdOperatorTable.ATAN));
      map.put("ATAN2", simple(SqlStdOperatorTable.ATAN2));
      map.put("CBRT", simple(SqlStdOperatorTable.CBRT));
      map.put("CEILING", simple(SqlStdOperatorTable.CEIL));
      map.put("COS", simple(SqlStdOperatorTable.COS));
      map.put("COT", simple(SqlStdOperatorTable.COT));
      map.put("DEGREES", simple(SqlStdOperatorTable.DEGREES));
      map.put("EXP", simple(SqlStdOperatorTable.EXP));
      map.put("FLOOR", simple(SqlStdOperatorTable.FLOOR));
      map.put("LOG", simple(SqlStdOperatorTable.LN));
      map.put("LOG10", simple(SqlStdOperatorTable.LOG10));
      map.put("MOD", simple(SqlStdOperatorTable.MOD));
      map.put("PI", simple(SqlStdOperatorTable.PI));
      map.put("POWER", simple(SqlStdOperatorTable.POWER));
      map.put("RADIANS", simple(SqlStdOperatorTable.RADIANS));
      map.put("RAND", simple(SqlStdOperatorTable.RAND));
      map.put("ROUND", simple(SqlStdOperatorTable.ROUND));
      map.put("SIGN", simple(SqlStdOperatorTable.SIGN));
      map.put("SIN", simple(SqlStdOperatorTable.SIN));
      map.put("SQRT", simple(SqlStdOperatorTable.SQRT));
      map.put("TAN", simple(SqlStdOperatorTable.TAN));
      map.put("TRUNCATE", simple(SqlStdOperatorTable.TRUNCATE));

      map.put("ASCII", simple(SqlStdOperatorTable.ASCII));
      map.put("CHAR", simple(SqlLibraryOperators.CHAR));
      map.put("CONCAT", simple(SqlStdOperatorTable.CONCAT));
      map.put("DIFFERENCE", simple(SqlLibraryOperators.DIFFERENCE));
      map.put("INSERT",
          new PermutingMakeCall(SqlStdOperatorTable.OVERLAY, new int[]{0, 2, 3, 1}));
      map.put("LCASE", simple(SqlStdOperatorTable.LOWER));
      map.put("LENGTH", simple(SqlStdOperatorTable.CHARACTER_LENGTH));
      map.put("LOCATE", simple(SqlStdOperatorTable.POSITION));
      map.put("LEFT", simple(SqlLibraryOperators.LEFT));
      map.put("LTRIM", trim(SqlTrimFunction.Flag.LEADING));
      map.put("REPEAT", simple(SqlLibraryOperators.REPEAT));
      map.put("REPLACE", simple(SqlStdOperatorTable.REPLACE));
      map.put("REVERSE", simple(SqlLibraryOperators.REVERSE));
      map.put("RIGHT", simple(SqlLibraryOperators.RIGHT));
      map.put("RTRIM", trim(SqlTrimFunction.Flag.TRAILING));
      map.put("SOUNDEX", simple(SqlLibraryOperators.SOUNDEX));
      map.put("SPACE", simple(SqlLibraryOperators.SPACE));
      map.put("SUBSTRING", simple(SqlStdOperatorTable.SUBSTRING));
      map.put("UCASE", simple(SqlStdOperatorTable.UPPER));

      map.put("YEAR", simple(SqlStdOperatorTable.YEAR));
      map.put("QUARTER", simple(SqlStdOperatorTable.QUARTER));
      map.put("MONTH", simple(SqlStdOperatorTable.MONTH));
      map.put("WEEK", simple(SqlStdOperatorTable.WEEK));
      map.put("DAYOFYEAR", simple(SqlStdOperatorTable.DAYOFYEAR));
      map.put("DAYOFMONTH", simple(SqlStdOperatorTable.DAYOFMONTH));
      map.put("DAYOFWEEK", simple(SqlStdOperatorTable.DAYOFWEEK));
      map.put("DAYNAME", simple(SqlLibraryOperators.DAYNAME));
      map.put("HOUR", simple(SqlStdOperatorTable.HOUR));
      map.put("MINUTE", simple(SqlStdOperatorTable.MINUTE));
      map.put("MONTHNAME", simple(SqlLibraryOperators.MONTHNAME));
      map.put("SECOND", simple(SqlStdOperatorTable.SECOND));

      map.put("CONVERT_TIMEZONE", simple(SqlLibraryOperators.CONVERT_TIMEZONE));
      map.put("CURDATE", simple(SqlStdOperatorTable.CURRENT_DATE));
      map.put("CURTIME", simple(SqlStdOperatorTable.LOCALTIME));
      map.put("NOW", simple(SqlStdOperatorTable.CURRENT_TIMESTAMP));
      map.put("TIMESTAMPADD", simple(SqlStdOperatorTable.TIMESTAMP_ADD));
      map.put("TIMESTAMPDIFF", simple(SqlStdOperatorTable.TIMESTAMP_DIFF));
      map.put("TO_DATE", simple(SqlLibraryOperators.TO_DATE));
      map.put("TO_TIMESTAMP", simple(SqlLibraryOperators.TO_TIMESTAMP));

      map.put("DATABASE", simple(SqlStdOperatorTable.CURRENT_CATALOG));
      map.put("IFNULL",
          new SimpleMakeCall(SqlStdOperatorTable.COALESCE) {
            @Override public SqlCall createCall(SqlParserPos pos,
                @Nullable SqlNode... operands) {
              assert 2 == operands.length;
              return super.createCall(pos, operands);
            }
          });
      map.put("USER", simple(SqlStdOperatorTable.CURRENT_USER));
      map.put("CONVERT",
          new SimpleMakeCall(SqlStdOperatorTable.CAST) {
            @Override public SqlCall createCall(SqlParserPos pos,
                @Nullable SqlNode... operands) {
              assert 2 == operands.length;
              SqlNode typeOperand = operands[1];
              assert typeOperand != null && typeOperand.getKind() == SqlKind.LITERAL
                  : "literal expected, got " + typeOperand;

              SqlJdbcDataTypeName jdbcType = ((SqlLiteral) typeOperand)
                  .getValueAs(SqlJdbcDataTypeName.class);

              return super.createCall(pos, operands[0], jdbcType.createDataType(typeOperand.pos));
            }
          });
      this.map = map.build();
    }

    private static MakeCall trim(SqlTrimFunction.Flag flag) {
      return new SimpleMakeCall(SqlStdOperatorTable.TRIM) {
        @Override public SqlCall createCall(SqlParserPos pos,
            @Nullable SqlNode... operands) {
          assert 1 == operands.length;
          return super.createCall(pos, flag.symbol(pos),
              SqlLiteral.createCharString(" ", SqlParserPos.ZERO),
              operands[0]);
        }
      };
    }

    private static MakeCall simple(SqlOperator operator) {
      return new SimpleMakeCall(operator);
    }

    /**
     * Tries to lookup a given function name JDBC to an internal
     * representation. Returns null if no function defined.
     */
    public @Nullable MakeCall lookup(String name) {
      return map.get(name);
    }
  }
}
