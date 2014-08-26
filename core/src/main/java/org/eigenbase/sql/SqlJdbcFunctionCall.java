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
package org.eigenbase.sql;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;

import static org.eigenbase.util.Static.RESOURCE;

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
 * <h3>NUMERIC FUNCTIONS</h3>
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
 * <td>Rh3ainder for integer1 / integer2</td>
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
 * <h3>STRING FUNCTIONS</h3>
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
 * <td>Characters of string with leading blank spaces rh3oved</td>
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
 * <h3>TIME and DATE FUNCTIONS</h3>
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
 * <h3>SYSTEM FUNCTIONS</h3>
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
 * <h3>CONVERSION FUNCTIONS</h3>
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
      "ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CEILING", "COS", "COT",
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
      "CURDATE", "CURTIME", "DAYNAME", "DAYOFMONTH", "DAYOFWEEK",
      "DAYOFYEAR", "HOUR", "MINUTE", "MONTH", "MONTHNAME", "NOW",
      "QUARTER", "SECOND", "TIMESTAMPADD", "TIMESTAMPDIFF",
      "WEEK", "YEAR");

  /** List of all system function names defined by JDBC. */
  private static final String SYSTEM_FUNCTIONS = constructFuncList(
      "DATABASE", "IFNULL", "USER");

  //~ Instance fields --------------------------------------------------------

  private final String jdbcName;
  private final MakeCall lookupMakeCallObj;
  private SqlCall lookupCall;

  private SqlNode[] thisOperands;

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

  public SqlCall createCall(
      SqlLiteral functionQualifier,
      SqlParserPos pos,
      SqlNode... operands) {
    thisOperands = operands;
    return super.createCall(functionQualifier, pos, operands);
  }

  public SqlCall getLookupCall() {
    if (null == lookupCall) {
      lookupCall =
          lookupMakeCallObj.createCall(thisOperands, SqlParserPos.ZERO);
    }
    return lookupCall;
  }

  public String getAllowedSignatures(String name) {
    return lookupMakeCallObj.operator.getAllowedSignatures(name);
  }

  public RelDataType deriveType(
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

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    // only expected to come here if validator called this method
    SqlCallBinding callBinding = (SqlCallBinding) opBinding;

    if (null == lookupMakeCallObj) {
      throw callBinding.newValidationError(
          RESOURCE.functionUndefined(getName()));
    }

    if (!lookupMakeCallObj.checkNumberOfArg(opBinding.getOperandCount())) {
      throw callBinding.newValidationError(
          RESOURCE.wrongNumberOfParam(getName(), thisOperands.length,
              getArgCountMismatchMsg()));
    }

    if (!lookupMakeCallObj.operator.checkOperandTypes(
        new SqlCallBinding(
            callBinding.getValidator(),
            callBinding.getScope(),
            getLookupCall()),
        false)) {
      throw callBinding.newValidationSignatureError();
    }
    return lookupMakeCallObj.operator.validateOperands(
        callBinding.getValidator(),
        callBinding.getScope(),
        getLookupCall());
  }

  private String getArgCountMismatchMsg() {
    StringBuilder ret = new StringBuilder();
    int[] possible = lookupMakeCallObj.getPossibleArgCounts();
    for (int i = 0; i < possible.length; i++) {
      if (i > 0) {
        ret.append(" or ");
      }
      ret.append(possible[i]);
    }
    ret.append(" parameter(s)");
    return ret.toString();
  }

  public void unparse(
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
   * @see java.sql.DatabaseMetaData#getNumericFunctions
   */
  public static String getNumericFunctions() {
    return NUMERIC_FUNCTIONS;
  }

  /**
   * @see java.sql.DatabaseMetaData#getStringFunctions
   */
  public static String getStringFunctions() {
    return STRING_FUNCTIONS;
  }

  /**
   * @see java.sql.DatabaseMetaData#getTimeDateFunctions
   */
  public static String getTimeDateFunctions() {
    return TIME_DATE_FUNCTIONS;
  }

  /**
   * @see java.sql.DatabaseMetaData#getSystemFunctions
   */
  public static String getSystemFunctions() {
    return SYSTEM_FUNCTIONS;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Represent a Strategy Object to create a {@link SqlCall} by providing the
   * feature of reording, adding/dropping operands.
   */
  private static class MakeCall {
    final SqlOperator operator;
    final int[] order;

    /**
     * List of the possible numbers of operands this function can take.
     */
    final int[] argCounts;

    private MakeCall(
        SqlOperator operator,
        int argCount) {
      this.operator = operator;
      this.order = null;
      this.argCounts = new int[]{argCount};
    }

    /**
     * Creates a MakeCall strategy object with reordering of operands.
     *
     * <p>The reordering is specified by an int array where the value of
     * element at position <code>i</code> indicates to which element in a
     * new SqlNode[] array the operand goes.
     *
     * @param operator Operator
     * @param order    Order
     * @pre order != null
     * @pre order[i] < order.length
     * @pre order.length > 0
     * @pre argCounts == order.length
     */
    MakeCall(SqlOperator operator, int argCount, int[] order) {
      assert order != null && order.length > 0;

      // Currently operation overloading when reordering is necessary is
      // NOT implemented
      Util.pre(argCount == order.length, "argCounts==order.length");
      this.operator = operator;
      this.order = order;
      this.argCounts = new int[]{order.length};

      // sanity checking ...
      for (int anOrder : order) {
        assert anOrder < order.length;
      }
    }

    final int[] getPossibleArgCounts() {
      return this.argCounts;
    }

    /**
     * Uses the data in {@link #order} to reorder a SqlNode[] array.
     *
     * @param operands Operands
     */
    protected SqlNode[] reorder(SqlNode[] operands) {
      assert operands.length == order.length;
      SqlNode[] newOrder = new SqlNode[operands.length];
      for (int i = 0; i < operands.length; i++) {
        assert operands[i] != null;
        int joyDivision = order[i];
        assert newOrder[joyDivision] == null : "mapping is not 1:1";
        newOrder[joyDivision] = operands[i];
      }
      return newOrder;
    }

    /**
     * Creates and return a {@link SqlCall}. If the MakeCall strategy object
     * was created with a reording specified the call will be created with
     * the operands reordered, otherwise no change of ordering is applied
     *
     * @param operands Operands
     */
    SqlCall createCall(
        SqlNode[] operands,
        SqlParserPos pos) {
      if (null == order) {
        return operator.createCall(pos, operands);
      }
      return operator.createCall(pos, reorder(operands));
    }

    /**
     * Returns false if number of arguments are unexpected, otherwise true.
     * This function is supposed to be called with an {@link SqlNode} array
     * of operands direct from the oven, e.g no reording or adding/dropping
     * of operands...else it would make much sense to have this methods
     */
    boolean checkNumberOfArg(int length) {
      for (int argCount : argCounts) {
        if (argCount == length) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Lookup table between JDBC functions and internal representation
   */
  private static class JdbcToInternalLookupTable {
    /**
     * The {@link org.eigenbase.util.Glossary#SINGLETON_PATTERN singleton}
     * instance.
     */
    static final JdbcToInternalLookupTable INSTANCE =
        new JdbcToInternalLookupTable();
    private final Map<String, MakeCall> map = new HashMap<String, MakeCall>();

    private JdbcToInternalLookupTable() {
      // A table of all functions can be found at
      // http://java.sun.com/products/jdbc/driverdevs.html
      // which is also provided in the javadoc for this class.
      // See also SqlOperatorTests.testJdbcFn, which contains the list.
      map.put(
          "ABS",
          new MakeCall(SqlStdOperatorTable.ABS, 1));
      map.put(
          "EXP",
          new MakeCall(SqlStdOperatorTable.EXP, 1));
      map.put(
          "LOG",
          new MakeCall(SqlStdOperatorTable.LN, 1));
      map.put(
          "LOG10",
          new MakeCall(SqlStdOperatorTable.LOG10, 1));
      map.put(
          "MOD",
          new MakeCall(SqlStdOperatorTable.MOD, 2));
      map.put(
          "POWER",
          new MakeCall(SqlStdOperatorTable.POWER, 2));

      map.put(
          "CONCAT",
          new MakeCall(SqlStdOperatorTable.CONCAT, 2));
      map.put(
          "INSERT",
          new MakeCall(
              SqlStdOperatorTable.OVERLAY,
              4,
              new int[]{0, 2, 3, 1}));
      map.put(
          "LCASE",
          new MakeCall(SqlStdOperatorTable.LOWER, 1));
      map.put(
          "LENGTH",
          new MakeCall(SqlStdOperatorTable.CHARACTER_LENGTH, 1));
      map.put(
          "LOCATE",
          new MakeCall(SqlStdOperatorTable.POSITION, 2));
      map.put(
          "LTRIM",
          new MakeCall(SqlStdOperatorTable.TRIM, 1) {
            @Override
            SqlCall createCall(
                SqlNode[] operands, SqlParserPos pos) {
              assert 1 == operands.length;
              return super.createCall(
                  new SqlNode[]{
                    SqlTrimFunction.Flag.LEADING.symbol(SqlParserPos.ZERO),
                    SqlLiteral.createCharString(" ", null),
                    operands[0]
                  },
                  pos);
            }
          });
      map.put(
          "RTRIM",
          new MakeCall(SqlStdOperatorTable.TRIM, 1) {
            @Override
            SqlCall createCall(
                SqlNode[] operands, SqlParserPos pos) {
              assert 1 == operands.length;
              return super.createCall(
                  new SqlNode[] {
                    SqlTrimFunction.Flag.TRAILING.symbol(SqlParserPos.ZERO),
                    SqlLiteral.createCharString(" ", null),
                    operands[0]
                  },
                  pos);
            }
          });
      map.put(
          "SUBSTRING",
          new MakeCall(SqlStdOperatorTable.SUBSTRING, 3));
      map.put(
          "UCASE",
          new MakeCall(SqlStdOperatorTable.UPPER, 1));

      map.put(
          "CURDATE",
          new MakeCall(SqlStdOperatorTable.CURRENT_DATE, 0));
      map.put(
          "CURTIME",
          new MakeCall(SqlStdOperatorTable.LOCALTIME, 0));
      map.put(
          "NOW",
          new MakeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP, 0));
    }

    /**
     * Tries to lookup a given function name JDBC to an internal
     * representation. Returns null if no function defined.
     */
    public MakeCall lookup(String name) {
      return map.get(name);
    }
  }
}

// End SqlJdbcFunctionCall.java
