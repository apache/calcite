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
package org.apache.calcite.sql.test;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.test.SqlTester.ParameterChecker;
import static org.apache.calcite.sql.test.SqlTester.ResultChecker;
import static org.apache.calcite.sql.test.SqlTester.TypeChecker;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Utility methods.
 */
public abstract class SqlTests {
  //~ Static fields/initializers ---------------------------------------------

  public static final TypeChecker INTEGER_TYPE_CHECKER =
      new SqlTypeChecker(SqlTypeName.INTEGER);

  public static final TypeChecker BOOLEAN_TYPE_CHECKER =
      new SqlTypeChecker(SqlTypeName.BOOLEAN);

  /**
   * Checker which allows any type.
   */
  public static final TypeChecker ANY_TYPE_CHECKER = type -> {
  };

  /**
   * Checker that allows any number or type of parameters.
   */
  public static final ParameterChecker ANY_PARAMETER_CHECKER = parameterRowType -> {
  };

  /**
   * Checker that allows any result.
   */
  public static final ResultChecker ANY_RESULT_CHECKER = result -> {
    while (true) {
      if (!result.next()) {
        break;
      }
    }
  };

  private static final Pattern LINE_COL_PATTERN =
      Pattern.compile("At line ([0-9]+), column ([0-9]+)");

  private static final Pattern LINE_COL_TWICE_PATTERN =
      Pattern.compile(
          "(?s)From line ([0-9]+), column ([0-9]+) to line ([0-9]+), column ([0-9]+): (.*)");

  /**
   * Helper function to get the string representation of a RelDataType
   * (include precision/scale but no charset or collation)
   *
   * @param sqlType Type
   * @return String representation of type
   */
  public static String getTypeString(RelDataType sqlType) {
    switch (sqlType.getSqlTypeName()) {
    case VARCHAR:
    case CHAR:
      String actual = sqlType.getSqlTypeName().name();
      if (sqlType.getPrecision() != RelDataType.PRECISION_NOT_SPECIFIED) {
        actual = actual + "(" + sqlType.getPrecision() + ")";
      }
      if (!sqlType.isNullable()) {
        actual += " NOT NULL";
      }
      return actual;

    default:
      return sqlType.getFullTypeString();
    }
  }

  public static String generateAggQuery(String expr, String[] inputValues) {
    StringBuilder buf = new StringBuilder();
    buf.append("SELECT ").append(expr).append(" FROM ");
    if (inputValues.length == 0) {
      buf.append("(VALUES 1) AS t(x) WHERE false");
    } else {
      buf.append("(");
      for (int i = 0; i < inputValues.length; i++) {
        if (i > 0) {
          buf.append(" UNION ALL ");
        }
        buf.append("SELECT ");
        String inputValue = inputValues[i];
        buf.append(inputValue).append(" AS x FROM (VALUES (1))");
      }
      buf.append(")");
    }
    return buf.toString();
  }

  public static String generateAggQueryWithMultipleArgs(String expr,
      String[][] inputValues) {
    int argCount = -1;
    for (String[] row : inputValues) {
      if (argCount == -1) {
        argCount = row.length;
      } else if (argCount != row.length) {
        throw new IllegalArgumentException("invalid test input: "
            + Arrays.toString(row));
      }
    }
    StringBuilder buf = new StringBuilder();
    buf.append("SELECT ").append(expr).append(" FROM ");
    if (inputValues.length == 0) {
      buf.append("(VALUES 1) AS t(x) WHERE false");
    } else {
      buf.append("(");
      for (int i = 0; i < inputValues.length; i++) {
        if (i > 0) {
          buf.append(" UNION ALL ");
        }
        buf.append("SELECT ");
        for (int j = 0; j < argCount; j++) {
          if (j != 0) {
            buf.append(", ");
          }
          String inputValue = inputValues[i][j];
          buf.append(inputValue).append(" AS x");
          if (j != 0) {
            buf.append(j + 1);
          }
        }
        buf.append(" FROM (VALUES (1))");
      }
      buf.append(")");
    }
    return buf.toString();
  }

  public static String generateWinAggQuery(
      String expr,
      String windowSpec,
      String[] inputValues) {
    StringBuilder buf = new StringBuilder();
    buf.append("SELECT ").append(expr).append(" OVER (").append(windowSpec)
        .append(") FROM (");
    for (int i = 0; i < inputValues.length; i++) {
      if (i > 0) {
        buf.append(" UNION ALL ");
      }
      buf.append("SELECT ");
      String inputValue = inputValues[i];
      buf.append(inputValue).append(" AS x FROM (VALUES (1))");
    }
    buf.append(")");
    return buf.toString();
  }

  /**
   * Compares the first column of a result set against a String-valued
   * reference set, disregarding order entirely.
   *
   * @param resultSet Result set
   * @param refSet    Expected results
   * @throws Exception .
   */
  public static void compareResultSet(
      ResultSet resultSet,
      Set<String> refSet) throws Exception {
    Set<String> actualSet = new HashSet<>();
    final int columnType = resultSet.getMetaData().getColumnType(1);
    final ColumnMetaData.Rep rep = rep(columnType);
    while (resultSet.next()) {
      final String s = resultSet.getString(1);
      final String s0 = s == null ? "0" : s;
      final boolean wasNull0 = resultSet.wasNull();
      actualSet.add(s);
      switch (rep) {
      case BOOLEAN:
        assertThat(resultSet.getBoolean(1), equalTo(Boolean.valueOf(s)));
        break;
      case BYTE:
      case SHORT:
      case INTEGER:
      case LONG:
        long l;
        try {
          l = Long.parseLong(s0);
        } catch (NumberFormatException e) {
          // Large integers come out in scientific format, say "5E+06"
          l = (long) Double.parseDouble(s0);
        }
        assertThat(resultSet.getByte(1), equalTo((byte) l));
        assertThat(resultSet.getShort(1), equalTo((short) l));
        assertThat(resultSet.getInt(1), equalTo((int) l));
        assertThat(resultSet.getLong(1), equalTo(l));
        break;
      case FLOAT:
      case DOUBLE:
        final double d = Double.parseDouble(s0);
        assertThat(resultSet.getFloat(1), equalTo((float) d));
        assertThat(resultSet.getDouble(1), equalTo(d));
        break;
      }
      final boolean wasNull1 = resultSet.wasNull();
      final Object object = resultSet.getObject(1);
      final boolean wasNull2 = resultSet.wasNull();
      assertThat(object == null, equalTo(wasNull0));
      assertThat(wasNull1, equalTo(wasNull0));
      assertThat(wasNull2, equalTo(wasNull0));
    }
    resultSet.close();
    assertEquals(refSet, actualSet);
  }

  private static ColumnMetaData.Rep rep(int columnType) {
    switch (columnType) {
    case Types.BOOLEAN:
      return ColumnMetaData.Rep.BOOLEAN;
    case Types.TINYINT:
      return ColumnMetaData.Rep.BYTE;
    case Types.SMALLINT:
      return ColumnMetaData.Rep.SHORT;
    case Types.INTEGER:
      return ColumnMetaData.Rep.INTEGER;
    case Types.BIGINT:
      return ColumnMetaData.Rep.LONG;
    case Types.REAL:
      return ColumnMetaData.Rep.FLOAT;
    case Types.DOUBLE:
      return ColumnMetaData.Rep.DOUBLE;
    case Types.TIME:
      return ColumnMetaData.Rep.JAVA_SQL_TIME;
    case Types.TIMESTAMP:
      return ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP;
    case Types.DATE:
      return ColumnMetaData.Rep.JAVA_SQL_DATE;
    default:
      return ColumnMetaData.Rep.OBJECT;
    }
  }

  /**
   * Compares the first column of a result set against a pattern. The result
   * set must return exactly one row.
   *
   * @param resultSet Result set
   * @param pattern   Expected pattern
   */
  public static void compareResultSetWithPattern(
      ResultSet resultSet,
      Pattern pattern) throws Exception {
    if (!resultSet.next()) {
      fail("Query returned 0 rows, expected 1");
    }
    String actual = resultSet.getString(1);
    if (resultSet.next()) {
      fail("Query returned 2 or more rows, expected 1");
    }
    if (!pattern.matcher(actual).matches()) {
      fail("Query returned '"
              + actual
              + "', expected '"
              + pattern.pattern()
              + "'");
    }
  }

  /**
   * Compares the first column of a result set against a numeric result,
   * within a given tolerance. The result set must return exactly one row.
   *
   * @param resultSet Result set
   * @param expected  Expected result
   * @param delta     Tolerance
   */
  public static void compareResultSetWithDelta(
      ResultSet resultSet,
      double expected,
      double delta) throws Exception {
    if (!resultSet.next()) {
      fail("Query returned 0 rows, expected 1");
    }
    double actual = resultSet.getDouble(1);
    if (resultSet.next()) {
      fail("Query returned 2 or more rows, expected 1");
    }
    if ((actual < (expected - delta)) || (actual > (expected + delta))) {
      fail("Query returned " + actual + ", expected " + expected
          + ((delta == 0) ? "" : ("+/-" + delta)));
    }
  }

  /**
   * Checks whether an exception matches the expected pattern. If
   * <code>sap</code> contains an error location, checks this too.
   *
   * @param ex                 Exception thrown
   * @param expectedMsgPattern Expected pattern
   * @param sap                Query and (optional) position in query
   * @param stage              Query processing stage
   */
  public static void checkEx(Throwable ex,
      String expectedMsgPattern,
      SqlParserUtil.StringAndPos sap,
      Stage stage) {
    if (null == ex) {
      if (expectedMsgPattern == null) {
        // No error expected, and no error happened.
        return;
      } else {
        throw new AssertionError("Expected query to throw exception, "
            + "but it did not; query [" + sap.sql
            + "]; expected [" + expectedMsgPattern + "]");
      }
    }
    Throwable actualException = ex;
    String actualMessage = actualException.getMessage();
    int actualLine = -1;
    int actualColumn = -1;
    int actualEndLine = 100;
    int actualEndColumn = 99;

    // Search for an CalciteContextException somewhere in the stack.
    CalciteContextException ece = null;
    for (Throwable x = ex; x != null; x = x.getCause()) {
      if (x instanceof CalciteContextException) {
        ece = (CalciteContextException) x;
        break;
      }
      if (x.getCause() == x) {
        break;
      }
    }

    // Search for a SqlParseException -- with its position set -- somewhere
    // in the stack.
    SqlParseException spe = null;
    for (Throwable x = ex; x != null; x = x.getCause()) {
      if ((x instanceof SqlParseException)
          && (((SqlParseException) x).getPos() != null)) {
        spe = (SqlParseException) x;
        break;
      }
      if (x.getCause() == x) {
        break;
      }
    }

    if (ece != null) {
      actualLine = ece.getPosLine();
      actualColumn = ece.getPosColumn();
      actualEndLine = ece.getEndPosLine();
      actualEndColumn = ece.getEndPosColumn();
      if (ece.getCause() != null) {
        actualException = ece.getCause();
        actualMessage = actualException.getMessage();
      }
    } else if (spe != null) {
      actualLine = spe.getPos().getLineNum();
      actualColumn = spe.getPos().getColumnNum();
      actualEndLine = spe.getPos().getEndLineNum();
      actualEndColumn = spe.getPos().getEndColumnNum();
      if (spe.getCause() != null) {
        actualException = spe.getCause();
        actualMessage = actualException.getMessage();
      }
    } else {
      final String message = ex.getMessage();
      if (message != null) {
        Matcher matcher = LINE_COL_TWICE_PATTERN.matcher(message);
        if (matcher.matches()) {
          actualLine = Integer.parseInt(matcher.group(1));
          actualColumn = Integer.parseInt(matcher.group(2));
          actualEndLine = Integer.parseInt(matcher.group(3));
          actualEndColumn = Integer.parseInt(matcher.group(4));
          actualMessage = matcher.group(5);
        } else {
          matcher = LINE_COL_PATTERN.matcher(message);
          if (matcher.matches()) {
            actualLine = Integer.parseInt(matcher.group(1));
            actualColumn = Integer.parseInt(matcher.group(2));
          } else {
            if (expectedMsgPattern != null
                && actualMessage.matches(expectedMsgPattern)) {
              return;
            }
          }
        }
      }
    }

    if (null == expectedMsgPattern) {
      actualException.printStackTrace();
      fail(stage.componentName + " threw unexpected exception"
          + "; query [" + sap.sql
          + "]; exception [" + actualMessage
          + "]; class [" + actualException.getClass()
          + "]; pos [line " + actualLine
          + " col " + actualColumn
          + " thru line " + actualLine
          + " col " + actualColumn + "]");
    }

    String sqlWithCarets;
    if (actualColumn <= 0
        || actualLine <= 0
        || actualEndColumn <= 0
        || actualEndLine <= 0) {
      if (sap.pos != null) {
        throw new AssertionError("Expected error to have position,"
            + " but actual error did not: "
            + " actual pos [line " + actualLine
            + " col " + actualColumn
            + " thru line " + actualEndLine + " col "
            + actualEndColumn + "]", actualException);
      }
      sqlWithCarets = sap.sql;
    } else {
      sqlWithCarets =
          SqlParserUtil.addCarets(
              sap.sql,
              actualLine,
              actualColumn,
              actualEndLine,
              actualEndColumn + 1);
      if (sap.pos == null) {
        throw new AssertionError("Actual error had a position, but expected "
            + "error did not. Add error position carets to sql:\n"
            + sqlWithCarets);
      }
    }

    if (actualMessage != null) {
      actualMessage = Util.toLinux(actualMessage);
    }

    if (actualMessage == null
        || !actualMessage.matches(expectedMsgPattern)) {
      actualException.printStackTrace();
      final String actualJavaRegexp =
          (actualMessage == null)
              ? "null"
              : TestUtil.quoteForJava(
              TestUtil.quotePattern(actualMessage));
      fail(stage.componentName + " threw different "
          + "exception than expected; query [" + sap.sql
          + "];\n"
          + " expected pattern [" + expectedMsgPattern
          + "];\n"
          + " actual [" + actualMessage
          + "];\n"
          + " actual as java regexp [" + actualJavaRegexp
          + "]; pos [" + actualLine
          + " col " + actualColumn
          + " thru line " + actualEndLine
          + " col " + actualEndColumn
          + "]; sql [" + sqlWithCarets + "]");
    } else if (sap.pos != null
        && (actualLine != sap.pos.getLineNum()
        || actualColumn != sap.pos.getColumnNum()
        || actualEndLine != sap.pos.getEndLineNum()
        || actualEndColumn != sap.pos.getEndColumnNum())) {
      fail(stage.componentName + " threw expected "
          + "exception [" + actualMessage
          + "];\nbut at pos [line " + actualLine
          + " col " + actualColumn
          + " thru line " + actualEndLine
          + " col " + actualEndColumn
          + "];\nsql [" + sqlWithCarets + "]");
    }
  }

  /** Stage of query processing */
  public enum Stage {
    PARSE("Parser"),
    VALIDATE("Validator"),
    RUNTIME("Executor");

    public final String componentName;

    Stage(String componentName) {
      this.componentName = componentName;
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Checks that a type matches a given SQL type. Does not care about
   * nullability.
   */
  private static class SqlTypeChecker implements TypeChecker {
    private final SqlTypeName typeName;

    SqlTypeChecker(SqlTypeName typeName) {
      this.typeName = typeName;
    }

    public void checkType(RelDataType type) {
      assertThat(type.toString(), is(typeName.toString()));
    }
  }

  /**
   * Type checker which compares types to a specified string.
   *
   * <p>The string contains "NOT NULL" constraints, but does not contain
   * collations and charsets. For example,
   *
   * <ul>
   * <li><code>INTEGER NOT NULL</code></li>
   * <li><code>BOOLEAN</code></li>
   * <li><code>DOUBLE NOT NULL MULTISET NOT NULL</code></li>
   * <li><code>CHAR(3) NOT NULL</code></li>
   * <li><code>RecordType(INTEGER X, VARCHAR(10) Y)</code></li>
   * </ul>
   */
  public static class StringTypeChecker implements TypeChecker {
    private final String expected;

    public StringTypeChecker(String expected) {
      this.expected = expected;
    }

    public void checkType(RelDataType type) {
      String actual = getTypeString(type);
      assertThat(actual, is(expected));
    }
  }

  public static ResultChecker createChecker(Object result, double delta) {
    if (result instanceof Pattern) {
      return new PatternResultChecker((Pattern) result);
    } else if (delta != 0) {
      assertTrue(result instanceof Number);
      return new ApproximateResultChecker((Number) result, delta);
    } else {
      Set<String> refSet = new HashSet<>();
      if (result == null) {
        refSet.add(null);
      } else if (result instanceof Collection) {
        //noinspection unchecked
        final Collection<String> collection = (Collection<String>) result;
        refSet.addAll(collection);
      } else {
        refSet.add(result.toString());
      }
      return new RefSetResultChecker(refSet);
    }
  }

  /**
   * Result checker that checks a result against a regular expression.
   */
  public static class PatternResultChecker implements ResultChecker {
    private final Pattern pattern;

    public PatternResultChecker(Pattern pattern) {
      this.pattern = pattern;
    }

    public void checkResult(ResultSet resultSet) throws Exception {
      compareResultSetWithPattern(resultSet, pattern);
    }
  }

  /**
   * Result checker that checks a result against an expected value. A delta
   * value is used for approximate values (double and float).
   */
  public static class ApproximateResultChecker implements ResultChecker {
    private final Number expected;
    private final double delta;

    public ApproximateResultChecker(Number expected, double delta) {
      this.expected = expected;
      this.delta = delta;
    }

    public void checkResult(ResultSet resultSet) throws Exception {
      compareResultSetWithDelta(
          resultSet,
          expected.doubleValue(),
          delta);
    }
  }

  /**
   * Result checker that checks a result against a list of expected strings.
   */
  public static class RefSetResultChecker implements ResultChecker {
    private final Set<String> expected;

    private RefSetResultChecker(Set<String> expected) {
      this.expected = expected;
    }

    public void checkResult(ResultSet resultSet) throws Exception {
      compareResultSet(resultSet, expected);
    }
  }
}

// End SqlTests.java
