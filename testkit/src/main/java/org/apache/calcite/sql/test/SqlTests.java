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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.test.SqlTester.ParameterChecker;
import static org.apache.calcite.sql.test.SqlTester.ResultChecker;
import static org.apache.calcite.sql.test.SqlTester.TypeChecker;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.fail;

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
  public static final TypeChecker ANY_TYPE_CHECKER = (sql, type) -> {
  };

  /**
   * Checker that allows any number or type of parameters.
   */
  public static final ParameterChecker ANY_PARAMETER_CHECKER = parameterRowType -> {
  };

  /**
   * Checker that allows any result.
   */
  public static final ResultChecker ANY_RESULT_CHECKER = (sql, result) -> {
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
   * (include precision/scale but no charset or collation).
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
        actual += RelDataTypeImpl.NON_NULLABLE_SUFFIX;
      }
      return actual;

    default:
      return sqlType.getFullTypeString();
    }
  }

  /** Returns a list of typical types. */
  public static List<RelDataType> getTypes(RelDataTypeFactory typeFactory) {
    final int maxPrecision =
        typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL);
    return ImmutableList.of(
        typeFactory.createSqlType(SqlTypeName.BOOLEAN),
        typeFactory.createSqlType(SqlTypeName.TINYINT),
        typeFactory.createSqlType(SqlTypeName.SMALLINT),
        typeFactory.createSqlType(SqlTypeName.INTEGER),
        typeFactory.createSqlType(SqlTypeName.BIGINT),
        typeFactory.createSqlType(SqlTypeName.DECIMAL),
        typeFactory.createSqlType(SqlTypeName.DECIMAL, 5),
        typeFactory.createSqlType(SqlTypeName.DECIMAL, 6, 2),
        typeFactory.createSqlType(SqlTypeName.DECIMAL, maxPrecision, 0),
        typeFactory.createSqlType(SqlTypeName.DECIMAL, maxPrecision, 5),

        // todo: test IntervalDayTime and IntervalYearMonth
        // todo: test Float, Real, Double

        typeFactory.createSqlType(SqlTypeName.CHAR, 5),
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 1),
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 20),
        typeFactory.createSqlType(SqlTypeName.BINARY, 3),
        typeFactory.createSqlType(SqlTypeName.VARBINARY, 4),
        typeFactory.createSqlType(SqlTypeName.DATE),
        typeFactory.createSqlType(SqlTypeName.TIME, 0),
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 0));
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
   * Checks whether an exception matches the expected pattern. If
   * <code>sap</code> contains an error location, checks this too.
   *
   * @param ex                 Exception thrown
   * @param expectedMsgPattern Expected pattern
   * @param sap                Query and (optional) position in query
   * @param stage              Query processing stage
   */
  public static void checkEx(@Nullable Throwable ex,
      @Nullable String expectedMsgPattern,
      StringAndPos sap,
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

    if (ex instanceof ExceptionInInitializerError) {
      ex = ((ExceptionInInitializerError) ex).getException();
    }

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

    // Search for an IllegalStateException somewhere in the stack.
    // These are thrown by the enumerable implementors when evaluating
    // an expression that produces an error.
    IllegalStateException ise = null;
    for (Throwable x = ex; x != null; x = x.getCause()) {
      if (x instanceof IllegalStateException) {
        ise = (IllegalStateException) x;
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
    } else if (ise != null) {
      Throwable[] suppressed = ise.getSuppressed();
      if (suppressed.length > 0) {
        actualException = suppressed[0];
        actualMessage = actualException.getMessage();
      }
    } else {
      actualMessage = ex.getMessage();
      if (ex instanceof NumberFormatException) {
        // The message from NumberFormatException is not very usable
        actualMessage = "Number has wrong format " + actualMessage;
      }
      if (actualMessage != null) {
        java.util.regex.Matcher matcher =
            LINE_COL_TWICE_PATTERN.matcher(actualMessage);
        if (matcher.matches()) {
          actualLine = Integer.parseInt(matcher.group(1));
          actualColumn = Integer.parseInt(matcher.group(2));
          actualEndLine = Integer.parseInt(matcher.group(3));
          actualEndColumn = Integer.parseInt(matcher.group(4));
          actualMessage = matcher.group(5);
        } else {
          matcher = LINE_COL_PATTERN.matcher(actualMessage);
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

    final String sqlWithCarets;
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

  /** Stage of query processing. */
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

    @Override public void checkType(Supplier<String> sql, RelDataType type) {
      assertThat(sql.get(), type, hasToString(typeName.toString()));
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

    @Override public void checkType(Supplier<String> sql, RelDataType type) {
      String actual = getTypeString(type);
      assertThat(sql.get(), actual, is(expected));
    }
  }

}
