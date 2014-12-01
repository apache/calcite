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
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.test.SqlTester.ResultChecker;
import static org.apache.calcite.sql.test.SqlTester.TypeChecker;

import static org.hamcrest.CoreMatchers.equalTo;
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
  public static final TypeChecker ANY_TYPE_CHECKER =
      new TypeChecker() {
        public void checkType(RelDataType type) {
        }
      };

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
      String actual = "VARCHAR(" + sqlType.getPrecision() + ")";
      return sqlType.isNullable() ? actual : (actual + " NOT NULL");
    case CHAR:
      actual = "CHAR(" + sqlType.getPrecision() + ")";
      return sqlType.isNullable() ? actual : (actual + " NOT NULL");
    default:

      // Get rid of the verbose charset/collation stuff.
      // TODO: There's probably a better way to do this.
      final String s = sqlType.getFullTypeString();
      return s.replace(
          " CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"",
          "");
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
    Set<String> actualSet = new HashSet<String>();
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
        final long l = Long.parseLong(s0);
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
    case Types.FLOAT:
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
      fail(
          "Query returned " + actual + ", expected " + expected + ((delta == 0)
              ? ""
              : ("+/-" + delta))
      );
    }
  }

  /**
   * Compares the first column of a result set against a String-valued
   * reference set, taking order into account.
   *
   * @param resultSet Result set
   * @param refList   Expected results
   * @throws Exception .
   */
  public static void compareResultList(
      ResultSet resultSet,
      List<String> refList) throws Exception {
    List<String> actualSet = new ArrayList<String>();
    while (resultSet.next()) {
      String s = resultSet.getString(1);
      actualSet.add(s);
    }
    resultSet.close();
    assertEquals(refList, actualSet);
  }

  /**
   * Compares the columns of a result set against several String-valued
   * reference lists, taking order into account.
   *
   * @param resultSet Result set
   * @param refLists  vararg of List&lt;String&gt;. The first list is compared
   *                  to the first column, the second list to the second column
   *                  and so on
   */
  public static void compareResultLists(
      ResultSet resultSet,
      List<String>... refLists) throws Exception {
    int numExpectedColumns = refLists.length;

    assertTrue(numExpectedColumns > 0);

    assertTrue(
        resultSet.getMetaData().getColumnCount() >= numExpectedColumns);

    int numExpectedRows = -1;

    List<List<String>> actualLists = new ArrayList<List<String>>();
    for (int i = 0; i < numExpectedColumns; i++) {
      actualLists.add(new ArrayList<String>());

      if (i == 0) {
        numExpectedRows = refLists[i].size();
      } else {
        assertEquals(
            "num rows differ across ref lists",
            numExpectedRows,
            refLists[i].size());
      }
    }

    while (resultSet.next()) {
      for (int i = 0; i < numExpectedColumns; i++) {
        String s = resultSet.getString(i + 1);

        actualLists.get(i).add(s);
      }
    }
    resultSet.close();

    for (int i = 0; i < numExpectedColumns; i++) {
      assertEquals(
          "column mismatch in column " + (i + 1),
          refLists[i],
          actualLists.get(i));
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
      assertEquals(
          typeName.toString(),
          type.toString());
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
      assertEquals(expected, actual);
    }
  }

  public static ResultChecker createChecker(Object result, double delta) {
    if (result instanceof Pattern) {
      return new PatternResultChecker((Pattern) result);
    } else if (delta != 0) {
      assertTrue(result instanceof Number);
      return new ApproximateResultChecker((Number) result, delta);
    } else {
      Set<String> refSet = new HashSet<String>();
      if (result == null) {
        refSet.add(null);
      } else if (result instanceof Collection) {
        refSet.addAll((Collection<String>) result);
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
