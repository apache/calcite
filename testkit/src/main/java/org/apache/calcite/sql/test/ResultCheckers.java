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
import org.apache.calcite.util.ImmutableNullableSet;
import org.apache.calcite.util.JdbcType;

import com.google.common.collect.ImmutableSet;

import org.hamcrest.Matcher;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.jupiter.api.Assertions.fail;

import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;

/** Utilities for {@link SqlTester.ResultChecker}. */
public class ResultCheckers {
  private ResultCheckers() {
  }

  public static SqlTester.ResultChecker isExactly(double value) {
    return new MatcherResultChecker<>(is(value),
        JdbcType.DOUBLE);
  }

  public static SqlTester.ResultChecker isExactly(String value) {
    return new MatcherResultChecker<>(is(new BigDecimal(value)),
        JdbcType.BIG_DECIMAL);
  }

  public static SqlTester.ResultChecker isExactDateTime(LocalDateTime dateTime) {
    return new MatcherResultChecker<>(
        is(BigDecimal.valueOf(dateTime.toInstant(ZoneOffset.UTC).toEpochMilli())),
        JdbcType.BIG_DECIMAL);
  }

  public static SqlTester.ResultChecker isExactTime(LocalTime time) {
    return new MatcherResultChecker<>(
        is((int) (time.toNanoOfDay() / 1000_000)),
        JdbcType.INTEGER);
  }

  public static SqlTester.ResultChecker isWithin(double value, double delta) {
    return new MatcherResultChecker<>(closeTo(value, delta), JdbcType.DOUBLE);
  }

  public static SqlTester.ResultChecker isSingle(double delta, String value) {
    assert delta == 0d; // if not zero, call a different method
    return isSingle(value);
  }

  public static SqlTester.ResultChecker isSingle(String value) {
    return new MatcherResultChecker<>(is(value),
        JdbcType.STRING_NULLABLE);
  }

  public static SqlTester.ResultChecker isSingle(boolean value) {
    return new MatcherResultChecker<>(is(value),
        JdbcType.BOOLEAN);
  }

  public static SqlTester.ResultChecker isSingle(int value) {
    return new MatcherResultChecker<>(is(value),
        JdbcType.INTEGER);
  }

  public static SqlTester.ResultChecker isDecimal(String value) {
    return new MatcherResultChecker<>(is(new BigDecimal(value)),
        JdbcType.BIG_DECIMAL);
  }

  public static SqlTester.ResultChecker isSet(String... values) {
    return new RefSetResultChecker(ImmutableSet.copyOf(values));
  }

  public static SqlTester.ResultChecker isNullValue() {
    return new RefSetResultChecker(Collections.singleton(null));
  }

  /**
   * Compares the first column of a result set against a String-valued
   * reference set, disregarding order entirely.
   *
   * @param sql       SQL to show in case of failure
   * @param resultSet Result set
   * @param refSet    Expected results
   * @throws Exception .
   */
  static void compareResultSet(String sql, ResultSet resultSet,
      Set<String> refSet) throws Exception {
    Set<String> actualSet = new HashSet<>();
    final int columnType = resultSet.getMetaData().getColumnType(1);
    final ColumnMetaData.Rep rep = rep(columnType);
    final String msg = "Query: " + sql;
    while (resultSet.next()) {
      final String s = resultSet.getString(1);
      final String s0 = s == null ? "0" : s;
      final boolean wasNull0 = resultSet.wasNull();
      actualSet.add(s);
      switch (rep) {
      case BOOLEAN:
      case PRIMITIVE_BOOLEAN:
        assertThat(msg, resultSet.getBoolean(1), equalTo(Boolean.valueOf(s)));
        break;
      case BYTE:
      case PRIMITIVE_BYTE:
      case SHORT:
      case PRIMITIVE_SHORT:
      case INTEGER:
      case PRIMITIVE_INT:
      case LONG:
      case PRIMITIVE_LONG:
        long l;
        try {
          l = parseLong(s0);
        } catch (NumberFormatException e) {
          // Large integers come out in scientific format, say "5E+06"
          l = (long) parseDouble(s0);
        }
        assertThat(msg, resultSet.getByte(1), equalTo((byte) l));
        assertThat(msg, resultSet.getShort(1), equalTo((short) l));
        assertThat(msg, resultSet.getInt(1), equalTo((int) l));
        assertThat(msg, resultSet.getLong(1), equalTo(l));
        break;
      case FLOAT:
      case PRIMITIVE_FLOAT:
      case DOUBLE:
      case PRIMITIVE_DOUBLE:
        final double d = parseDouble(s0);
        assertThat(msg, resultSet.getFloat(1), equalTo((float) d));
        assertThat(msg, resultSet.getDouble(1), equalTo(d));
        break;
      default:
        // fall through; no type-specific validation is necessary
      }
      final boolean wasNull1 = resultSet.wasNull();
      final Object object = resultSet.getObject(1);
      final boolean wasNull2 = resultSet.wasNull();
      assertThat(msg, object == null, equalTo(wasNull0));
      assertThat(msg, wasNull1, equalTo(wasNull0));
      assertThat(msg, wasNull2, equalTo(wasNull0));
    }
    resultSet.close();
    assertThat(msg, actualSet, is(refSet));
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
   * @param sql       SQL to show in case of failure
   * @param resultSet Result set
   * @param pattern   Expected pattern
   */
  static void compareResultSetWithPattern(String sql, ResultSet resultSet,
      Pattern pattern) throws Exception {
    if (!resultSet.next()) {
      fail("Query \"" + sql + "\"returned 0 rows, expected 1");
    }
    String actual = resultSet.getString(1);
    if (resultSet.next()) {
      fail("Query \"" + sql + "\"returned 2 or more rows, expected 1");
    }
    if (!pattern.matcher(actual).matches()) {
      fail("Query \"" + sql + "\"returned '"
              + actual
              + "', expected '"
              + pattern.pattern()
              + "'");
    }
  }

  /**
   * Compares the first column of a result set against a {@link Matcher}.
   * The result set must return exactly one row.
   *
   * @param sql       SQL to show in case of failure
   * @param resultSet Result set
   * @param matcher   Matcher
   *
   * @param <T> Value type
   */
  static <T> void compareResultSetWithMatcher(String sql, ResultSet resultSet,
      JdbcType<T> jdbcType, Matcher<T> matcher) throws Exception {
    if (!resultSet.next()) {
      fail("Query returned 0 rows, expected 1");
    }
    T actual = jdbcType.get(1, resultSet);
    if (resultSet.next()) {
      fail("Query returned 2 or more rows, expected 1");
    }
    assertThat("Query: " + sql, actual, matcher);
  }

  /** Creates a ResultChecker that accesses a column of a given type
   * and then uses a Hamcrest matcher to check the value. */
  public static <T> SqlTester.ResultChecker createChecker(Matcher<T> matcher,
      JdbcType<T> jdbcType) {
    return new MatcherResultChecker<>(matcher, jdbcType);
  }

  /** Creates a ResultChecker from an expected result.
   *
   * <p>The result may be a {@link SqlTester.ResultChecker},
   * a regular expression ({@link Pattern}),
   * a Hamcrest {@link Matcher},
   * a {@link Collection} of strings (representing the values of one column).
   *
   * <p>If none of the above, the value is converted to a string and compared
   * with the value of a single column, single row result set that is converted
   * to a string.
   */
  public static SqlTester.ResultChecker createChecker(Object result) {
    requireNonNull(result, "to check for a null result, use isNullValue()");
    if (result instanceof Pattern) {
      return new PatternResultChecker((Pattern) result);
    } else if (result instanceof SqlTester.ResultChecker) {
      return (SqlTester.ResultChecker) result;
    } else if (result instanceof Matcher) {
      //noinspection unchecked,rawtypes
      return createChecker((Matcher) result, JdbcType.DOUBLE);
    } else if (result instanceof Collection) {
      //noinspection unchecked
      final Collection<String> collection = (Collection<String>) result;
      return new RefSetResultChecker(ImmutableNullableSet.copyOf(collection));
    } else {
      return isSingle(result.toString());
    }
  }

  /**
   * Result checker that checks a result against a regular expression.
   */
  static class PatternResultChecker implements SqlTester.ResultChecker {
    final Pattern pattern;

    PatternResultChecker(Pattern pattern) {
      this.pattern = requireNonNull(pattern, "pattern");
    }

    @Override public void checkResult(String sql, ResultSet resultSet) throws Exception {
      compareResultSetWithPattern(sql, resultSet, pattern);
    }
  }

  /**
   * Result checker that checks a result using a {@link org.hamcrest.Matcher}.
   *
   * @param <T> Result type
   */
  static class MatcherResultChecker<T> implements SqlTester.ResultChecker {
    private final Matcher<T> matcher;
    private final JdbcType<T> jdbcType;

    MatcherResultChecker(Matcher<T> matcher, JdbcType<T> jdbcType) {
      this.matcher = requireNonNull(matcher, "matcher");
      this.jdbcType = requireNonNull(jdbcType, "jdbcType");
    }

    @Override public void checkResult(String sql, ResultSet resultSet) throws Exception {
      compareResultSetWithMatcher(sql, resultSet, jdbcType, matcher);
    }
  }

  /**
   * Result checker that checks a result against a list of expected strings.
   */
  static class RefSetResultChecker implements SqlTester.ResultChecker {
    private final Set<String> expected;

    RefSetResultChecker(Set<String> expected) {
      this.expected = ImmutableNullableSet.copyOf(expected);
    }

    @Override public void checkResult(String sql, ResultSet resultSet) throws Exception {
      compareResultSet(sql, resultSet, expected);
    }
  }
}
