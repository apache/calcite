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

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.base.Strings;

import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test of {@link SqlNode} and other SQL AST classes.
 */
class SqlNodeTest {
  @Test void testSqlNodeList() {
    SqlParserPos zero = SqlParserPos.ZERO;
    checkList(new SqlNodeList(zero));
    checkList(SqlNodeList.SINGLETON_STAR);
    checkList(SqlNodeList.SINGLETON_EMPTY);
    checkList(
        SqlNodeList.of(zero,
            Arrays.asList(SqlLiteral.createCharString("x", zero),
                new SqlIdentifier("y", zero))));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6161">[CALCITE-6161]
   * The equalsDeep of sqlCall should compare sqlOperator's sqlKind</a>.
   */
  @Test void testSameNameButDifferent() {
    final SqlParserPos zero = SqlParserPos.ZERO;
    final List<SqlNode> operandList =
        Arrays.asList(new SqlIdentifier("x", zero), new SqlIdentifier("y", zero));
    final SqlNode unaryOperatorNode =
        new SqlBasicCall(SqlStdOperatorTable.UNARY_MINUS, operandList, zero);
    final SqlNode binaryOperatorNode =
        new SqlBasicCall(SqlStdOperatorTable.MINUS, operandList, zero);
    assertThat(unaryOperatorNode.equalsDeep(binaryOperatorNode, Litmus.IGNORE), is(false));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4402">[CALCITE-4402]
   * SqlCall#equalsDeep does not take into account the function quantifier</a>.
   */
  @Test void testCountEqualsDeep() {
    assertThat("count(a)", isEqualsDeep("count(a)"));
    assertThat("count(distinct a)", isEqualsDeep("count(distinct a)"));
    assertThat("count(distinct a)", not(isEqualsDeep("count(a)")));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6100">[CALCITE-6100]
   * The equalsDeep of SqlRowTypeNameSpec should return true if two SqlRowTypeNameSpecs are
   * structurally equivalent</a>.
   */
  @Test void testRowEqualsDeep() {
    assertThat("CAST(a AS ROW(field INTEGER))",
        isEqualsDeep("CAST(a AS ROW(field INTEGER))"));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7731">[CALCITE-7731]
   * Bound plain-notation expansion of DECIMAL literals to prevent parse-time
   * OutOfMemoryError</a>. A {@link SqlNumericLiteral} whose value's plain-notation
   * expansion would exceed the configured bound must fail when unparsed, even if
   * the literal was not created via the SQL parser. */
  @Test void testNumericLiteralToValueExceedingPlainNotationBound() {
    // A literal with a 20,001-digit fractional expansion comfortably exceeds
    // the default 10,000 digit bound, without requiring a multi-gigabyte
    // allocation to build.
    final String digits = "0." + Strings.repeat("0", 20_000) + "1";
    final SqlNumericLiteral literal =
        SqlLiteral.createExactNumeric(digits, SqlParserPos.ZERO);
    final IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, literal::toValue);
    assertThat(e.getMessage(),
        containsString("exceeds the configured plain-notation bound"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4543">[CALCITE-4543]
   * Interval literal loses a fractional second when it has scale greater
   * than 3</a>. */
  @Test void testIntervalLiteralKeepsSubMillisecondFraction()
      throws SqlParseException {
    final SqlLiteral second9 =
        (SqlLiteral) parseExpression("INTERVAL '1.123456789' SECOND(1,9)");
    assertThat(second9.getValueAs(BigDecimal.class),
        is(new BigDecimal("1123.456789")));

    final SqlLiteral negative =
        (SqlLiteral) parseExpression("INTERVAL -'1.123456789' SECOND(1,9)");
    assertThat(negative.getValueAs(BigDecimal.class),
        is(new BigDecimal("-1123.456789")));

    final SqlLiteral dayToSecond9 =
        (SqlLiteral) parseExpression(
            "INTERVAL '1 02:03:04.123456789' DAY(2) TO SECOND(9)");
    assertThat(dayToSecond9.getValueAs(BigDecimal.class),
        is(new BigDecimal("93784123.456789")));

    // A qualifier that declares nothing below the millisecond is unaffected
    final SqlLiteral second3 =
        (SqlLiteral) parseExpression("INTERVAL '1.123' SECOND(1,3)");
    assertThat(second3.getValueAs(BigDecimal.class),
        is(new BigDecimal("1123")));
  }

  private static Matcher<String> isEqualsDeep(String sqlExpected) {
    return new CustomTypeSafeMatcher<String>("isDeepEqual") {
      @Override protected boolean matchesSafely(String sqlActual) {
        try {
          SqlNode sqlNodeActual = parseExpression(sqlActual);
          SqlNode sqlNodeExpected = parseExpression(sqlExpected);

          return sqlNodeActual.equalsDeep(sqlNodeExpected, Litmus.IGNORE);
        } catch (SqlParseException e) {
          throw TestUtil.rethrow(e);
        }
      }
    };
  }

  private static SqlNode parseExpression(String sql) throws SqlParseException {
    return SqlParser.create(sql).parseExpression();
  }

  /** Compares a list to its own backing list. */
  private void checkList(SqlNodeList nodeList) {
    checkLists(nodeList, nodeList.getList(), 0);
  }

  /** Checks that two lists are identical. */
  private <E> void checkLists(List<E> list0, List<E> list1, int depth) {
    assertThat(list0.hashCode(), is(list1.hashCode()));
    assertThat(list0.equals(list1), is(true));
    assertThat(list0, hasSize(list1.size()));
    assertThat(list0.isEmpty(), is(list1.isEmpty()));
    if (!list0.isEmpty()) {
      assertThat(list0.get(0), sameInstance(list1.get(0)));
      assertThat(Util.last(list0), sameInstance(Util.last(list1)));
      if (depth == 0) {
        checkLists(Util.skip(list0, 1), Util.skip(list1, 1), depth + 1);
      }
    }
    assertThat(collect(list0), is(list1));
    assertThat(collect(list1), is(list0));
  }

  private static <E> List<E> collect(Iterable<E> iterable) {
    final List<E> list = new ArrayList<>();
    for (E e : iterable) {
      list.add(e);
    }
    return list;
  }
}
