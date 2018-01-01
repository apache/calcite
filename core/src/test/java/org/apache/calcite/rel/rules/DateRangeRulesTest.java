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
package org.apache.calcite.rel.rules;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.RexImplicationCheckerTest.Fixture;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.RangeSet;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Unit tests for {@link DateRangeRules} algorithms. */
public class DateRangeRulesTest {

  @Test public void testExtractYearFromDateColumn() {
    final Fixture2 f = new Fixture2();

    final RexNode e = f.eq(f.literal(2014), f.exYear);
    assertThat(DateRangeRules.extractTimeUnits(e),
        is(set(TimeUnitRange.YEAR)));
    assertThat(DateRangeRules.extractTimeUnits(f.dec), is(set()));
    assertThat(DateRangeRules.extractTimeUnits(f.literal(1)), is(set()));

    // extract YEAR from a DATE column
    checkDateRange(f, e, is("AND(>=($9, 2014-01-01), <($9, 2015-01-01))"));
    checkDateRange(f, f.eq(f.exYear, f.literal(2014)),
        is("AND(>=($9, 2014-01-01), <($9, 2015-01-01))"));
    checkDateRange(f, f.ge(f.exYear, f.literal(2014)),
        is(">=($9, 2014-01-01)"));
    checkDateRange(f, f.gt(f.exYear, f.literal(2014)),
        is(">=($9, 2015-01-01)"));
    checkDateRange(f, f.lt(f.exYear, f.literal(2014)),
        is("<($9, 2014-01-01)"));
    checkDateRange(f, f.le(f.exYear, f.literal(2014)),
        is("<($9, 2015-01-01)"));
    checkDateRange(f, f.ne(f.exYear, f.literal(2014)),
        is("<>(EXTRACT(FLAG(YEAR), $9), 2014)"));
  }

  @Test public void testExtractYearFromTimestampColumn() {
    final Fixture2 f = new Fixture2();
    checkDateRange(f, f.eq(f.exYear, f.literal(2014)),
        is("AND(>=($9, 2014-01-01), <($9, 2015-01-01))"));
    checkDateRange(f, f.ge(f.exYear, f.literal(2014)),
        is(">=($9, 2014-01-01)"));
    checkDateRange(f, f.gt(f.exYear, f.literal(2014)),
        is(">=($9, 2015-01-01)"));
    checkDateRange(f, f.lt(f.exYear, f.literal(2014)),
        is("<($9, 2014-01-01)"));
    checkDateRange(f, f.le(f.exYear, f.literal(2014)),
        is("<($9, 2015-01-01)"));
    checkDateRange(f, f.ne(f.exYear, f.literal(2014)),
        is("<>(EXTRACT(FLAG(YEAR), $9), 2014)"));
  }

  @Test public void testExtractYearAndMonthFromDateColumn() {
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.and(f.eq(f.exYear, f.literal(2014)), f.eq(f.exMonth, f.literal(6))),
        is("AND(AND(>=($9, 2014-01-01), <($9, 2015-01-01)),"
            + " AND(>=($9, 2014-06-01), <($9, 2014-07-01)))"),
        is("AND(>=($9, 2014-01-01), <($9, 2015-01-01),"
            + " >=($9, 2014-06-01), <($9, 2014-07-01))"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1601">[CALCITE-1601]
   * DateRangeRules loses OR filters</a>. */
  @Test public void testExtractYearAndMonthFromDateColumn2() {
    final Fixture2 f = new Fixture2();
    final String s1 = "AND("
        + "AND(>=($9, 2000-01-01), <($9, 2001-01-01)),"
        + " OR("
        + "AND(>=($9, 2000-02-01), <($9, 2000-03-01)), "
        + "AND(>=($9, 2000-03-01), <($9, 2000-04-01)), "
        + "AND(>=($9, 2000-05-01), <($9, 2000-06-01))))";
    final String s2 = "AND(>=($9, 2000-01-01), <($9, 2001-01-01),"
        + " OR("
        + "AND(>=($9, 2000-02-01), <($9, 2000-03-01)), "
        + "AND(>=($9, 2000-03-01), <($9, 2000-04-01)), "
        + "AND(>=($9, 2000-05-01), <($9, 2000-06-01))))";
    final RexNode e =
        f.and(f.eq(f.exYear, f.literal(2000)),
            f.or(f.eq(f.exMonth, f.literal(2)),
                f.eq(f.exMonth, f.literal(3)),
                f.eq(f.exMonth, f.literal(5))));
    checkDateRange(f, e, is(s1), is(s2));
  }

  @Test public void testExtractYearAndDayFromDateColumn() {
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.and(f.eq(f.exYear, f.literal(2010)), f.eq(f.exDay, f.literal(31))),
        is("AND(AND(>=($9, 2010-01-01), <($9, 2011-01-01)),"
            + " OR(AND(>=($9, 2010-01-31), <($9, 2010-02-01)),"
            + " AND(>=($9, 2010-03-31), <($9, 2010-04-01)),"
            + " AND(>=($9, 2010-05-31), <($9, 2010-06-01)),"
            + " AND(>=($9, 2010-07-31), <($9, 2010-08-01)),"
            + " AND(>=($9, 2010-08-31), <($9, 2010-09-01)),"
            + " AND(>=($9, 2010-10-31), <($9, 2010-11-01)),"
            + " AND(>=($9, 2010-12-31), <($9, 2011-01-01))))"));

  }

  @Test public void testExtractYearMonthDayFromDateColumn() {
    final Fixture2 f = new Fixture2();
    // The following condition finds the 2 leap days between 2010 and 2020,
    // namely 29th February 2012 and 2016.
    //
    // Currently there are redundant conditions, e.g.
    // "AND(>=($9, 2011-01-01), <($9, 2020-01-01))". We should remove them by
    // folding intervals.
    checkDateRange(f,
        f.and(f.gt(f.exYear, f.literal(2010)), f.lt(f.exYear, f.literal(2020)),
            f.eq(f.exMonth, f.literal(2)), f.eq(f.exDay, f.literal(29))),
        is("AND(>=($9, 2011-01-01),"
            + " AND(>=($9, 2011-01-01), <($9, 2020-01-01)),"
            + " OR(AND(>=($9, 2011-02-01), <($9, 2011-03-01)),"
            + " AND(>=($9, 2012-02-01), <($9, 2012-03-01)),"
            + " AND(>=($9, 2013-02-01), <($9, 2013-03-01)),"
            + " AND(>=($9, 2014-02-01), <($9, 2014-03-01)),"
            + " AND(>=($9, 2015-02-01), <($9, 2015-03-01)),"
            + " AND(>=($9, 2016-02-01), <($9, 2016-03-01)),"
            + " AND(>=($9, 2017-02-01), <($9, 2017-03-01)),"
            + " AND(>=($9, 2018-02-01), <($9, 2018-03-01)),"
            + " AND(>=($9, 2019-02-01), <($9, 2019-03-01))),"
            + " OR(AND(>=($9, 2012-02-29), <($9, 2012-03-01)),"
            + " AND(>=($9, 2016-02-29), <($9, 2016-03-01))))"));
  }

  @Test public void testExtractYearMonthDayFromTimestampColumn() {
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.and(f.gt(f.exYear, f.literal(2010)),
            f.lt(f.exYear, f.literal(2020)),
            f.eq(f.exMonth, f.literal(2)), f.eq(f.exDay, f.literal(29))),
        is("AND(>=($9, 2011-01-01),"
            + " AND(>=($9, 2011-01-01), <($9, 2020-01-01)),"
            + " OR(AND(>=($9, 2011-02-01), <($9, 2011-03-01)),"
            + " AND(>=($9, 2012-02-01), <($9, 2012-03-01)),"
            + " AND(>=($9, 2013-02-01), <($9, 2013-03-01)),"
            + " AND(>=($9, 2014-02-01), <($9, 2014-03-01)),"
            + " AND(>=($9, 2015-02-01), <($9, 2015-03-01)),"
            + " AND(>=($9, 2016-02-01), <($9, 2016-03-01)),"
            + " AND(>=($9, 2017-02-01), <($9, 2017-03-01)),"
            + " AND(>=($9, 2018-02-01), <($9, 2018-03-01)),"
            + " AND(>=($9, 2019-02-01), <($9, 2019-03-01))),"
            + " OR(AND(>=($9, 2012-02-29), <($9, 2012-03-01)),"
            + " AND(>=($9, 2016-02-29), <($9, 2016-03-01))))"));
  }

  /** Test case #1 for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1658">[CALCITE-1658]
   * DateRangeRules issues</a>. */
  @Test public void testExtractWithOrCondition1() {
    // (EXTRACT(YEAR FROM __time) = 2000
    //    AND EXTRACT(MONTH FROM __time) IN (2, 3, 5))
    // OR (EXTRACT(YEAR FROM __time) = 2001
    //    AND EXTRACT(MONTH FROM __time) = 1)
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.or(
            f.and(f.eq(f.exYear, f.literal(2000)),
                f.or(f.eq(f.exMonth, f.literal(2)),
                    f.eq(f.exMonth, f.literal(3)),
                    f.eq(f.exMonth, f.literal(5)))),
            f.and(f.eq(f.exYear, f.literal(2001)),
                f.eq(f.exMonth, f.literal(1)))),
        is("OR(AND(AND(>=($9, 2000-01-01), <($9, 2001-01-01)),"
            + " OR(AND(>=($9, 2000-02-01), <($9, 2000-03-01)),"
            + " AND(>=($9, 2000-03-01), <($9, 2000-04-01)),"
            + " AND(>=($9, 2000-05-01), <($9, 2000-06-01)))),"
            + " AND(AND(>=($9, 2001-01-01), <($9, 2002-01-01)),"
            + " AND(>=($9, 2001-01-01), <($9, 2001-02-01))))"));
  }

  /** Test case #2 for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1658">[CALCITE-1658]
   * DateRangeRules issues</a>. */
  @Test public void testExtractWithOrCondition2() {
    // EXTRACT(YEAR FROM __time) IN (2000, 2001)
    //   AND ((EXTRACT(YEAR FROM __time) = 2000
    //         AND EXTRACT(MONTH FROM __time) IN (2, 3, 5))
    //     OR (EXTRACT(YEAR FROM __time) = 2001
    //       AND EXTRACT(MONTH FROM __time) = 1))
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.and(
            f.or(f.eq(f.exYear, f.literal(2000)),
                f.eq(f.exYear, f.literal(2001))),
            f.or(
                f.and(f.eq(f.exYear, f.literal(2000)),
                    f.or(f.eq(f.exMonth, f.literal(2)),
                        f.eq(f.exMonth, f.literal(3)),
                        f.eq(f.exMonth, f.literal(5)))),
                f.and(f.eq(f.exYear, f.literal(2001)),
                    f.eq(f.exMonth, f.literal(1))))),
        is("AND(OR(AND(>=($9, 2000-01-01), <($9, 2001-01-01)),"
            + " AND(>=($9, 2001-01-01), <($9, 2002-01-01))),"
            + " OR(AND(AND(>=($9, 2000-01-01), <($9, 2001-01-01)),"
            + " OR(AND(>=($9, 2000-02-01), <($9, 2000-03-01)),"
            + " AND(>=($9, 2000-03-01), <($9, 2000-04-01)),"
            + " AND(>=($9, 2000-05-01), <($9, 2000-06-01)))),"
            + " AND(AND(>=($9, 2001-01-01), <($9, 2002-01-01)),"
            + " AND(>=($9, 2001-01-01), <($9, 2001-02-01)))))"));
  }

  /** Test case #3 for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1658">[CALCITE-1658]
   * DateRangeRules issues</a>. */
  @Test public void testExtractPartialRewriteForNotEqualsYear() {
    // EXTRACT(YEAR FROM __time) <> 2000
    // AND ((EXTRACT(YEAR FROM __time) = 2000
    //     AND EXTRACT(MONTH FROM __time) IN (2, 3, 5))
    //   OR (EXTRACT(YEAR FROM __time) = 2001
    //     AND EXTRACT(MONTH FROM __time) = 1))
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.and(
            f.ne(f.exYear, f.literal(2000)),
            f.or(
                f.and(f.eq(f.exYear, f.literal(2000)),
                    f.or(f.eq(f.exMonth, f.literal(2)),
                        f.eq(f.exMonth, f.literal(3)),
                        f.eq(f.exMonth, f.literal(5)))),
                f.and(f.eq(f.exYear, f.literal(2001)),
                    f.eq(f.exMonth, f.literal(1))))),
        is("AND(<>(EXTRACT(FLAG(YEAR), $9), 2000),"
            + " OR(AND(AND(>=($9, 2000-01-01), <($9, 2001-01-01)),"
            + " OR(AND(>=($9, 2000-02-01), <($9, 2000-03-01)),"
            + " AND(>=($9, 2000-03-01), <($9, 2000-04-01)),"
            + " AND(>=($9, 2000-05-01), <($9, 2000-06-01)))),"
            + " AND(AND(>=($9, 2001-01-01), <($9, 2002-01-01)),"
            + " AND(>=($9, 2001-01-01), <($9, 2001-02-01)))))"));
  }

  /** Test case #4 for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1658">[CALCITE-1658]
   * DateRangeRules issues</a>. */
  @Test public void testExtractPartialRewriteForInMonth() {
    // EXTRACT(MONTH FROM __time) in (1, 2, 3, 4, 5)
    // AND ((EXTRACT(YEAR FROM __time) = 2000
    //     AND EXTRACT(MONTH FROM __time) IN (2, 3, 5))
    //   OR (EXTRACT(YEAR FROM __time) = 2001
    //     AND EXTRACT(MONTH FROM __time) = 1))
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.and(
            f.or(f.eq(f.exMonth, f.literal(1)),
                f.eq(f.exMonth, f.literal(2)),
                f.eq(f.exMonth, f.literal(3)),
                f.eq(f.exMonth, f.literal(4)),
                f.eq(f.exMonth, f.literal(5))),
            f.or(
                f.and(f.eq(f.exYear, f.literal(2000)),
                    f.or(f.eq(f.exMonth, f.literal(2)),
                        f.eq(f.exMonth, f.literal(3)),
                        f.eq(f.exMonth, f.literal(5)))),
                f.and(f.eq(f.exYear, f.literal(2001)),
                    f.eq(f.exMonth, f.literal(1))))),
        is("AND(OR(=(EXTRACT(FLAG(MONTH), $9), 1),"
            + " =(EXTRACT(FLAG(MONTH), $9), 2),"
            + " =(EXTRACT(FLAG(MONTH), $9), 3),"
            + " =(EXTRACT(FLAG(MONTH), $9), 4),"
            + " =(EXTRACT(FLAG(MONTH), $9), 5)),"
            + " OR(AND(AND(>=($9, 2000-01-01), <($9, 2001-01-01)),"
            + " OR(AND(>=($9, 2000-02-01), <($9, 2000-03-01)),"
            + " AND(>=($9, 2000-03-01), <($9, 2000-04-01)),"
            + " AND(>=($9, 2000-05-01), <($9, 2000-06-01)))),"
            + " AND(AND(>=($9, 2001-01-01), <($9, 2002-01-01)),"
            + " AND(>=($9, 2001-01-01), <($9, 2001-02-01)))))"));
  }

  private static Set<TimeUnitRange> set(TimeUnitRange... es) {
    return ImmutableSet.copyOf(es);
  }

  private void checkDateRange(Fixture f, RexNode e, Matcher<String> matcher) {
    checkDateRange(f, e, matcher, CoreMatchers.any(String.class));
  }

  private void checkDateRange(Fixture f, RexNode e, Matcher<String> matcher,
      Matcher<String> simplifyMatcher) {
    final Map<String, RangeSet<Calendar>> operandRanges = new HashMap<>();
    final ImmutableSortedSet<TimeUnitRange> timeUnits =
        DateRangeRules.extractTimeUnits(e);
    for (TimeUnitRange timeUnit : timeUnits) {
      e = e.accept(
          new DateRangeRules.ExtractShuttle(f.rexBuilder, timeUnit,
              operandRanges, timeUnits));
    }
    assertThat(e.toString(), matcher);
    final RexNode e2 = f.simplify.simplify(e);
    assertThat(e2.toString(), simplifyMatcher);
  }

  /** Common expressions across tests. */
  private static class Fixture2 extends Fixture {
    private final RexNode exYear;
    private final RexNode exMonth;
    private final RexNode exDay;

    Fixture2() {
      exYear = rexBuilder.makeCall(SqlStdOperatorTable.EXTRACT,
          ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.YEAR), ts));
      exMonth = rexBuilder.makeCall(intRelDataType,
          SqlStdOperatorTable.EXTRACT,
          ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.MONTH), ts));
      exDay = rexBuilder.makeCall(intRelDataType,
          SqlStdOperatorTable.EXTRACT,
          ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.DAY), ts));
    }
  }
}

// End DateRangeRulesTest.java
