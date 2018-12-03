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
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.Calendar;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Unit tests for {@link DateRangeRules} algorithms. */
public class DateRangeRulesTest {

  @Test public void testExtractYearFromDateColumn() {
    final Fixture2 f = new Fixture2();

    final RexNode e = f.eq(f.literal(2014), f.exYearD);
    assertThat(DateRangeRules.extractTimeUnits(e),
        is(set(TimeUnitRange.YEAR)));
    assertThat(DateRangeRules.extractTimeUnits(f.dec), is(set()));
    assertThat(DateRangeRules.extractTimeUnits(f.literal(1)), is(set()));

    // extract YEAR from a DATE column
    checkDateRange(f, e, is("AND(>=($8, 2014-01-01), <($8, 2015-01-01))"));
    checkDateRange(f, f.eq(f.exYearD, f.literal(2014)),
        is("AND(>=($8, 2014-01-01), <($8, 2015-01-01))"));
    checkDateRange(f, f.ge(f.exYearD, f.literal(2014)),
        is(">=($8, 2014-01-01)"));
    checkDateRange(f, f.gt(f.exYearD, f.literal(2014)),
        is(">=($8, 2015-01-01)"));
    checkDateRange(f, f.lt(f.exYearD, f.literal(2014)),
        is("<($8, 2014-01-01)"));
    checkDateRange(f, f.le(f.exYearD, f.literal(2014)),
        is("<($8, 2015-01-01)"));
    checkDateRange(f, f.ne(f.exYearD, f.literal(2014)),
        is("<>(EXTRACT(FLAG(YEAR), $8), 2014)"));
  }

  @Test public void testExtractYearFromTimestampColumn() {
    final Fixture2 f = new Fixture2();
    checkDateRange(f, f.eq(f.exYearTs, f.literal(2014)),
        is("AND(>=($9, 2014-01-01 00:00:00), <($9, 2015-01-01 00:00:00))"));
    checkDateRange(f, f.ge(f.exYearTs, f.literal(2014)),
        is(">=($9, 2014-01-01 00:00:00)"));
    checkDateRange(f, f.gt(f.exYearTs, f.literal(2014)),
        is(">=($9, 2015-01-01 00:00:00)"));
    checkDateRange(f, f.lt(f.exYearTs, f.literal(2014)),
        is("<($9, 2014-01-01 00:00:00)"));
    checkDateRange(f, f.le(f.exYearTs, f.literal(2014)),
        is("<($9, 2015-01-01 00:00:00)"));
    checkDateRange(f, f.ne(f.exYearTs, f.literal(2014)),
        is("<>(EXTRACT(FLAG(YEAR), $9), 2014)"));
  }

  @Test public void testExtractYearAndMonthFromDateColumn() {
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.and(f.eq(f.exYearD, f.literal(2014)), f.eq(f.exMonthD, f.literal(6))),
        "UTC",
        is("AND(AND(>=($8, 2014-01-01), <($8, 2015-01-01)),"
            + " AND(>=($8, 2014-06-01), <($8, 2014-07-01)))"),
        is("AND(>=($8, 2014-01-01), <($8, 2015-01-01),"
            + " >=($8, 2014-06-01), <($8, 2014-07-01))"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1601">[CALCITE-1601]
   * DateRangeRules loses OR filters</a>. */
  @Test public void testExtractYearAndMonthFromDateColumn2() {
    final Fixture2 f = new Fixture2();
    final String s1 = "AND("
        + "AND(>=($8, 2000-01-01), <($8, 2001-01-01)),"
        + " OR("
        + "AND(>=($8, 2000-02-01), <($8, 2000-03-01)), "
        + "AND(>=($8, 2000-03-01), <($8, 2000-04-01)), "
        + "AND(>=($8, 2000-05-01), <($8, 2000-06-01))))";
    final String s2 = "AND(>=($8, 2000-01-01), <($8, 2001-01-01),"
        + " OR("
        + "AND(>=($8, 2000-02-01), <($8, 2000-03-01)), "
        + "AND(>=($8, 2000-03-01), <($8, 2000-04-01)), "
        + "AND(>=($8, 2000-05-01), <($8, 2000-06-01))))";
    final RexNode e =
        f.and(f.eq(f.exYearD, f.literal(2000)),
            f.or(f.eq(f.exMonthD, f.literal(2)),
                f.eq(f.exMonthD, f.literal(3)),
                f.eq(f.exMonthD, f.literal(5))));
    checkDateRange(f, e, "UTC", is(s1), is(s2));
  }

  @Test public void testExtractYearAndDayFromDateColumn() {
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.and(f.eq(f.exYearD, f.literal(2010)), f.eq(f.exDayD, f.literal(31))),
        is("AND(AND(>=($8, 2010-01-01), <($8, 2011-01-01)),"
            + " OR(AND(>=($8, 2010-01-31), <($8, 2010-02-01)),"
            + " AND(>=($8, 2010-03-31), <($8, 2010-04-01)),"
            + " AND(>=($8, 2010-05-31), <($8, 2010-06-01)),"
            + " AND(>=($8, 2010-07-31), <($8, 2010-08-01)),"
            + " AND(>=($8, 2010-08-31), <($8, 2010-09-01)),"
            + " AND(>=($8, 2010-10-31), <($8, 2010-11-01)),"
            + " AND(>=($8, 2010-12-31), <($8, 2011-01-01))))"));

  }

  @Test public void testExtractYearMonthDayFromDateColumn() {
    final Fixture2 f = new Fixture2();
    // The following condition finds the 2 leap days between 2010 and 2020,
    // namely 29th February 2012 and 2016.
    //
    // Currently there are redundant conditions, e.g.
    // "AND(>=($8, 2011-01-01), <($8, 2020-01-01))". We should remove them by
    // folding intervals.
    checkDateRange(f,
        f.and(f.gt(f.exYearD, f.literal(2010)),
            f.lt(f.exYearD, f.literal(2020)),
            f.eq(f.exMonthD, f.literal(2)), f.eq(f.exDayD, f.literal(29))),
        is("AND(>=($8, 2011-01-01),"
            + " AND(>=($8, 2011-01-01), <($8, 2020-01-01)),"
            + " OR(AND(>=($8, 2011-02-01), <($8, 2011-03-01)),"
            + " AND(>=($8, 2012-02-01), <($8, 2012-03-01)),"
            + " AND(>=($8, 2013-02-01), <($8, 2013-03-01)),"
            + " AND(>=($8, 2014-02-01), <($8, 2014-03-01)),"
            + " AND(>=($8, 2015-02-01), <($8, 2015-03-01)),"
            + " AND(>=($8, 2016-02-01), <($8, 2016-03-01)),"
            + " AND(>=($8, 2017-02-01), <($8, 2017-03-01)),"
            + " AND(>=($8, 2018-02-01), <($8, 2018-03-01)),"
            + " AND(>=($8, 2019-02-01), <($8, 2019-03-01))),"
            + " OR(AND(>=($8, 2012-02-29), <($8, 2012-03-01)),"
            + " AND(>=($8, 2016-02-29), <($8, 2016-03-01))))"));
  }

  @Test public void testExtractYearMonthDayFromTimestampColumn() {
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.and(f.gt(f.exYearD, f.literal(2010)),
            f.lt(f.exYearD, f.literal(2020)),
            f.eq(f.exMonthD, f.literal(2)), f.eq(f.exDayD, f.literal(29))),
        is("AND(>=($8, 2011-01-01),"
            + " AND(>=($8, 2011-01-01), <($8, 2020-01-01)),"
            + " OR(AND(>=($8, 2011-02-01), <($8, 2011-03-01)),"
            + " AND(>=($8, 2012-02-01), <($8, 2012-03-01)),"
            + " AND(>=($8, 2013-02-01), <($8, 2013-03-01)),"
            + " AND(>=($8, 2014-02-01), <($8, 2014-03-01)),"
            + " AND(>=($8, 2015-02-01), <($8, 2015-03-01)),"
            + " AND(>=($8, 2016-02-01), <($8, 2016-03-01)),"
            + " AND(>=($8, 2017-02-01), <($8, 2017-03-01)),"
            + " AND(>=($8, 2018-02-01), <($8, 2018-03-01)),"
            + " AND(>=($8, 2019-02-01), <($8, 2019-03-01))),"
            + " OR(AND(>=($8, 2012-02-29), <($8, 2012-03-01)),"
            + " AND(>=($8, 2016-02-29), <($8, 2016-03-01))))"));
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
            f.and(f.eq(f.exYearD, f.literal(2000)),
                f.or(f.eq(f.exMonthD, f.literal(2)),
                    f.eq(f.exMonthD, f.literal(3)),
                    f.eq(f.exMonthD, f.literal(5)))),
            f.and(f.eq(f.exYearD, f.literal(2001)),
                f.eq(f.exMonthD, f.literal(1)))),
        is("OR(AND(AND(>=($8, 2000-01-01), <($8, 2001-01-01)),"
            + " OR(AND(>=($8, 2000-02-01), <($8, 2000-03-01)),"
            + " AND(>=($8, 2000-03-01), <($8, 2000-04-01)),"
            + " AND(>=($8, 2000-05-01), <($8, 2000-06-01)))),"
            + " AND(AND(>=($8, 2001-01-01), <($8, 2002-01-01)),"
            + " AND(>=($8, 2001-01-01), <($8, 2001-02-01))))"));
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
            f.or(f.eq(f.exYearD, f.literal(2000)),
                f.eq(f.exYearD, f.literal(2001))),
            f.or(
                f.and(f.eq(f.exYearD, f.literal(2000)),
                    f.or(f.eq(f.exMonthD, f.literal(2)),
                        f.eq(f.exMonthD, f.literal(3)),
                        f.eq(f.exMonthD, f.literal(5)))),
                f.and(f.eq(f.exYearD, f.literal(2001)),
                    f.eq(f.exMonthD, f.literal(1))))),
        is("AND(OR(AND(>=($8, 2000-01-01), <($8, 2001-01-01)),"
            + " AND(>=($8, 2001-01-01), <($8, 2002-01-01))),"
            + " OR(AND(AND(>=($8, 2000-01-01), <($8, 2001-01-01)),"
            + " OR(AND(>=($8, 2000-02-01), <($8, 2000-03-01)),"
            + " AND(>=($8, 2000-03-01), <($8, 2000-04-01)),"
            + " AND(>=($8, 2000-05-01), <($8, 2000-06-01)))),"
            + " AND(AND(>=($8, 2001-01-01), <($8, 2002-01-01)),"
            + " AND(>=($8, 2001-01-01), <($8, 2001-02-01)))))"));
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
            f.ne(f.exYearD, f.literal(2000)),
            f.or(
                f.and(f.eq(f.exYearD, f.literal(2000)),
                    f.or(f.eq(f.exMonthD, f.literal(2)),
                        f.eq(f.exMonthD, f.literal(3)),
                        f.eq(f.exMonthD, f.literal(5)))),
                f.and(f.eq(f.exYearD, f.literal(2001)),
                    f.eq(f.exMonthD, f.literal(1))))),
        is("AND(<>(EXTRACT(FLAG(YEAR), $8), 2000),"
            + " OR(AND(AND(>=($8, 2000-01-01), <($8, 2001-01-01)),"
            + " OR(AND(>=($8, 2000-02-01), <($8, 2000-03-01)),"
            + " AND(>=($8, 2000-03-01), <($8, 2000-04-01)),"
            + " AND(>=($8, 2000-05-01), <($8, 2000-06-01)))),"
            + " AND(AND(>=($8, 2001-01-01), <($8, 2002-01-01)),"
            + " AND(>=($8, 2001-01-01), <($8, 2001-02-01)))))"));
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
            f.or(f.eq(f.exMonthD, f.literal(1)),
                f.eq(f.exMonthD, f.literal(2)),
                f.eq(f.exMonthD, f.literal(3)),
                f.eq(f.exMonthD, f.literal(4)),
                f.eq(f.exMonthD, f.literal(5))),
            f.or(
                f.and(f.eq(f.exYearD, f.literal(2000)),
                    f.or(f.eq(f.exMonthD, f.literal(2)),
                        f.eq(f.exMonthD, f.literal(3)),
                        f.eq(f.exMonthD, f.literal(5)))),
                f.and(f.eq(f.exYearD, f.literal(2001)),
                    f.eq(f.exMonthD, f.literal(1))))),
        is("AND(OR(=(EXTRACT(FLAG(MONTH), $8), 1),"
            + " =(EXTRACT(FLAG(MONTH), $8), 2),"
            + " =(EXTRACT(FLAG(MONTH), $8), 3),"
            + " =(EXTRACT(FLAG(MONTH), $8), 4),"
            + " =(EXTRACT(FLAG(MONTH), $8), 5)),"
            + " OR(AND(AND(>=($8, 2000-01-01), <($8, 2001-01-01)),"
            + " OR(AND(>=($8, 2000-02-01), <($8, 2000-03-01)),"
            + " AND(>=($8, 2000-03-01), <($8, 2000-04-01)),"
            + " AND(>=($8, 2000-05-01), <($8, 2000-06-01)))),"
            + " AND(AND(>=($8, 2001-01-01), <($8, 2002-01-01)),"
            + " AND(>=($8, 2001-01-01), <($8, 2001-02-01)))))"));
  }

  @Test public void testExtractRewriteForInvalidMonthComparison() {
    // "EXTRACT(MONTH FROM ts) = 14" will never be TRUE
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.and(f.eq(f.exYearTs, f.literal(2010)),
            f.eq(f.exMonthTs, f.literal(14))),
        is("AND(AND(>=($9, 2010-01-01 00:00:00), <($9, 2011-01-01 00:00:00)),"
            + " false)"));

    // "EXTRACT(MONTH FROM ts) = 0" will never be TRUE
    checkDateRange(f,
        f.and(f.eq(f.exYearTs, f.literal(2010)),
            f.eq(f.exMonthTs, f.literal(0))),
        is("AND(AND(>=($9, 2010-01-01 00:00:00), <($9, 2011-01-01 00:00:00)),"
            + " false)"));

    // "EXTRACT(MONTH FROM ts) = 13" will never be TRUE
    checkDateRange(f,
        f.and(f.eq(f.exYearTs, f.literal(2010)),
            f.eq(f.exMonthTs, f.literal(13))),
        is("AND(AND(>=($9, 2010-01-01 00:00:00), <($9, 2011-01-01 00:00:00)),"
            + " false)"));

    // "EXTRACT(MONTH FROM ts) = 12" might be TRUE
    // Careful with boundaries, because Calendar.DECEMBER = 11
    checkDateRange(f,
        f.and(f.eq(f.exYearTs, f.literal(2010)),
            f.eq(f.exMonthTs, f.literal(12))),
        is("AND(AND(>=($9, 2010-01-01 00:00:00), <($9, 2011-01-01 00:00:00)),"
            + " AND(>=($9, 2010-12-01 00:00:00), <($9, 2011-01-01 00:00:00)))"));

    // "EXTRACT(MONTH FROM ts) = 1" can happen
    // Careful with boundaries, because Calendar.JANUARY = 0
    checkDateRange(f,
        f.and(f.eq(f.exYearTs, f.literal(2010)),
            f.eq(f.exMonthTs, f.literal(1))),
        is("AND(AND(>=($9, 2010-01-01 00:00:00), <($9, 2011-01-01 00:00:00)),"
            + " AND(>=($9, 2010-01-01 00:00:00), <($9, 2010-02-01 00:00:00)))"));
  }

  @Test public void testExtractRewriteForInvalidDayComparison() {
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.and(f.eq(f.exYearTs, f.literal(2010)),
            f.eq(f.exMonthTs, f.literal(11)),
            f.eq(f.exDayTs, f.literal(32))),
        is("AND(AND(>=($9, 2010-01-01 00:00:00), <($9, 2011-01-01 00:00:00)),"
            + " AND(>=($9, 2010-11-01 00:00:00), <($9, 2010-12-01 00:00:00)), false)"));
    // Feb 31 is an invalid date
    checkDateRange(f,
        f.and(f.eq(f.exYearTs, f.literal(2010)),
            f.eq(f.exMonthTs, f.literal(2)),
            f.eq(f.exDayTs, f.literal(31))),
        is("AND(AND(>=($9, 2010-01-01 00:00:00), <($9, 2011-01-01 00:00:00)),"
            + " AND(>=($9, 2010-02-01 00:00:00), <($9, 2010-03-01 00:00:00)), false)"));
  }

  @Test public void testUnboundYearExtractRewrite() {
    final Fixture2 f = new Fixture2();
    // No lower bound on YEAR
    checkDateRange(f,
        f.and(f.le(f.exYearTs, f.literal(2010)),
            f.eq(f.exMonthTs, f.literal(11)),
            f.eq(f.exDayTs, f.literal(2))),
        is("AND(<($9, 2011-01-01 00:00:00), =(EXTRACT(FLAG(MONTH), $9), 11),"
            + " =(EXTRACT(FLAG(DAY), $9), 2))"));

    // No upper bound on YEAR
    checkDateRange(f,
        f.and(f.ge(f.exYearTs, f.literal(2010)),
            f.eq(f.exMonthTs, f.literal(11)),
            f.eq(f.exDayTs, f.literal(2))),
        // Since the year does not have a upper bound, MONTH and DAY cannot be replaced
        is("AND(>=($9, 2010-01-01 00:00:00), =(EXTRACT(FLAG(MONTH), $9), 11),"
            + " =(EXTRACT(FLAG(DAY), $9), 2))"));

    // No lower/upper bound on YEAR for individual rexNodes.
    checkDateRange(f,
        f.and(f.le(f.exYearTs, f.literal(2010)),
            f.ge(f.exYearTs, f.literal(2010)),
            f.eq(f.exMonthTs, f.literal(5))),
        is("AND(<($9, 2011-01-01 00:00:00), AND(>=($9, 2010-01-01 00:00:00),"
            + " <($9, 2011-01-01 00:00:00)), AND(>=($9, 2010-05-01 00:00:00),"
            + " <($9, 2010-06-01 00:00:00)))"));
  }

  // Test reWrite with multiple operands
  @Test public void testExtractRewriteMultipleOperands() {
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.and(f.eq(f.exYearTs, f.literal(2010)),
            f.eq(f.exMonthTs, f.literal(10)),
            f.eq(f.exMonthD, f.literal(5))),
        is("AND(AND(>=($9, 2010-01-01 00:00:00), <($9, 2011-01-01 00:00:00)),"
            + " AND(>=($9, 2010-10-01 00:00:00), <($9, 2010-11-01 00:00:00)),"
            + " =(EXTRACT(FLAG(MONTH), $8), 5))"));

    checkDateRange(f,
        f.and(f.eq(f.exYearTs, f.literal(2010)),
            f.eq(f.exMonthTs, f.literal(10)),
            f.eq(f.exYearD, f.literal(2011)),
            f.eq(f.exMonthD, f.literal(5))),
        is("AND(AND(>=($9, 2010-01-01 00:00:00), <($9, 2011-01-01 00:00:00)),"
            + " AND(>=($9, 2010-10-01 00:00:00), <($9, 2010-11-01 00:00:00)),"
            + " AND(>=($8, 2011-01-01), <($8, 2012-01-01)), AND(>=($8, 2011-05-01),"
            + " <($8, 2011-06-01)))"));
  }

  @Test public void testFloorEqRewrite() {
    final Calendar c = Util.calendar();
    c.clear();
    c.set(2010, Calendar.FEBRUARY, 10, 11, 12, 05);
    final Fixture2 f = new Fixture2();
    // Always False
    checkDateRange(f, f.eq(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("false"));
    checkDateRange(f, f.eq(f.timestampLiteral(TimestampString.fromCalendarFields(c)), f.floorYear),
        is("false"));

    c.clear();
    c.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
    checkDateRange(f, f.eq(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>=($9, 2010-01-01 00:00:00), <($9, 2011-01-01 00:00:00))"));

    c.set(2010, Calendar.FEBRUARY, 1, 0, 0, 0);
    checkDateRange(f, f.eq(f.floorMonth, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>=($9, 2010-02-01 00:00:00), <($9, 2010-03-01 00:00:00))"));

    c.set(2010, Calendar.DECEMBER, 1, 0, 0, 0);
    checkDateRange(f, f.eq(f.floorMonth, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>=($9, 2010-12-01 00:00:00), <($9, 2011-01-01 00:00:00))"));

    c.set(2010, Calendar.FEBRUARY, 4, 0, 0, 0);
    checkDateRange(f, f.eq(f.floorDay, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>=($9, 2010-02-04 00:00:00), <($9, 2010-02-05 00:00:00))"));

    c.set(2010, Calendar.DECEMBER, 31, 0, 0, 0);
    checkDateRange(f, f.eq(f.floorDay, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>=($9, 2010-12-31 00:00:00), <($9, 2011-01-01 00:00:00))"));

    c.set(2010, Calendar.FEBRUARY, 4, 4, 0, 0);
    checkDateRange(f, f.eq(f.floorHour, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>=($9, 2010-02-04 04:00:00), <($9, 2010-02-04 05:00:00))"));

    c.set(2010, Calendar.DECEMBER, 31, 23, 0, 0);
    checkDateRange(f, f.eq(f.floorHour, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>=($9, 2010-12-31 23:00:00), <($9, 2011-01-01 00:00:00))"));

    c.set(2010, Calendar.FEBRUARY, 4, 2, 32, 0);
    checkDateRange(f,
        f.eq(f.floorMinute, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>=($9, 2010-02-04 02:32:00), <($9, 2010-02-04 02:33:00))"));

    c.set(2010, Calendar.FEBRUARY, 4, 2, 59, 0);
    checkDateRange(f,
        f.eq(f.floorMinute, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>=($9, 2010-02-04 02:59:00), <($9, 2010-02-04 03:00:00))"));
  }

  @Test public void testFloorLtRewrite() {
    final Calendar c = Util.calendar();

    c.clear();
    c.set(2010, Calendar.FEBRUARY, 10, 11, 12, 05);
    final Fixture2 f = new Fixture2();
    checkDateRange(f, f.lt(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("<($9, 2011-01-01 00:00:00)"));

    c.clear();
    c.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
    checkDateRange(f, f.lt(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("<($9, 2010-01-01 00:00:00)"));
  }

  @Test public void testFloorLeRewrite() {
    final Calendar c = Util.calendar();
    c.clear();
    c.set(2010, Calendar.FEBRUARY, 10, 11, 12, 05);
    final Fixture2 f = new Fixture2();
    checkDateRange(f, f.le(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("<($9, 2011-01-01 00:00:00)"));

    c.clear();
    c.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
    checkDateRange(f, f.le(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("<($9, 2011-01-01 00:00:00)"));
  }

  @Test public void testFloorGtRewrite() {
    final Calendar c = Util.calendar();
    c.clear();
    c.set(2010, Calendar.FEBRUARY, 10, 11, 12, 05);
    final Fixture2 f = new Fixture2();
    checkDateRange(f, f.gt(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is(">=($9, 2011-01-01 00:00:00)"));

    c.clear();
    c.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
    checkDateRange(f, f.gt(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is(">=($9, 2011-01-01 00:00:00)"));
  }

  @Test public void testFloorGeRewrite() {
    final Calendar c = Util.calendar();
    c.clear();
    c.set(2010, Calendar.FEBRUARY, 10, 11, 12, 05);
    final Fixture2 f = new Fixture2();
    checkDateRange(f, f.ge(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is(">=($9, 2011-01-01 00:00:00)"));

    c.clear();
    c.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
    checkDateRange(f, f.ge(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is(">=($9, 2010-01-01 00:00:00)"));
  }

  @Test public void testFloorExtractBothRewrite() {
    final Calendar c = Util.calendar();
    c.clear();
    Fixture2 f = new Fixture2();
    c.clear();
    c.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
    checkDateRange(f,
        f.and(f.eq(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
            f.eq(f.exMonthTs, f.literal(5))),
        is("AND(AND(>=($9, 2010-01-01 00:00:00), <($9, 2011-01-01 00:00:00)),"
            + " AND(>=($9, 2010-05-01 00:00:00), <($9, 2010-06-01 00:00:00)))"));

    // No lower range for floor
    checkDateRange(f,
        f.and(f.le(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
            f.eq(f.exMonthTs, f.literal(5))),
        is("AND(<($9, 2011-01-01 00:00:00), =(EXTRACT(FLAG(MONTH), $9), 5))"));

    // No lower range for floor
    checkDateRange(f,
        f.and(f.gt(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
            f.eq(f.exMonthTs, f.literal(5))),
        is("AND(>=($9, 2011-01-01 00:00:00), =(EXTRACT(FLAG(MONTH), $9), 5))"));

    // No upper range for individual floor rexNodes, but combined results in bounded interval
    checkDateRange(f,
        f.and(f.le(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
            f.eq(f.exMonthTs, f.literal(5)),
            f.ge(f.floorYear, f.timestampLiteral(TimestampString.fromCalendarFields(c)))),
        is("AND(<($9, 2011-01-01 00:00:00), AND(>=($9, 2010-05-01 00:00:00),"
            + " <($9, 2010-06-01 00:00:00)), >=($9, 2010-01-01 00:00:00))"));

  }

  @Test public void testCeilEqRewrite() {
    final Calendar c = Util.calendar();
    c.clear();
    c.set(2010, Calendar.FEBRUARY, 10, 11, 12, 05);
    final Fixture2 f = new Fixture2();
    // Always False
    checkDateRange(f, f.eq(f.ceilYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("false"));
    checkDateRange(f, f.eq(f.timestampLiteral(TimestampString.fromCalendarFields(c)), f.ceilYear),
        is("false"));

    c.clear();
    c.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
    checkDateRange(f, f.eq(f.ceilYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>($9, 2009-01-01 00:00:00), <=($9, 2010-01-01 00:00:00))"));

    c.set(2010, Calendar.FEBRUARY, 1, 0, 0, 0);
    checkDateRange(f, f.eq(f.ceilMonth, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>($9, 2010-01-01 00:00:00), <=($9, 2010-02-01 00:00:00))"));

    c.set(2010, Calendar.DECEMBER, 1, 0, 0, 0);
    checkDateRange(f, f.eq(f.ceilMonth, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>($9, 2010-11-01 00:00:00), <=($9, 2010-12-01 00:00:00))"));

    c.set(2010, Calendar.FEBRUARY, 4, 0, 0, 0);
    checkDateRange(f, f.eq(f.ceilDay, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>($9, 2010-02-03 00:00:00), <=($9, 2010-02-04 00:00:00))"));

    c.set(2010, Calendar.DECEMBER, 31, 0, 0, 0);
    checkDateRange(f, f.eq(f.ceilDay, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>($9, 2010-12-30 00:00:00), <=($9, 2010-12-31 00:00:00))"));

    c.set(2010, Calendar.FEBRUARY, 4, 4, 0, 0);
    checkDateRange(f, f.eq(f.ceilHour, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>($9, 2010-02-04 03:00:00), <=($9, 2010-02-04 04:00:00))"));

    c.set(2010, Calendar.DECEMBER, 31, 23, 0, 0);
    checkDateRange(f, f.eq(f.ceilHour, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>($9, 2010-12-31 22:00:00), <=($9, 2010-12-31 23:00:00))"));

    c.set(2010, Calendar.FEBRUARY, 4, 2, 32, 0);
    checkDateRange(f,
        f.eq(f.ceilMinute, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>($9, 2010-02-04 02:31:00), <=($9, 2010-02-04 02:32:00))"));

    c.set(2010, Calendar.FEBRUARY, 4, 2, 59, 0);
    checkDateRange(f,
        f.eq(f.ceilMinute, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("AND(>($9, 2010-02-04 02:58:00), <=($9, 2010-02-04 02:59:00))"));
  }

  @Test public void testCeilLtRewrite() {
    final Calendar c = Util.calendar();

    c.clear();
    c.set(2010, Calendar.FEBRUARY, 10, 11, 12, 05);
    final Fixture2 f = new Fixture2();
    checkDateRange(f, f.lt(f.ceilYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("<=($9, 2010-01-01 00:00:00)"));

    c.clear();
    c.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
    checkDateRange(f, f.lt(f.ceilYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("<=($9, 2009-01-01 00:00:00)"));
  }

  @Test public void testCeilLeRewrite() {
    final Calendar c = Util.calendar();
    c.clear();
    c.set(2010, Calendar.FEBRUARY, 10, 11, 12, 05);
    final Fixture2 f = new Fixture2();
    checkDateRange(f, f.le(f.ceilYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("<=($9, 2010-01-01 00:00:00)"));

    c.clear();
    c.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
    checkDateRange(f, f.le(f.ceilYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is("<=($9, 2010-01-01 00:00:00)"));
  }

  @Test public void testCeilGtRewrite() {
    final Calendar c = Util.calendar();
    c.clear();
    c.set(2010, Calendar.FEBRUARY, 10, 11, 12, 05);
    final Fixture2 f = new Fixture2();
    checkDateRange(f, f.gt(f.ceilYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is(">($9, 2010-01-01 00:00:00)"));

    c.clear();
    c.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
    checkDateRange(f, f.gt(f.ceilYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is(">($9, 2010-01-01 00:00:00)"));
  }

  @Test public void testCeilGeRewrite() {
    final Calendar c = Util.calendar();
    c.clear();
    c.set(2010, Calendar.FEBRUARY, 10, 11, 12, 05);
    final Fixture2 f = new Fixture2();
    checkDateRange(f, f.ge(f.ceilYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is(">($9, 2010-01-01 00:00:00)"));

    c.clear();
    c.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
    checkDateRange(f, f.ge(f.ceilYear, f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        is(">($9, 2009-01-01 00:00:00)"));
  }

  @Test public void testFloorRewriteWithTimezone() {
    final Calendar c = Util.calendar();
    c.clear();
    c.set(2010, Calendar.FEBRUARY, 1, 11, 30, 0);
    final Fixture2 f = new Fixture2();
    checkDateRange(f,
        f.eq(f.floorHour,
            f.timestampLocalTzLiteral(TimestampString.fromCalendarFields(c))),
        "IST",
        is("AND(>=($9, 2010-02-01 17:00:00), <($9, 2010-02-01 18:00:00))"),
        CoreMatchers.any(String.class));

    c.clear();
    c.set(2010, Calendar.FEBRUARY, 1, 11, 00, 0);
    checkDateRange(f,
        f.eq(f.floorHour,
            f.timestampLiteral(TimestampString.fromCalendarFields(c))),
        "IST",
        is("AND(>=($9, 2010-02-01 11:00:00), <($9, 2010-02-01 12:00:00))"),
        CoreMatchers.any(String.class));

    c.clear();
    c.set(2010, Calendar.FEBRUARY, 1, 00, 00, 0);
    checkDateRange(f,
        f.eq(f.floorHour, f.dateLiteral(DateString.fromCalendarFields(c))),
        "IST",
        is("AND(>=($9, 2010-02-01 00:00:00), <($9, 2010-02-01 01:00:00))"),
        CoreMatchers.any(String.class));
  }

  private static Set<TimeUnitRange> set(TimeUnitRange... es) {
    return ImmutableSet.copyOf(es);
  }

  private void checkDateRange(Fixture f, RexNode e, Matcher<String> matcher) {
    checkDateRange(f, e, "UTC", matcher, CoreMatchers.any(String.class));
  }

  private void checkDateRange(Fixture f, RexNode e, String timeZone,
      Matcher<String> matcher, Matcher<String> simplifyMatcher) {
    e = DateRangeRules.replaceTimeUnits(f.rexBuilder, e, timeZone);
    assertThat(e.toString(), matcher);
    final RexNode e2 = f.simplify.simplify(e);
    assertThat(e2.toString(), simplifyMatcher);
  }

  /** Common expressions across tests. */
  private static class Fixture2 extends Fixture {
    private final RexNode exYearTs; // EXTRACT YEAR from TIMESTAMP field
    private final RexNode exMonthTs; // EXTRACT MONTH from TIMESTAMP field
    private final RexNode exDayTs; // EXTRACT DAY from TIMESTAMP field
    private final RexNode exYearD; // EXTRACT YEAR from DATE field
    private final RexNode exMonthD; // EXTRACT MONTH from DATE field
    private final RexNode exDayD; // EXTRACT DAY from DATE field

    private final RexNode floorYear;
    private final RexNode floorMonth;
    private final RexNode floorDay;
    private final RexNode floorHour;
    private final RexNode floorMinute;

    private final RexNode ceilYear;
    private final RexNode ceilMonth;
    private final RexNode ceilDay;
    private final RexNode ceilHour;
    private final RexNode ceilMinute;

    Fixture2() {
      exYearTs = rexBuilder.makeCall(SqlStdOperatorTable.EXTRACT,
          ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.YEAR), ts));
      exMonthTs = rexBuilder.makeCall(intRelDataType,
          SqlStdOperatorTable.EXTRACT,
          ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.MONTH), ts));
      exDayTs = rexBuilder.makeCall(intRelDataType,
          SqlStdOperatorTable.EXTRACT,
          ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.DAY), ts));
      exYearD = rexBuilder.makeCall(SqlStdOperatorTable.EXTRACT,
          ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.YEAR), d));
      exMonthD = rexBuilder.makeCall(intRelDataType,
          SqlStdOperatorTable.EXTRACT,
          ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.MONTH), d));
      exDayD = rexBuilder.makeCall(intRelDataType,
          SqlStdOperatorTable.EXTRACT,
          ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.DAY), d));

      floorYear = rexBuilder.makeCall(intRelDataType, SqlStdOperatorTable.FLOOR,
          ImmutableList.of(ts, rexBuilder.makeFlag(TimeUnitRange.YEAR)));
      floorMonth = rexBuilder.makeCall(intRelDataType, SqlStdOperatorTable.FLOOR,
          ImmutableList.of(ts, rexBuilder.makeFlag(TimeUnitRange.MONTH)));
      floorDay = rexBuilder.makeCall(intRelDataType, SqlStdOperatorTable.FLOOR,
          ImmutableList.of(ts, rexBuilder.makeFlag(TimeUnitRange.DAY)));
      floorHour = rexBuilder.makeCall(intRelDataType, SqlStdOperatorTable.FLOOR,
          ImmutableList.of(ts, rexBuilder.makeFlag(TimeUnitRange.HOUR)));
      floorMinute = rexBuilder.makeCall(intRelDataType, SqlStdOperatorTable.FLOOR,
          ImmutableList.of(ts, rexBuilder.makeFlag(TimeUnitRange.MINUTE)));

      ceilYear = rexBuilder.makeCall(intRelDataType, SqlStdOperatorTable.CEIL,
          ImmutableList.of(ts, rexBuilder.makeFlag(TimeUnitRange.YEAR)));
      ceilMonth = rexBuilder.makeCall(intRelDataType, SqlStdOperatorTable.CEIL,
          ImmutableList.of(ts, rexBuilder.makeFlag(TimeUnitRange.MONTH)));
      ceilDay = rexBuilder.makeCall(intRelDataType, SqlStdOperatorTable.CEIL,
          ImmutableList.of(ts, rexBuilder.makeFlag(TimeUnitRange.DAY)));
      ceilHour = rexBuilder.makeCall(intRelDataType, SqlStdOperatorTable.CEIL,
          ImmutableList.of(ts, rexBuilder.makeFlag(TimeUnitRange.HOUR)));
      ceilMinute = rexBuilder.makeCall(intRelDataType, SqlStdOperatorTable.CEIL,
          ImmutableList.of(ts, rexBuilder.makeFlag(TimeUnitRange.MINUTE)));
    }
  }
}

// End DateRangeRulesTest.java
