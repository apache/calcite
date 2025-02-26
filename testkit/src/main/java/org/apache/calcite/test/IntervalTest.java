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
package org.apache.calcite.test;

/** Test cases for intervals.
 *
 * <p>Called, with varying implementations of {@link Fixture},
 * from both parser and validator test.
 */
public class IntervalTest {
  private final Fixture f;

  public IntervalTest(Fixture fixture) {
    this.f = fixture;
  }

  /** Runs all tests. */
  public void testAll() {
    // Tests that should pass both parser and validator
    subTestIntervalYearPositive();
    subTestIntervalYearToMonthPositive();
    subTestIntervalMonthPositive();
    subTestIntervalDayPositive();
    subTestIntervalDayToHourPositive();
    subTestIntervalDayToMinutePositive();
    subTestIntervalDayToSecondPositive();
    subTestIntervalHourPositive();
    subTestIntervalHourToMinutePositive();
    subTestIntervalHourToSecondPositive();
    subTestIntervalMinutePositive();
    subTestIntervalMinuteToSecondPositive();
    subTestIntervalSecondPositive();
    subTestIntervalWeekPositive();
    subTestIntervalQuarterPositive();
    subTestIntervalPlural();

    // Tests that should pass parser but fail validator
    subTestIntervalYearNegative();
    subTestIntervalYearToMonthNegative();
    subTestIntervalMonthNegative();
    subTestIntervalDayNegative();
    subTestIntervalDayToHourNegative();
    subTestIntervalDayToMinuteNegative();
    subTestIntervalDayToSecondNegative();
    subTestIntervalHourNegative();
    subTestIntervalHourToMinuteNegative();
    subTestIntervalHourToSecondNegative();
    subTestIntervalMinuteNegative();
    subTestIntervalMinuteToSecondNegative();
    subTestIntervalSecondNegative();

    subTestMisc();
  }

  /**
   * Runs tests for INTERVAL... YEAR that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalYearPositive() {
    // default precision
    f.expr("INTERVAL '1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    f.expr("INTERVAL '99' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '1' YEAR(2)")
        .columnType("INTERVAL YEAR(2) NOT NULL");
    f.expr("INTERVAL '99' YEAR(2)")
        .columnType("INTERVAL YEAR(2) NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647' YEAR(10)")
        .columnType("INTERVAL YEAR(10) NOT NULL");

    // min precision
    f.expr("INTERVAL '0' YEAR(1)")
        .columnType("INTERVAL YEAR(1) NOT NULL");

    // alternate precision
    f.expr("INTERVAL '1234' YEAR(4)")
        .columnType("INTERVAL YEAR(4) NOT NULL");

    // sign
    f.expr("INTERVAL '+1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    f.expr("INTERVAL '-1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    f.expr("INTERVAL +'1' YEAR")
        .assertParse("INTERVAL '1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    f.expr("INTERVAL +'+1' YEAR")
        .assertParse("INTERVAL '+1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    f.expr("INTERVAL +'-1' YEAR")
        .assertParse("INTERVAL '-1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    f.expr("INTERVAL -'1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    f.expr("INTERVAL -'+1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    f.expr("INTERVAL -'-1' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... YEAR TO MONTH that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalYearToMonthPositive() {
    // default precision
    f.expr("INTERVAL '1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    f.expr("INTERVAL '99-11' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    f.expr("INTERVAL '99-0' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '1-2' YEAR(2) TO MONTH")
        .columnType("INTERVAL YEAR(2) TO MONTH NOT NULL");
    f.expr("INTERVAL '99-11' YEAR(2) TO MONTH")
        .columnType("INTERVAL YEAR(2) TO MONTH NOT NULL");
    f.expr("INTERVAL '99-0' YEAR(2) TO MONTH")
        .columnType("INTERVAL YEAR(2) TO MONTH NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647-11' YEAR(10) TO MONTH")
        .columnType("INTERVAL YEAR(10) TO MONTH NOT NULL");

    // min precision
    f.expr("INTERVAL '0-0' YEAR(1) TO MONTH")
        .columnType("INTERVAL YEAR(1) TO MONTH NOT NULL");

    // alternate precision
    f.expr("INTERVAL '2006-2' YEAR(4) TO MONTH")
        .columnType("INTERVAL YEAR(4) TO MONTH NOT NULL");

    // sign
    f.expr("INTERVAL '-1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    f.expr("INTERVAL '+1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    f.expr("INTERVAL +'1-2' YEAR TO MONTH")
        .assertParse("INTERVAL '1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    f.expr("INTERVAL +'-1-2' YEAR TO MONTH")
        .assertParse("INTERVAL '-1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    f.expr("INTERVAL +'+1-2' YEAR TO MONTH")
        .assertParse("INTERVAL '+1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    f.expr("INTERVAL -'1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    f.expr("INTERVAL -'-1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    f.expr("INTERVAL -'+1-2' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... MONTH that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalMonthPositive() {
    // default precision
    f.expr("INTERVAL '1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    f.expr("INTERVAL '99' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '1' MONTH(2)")
        .columnType("INTERVAL MONTH(2) NOT NULL");
    f.expr("INTERVAL '99' MONTH(2)")
        .columnType("INTERVAL MONTH(2) NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647' MONTH(10)")
        .columnType("INTERVAL MONTH(10) NOT NULL");

    // min precision
    f.expr("INTERVAL '0' MONTH(1)")
        .columnType("INTERVAL MONTH(1) NOT NULL");

    // alternate precision
    f.expr("INTERVAL '1234' MONTH(4)")
        .columnType("INTERVAL MONTH(4) NOT NULL");

    // sign
    f.expr("INTERVAL '+1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    f.expr("INTERVAL '-1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    f.expr("INTERVAL +'1' MONTH")
        .assertParse("INTERVAL '1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    f.expr("INTERVAL +'+1' MONTH")
        .assertParse("INTERVAL '+1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    f.expr("INTERVAL +'-1' MONTH")
        .assertParse("INTERVAL '-1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    f.expr("INTERVAL -'1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    f.expr("INTERVAL -'+1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    f.expr("INTERVAL -'-1' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... DAY that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalDayPositive() {
    // default precision
    f.expr("INTERVAL '1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    f.expr("INTERVAL '99' DAY")
        .columnType("INTERVAL DAY NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '1' DAY(2)")
        .columnType("INTERVAL DAY(2) NOT NULL");
    f.expr("INTERVAL '99' DAY(2)")
        .columnType("INTERVAL DAY(2) NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647' DAY(10)")
        .columnType("INTERVAL DAY(10) NOT NULL");

    // min precision
    f.expr("INTERVAL '0' DAY(1)")
        .columnType("INTERVAL DAY(1) NOT NULL");

    // alternate precision
    f.expr("INTERVAL '1234' DAY(4)")
        .columnType("INTERVAL DAY(4) NOT NULL");

    // sign
    f.expr("INTERVAL '+1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    f.expr("INTERVAL '-1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    f.expr("INTERVAL +'1' DAY")
        .assertParse("INTERVAL '1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    f.expr("INTERVAL +'+1' DAY")
        .assertParse("INTERVAL '+1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    f.expr("INTERVAL +'-1' DAY")
        .assertParse("INTERVAL '-1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    f.expr("INTERVAL -'1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    f.expr("INTERVAL -'+1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    f.expr("INTERVAL -'-1' DAY")
        .columnType("INTERVAL DAY NOT NULL");
  }

  public void subTestIntervalDayToHourPositive() {
    // default precision
    f.expr("INTERVAL '1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    f.expr("INTERVAL '99 23' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    f.expr("INTERVAL '99 0' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '1 2' DAY(2) TO HOUR")
        .columnType("INTERVAL DAY(2) TO HOUR NOT NULL");
    f.expr("INTERVAL '99 23' DAY(2) TO HOUR")
        .columnType("INTERVAL DAY(2) TO HOUR NOT NULL");
    f.expr("INTERVAL '99 0' DAY(2) TO HOUR")
        .columnType("INTERVAL DAY(2) TO HOUR NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647 23' DAY(10) TO HOUR")
        .columnType("INTERVAL DAY(10) TO HOUR NOT NULL");

    // min precision
    f.expr("INTERVAL '0 0' DAY(1) TO HOUR")
        .columnType("INTERVAL DAY(1) TO HOUR NOT NULL");

    // alternate precision
    f.expr("INTERVAL '2345 2' DAY(4) TO HOUR")
        .columnType("INTERVAL DAY(4) TO HOUR NOT NULL");

    // sign
    f.expr("INTERVAL '-1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    f.expr("INTERVAL '+1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    f.expr("INTERVAL +'1 2' DAY TO HOUR")
        .assertParse("INTERVAL '1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    f.expr("INTERVAL +'-1 2' DAY TO HOUR")
        .assertParse("INTERVAL '-1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    f.expr("INTERVAL +'+1 2' DAY TO HOUR")
        .assertParse("INTERVAL '+1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    f.expr("INTERVAL -'1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    f.expr("INTERVAL -'-1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    f.expr("INTERVAL -'+1 2' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... DAY TO MINUTE that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalDayToMinutePositive() {
    // default precision
    f.expr("INTERVAL '1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    f.expr("INTERVAL '99 23:59' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    f.expr("INTERVAL '99 0:0' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '1 2:3' DAY(2) TO MINUTE")
        .columnType("INTERVAL DAY(2) TO MINUTE NOT NULL");
    f.expr("INTERVAL '99 23:59' DAY(2) TO MINUTE")
        .columnType("INTERVAL DAY(2) TO MINUTE NOT NULL");
    f.expr("INTERVAL '99 0:0' DAY(2) TO MINUTE")
        .columnType("INTERVAL DAY(2) TO MINUTE NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647 23:59' DAY(10) TO MINUTE")
        .columnType("INTERVAL DAY(10) TO MINUTE NOT NULL");

    // min precision
    f.expr("INTERVAL '0 0:0' DAY(1) TO MINUTE")
        .columnType("INTERVAL DAY(1) TO MINUTE NOT NULL");

    // alternate precision
    f.expr("INTERVAL '2345 6:7' DAY(4) TO MINUTE")
        .columnType("INTERVAL DAY(4) TO MINUTE NOT NULL");

    // sign
    f.expr("INTERVAL '-1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    f.expr("INTERVAL '+1 2:3' DAY TO MINUTE")
        .assertParse("INTERVAL '+1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    f.expr("INTERVAL +'1 2:3' DAY TO MINUTE")
        .assertParse("INTERVAL '1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    f.expr("INTERVAL +'-1 2:3' DAY TO MINUTE")
        .assertParse("INTERVAL '-1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    f.expr("INTERVAL +'+1 2:3' DAY TO MINUTE")
        .assertParse("INTERVAL '+1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    f.expr("INTERVAL -'1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    f.expr("INTERVAL -'-1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    f.expr("INTERVAL -'+1 2:3' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... DAY TO SECOND that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalDayToSecondPositive() {
    // default precision
    f.expr("INTERVAL '1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    f.expr("INTERVAL '99 23:59:59' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    f.expr("INTERVAL '99 0:0:0' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    f.expr("INTERVAL '99 23:59:59.999999' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    f.expr("INTERVAL '99 0:0:0.0' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '1 2:3:4' DAY(2) TO SECOND")
        .columnType("INTERVAL DAY(2) TO SECOND NOT NULL");
    f.expr("INTERVAL '99 23:59:59' DAY(2) TO SECOND")
        .columnType("INTERVAL DAY(2) TO SECOND NOT NULL");
    f.expr("INTERVAL '99 0:0:0' DAY(2) TO SECOND")
        .columnType("INTERVAL DAY(2) TO SECOND NOT NULL");
    f.expr("INTERVAL '99 23:59:59.999999' DAY TO SECOND(6)")
        .columnType("INTERVAL DAY TO SECOND(6) NOT NULL");
    f.expr("INTERVAL '99 0:0:0.0' DAY TO SECOND(6)")
        .columnType("INTERVAL DAY TO SECOND(6) NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647 23:59:59' DAY(10) TO SECOND")
        .columnType("INTERVAL DAY(10) TO SECOND NOT NULL");
    f.expr("INTERVAL '2147483647 23:59:59.999999999' DAY(10) TO SECOND(9)")
        .columnType("INTERVAL DAY(10) TO SECOND(9) NOT NULL");

    // min precision
    f.expr("INTERVAL '0 0:0:0' DAY(1) TO SECOND")
        .columnType("INTERVAL DAY(1) TO SECOND NOT NULL");
    f.expr("INTERVAL '0 0:0:0.0' DAY(1) TO SECOND(1)")
        .columnType("INTERVAL DAY(1) TO SECOND(1) NOT NULL");

    // alternate precision
    f.expr("INTERVAL '2345 6:7:8' DAY(4) TO SECOND")
        .columnType("INTERVAL DAY(4) TO SECOND NOT NULL");
    f.expr("INTERVAL '2345 6:7:8.9012' DAY(4) TO SECOND(4)")
        .columnType("INTERVAL DAY(4) TO SECOND(4) NOT NULL");

    // sign
    f.expr("INTERVAL '-1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    f.expr("INTERVAL '+1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    f.expr("INTERVAL +'1 2:3:4' DAY TO SECOND")
        .assertParse("INTERVAL '1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    f.expr("INTERVAL +'-1 2:3:4' DAY TO SECOND")
        .assertParse("INTERVAL '-1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    f.expr("INTERVAL +'+1 2:3:4' DAY TO SECOND")
        .assertParse("INTERVAL '+1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    f.expr("INTERVAL -'1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    f.expr("INTERVAL -'-1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
    f.expr("INTERVAL -'+1 2:3:4' DAY TO SECOND")
        .columnType("INTERVAL DAY TO SECOND NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... HOUR that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalHourPositive() {
    // default precision
    f.expr("INTERVAL '1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    f.expr("INTERVAL '99' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '1' HOUR(2)")
        .columnType("INTERVAL HOUR(2) NOT NULL");
    f.expr("INTERVAL '99' HOUR(2)")
        .columnType("INTERVAL HOUR(2) NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647' HOUR(10)")
        .columnType("INTERVAL HOUR(10) NOT NULL");

    // min precision
    f.expr("INTERVAL '0' HOUR(1)")
        .columnType("INTERVAL HOUR(1) NOT NULL");

    // alternate precision
    f.expr("INTERVAL '1234' HOUR(4)")
        .columnType("INTERVAL HOUR(4) NOT NULL");

    // sign
    f.expr("INTERVAL '+1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    f.expr("INTERVAL '-1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    f.expr("INTERVAL +'1' HOUR")
        .assertParse("INTERVAL '1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    f.expr("INTERVAL +'+1' HOUR")
        .assertParse("INTERVAL '+1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    f.expr("INTERVAL +'-1' HOUR")
        .assertParse("INTERVAL '-1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    f.expr("INTERVAL -'1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    f.expr("INTERVAL -'+1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    f.expr("INTERVAL -'-1' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... HOUR TO MINUTE that should pass both parser
   * and validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalHourToMinutePositive() {
    // default precision
    f.expr("INTERVAL '2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    f.expr("INTERVAL '23:59' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    f.expr("INTERVAL '99:0' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '2:3' HOUR(2) TO MINUTE")
        .columnType("INTERVAL HOUR(2) TO MINUTE NOT NULL");
    f.expr("INTERVAL '23:59' HOUR(2) TO MINUTE")
        .columnType("INTERVAL HOUR(2) TO MINUTE NOT NULL");
    f.expr("INTERVAL '99:0' HOUR(2) TO MINUTE")
        .columnType("INTERVAL HOUR(2) TO MINUTE NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647:59' HOUR(10) TO MINUTE")
        .columnType("INTERVAL HOUR(10) TO MINUTE NOT NULL");

    // min precision
    f.expr("INTERVAL '0:0' HOUR(1) TO MINUTE")
        .columnType("INTERVAL HOUR(1) TO MINUTE NOT NULL");

    // alternate precision
    f.expr("INTERVAL '2345:7' HOUR(4) TO MINUTE")
        .columnType("INTERVAL HOUR(4) TO MINUTE NOT NULL");

    // sign
    f.expr("INTERVAL '-1:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    f.expr("INTERVAL '+1:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    f.expr("INTERVAL +'2:3' HOUR TO MINUTE")
        .assertParse("INTERVAL '2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    f.expr("INTERVAL +'-2:3' HOUR TO MINUTE")
        .assertParse("INTERVAL '-2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    f.expr("INTERVAL +'+2:3' HOUR TO MINUTE")
        .assertParse("INTERVAL '+2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    f.expr("INTERVAL -'2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    f.expr("INTERVAL -'-2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    f.expr("INTERVAL -'+2:3' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... HOUR TO SECOND that should pass both parser
   * and validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalHourToSecondPositive() {
    // default precision
    f.expr("INTERVAL '2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    f.expr("INTERVAL '23:59:59' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    f.expr("INTERVAL '99:0:0' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    f.expr("INTERVAL '23:59:59.999999' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    f.expr("INTERVAL '99:0:0.0' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '2:3:4' HOUR(2) TO SECOND")
        .columnType("INTERVAL HOUR(2) TO SECOND NOT NULL");
    f.expr("INTERVAL '99:59:59' HOUR(2) TO SECOND")
        .columnType("INTERVAL HOUR(2) TO SECOND NOT NULL");
    f.expr("INTERVAL '99:0:0' HOUR(2) TO SECOND")
        .columnType("INTERVAL HOUR(2) TO SECOND NOT NULL");
    f.expr("INTERVAL '99:59:59.999999' HOUR TO SECOND(6)")
        .columnType("INTERVAL HOUR TO SECOND(6) NOT NULL");
    f.expr("INTERVAL '99:0:0.0' HOUR TO SECOND(6)")
        .columnType("INTERVAL HOUR TO SECOND(6) NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647:59:59' HOUR(10) TO SECOND")
        .columnType("INTERVAL HOUR(10) TO SECOND NOT NULL");
    f.expr("INTERVAL '2147483647:59:59.999999999' HOUR(10) TO SECOND(9)")
        .columnType("INTERVAL HOUR(10) TO SECOND(9) NOT NULL");

    // min precision
    f.expr("INTERVAL '0:0:0' HOUR(1) TO SECOND")
        .columnType("INTERVAL HOUR(1) TO SECOND NOT NULL");
    f.expr("INTERVAL '0:0:0.0' HOUR(1) TO SECOND(1)")
        .columnType("INTERVAL HOUR(1) TO SECOND(1) NOT NULL");

    // alternate precision
    f.expr("INTERVAL '2345:7:8' HOUR(4) TO SECOND")
        .columnType("INTERVAL HOUR(4) TO SECOND NOT NULL");
    f.expr("INTERVAL '2345:7:8.9012' HOUR(4) TO SECOND(4)")
        .columnType("INTERVAL HOUR(4) TO SECOND(4) NOT NULL");

    // sign
    f.expr("INTERVAL '-2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    f.expr("INTERVAL '+2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    f.expr("INTERVAL +'2:3:4' HOUR TO SECOND")
        .assertParse("INTERVAL '2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    f.expr("INTERVAL +'-2:3:4' HOUR TO SECOND")
        .assertParse("INTERVAL '-2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    f.expr("INTERVAL +'+2:3:4' HOUR TO SECOND")
        .assertParse("INTERVAL '+2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    f.expr("INTERVAL -'2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    f.expr("INTERVAL -'-2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    f.expr("INTERVAL -'+2:3:4' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... MINUTE that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalMinutePositive() {
    // default precision
    f.expr("INTERVAL '1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    f.expr("INTERVAL '99' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '1' MINUTE(2)")
        .columnType("INTERVAL MINUTE(2) NOT NULL");
    f.expr("INTERVAL '99' MINUTE(2)")
        .columnType("INTERVAL MINUTE(2) NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647' MINUTE(10)")
        .columnType("INTERVAL MINUTE(10) NOT NULL");

    // min precision
    f.expr("INTERVAL '0' MINUTE(1)")
        .columnType("INTERVAL MINUTE(1) NOT NULL");

    // alternate precision
    f.expr("INTERVAL '1234' MINUTE(4)")
        .columnType("INTERVAL MINUTE(4) NOT NULL");

    // sign
    f.expr("INTERVAL '+1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    f.expr("INTERVAL '-1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    f.expr("INTERVAL +'1' MINUTE")
        .assertParse("INTERVAL '1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    f.expr("INTERVAL +'+1' MINUTE")
        .assertParse("INTERVAL '+1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    f.expr("INTERVAL +'-1' MINUTE")
        .assertParse("INTERVAL '-1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    f.expr("INTERVAL -'1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    f.expr("INTERVAL -'+1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    f.expr("INTERVAL -'-1' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... MINUTE TO SECOND that should pass both parser
   * and validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalMinuteToSecondPositive() {
    // default precision
    f.expr("INTERVAL '2:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    f.expr("INTERVAL '59:59' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    f.expr("INTERVAL '99:0' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    f.expr("INTERVAL '59:59.999999' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    f.expr("INTERVAL '99:0.0' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '2:4' MINUTE(2) TO SECOND")
        .columnType("INTERVAL MINUTE(2) TO SECOND NOT NULL");
    f.expr("INTERVAL '99:59' MINUTE(2) TO SECOND")
        .columnType("INTERVAL MINUTE(2) TO SECOND NOT NULL");
    f.expr("INTERVAL '99:0' MINUTE(2) TO SECOND")
        .columnType("INTERVAL MINUTE(2) TO SECOND NOT NULL");
    f.expr("INTERVAL '99:59.999999' MINUTE TO SECOND(6)")
        .columnType("INTERVAL MINUTE TO SECOND(6) NOT NULL");
    f.expr("INTERVAL '99:0.0' MINUTE TO SECOND(6)")
        .columnType("INTERVAL MINUTE TO SECOND(6) NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647:59' MINUTE(10) TO SECOND")
        .columnType("INTERVAL MINUTE(10) TO SECOND NOT NULL");
    f.expr("INTERVAL '2147483647:59.999999999' MINUTE(10) TO SECOND(9)")
        .columnType("INTERVAL MINUTE(10) TO SECOND(9) NOT NULL");

    // min precision
    f.expr("INTERVAL '0:0' MINUTE(1) TO SECOND")
        .columnType("INTERVAL MINUTE(1) TO SECOND NOT NULL");
    f.expr("INTERVAL '0:0.0' MINUTE(1) TO SECOND(1)")
        .columnType("INTERVAL MINUTE(1) TO SECOND(1) NOT NULL");

    // alternate precision
    f.expr("INTERVAL '2345:8' MINUTE(4) TO SECOND")
        .columnType("INTERVAL MINUTE(4) TO SECOND NOT NULL");
    f.expr("INTERVAL '2345:7.8901' MINUTE(4) TO SECOND(4)")
        .columnType("INTERVAL MINUTE(4) TO SECOND(4) NOT NULL");

    // sign
    f.expr("INTERVAL '-3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    f.expr("INTERVAL '+3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    f.expr("INTERVAL +'3:4' MINUTE TO SECOND")
        .assertParse("INTERVAL '3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    f.expr("INTERVAL +'-3:4' MINUTE TO SECOND")
        .assertParse("INTERVAL '-3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    f.expr("INTERVAL +'+3:4' MINUTE TO SECOND")
        .assertParse("INTERVAL '+3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    f.expr("INTERVAL -'3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    f.expr("INTERVAL -'-3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    f.expr("INTERVAL -'+3:4' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... SECOND that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalSecondPositive() {
    // default precision
    f.expr("INTERVAL '1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    f.expr("INTERVAL '99' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '1' SECOND(2)")
        .columnType("INTERVAL SECOND(2) NOT NULL");
    f.expr("INTERVAL '99' SECOND(2)")
        .columnType("INTERVAL SECOND(2) NOT NULL");
    f.expr("INTERVAL '1' SECOND(2, 6)")
        .columnType("INTERVAL SECOND(2, 6) NOT NULL");
    f.expr("INTERVAL '99' SECOND(2, 6)")
        .columnType("INTERVAL SECOND(2, 6) NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647' SECOND(10)")
        .columnType("INTERVAL SECOND(10) NOT NULL");
    f.expr("INTERVAL '2147483647.999999999' SECOND(10, 9)")
        .columnType("INTERVAL SECOND(10, 9) NOT NULL");

    // min precision
    f.expr("INTERVAL '0' SECOND(1)")
        .columnType("INTERVAL SECOND(1) NOT NULL");
    f.expr("INTERVAL '0.0' SECOND(1, 1)")
        .columnType("INTERVAL SECOND(1, 1) NOT NULL");

    // alternate precision
    f.expr("INTERVAL '1234' SECOND(4)")
        .columnType("INTERVAL SECOND(4) NOT NULL");
    f.expr("INTERVAL '1234.56789' SECOND(4, 5)")
        .columnType("INTERVAL SECOND(4, 5) NOT NULL");

    // sign
    f.expr("INTERVAL '+1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    f.expr("INTERVAL '-1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    f.expr("INTERVAL +'1' SECOND")
        .assertParse("INTERVAL '1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    f.expr("INTERVAL +'+1' SECOND")
        .assertParse("INTERVAL '+1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    f.expr("INTERVAL +'-1' SECOND")
        .assertParse("INTERVAL '-1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    f.expr("INTERVAL -'1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    f.expr("INTERVAL -'+1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    f.expr("INTERVAL -'-1' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... YEAR that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalYearNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL '-' YEAR")
        .fails("Illegal interval literal format '-' for INTERVAL YEAR.*");
    f.wholeExpr("INTERVAL '1-2' YEAR")
        .fails("Illegal interval literal format '1-2' for INTERVAL YEAR.*");
    f.wholeExpr("INTERVAL '1.2' YEAR")
        .fails("Illegal interval literal format '1.2' for INTERVAL YEAR.*");
    f.wholeExpr("INTERVAL '1 2' YEAR")
        .fails("Illegal interval literal format '1 2' for INTERVAL YEAR.*");
    f.wholeExpr("INTERVAL '1-2' YEAR(2)")
        .fails("Illegal interval literal format '1-2' for INTERVAL YEAR\\(2\\)");
    f.wholeExpr("INTERVAL 'bogus text' YEAR")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL YEAR.*");

    // negative field values
    f.wholeExpr("INTERVAL '--1' YEAR")
        .fails("Illegal interval literal format '--1' for INTERVAL YEAR.*");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    f.wholeExpr("INTERVAL '100' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
    f.wholeExpr("INTERVAL '100' YEAR(2)")
        .fails("Interval field value 100 exceeds precision of YEAR\\(2\\) field.*");
    f.wholeExpr("INTERVAL '1000' YEAR(3)")
        .fails("Interval field value 1,000 exceeds precision of YEAR\\(3\\) field.*");
    f.wholeExpr("INTERVAL '-1000' YEAR(3)")
        .fails("Interval field value -1,000 exceeds precision of YEAR\\(3\\) field.*");
    f.wholeExpr("INTERVAL '2147483648' YEAR(10)")
        .fails("Interval field value 2,147,483,648 exceeds precision of "
            + "YEAR\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648' YEAR(10)")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "YEAR\\(10\\) field");

    // precision > maximum
    f.expr("INTERVAL '1' ^YEAR(11)^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL YEAR\\(11\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0' ^YEAR(0)^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL YEAR\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... YEAR TO MONTH that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalYearToMonthNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL '-' YEAR TO MONTH")
        .fails("Illegal interval literal format '-' for INTERVAL YEAR TO MONTH");
    f.wholeExpr("INTERVAL '1' YEAR TO MONTH")
        .fails("Illegal interval literal format '1' for INTERVAL YEAR TO MONTH");
    f.wholeExpr("INTERVAL '1:2' YEAR TO MONTH")
        .fails("Illegal interval literal format '1:2' for INTERVAL YEAR TO MONTH");
    f.wholeExpr("INTERVAL '1.2' YEAR TO MONTH")
        .fails("Illegal interval literal format '1.2' for INTERVAL YEAR TO MONTH");
    f.wholeExpr("INTERVAL '1 2' YEAR TO MONTH")
        .fails("Illegal interval literal format '1 2' for INTERVAL YEAR TO MONTH");
    f.wholeExpr("INTERVAL '1:2' YEAR(2) TO MONTH")
        .fails("Illegal interval literal format '1:2' for "
            + "INTERVAL YEAR\\(2\\) TO MONTH");
    f.wholeExpr("INTERVAL 'bogus text' YEAR TO MONTH")
        .fails("Illegal interval literal format 'bogus text' for "
            + "INTERVAL YEAR TO MONTH");

    // negative field values
    f.wholeExpr("INTERVAL '--1-2' YEAR TO MONTH")
        .fails("Illegal interval literal format '--1-2' for "
            + "INTERVAL YEAR TO MONTH");
    f.wholeExpr("INTERVAL '1--2' YEAR TO MONTH")
        .fails("Illegal interval literal format '1--2' for "
            + "INTERVAL YEAR TO MONTH");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    f.wholeExpr("INTERVAL '100-0' YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    f.wholeExpr("INTERVAL '100-0' YEAR(2) TO MONTH")
        .fails("Interval field value 100 exceeds precision of YEAR\\(2\\) field.*");
    f.wholeExpr("INTERVAL '1000-0' YEAR(3) TO MONTH")
        .fails("Interval field value 1,000 exceeds precision of YEAR\\(3\\) field.*");
    f.wholeExpr("INTERVAL '-1000-0' YEAR(3) TO MONTH")
        .fails("Interval field value -1,000 exceeds precision of YEAR\\(3\\) field.*");
    f.wholeExpr("INTERVAL '2147483648-0' YEAR(10) TO MONTH")
        .fails("Interval field value 2,147,483,648 exceeds precision of YEAR\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648-0' YEAR(10) TO MONTH")
        .fails("Interval field value -2,147,483,648 exceeds precision of YEAR\\(10\\) field.*");
    f.wholeExpr("INTERVAL '1-12' YEAR TO MONTH")
        .fails("Illegal interval literal format '1-12' for INTERVAL YEAR TO MONTH.*");

    // precision > maximum
    f.expr("INTERVAL '1-1' ^YEAR(11) TO MONTH^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL YEAR\\(11\\) TO MONTH");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0-0' ^YEAR(0) TO MONTH^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL YEAR\\(0\\) TO MONTH");
  }

  /**
   * Runs tests for INTERVAL... WEEK that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalWeekPositive() {
    // default precision
    f.expr("INTERVAL '1' WEEK")
        .columnType("INTERVAL WEEK NOT NULL");
    f.expr("INTERVAL '99' WEEK")
        .columnType("INTERVAL WEEK NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '1' WEEK(2)")
        .columnType("INTERVAL WEEK(2) NOT NULL");
    f.expr("INTERVAL '99' WEEK(2)")
        .columnType("INTERVAL WEEK(2) NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647' WEEK(10)")
        .columnType("INTERVAL WEEK(10) NOT NULL");

    // min precision
    f.expr("INTERVAL '0' WEEK(1)")
        .columnType("INTERVAL WEEK(1) NOT NULL");

    // alternate precision
    f.expr("INTERVAL '1234' WEEK(4)")
        .columnType("INTERVAL WEEK(4) NOT NULL");

    // sign
    f.expr("INTERVAL '+1' WEEK")
        .columnType("INTERVAL WEEK NOT NULL");
    f.expr("INTERVAL '-1' WEEK")
        .columnType("INTERVAL WEEK NOT NULL");
    f.expr("INTERVAL +'1' WEEK")
        .assertParse("INTERVAL '1' WEEK")
        .columnType("INTERVAL WEEK NOT NULL");
    f.expr("INTERVAL +'+1' WEEK")
        .assertParse("INTERVAL '+1' WEEK")
        .columnType("INTERVAL WEEK NOT NULL");
    f.expr("INTERVAL +'-1' WEEK")
        .assertParse("INTERVAL '-1' WEEK")
        .columnType("INTERVAL WEEK NOT NULL");
    f.expr("INTERVAL -'1' WEEK")
        .columnType("INTERVAL WEEK NOT NULL");
    f.expr("INTERVAL -'+1' WEEK")
        .columnType("INTERVAL WEEK NOT NULL");
    f.expr("INTERVAL -'-1' WEEK")
        .columnType("INTERVAL WEEK NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... QUARTER that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalQuarterPositive() {
    // default precision
    f.expr("INTERVAL '1' QUARTER")
        .columnType("INTERVAL QUARTER NOT NULL");
    f.expr("INTERVAL '99' QUARTER")
        .columnType("INTERVAL QUARTER NOT NULL");

    // explicit precision equal to default
    f.expr("INTERVAL '1' QUARTER(2)")
        .columnType("INTERVAL QUARTER(2) NOT NULL");
    f.expr("INTERVAL '99' QUARTER(2)")
        .columnType("INTERVAL QUARTER(2) NOT NULL");

    // max precision
    f.expr("INTERVAL '2147483647' QUARTER(10)")
        .columnType("INTERVAL QUARTER(10) NOT NULL");

    // min precision
    f.expr("INTERVAL '0' QUARTER(1)")
        .columnType("INTERVAL QUARTER(1) NOT NULL");

    // alternate precision
    f.expr("INTERVAL '1234' QUARTER(4)")
        .columnType("INTERVAL QUARTER(4) NOT NULL");

    // sign
    f.expr("INTERVAL '+1' QUARTER")
        .columnType("INTERVAL QUARTER NOT NULL");
    f.expr("INTERVAL '-1' QUARTER")
        .columnType("INTERVAL QUARTER NOT NULL");
    f.expr("INTERVAL +'1' QUARTER")
        .assertParse("INTERVAL '1' QUARTER")
        .columnType("INTERVAL QUARTER NOT NULL");
    f.expr("INTERVAL +'+1' QUARTER")
        .assertParse("INTERVAL '+1' QUARTER")
        .columnType("INTERVAL QUARTER NOT NULL");
    f.expr("INTERVAL +'-1' QUARTER")
        .assertParse("INTERVAL '-1' QUARTER")
        .columnType("INTERVAL QUARTER NOT NULL");
    f.expr("INTERVAL -'1' QUARTER")
        .columnType("INTERVAL QUARTER NOT NULL");
    f.expr("INTERVAL -'+1' QUARTER")
        .columnType("INTERVAL QUARTER NOT NULL");
    f.expr("INTERVAL -'-1' QUARTER")
        .columnType("INTERVAL QUARTER NOT NULL");
  }

  public void subTestIntervalPlural() {
    f.expr("INTERVAL '+2' SECONDS")
        .assertParse("INTERVAL '+2' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    f.expr("INTERVAL '+2' HOURS")
        .assertParse("INTERVAL '+2' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    f.expr("INTERVAL '+2' DAYS")
        .assertParse("INTERVAL '+2' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    f.expr("INTERVAL '+2' WEEKS")
        .assertParse("INTERVAL '+2' WEEK")
        .columnType("INTERVAL WEEK NOT NULL");
    f.expr("INTERVAL '+2' QUARTERS")
        .assertParse("INTERVAL '+2' QUARTER")
        .columnType("INTERVAL QUARTER NOT NULL");
    f.expr("INTERVAL '+2' MONTHS")
        .assertParse("INTERVAL '+2' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    f.expr("INTERVAL '+2' YEARS")
        .assertParse("INTERVAL '+2' YEAR")
        .columnType("INTERVAL YEAR NOT NULL");
  }

  /**
   * Runs tests for INTERVAL... MONTH that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalMonthNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL '-' MONTH")
        .fails("Illegal interval literal format '-' for INTERVAL MONTH.*");
    f.wholeExpr("INTERVAL '1-2' MONTH")
        .fails("Illegal interval literal format '1-2' for INTERVAL MONTH.*");
    f.wholeExpr("INTERVAL '1.2' MONTH")
        .fails("Illegal interval literal format '1.2' for INTERVAL MONTH.*");
    f.wholeExpr("INTERVAL '1 2' MONTH")
        .fails("Illegal interval literal format '1 2' for INTERVAL MONTH.*");
    f.wholeExpr("INTERVAL '1-2' MONTH(2)")
        .fails("Illegal interval literal format '1-2' for INTERVAL MONTH\\(2\\)");
    f.wholeExpr("INTERVAL 'bogus text' MONTH")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL MONTH.*");

    // negative field values
    f.wholeExpr("INTERVAL '--1' MONTH")
        .fails("Illegal interval literal format '--1' for INTERVAL MONTH.*");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    f.wholeExpr("INTERVAL '100' MONTH")
        .columnType("INTERVAL MONTH NOT NULL");
    f.wholeExpr("INTERVAL '100' MONTH(2)")
        .fails("Interval field value 100 exceeds precision of MONTH\\(2\\) field.*");
    f.wholeExpr("INTERVAL '1000' MONTH(3)")
        .fails("Interval field value 1,000 exceeds precision of MONTH\\(3\\) field.*");
    f.wholeExpr("INTERVAL '-1000' MONTH(3)")
        .fails("Interval field value -1,000 exceeds precision of MONTH\\(3\\) field.*");
    f.wholeExpr("INTERVAL '2147483648' MONTH(10)")
        .fails("Interval field value 2,147,483,648 exceeds precision of MONTH\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648' MONTH(10)")
        .fails("Interval field value -2,147,483,648 exceeds precision of MONTH\\(10\\) field.*");

    // precision > maximum
    f.expr("INTERVAL '1' ^MONTH(11)^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL MONTH\\(11\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0' ^MONTH(0)^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL MONTH\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... DAY that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalDayNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL '-' DAY")
        .fails("Illegal interval literal format '-' for INTERVAL DAY.*");
    f.wholeExpr("INTERVAL '1-2' DAY")
        .fails("Illegal interval literal format '1-2' for INTERVAL DAY.*");
    f.wholeExpr("INTERVAL '1.2' DAY")
        .fails("Illegal interval literal format '1.2' for INTERVAL DAY.*");
    f.wholeExpr("INTERVAL '1 2' DAY")
        .fails("Illegal interval literal format '1 2' for INTERVAL DAY.*");
    f.wholeExpr("INTERVAL '1:2' DAY")
        .fails("Illegal interval literal format '1:2' for INTERVAL DAY.*");
    f.wholeExpr("INTERVAL '1-2' DAY(2)")
        .fails("Illegal interval literal format '1-2' for INTERVAL DAY\\(2\\)");
    f.wholeExpr("INTERVAL 'bogus text' DAY")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL DAY.*");

    // negative field values
    f.wholeExpr("INTERVAL '--1' DAY")
        .fails("Illegal interval literal format '--1' for INTERVAL DAY.*");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    f.wholeExpr("INTERVAL '100' DAY")
        .columnType("INTERVAL DAY NOT NULL");
    f.wholeExpr("INTERVAL '100' DAY(2)")
        .fails("Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
    f.wholeExpr("INTERVAL '1000' DAY(3)")
        .fails("Interval field value 1,000 exceeds precision of DAY\\(3\\) field.*");
    f.wholeExpr("INTERVAL '-1000' DAY(3)")
        .fails("Interval field value -1,000 exceeds precision of DAY\\(3\\) field.*");
    f.wholeExpr("INTERVAL '2147483648' DAY(10)")
        .fails("Interval field value 2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648' DAY(10)")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");

    // precision > maximum
    f.expr("INTERVAL '1' ^DAY(11)^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL DAY\\(11\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0' ^DAY(0)^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL DAY\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... DAY TO HOUR that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalDayToHourNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL '-' DAY TO HOUR")
        .fails("Illegal interval literal format '-' for INTERVAL DAY TO HOUR");
    f.wholeExpr("INTERVAL '1' DAY TO HOUR")
        .fails("Illegal interval literal format '1' for INTERVAL DAY TO HOUR");
    f.wholeExpr("INTERVAL '1:2' DAY TO HOUR")
        .fails("Illegal interval literal format '1:2' for INTERVAL DAY TO HOUR");
    f.wholeExpr("INTERVAL '1.2' DAY TO HOUR")
        .fails("Illegal interval literal format '1.2' for INTERVAL DAY TO HOUR");
    f.wholeExpr("INTERVAL '1 x' DAY TO HOUR")
        .fails("Illegal interval literal format '1 x' for INTERVAL DAY TO HOUR");
    f.wholeExpr("INTERVAL ' ' DAY TO HOUR")
        .fails("Illegal interval literal format ' ' for INTERVAL DAY TO HOUR");
    f.wholeExpr("INTERVAL '1:2' DAY(2) TO HOUR")
        .fails("Illegal interval literal format '1:2' for "
            + "INTERVAL DAY\\(2\\) TO HOUR");
    f.wholeExpr("INTERVAL 'bogus text' DAY TO HOUR")
        .fails("Illegal interval literal format 'bogus text' for "
            + "INTERVAL DAY TO HOUR");

    // negative field values
    f.wholeExpr("INTERVAL '--1 1' DAY TO HOUR")
        .fails("Illegal interval literal format '--1 1' for INTERVAL DAY TO HOUR");
    f.wholeExpr("INTERVAL '1 -1' DAY TO HOUR")
        .fails("Illegal interval literal format '1 -1' for INTERVAL DAY TO HOUR");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    f.wholeExpr("INTERVAL '100 0' DAY TO HOUR")
        .columnType("INTERVAL DAY TO HOUR NOT NULL");
    f.wholeExpr("INTERVAL '100 0' DAY(2) TO HOUR")
        .fails("Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
    f.wholeExpr("INTERVAL '1000 0' DAY(3) TO HOUR")
        .fails("Interval field value 1,000 exceeds precision of DAY\\(3\\) field.*");
    f.wholeExpr("INTERVAL '-1000 0' DAY(3) TO HOUR")
        .fails("Interval field value -1,000 exceeds precision of DAY\\(3\\) field.*");
    f.wholeExpr("INTERVAL '2147483648 0' DAY(10) TO HOUR")
        .fails("Interval field value 2,147,483,648 exceeds precision of DAY\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648 0' DAY(10) TO HOUR")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");
    f.wholeExpr("INTERVAL '1 24' DAY TO HOUR")
        .fails("Illegal interval literal format '1 24' for INTERVAL DAY TO HOUR.*");

    // precision > maximum
    f.expr("INTERVAL '1 1' ^DAY(11) TO HOUR^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL DAY\\(11\\) TO HOUR");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0 0' ^DAY(0) TO HOUR^")
        .fails("Interval leading field precision '0' out of range for INTERVAL DAY\\(0\\) TO HOUR");
  }

  /**
   * Runs tests for INTERVAL... DAY TO MINUTE that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalDayToMinuteNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL ' :' DAY TO MINUTE")
        .fails("Illegal interval literal format ' :' for INTERVAL DAY TO MINUTE");
    f.wholeExpr("INTERVAL '1' DAY TO MINUTE")
        .fails("Illegal interval literal format '1' for INTERVAL DAY TO MINUTE");
    f.wholeExpr("INTERVAL '1 2' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 2' for INTERVAL DAY TO MINUTE");
    f.wholeExpr("INTERVAL '1:2' DAY TO MINUTE")
        .fails("Illegal interval literal format '1:2' for INTERVAL DAY TO MINUTE");
    f.wholeExpr("INTERVAL '1.2' DAY TO MINUTE")
        .fails("Illegal interval literal format '1.2' for INTERVAL DAY TO MINUTE");
    f.wholeExpr("INTERVAL 'x 1:1' DAY TO MINUTE")
        .fails("Illegal interval literal format 'x 1:1' for INTERVAL DAY TO MINUTE");
    f.wholeExpr("INTERVAL '1 x:1' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 x:1' for INTERVAL DAY TO MINUTE");
    f.wholeExpr("INTERVAL '1 1:x' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 1:x' for INTERVAL DAY TO MINUTE");
    f.wholeExpr("INTERVAL '1 1:2:3' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 1:2:3' for INTERVAL DAY TO MINUTE");
    f.wholeExpr("INTERVAL '1 1:1:1.2' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 1:1:1.2' for INTERVAL DAY TO MINUTE");
    f.wholeExpr("INTERVAL '1 1:2:3' DAY(2) TO MINUTE")
        .fails("Illegal interval literal format '1 1:2:3' for "
            + "INTERVAL DAY\\(2\\) TO MINUTE");
    f.wholeExpr("INTERVAL '1 1' DAY(2) TO MINUTE")
        .fails("Illegal interval literal format '1 1' for "
            + "INTERVAL DAY\\(2\\) TO MINUTE");
    f.wholeExpr("INTERVAL 'bogus text' DAY TO MINUTE")
        .fails("Illegal interval literal format 'bogus text' for "
            + "INTERVAL DAY TO MINUTE");

    // negative field values
    f.wholeExpr("INTERVAL '--1 1:1' DAY TO MINUTE")
        .fails("Illegal interval literal format '--1 1:1' for INTERVAL DAY TO MINUTE");
    f.wholeExpr("INTERVAL '1 -1:1' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 -1:1' for INTERVAL DAY TO MINUTE");
    f.wholeExpr("INTERVAL '1 1:-1' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 1:-1' for INTERVAL DAY TO MINUTE");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    f.wholeExpr("INTERVAL '100 0:0' DAY TO MINUTE")
        .columnType("INTERVAL DAY TO MINUTE NOT NULL");
    f.wholeExpr("INTERVAL '100 0:0' DAY(2) TO MINUTE")
        .fails("Interval field value 100 exceeds precision of DAY\\(2\\) field.*");
    f.wholeExpr("INTERVAL '1000 0:0' DAY(3) TO MINUTE")
        .fails("Interval field value 1,000 exceeds precision of DAY\\(3\\) field.*");
    f.wholeExpr("INTERVAL '-1000 0:0' DAY(3) TO MINUTE")
        .fails("Interval field value -1,000 exceeds precision of DAY\\(3\\) field.*");
    f.wholeExpr("INTERVAL '2147483648 0:0' DAY(10) TO MINUTE")
        .fails("Interval field value 2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648 0:0' DAY(10) TO MINUTE")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");
    f.wholeExpr("INTERVAL '1 24:1' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 24:1' for "
            + "INTERVAL DAY TO MINUTE.*");
    f.wholeExpr("INTERVAL '1 1:60' DAY TO MINUTE")
        .fails("Illegal interval literal format '1 1:60' for INTERVAL DAY TO MINUTE.*");

    // precision > maximum
    f.expr("INTERVAL '1 1:1' ^DAY(11) TO MINUTE^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL DAY\\(11\\) TO MINUTE");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0 0' ^DAY(0) TO MINUTE^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL DAY\\(0\\) TO MINUTE");
  }

  /**
   * Runs tests for INTERVAL... DAY TO SECOND that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalDayToSecondNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL ' ::' DAY TO SECOND")
        .fails("Illegal interval literal format ' ::' for INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL ' ::.' DAY TO SECOND")
        .fails("Illegal interval literal format ' ::\\.' for INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1' DAY TO SECOND")
        .fails("Illegal interval literal format '1' for INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1 2' DAY TO SECOND")
        .fails("Illegal interval literal format '1 2' for INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1:2' DAY TO SECOND")
        .fails("Illegal interval literal format '1:2' for "
            + "INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1.2' DAY TO SECOND")
        .fails("Illegal interval literal format '1\\.2' for "
            + "INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1 1:2' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:2' for "
            + "INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1 1:2:x' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:2:x' for "
            + "INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1:2:3' DAY TO SECOND")
        .fails("Illegal interval literal format '1:2:3' for "
            + "INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1:1:1.2' DAY TO SECOND")
        .fails("Illegal interval literal format '1:1:1\\.2' for "
            + "INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1 1:2' DAY(2) TO SECOND")
        .fails("Illegal interval literal format '1 1:2' for "
            + "INTERVAL DAY\\(2\\) TO SECOND");
    f.wholeExpr("INTERVAL '1 1' DAY(2) TO SECOND")
        .fails("Illegal interval literal format '1 1' for "
            + "INTERVAL DAY\\(2\\) TO SECOND");
    f.wholeExpr("INTERVAL 'bogus text' DAY TO SECOND")
        .fails("Illegal interval literal format 'bogus text' for "
            + "INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '2345 6:7:8901' DAY TO SECOND(4)")
        .fails("Illegal interval literal format '2345 6:7:8901' for "
            + "INTERVAL DAY TO SECOND\\(4\\)");

    // negative field values
    f.wholeExpr("INTERVAL '--1 1:1:1' DAY TO SECOND")
        .fails("Illegal interval literal format '--1 1:1:1' for "
            + "INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1 -1:1:1' DAY TO SECOND")
        .fails("Illegal interval literal format '1 -1:1:1' for "
            + "INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1 1:-1:1' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:-1:1' for "
            + "INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1 1:1:-1' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:1:-1' for "
            + "INTERVAL DAY TO SECOND");
    f.wholeExpr("INTERVAL '1 1:1:1.-1' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:1:1.-1' for "
            + "INTERVAL DAY TO SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    f.wholeExpr("INTERVAL '100 0' DAY TO SECOND")
        .fails("Illegal interval literal format '100 0' for "
            + "INTERVAL DAY TO SECOND.*");
    f.wholeExpr("INTERVAL '100 0' DAY(2) TO SECOND")
        .fails("Illegal interval literal format '100 0' for "
            + "INTERVAL DAY\\(2\\) TO SECOND.*");
    f.wholeExpr("INTERVAL '1000 0' DAY(3) TO SECOND")
        .fails("Illegal interval literal format '1000 0' for "
            + "INTERVAL DAY\\(3\\) TO SECOND.*");
    f.wholeExpr("INTERVAL '-1000 0' DAY(3) TO SECOND")
        .fails("Illegal interval literal format '-1000 0' for "
            + "INTERVAL DAY\\(3\\) TO SECOND.*");
    f.wholeExpr("INTERVAL '2147483648 1:1:0' DAY(10) TO SECOND")
        .fails("Interval field value 2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648 1:1:0' DAY(10) TO SECOND")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "DAY\\(10\\) field.*");
    f.wholeExpr("INTERVAL '2147483648 0' DAY(10) TO SECOND")
        .fails("Illegal interval literal format '2147483648 0' for "
            + "INTERVAL DAY\\(10\\) TO SECOND.*");
    f.wholeExpr("INTERVAL '-2147483648 0' DAY(10) TO SECOND")
        .fails("Illegal interval literal format '-2147483648 0' for "
            + "INTERVAL DAY\\(10\\) TO SECOND.*");
    f.wholeExpr("INTERVAL '1 24:1:1' DAY TO SECOND")
        .fails("Illegal interval literal format '1 24:1:1' for "
            + "INTERVAL DAY TO SECOND.*");
    f.wholeExpr("INTERVAL '1 1:60:1' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:60:1' for "
            + "INTERVAL DAY TO SECOND.*");
    f.wholeExpr("INTERVAL '1 1:1:60' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:1:60' for "
            + "INTERVAL DAY TO SECOND.*");
    f.wholeExpr("INTERVAL '1 1:1:1.0000001' DAY TO SECOND")
        .fails("Illegal interval literal format '1 1:1:1\\.0000001' for "
            + "INTERVAL DAY TO SECOND.*");
    f.wholeExpr("INTERVAL '1 1:1:1.0001' DAY TO SECOND(3)")
        .fails("Illegal interval literal format '1 1:1:1\\.0001' for "
            + "INTERVAL DAY TO SECOND\\(3\\).*");

    // precision > maximum
    f.expr("INTERVAL '1 1' ^DAY(11) TO SECOND^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL DAY\\(11\\) TO SECOND");
    f.expr("INTERVAL '1 1' ^DAY TO SECOND(10)^")
        .fails("Interval fractional second precision '10' out of range for "
            + "INTERVAL DAY TO SECOND\\(10\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0 0:0:0' ^DAY(0) TO SECOND^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL DAY\\(0\\) TO SECOND");
  }

  /**
   * Runs tests for INTERVAL... HOUR that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalHourNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL '-' HOUR")
        .fails("Illegal interval literal format '-' for INTERVAL HOUR.*");
    f.wholeExpr("INTERVAL '1-2' HOUR")
        .fails("Illegal interval literal format '1-2' for INTERVAL HOUR.*");
    f.wholeExpr("INTERVAL '1.2' HOUR")
        .fails("Illegal interval literal format '1.2' for INTERVAL HOUR.*");
    f.wholeExpr("INTERVAL '1 2' HOUR")
        .fails("Illegal interval literal format '1 2' for INTERVAL HOUR.*");
    f.wholeExpr("INTERVAL '1:2' HOUR")
        .fails("Illegal interval literal format '1:2' for INTERVAL HOUR.*");
    f.wholeExpr("INTERVAL '1-2' HOUR(2)")
        .fails("Illegal interval literal format '1-2' for INTERVAL HOUR\\(2\\)");
    f.wholeExpr("INTERVAL 'bogus text' HOUR")
        .fails("Illegal interval literal format 'bogus text' for "
            + "INTERVAL HOUR.*");

    // negative field values
    f.wholeExpr("INTERVAL '--1' HOUR")
        .fails("Illegal interval literal format '--1' for INTERVAL HOUR.*");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    f.wholeExpr("INTERVAL '100' HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    f.wholeExpr("INTERVAL '100' HOUR(2)")
        .fails("Interval field value 100 exceeds precision of "
            + "HOUR\\(2\\) field.*");
    f.wholeExpr("INTERVAL '1000' HOUR(3)")
        .fails("Interval field value 1,000 exceeds precision of "
            + "HOUR\\(3\\) field.*");
    f.wholeExpr("INTERVAL '-1000' HOUR(3)")
        .fails("Interval field value -1,000 exceeds precision of "
            + "HOUR\\(3\\) field.*");
    f.wholeExpr("INTERVAL '2147483648' HOUR(10)")
        .fails("Interval field value 2,147,483,648 exceeds precision of "
            + "HOUR\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648' HOUR(10)")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "HOUR\\(10\\) field.*");

    // precision > maximum
    f.expr("INTERVAL '1' ^HOUR(11)^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL HOUR\\(11\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0' ^HOUR(0)^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL HOUR\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... HOUR TO MINUTE that should pass parser but
   * fail validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalHourToMinuteNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL ':' HOUR TO MINUTE")
        .fails("Illegal interval literal format ':' for INTERVAL HOUR TO MINUTE");
    f.wholeExpr("INTERVAL '1' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1' for INTERVAL HOUR TO MINUTE");
    f.wholeExpr("INTERVAL '1:x' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1:x' for INTERVAL HOUR TO MINUTE");
    f.wholeExpr("INTERVAL '1.2' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1.2' for INTERVAL HOUR TO MINUTE");
    f.wholeExpr("INTERVAL '1 2' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1 2' for INTERVAL HOUR TO MINUTE");
    f.wholeExpr("INTERVAL '1:2:3' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1:2:3' for INTERVAL HOUR TO MINUTE");
    f.wholeExpr("INTERVAL '1 2' HOUR(2) TO MINUTE")
        .fails("Illegal interval literal format '1 2' for "
            + "INTERVAL HOUR\\(2\\) TO MINUTE");
    f.wholeExpr("INTERVAL 'bogus text' HOUR TO MINUTE")
        .fails("Illegal interval literal format 'bogus text' for "
            + "INTERVAL HOUR TO MINUTE");

    // negative field values
    f.wholeExpr("INTERVAL '--1:1' HOUR TO MINUTE")
        .fails("Illegal interval literal format '--1:1' for INTERVAL HOUR TO MINUTE");
    f.wholeExpr("INTERVAL '1:-1' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1:-1' for INTERVAL HOUR TO MINUTE");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    f.wholeExpr("INTERVAL '100:0' HOUR TO MINUTE")
        .columnType("INTERVAL HOUR TO MINUTE NOT NULL");
    f.wholeExpr("INTERVAL '100:0' HOUR(2) TO MINUTE")
        .fails("Interval field value 100 exceeds precision of HOUR\\(2\\) field.*");
    f.wholeExpr("INTERVAL '1000:0' HOUR(3) TO MINUTE")
        .fails("Interval field value 1,000 exceeds precision of HOUR\\(3\\) field.*");
    f.wholeExpr("INTERVAL '-1000:0' HOUR(3) TO MINUTE")
        .fails("Interval field value -1,000 exceeds precision of HOUR\\(3\\) field.*");
    f.wholeExpr("INTERVAL '2147483648:0' HOUR(10) TO MINUTE")
        .fails("Interval field value 2,147,483,648 exceeds precision of HOUR\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648:0' HOUR(10) TO MINUTE")
        .fails("Interval field value -2,147,483,648 exceeds precision of HOUR\\(10\\) field.*");
    f.wholeExpr("INTERVAL '1:60' HOUR TO MINUTE")
        .fails("Illegal interval literal format '1:60' for INTERVAL HOUR TO MINUTE.*");

    // precision > maximum
    f.expr("INTERVAL '1:1' ^HOUR(11) TO MINUTE^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL HOUR\\(11\\) TO MINUTE");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0:0' ^HOUR(0) TO MINUTE^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL HOUR\\(0\\) TO MINUTE");
  }

  /**
   * Runs tests for INTERVAL... HOUR TO SECOND that should pass parser but
   * fail validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalHourToSecondNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL '::' HOUR TO SECOND")
        .fails("Illegal interval literal format '::' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '::.' HOUR TO SECOND")
        .fails("Illegal interval literal format '::\\.' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '1' HOUR TO SECOND")
        .fails("Illegal interval literal format '1' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '1 2' HOUR TO SECOND")
        .fails("Illegal interval literal format '1 2' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '1:2' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:2' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '1.2' HOUR TO SECOND")
        .fails("Illegal interval literal format '1\\.2' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '1 1:2' HOUR TO SECOND")
        .fails("Illegal interval literal format '1 1:2' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '1:2:x' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:2:x' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '1:x:3' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:x:3' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '1:1:1.x' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:1:1\\.x' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '1 1:2' HOUR(2) TO SECOND")
        .fails("Illegal interval literal format '1 1:2' for INTERVAL HOUR\\(2\\) TO SECOND");
    f.wholeExpr("INTERVAL '1 1' HOUR(2) TO SECOND")
        .fails("Illegal interval literal format '1 1' for INTERVAL HOUR\\(2\\) TO SECOND");
    f.wholeExpr("INTERVAL 'bogus text' HOUR TO SECOND")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '6:7:8901' HOUR TO SECOND(4)")
        .fails("Illegal interval literal format '6:7:8901' for INTERVAL HOUR TO SECOND\\(4\\)");

    // negative field values
    f.wholeExpr("INTERVAL '--1:1:1' HOUR TO SECOND")
        .fails("Illegal interval literal format '--1:1:1' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '1:-1:1' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:-1:1' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '1:1:-1' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:1:-1' for INTERVAL HOUR TO SECOND");
    f.wholeExpr("INTERVAL '1:1:1.-1' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:1:1\\.-1' for INTERVAL HOUR TO SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    f.wholeExpr("INTERVAL '100:0:0' HOUR TO SECOND")
        .columnType("INTERVAL HOUR TO SECOND NOT NULL");
    f.wholeExpr("INTERVAL '100:0:0' HOUR(2) TO SECOND")
        .fails("Interval field value 100 exceeds precision of "
            + "HOUR\\(2\\) field.*");
    f.wholeExpr("INTERVAL '1000:0:0' HOUR(3) TO SECOND")
        .fails("Interval field value 1,000 exceeds precision of "
            + "HOUR\\(3\\) field.*");
    f.wholeExpr("INTERVAL '-1000:0:0' HOUR(3) TO SECOND")
        .fails("Interval field value -1,000 exceeds precision of "
            + "HOUR\\(3\\) field.*");
    f.wholeExpr("INTERVAL '2147483648:0:0' HOUR(10) TO SECOND")
        .fails("Interval field value 2,147,483,648 exceeds precision of "
            + "HOUR\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648:0:0' HOUR(10) TO SECOND")
        .fails("Interval field value -2,147,483,648 exceeds precision of "
            + "HOUR\\(10\\) field.*");
    f.wholeExpr("INTERVAL '1:60:1' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:60:1' for "
            + "INTERVAL HOUR TO SECOND.*");
    f.wholeExpr("INTERVAL '1:1:60' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:1:60' for "
            + "INTERVAL HOUR TO SECOND.*");
    f.wholeExpr("INTERVAL '1:1:1.0000001' HOUR TO SECOND")
        .fails("Illegal interval literal format '1:1:1\\.0000001' for "
            + "INTERVAL HOUR TO SECOND.*");
    f.wholeExpr("INTERVAL '1:1:1.0001' HOUR TO SECOND(3)")
        .fails("Illegal interval literal format '1:1:1\\.0001' for "
            + "INTERVAL HOUR TO SECOND\\(3\\).*");

    // precision > maximum
    f.expr("INTERVAL '1:1:1' ^HOUR(11) TO SECOND^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL HOUR\\(11\\) TO SECOND");
    f.expr("INTERVAL '1:1:1' ^HOUR TO SECOND(10)^")
        .fails("Interval fractional second precision '10' out of range for "
            + "INTERVAL HOUR TO SECOND\\(10\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0:0:0' ^HOUR(0) TO SECOND^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL HOUR\\(0\\) TO SECOND");
  }

  /**
   * Runs tests for INTERVAL... MINUTE that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalMinuteNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL '-' MINUTE")
        .fails("Illegal interval literal format '-' for INTERVAL MINUTE.*");
    f.wholeExpr("INTERVAL '1-2' MINUTE")
        .fails("Illegal interval literal format '1-2' for INTERVAL MINUTE.*");
    f.wholeExpr("INTERVAL '1.2' MINUTE")
        .fails("Illegal interval literal format '1.2' for INTERVAL MINUTE.*");
    f.wholeExpr("INTERVAL '1 2' MINUTE")
        .fails("Illegal interval literal format '1 2' for INTERVAL MINUTE.*");
    f.wholeExpr("INTERVAL '1:2' MINUTE")
        .fails("Illegal interval literal format '1:2' for INTERVAL MINUTE.*");
    f.wholeExpr("INTERVAL '1-2' MINUTE(2)")
        .fails("Illegal interval literal format '1-2' for INTERVAL MINUTE\\(2\\)");
    f.wholeExpr("INTERVAL 'bogus text' MINUTE")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL MINUTE.*");

    // negative field values
    f.wholeExpr("INTERVAL '--1' MINUTE")
        .fails("Illegal interval literal format '--1' for INTERVAL MINUTE.*");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    f.wholeExpr("INTERVAL '100' MINUTE")
        .columnType("INTERVAL MINUTE NOT NULL");
    f.wholeExpr("INTERVAL '100' MINUTE(2)")
        .fails("Interval field value 100 exceeds precision of MINUTE\\(2\\) field.*");
    f.wholeExpr("INTERVAL '1000' MINUTE(3)")
        .fails("Interval field value 1,000 exceeds precision of MINUTE\\(3\\) field.*");
    f.wholeExpr("INTERVAL '-1000' MINUTE(3)")
        .fails("Interval field value -1,000 exceeds precision of MINUTE\\(3\\) field.*");
    f.wholeExpr("INTERVAL '2147483648' MINUTE(10)")
        .fails("Interval field value 2,147,483,648 exceeds precision of MINUTE\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648' MINUTE(10)")
        .fails("Interval field value -2,147,483,648 exceeds precision of MINUTE\\(10\\) field.*");

    // precision > maximum
    f.expr("INTERVAL '1' ^MINUTE(11)^")
        .fails("Interval leading field precision '11' out of range for "
            + "INTERVAL MINUTE\\(11\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0' ^MINUTE(0)^")
        .fails("Interval leading field precision '0' out of range for "
            + "INTERVAL MINUTE\\(0\\)");
  }

  /**
   * Runs tests for INTERVAL... MINUTE TO SECOND that should pass parser but
   * fail validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalMinuteToSecondNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL ':' MINUTE TO SECOND")
        .fails("Illegal interval literal format ':' for INTERVAL MINUTE TO SECOND");
    f.wholeExpr("INTERVAL ':.' MINUTE TO SECOND")
        .fails("Illegal interval literal format ':\\.' for INTERVAL MINUTE TO SECOND");
    f.wholeExpr("INTERVAL '1' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1' for INTERVAL MINUTE TO SECOND");
    f.wholeExpr("INTERVAL '1 2' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1 2' for INTERVAL MINUTE TO SECOND");
    f.wholeExpr("INTERVAL '1.2' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1\\.2' for INTERVAL MINUTE TO SECOND");
    f.wholeExpr("INTERVAL '1 1:2' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1 1:2' for INTERVAL MINUTE TO SECOND");
    f.wholeExpr("INTERVAL '1:x' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1:x' for INTERVAL MINUTE TO SECOND");
    f.wholeExpr("INTERVAL 'x:3' MINUTE TO SECOND")
        .fails("Illegal interval literal format 'x:3' for INTERVAL MINUTE TO SECOND");
    f.wholeExpr("INTERVAL '1:1.x' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1:1\\.x' for INTERVAL MINUTE TO SECOND");
    f.wholeExpr("INTERVAL '1 1:2' MINUTE(2) TO SECOND")
        .fails("Illegal interval literal format '1 1:2' for INTERVAL MINUTE\\(2\\) TO SECOND");
    f.wholeExpr("INTERVAL '1 1' MINUTE(2) TO SECOND")
        .fails("Illegal interval literal format '1 1' for INTERVAL MINUTE\\(2\\) TO SECOND");
    f.wholeExpr("INTERVAL 'bogus text' MINUTE TO SECOND")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL MINUTE TO SECOND");
    f.wholeExpr("INTERVAL '7:8901' MINUTE TO SECOND(4)")
        .fails("Illegal interval literal format '7:8901' for INTERVAL MINUTE TO SECOND\\(4\\)");

    // negative field values
    f.wholeExpr("INTERVAL '--1:1' MINUTE TO SECOND")
        .fails("Illegal interval literal format '--1:1' for INTERVAL MINUTE TO SECOND");
    f.wholeExpr("INTERVAL '1:-1' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1:-1' for INTERVAL MINUTE TO SECOND");
    f.wholeExpr("INTERVAL '1:1.-1' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1:1.-1' for INTERVAL MINUTE TO SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    f.wholeExpr("INTERVAL '100:0' MINUTE TO SECOND")
        .columnType("INTERVAL MINUTE TO SECOND NOT NULL");
    f.wholeExpr("INTERVAL '100:0' MINUTE(2) TO SECOND")
        .fails("Interval field value 100 exceeds precision of MINUTE\\(2\\) field.*");
    f.wholeExpr("INTERVAL '1000:0' MINUTE(3) TO SECOND")
        .fails("Interval field value 1,000 exceeds precision of MINUTE\\(3\\) field.*");
    f.wholeExpr("INTERVAL '-1000:0' MINUTE(3) TO SECOND")
        .fails("Interval field value -1,000 exceeds precision of MINUTE\\(3\\) field.*");
    f.wholeExpr("INTERVAL '2147483648:0' MINUTE(10) TO SECOND")
        .fails("Interval field value 2,147,483,648 exceeds precision of MINUTE\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648:0' MINUTE(10) TO SECOND")
        .fails("Interval field value -2,147,483,648 exceeds precision of MINUTE\\(10\\) field.*");
    f.wholeExpr("INTERVAL '1:60' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1:60' for"
            + " INTERVAL MINUTE TO SECOND.*");
    f.wholeExpr("INTERVAL '1:1.0000001' MINUTE TO SECOND")
        .fails("Illegal interval literal format '1:1\\.0000001' for"
            + " INTERVAL MINUTE TO SECOND.*");
    f.wholeExpr("INTERVAL '1:1:1.0001' MINUTE TO SECOND(3)")
        .fails("Illegal interval literal format '1:1:1\\.0001' for"
            + " INTERVAL MINUTE TO SECOND\\(3\\).*");

    // precision > maximum
    f.expr("INTERVAL '1:1' ^MINUTE(11) TO SECOND^")
        .fails("Interval leading field precision '11' out of range for"
            + " INTERVAL MINUTE\\(11\\) TO SECOND");
    f.expr("INTERVAL '1:1' ^MINUTE TO SECOND(10)^")
        .fails("Interval fractional second precision '10' out of range for"
            + " INTERVAL MINUTE TO SECOND\\(10\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0:0' ^MINUTE(0) TO SECOND^")
        .fails("Interval leading field precision '0' out of range for"
            + " INTERVAL MINUTE\\(0\\) TO SECOND");
  }

  /**
   * Runs tests for INTERVAL... SECOND that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlParserTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXNegative() tests.
   */
  public void subTestIntervalSecondNegative() {
    // Qualifier - field mismatches
    f.wholeExpr("INTERVAL ':' SECOND")
        .fails("Illegal interval literal format ':' for INTERVAL SECOND.*");
    f.wholeExpr("INTERVAL '.' SECOND")
        .fails("Illegal interval literal format '\\.' for INTERVAL SECOND.*");
    f.wholeExpr("INTERVAL '1-2' SECOND")
        .fails("Illegal interval literal format '1-2' for INTERVAL SECOND.*");
    f.wholeExpr("INTERVAL '1.x' SECOND")
        .fails("Illegal interval literal format '1\\.x' for INTERVAL SECOND.*");
    f.wholeExpr("INTERVAL 'x.1' SECOND")
        .fails("Illegal interval literal format 'x\\.1' for INTERVAL SECOND.*");
    f.wholeExpr("INTERVAL '1 2' SECOND")
        .fails("Illegal interval literal format '1 2' for INTERVAL SECOND.*");
    f.wholeExpr("INTERVAL '1:2' SECOND")
        .fails("Illegal interval literal format '1:2' for INTERVAL SECOND.*");
    f.wholeExpr("INTERVAL '1-2' SECOND(2)")
        .fails("Illegal interval literal format '1-2' for INTERVAL SECOND\\(2\\)");
    f.wholeExpr("INTERVAL 'bogus text' SECOND")
        .fails("Illegal interval literal format 'bogus text' for INTERVAL SECOND.*");

    // negative field values
    f.wholeExpr("INTERVAL '--1' SECOND")
        .fails("Illegal interval literal format '--1' for INTERVAL SECOND.*");
    f.wholeExpr("INTERVAL '1.-1' SECOND")
        .fails("Illegal interval literal format '1.-1' for INTERVAL SECOND.*");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    f.wholeExpr("INTERVAL '100' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    f.wholeExpr("INTERVAL '100' SECOND(2)")
        .fails("Interval field value 100 exceeds precision of SECOND\\(2\\) field.*");
    f.wholeExpr("INTERVAL '1000' SECOND(3)")
        .fails("Interval field value 1,000 exceeds precision of SECOND\\(3\\) field.*");
    f.wholeExpr("INTERVAL '-1000' SECOND(3)")
        .fails("Interval field value -1,000 exceeds precision of SECOND\\(3\\) field.*");
    f.wholeExpr("INTERVAL '2147483648' SECOND(10)")
        .fails("Interval field value 2,147,483,648 exceeds precision of SECOND\\(10\\) field.*");
    f.wholeExpr("INTERVAL '-2147483648' SECOND(10)")
        .fails("Interval field value -2,147,483,648 exceeds precision of SECOND\\(10\\) field.*");
    f.wholeExpr("INTERVAL '1.0000001' SECOND")
        .fails("Illegal interval literal format '1\\.0000001' for INTERVAL SECOND.*");
    f.wholeExpr("INTERVAL '1.0000001' SECOND(2)")
        .fails("Illegal interval literal format '1\\.0000001' for INTERVAL SECOND\\(2\\).*");
    f.wholeExpr("INTERVAL '1.0001' SECOND(2, 3)")
        .fails("Illegal interval literal format '1\\.0001' for INTERVAL SECOND\\(2, 3\\).*");
    f.wholeExpr("INTERVAL '1.0000000001' SECOND(2, 9)")
        .fails("Illegal interval literal format '1\\.0000000001' for"
            + " INTERVAL SECOND\\(2, 9\\).*");

    // precision > maximum
    f.expr("INTERVAL '1' ^SECOND(11)^")
        .fails("Interval leading field precision '11' out of range for"
            + " INTERVAL SECOND\\(11\\)");
    f.expr("INTERVAL '1.1' ^SECOND(1, 10)^")
        .fails("Interval fractional second precision '10' out of range for"
            + " INTERVAL SECOND\\(1, 10\\)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    f.expr("INTERVAL '0' ^SECOND(0)^")
        .fails("Interval leading field precision '0' out of range for"
            + " INTERVAL SECOND\\(0\\)");
  }

  public void subTestMisc() {
    // Miscellaneous
    // fractional value is not OK, even if it is 0
    f.wholeExpr("INTERVAL '1.0' HOUR")
        .fails("Illegal interval literal format '1.0' for INTERVAL HOUR");
    // only seconds are allowed to have a fractional part
    f.expr("INTERVAL '1.0' SECOND")
        .columnType("INTERVAL SECOND NOT NULL");
    // leading zeros do not cause precision to be exceeded
    f.expr("INTERVAL '0999' MONTH(3)")
        .columnType("INTERVAL MONTH(3) NOT NULL");
  }

  /** Fluent interface for binding an expression to create a fixture that can
   * be used to validate, check AST, or check type. */
  public interface Fixture {
    Fixture2 expr(String s);
    Fixture2 wholeExpr(String s);
  }

  /** Fluent interface to validate an expression. */
  public interface Fixture2 {
    /** Checks that the expression is valid in the parser
     * but invalid (with the given error message) in the validator. */
    void fails(String expected);

    /** Checks that the expression is valid in the parser and validator
     * and has the given column type. */
    void columnType(String expectedType);

    /** Checks that the expression parses successfully and produces the given
     * SQL when unparsed. */
    Fixture2 assertParse(String expectedAst);
  }
}
