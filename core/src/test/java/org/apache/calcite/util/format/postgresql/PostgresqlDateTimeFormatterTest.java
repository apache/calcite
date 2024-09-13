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
package org.apache.calcite.util.format.postgresql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for {@link PostgresqlDateTimeFormatter}.
 */
public class PostgresqlDateTimeFormatterTest {
  private static final ZoneId TIME_ZONE = ZoneId.systemDefault();

  private static final ZonedDateTime DAY_1_CE = createDateTime(1, 1, 1, 0, 0, 0, 0);
  private static final ZonedDateTime APR_17_2024 = createDateTime(2024, 4, 17, 0, 0, 0, 0);
  private static final ZonedDateTime JAN_1_2001 = createDateTime(2001, 1, 1, 0, 0, 0, 0);
  private static final ZonedDateTime JAN_1_2024 = createDateTime(2024, 1, 1, 0, 0, 0, 0);

  @ParameterizedTest
  @ValueSource(strings = {"HH12", "HH"})
  void testHH12(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertEquals("12", PostgresqlDateTimeFormatter.toChar(pattern, midnight));
    assertEquals("06", PostgresqlDateTimeFormatter.toChar(pattern, morning));
    assertEquals("12", PostgresqlDateTimeFormatter.toChar(pattern, noon));
    assertEquals("06", PostgresqlDateTimeFormatter.toChar(pattern, evening));
    assertEquals(
        "12", PostgresqlDateTimeFormatter.toChar("FM" + pattern,
            midnight));
    assertEquals(
        "6", PostgresqlDateTimeFormatter.toChar("FM" + pattern,
            morning));
    assertEquals(
        "12", PostgresqlDateTimeFormatter.toChar("FM" + pattern,
            noon));
    assertEquals(
        "6", PostgresqlDateTimeFormatter.toChar("FM" + pattern,
            evening));

    final ZonedDateTime hourOne = createDateTime(2024, 1, 1, 1, 0, 0, 0);
    final ZonedDateTime hourTwo = createDateTime(2024, 1, 1, 2, 0, 0, 0);
    final ZonedDateTime hourThree = createDateTime(2024, 1, 1, 3, 0, 0, 0);
    assertEquals(
        "12TH", PostgresqlDateTimeFormatter.toChar(pattern + "TH",
            midnight));
    assertEquals(
        "01ST", PostgresqlDateTimeFormatter.toChar(pattern + "TH",
            hourOne));
    assertEquals(
        "02ND", PostgresqlDateTimeFormatter.toChar(pattern + "TH",
            hourTwo));
    assertEquals(
        "03RD", PostgresqlDateTimeFormatter.toChar(pattern + "TH",
            hourThree));
    assertEquals(
        "12th", PostgresqlDateTimeFormatter.toChar(pattern + "th",
            midnight));
    assertEquals(
        "01st", PostgresqlDateTimeFormatter.toChar(pattern + "th",
            hourOne));
    assertEquals(
        "02nd", PostgresqlDateTimeFormatter.toChar(pattern + "th",
            hourTwo));
    assertEquals(
        "03rd", PostgresqlDateTimeFormatter.toChar(pattern + "th",
            hourThree));

    assertEquals(
        "2nd", PostgresqlDateTimeFormatter.toChar(
            "FM" + pattern + "th", hourTwo));
  }

  @Test void testHH24() {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertEquals("00", PostgresqlDateTimeFormatter.toChar("HH24", midnight));
    assertEquals("06", PostgresqlDateTimeFormatter.toChar("HH24", morning));
    assertEquals("12", PostgresqlDateTimeFormatter.toChar("HH24", noon));
    assertEquals("18", PostgresqlDateTimeFormatter.toChar("HH24", evening));
    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMHH24", midnight));
    assertEquals("6", PostgresqlDateTimeFormatter.toChar("FMHH24", morning));
    assertEquals("12", PostgresqlDateTimeFormatter.toChar("FMHH24", noon));
    assertEquals("18", PostgresqlDateTimeFormatter.toChar("FMHH24", evening));

    final ZonedDateTime hourOne = createDateTime(2024, 1, 1, 1, 0, 0, 0);
    final ZonedDateTime hourTwo = createDateTime(2024, 1, 1, 2, 0, 0, 0);
    final ZonedDateTime hourThree = createDateTime(2024, 1, 1, 3, 0, 0, 0);
    assertEquals("00TH", PostgresqlDateTimeFormatter.toChar("HH24TH", midnight));
    assertEquals("01ST", PostgresqlDateTimeFormatter.toChar("HH24TH", hourOne));
    assertEquals("02ND", PostgresqlDateTimeFormatter.toChar("HH24TH", hourTwo));
    assertEquals("03RD", PostgresqlDateTimeFormatter.toChar("HH24TH", hourThree));
    assertEquals("00th", PostgresqlDateTimeFormatter.toChar("HH24th", midnight));
    assertEquals("01st", PostgresqlDateTimeFormatter.toChar("HH24th", hourOne));
    assertEquals("02nd", PostgresqlDateTimeFormatter.toChar("HH24th", hourTwo));
    assertEquals("03rd", PostgresqlDateTimeFormatter.toChar("HH24th", hourThree));

    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMHH24th", hourTwo));
  }

  @Test void testMI() {
    final ZonedDateTime minute0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime minute2 = createDateTime(2024, 1, 1, 0, 2, 0, 0);
    final ZonedDateTime minute15 = createDateTime(2024, 1, 1, 0, 15, 0, 0);

    assertEquals("00", PostgresqlDateTimeFormatter.toChar("MI", minute0));
    assertEquals("02", PostgresqlDateTimeFormatter.toChar("MI", minute2));
    assertEquals("15", PostgresqlDateTimeFormatter.toChar("MI", minute15));

    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMMI", minute0));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("FMMI", minute2));
    assertEquals("15", PostgresqlDateTimeFormatter.toChar("FMMI", minute15));

    assertEquals("00TH", PostgresqlDateTimeFormatter.toChar("MITH", minute0));
    assertEquals("02ND", PostgresqlDateTimeFormatter.toChar("MITH", minute2));
    assertEquals("15TH", PostgresqlDateTimeFormatter.toChar("MITH", minute15));
    assertEquals("00th", PostgresqlDateTimeFormatter.toChar("MIth", minute0));
    assertEquals("02nd", PostgresqlDateTimeFormatter.toChar("MIth", minute2));
    assertEquals("15th", PostgresqlDateTimeFormatter.toChar("MIth", minute15));

    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMMIth", minute2));
    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMMInd", minute2));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SSSSS", "SSSS"})
  void testSSSSS(String pattern) {
    final ZonedDateTime second0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime second1001 = createDateTime(2024, 1, 1, 0, 16, 41, 0);
    final ZonedDateTime endOfDay = createDateTime(2024, 1, 1, 23, 59, 59, 0);

    assertEquals("0", PostgresqlDateTimeFormatter.toChar(pattern, second0));
    assertEquals("1001", PostgresqlDateTimeFormatter.toChar(pattern, second1001));
    assertEquals("86399", PostgresqlDateTimeFormatter.toChar(pattern, endOfDay));

    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FM" + pattern, second0));
    assertEquals("1001", PostgresqlDateTimeFormatter.toChar("FM" + pattern, second1001));
    assertEquals("86399", PostgresqlDateTimeFormatter.toChar("FM" + pattern, endOfDay));

    assertEquals("0TH", PostgresqlDateTimeFormatter.toChar(pattern + "TH", second0));
    assertEquals("1001ST", PostgresqlDateTimeFormatter.toChar(pattern + "TH", second1001));
    assertEquals("86399TH", PostgresqlDateTimeFormatter.toChar(pattern + "TH", endOfDay));
    assertEquals("0th", PostgresqlDateTimeFormatter.toChar(pattern + "th", second0));
    assertEquals("1001st", PostgresqlDateTimeFormatter.toChar(pattern + "th", second1001));
    assertEquals("86399th", PostgresqlDateTimeFormatter.toChar(pattern + "th", endOfDay));

    assertEquals("1001st", PostgresqlDateTimeFormatter.toChar("FM" + pattern + "th", second1001));
    assertEquals("1001nd", PostgresqlDateTimeFormatter.toChar("FM" + pattern + "nd", second1001));
  }

  @Test void testSS() {
    final ZonedDateTime second0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime second2 = createDateTime(2024, 1, 1, 0, 0, 2, 0);
    final ZonedDateTime second15 = createDateTime(2024, 1, 1, 0, 0, 15, 0);

    assertEquals("00", PostgresqlDateTimeFormatter.toChar("SS", second0));
    assertEquals("02", PostgresqlDateTimeFormatter.toChar("SS", second2));
    assertEquals("15", PostgresqlDateTimeFormatter.toChar("SS", second15));

    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMSS", second0));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("FMSS", second2));
    assertEquals("15", PostgresqlDateTimeFormatter.toChar("FMSS", second15));

    assertEquals("00TH", PostgresqlDateTimeFormatter.toChar("SSTH", second0));
    assertEquals("02ND", PostgresqlDateTimeFormatter.toChar("SSTH", second2));
    assertEquals("15TH", PostgresqlDateTimeFormatter.toChar("SSTH", second15));
    assertEquals("00th", PostgresqlDateTimeFormatter.toChar("SSth", second0));
    assertEquals("02nd", PostgresqlDateTimeFormatter.toChar("SSth", second2));
    assertEquals("15th", PostgresqlDateTimeFormatter.toChar("SSth", second15));

    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMSSth", second2));
    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMSSnd", second2));
  }

  @ParameterizedTest
  @ValueSource(strings = {"MS", "FF3"})
  void testMS(String pattern) {
    final ZonedDateTime ms0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime ms2 = createDateTime(2024, 1, 1, 0, 0, 2, 2000000);
    final ZonedDateTime ms15 = createDateTime(2024, 1, 1, 0, 0, 15, 15000000);

    assertEquals("000", PostgresqlDateTimeFormatter.toChar(pattern, ms0));
    assertEquals("002", PostgresqlDateTimeFormatter.toChar(pattern, ms2));
    assertEquals("015", PostgresqlDateTimeFormatter.toChar(pattern, ms15));

    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FM" + pattern, ms0));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("FM" + pattern, ms2));
    assertEquals("15", PostgresqlDateTimeFormatter.toChar("FM" + pattern, ms15));

    assertEquals("000TH", PostgresqlDateTimeFormatter.toChar(pattern + "TH", ms0));
    assertEquals("002ND", PostgresqlDateTimeFormatter.toChar(pattern + "TH", ms2));
    assertEquals("015TH", PostgresqlDateTimeFormatter.toChar(pattern + "TH", ms15));
    assertEquals("000th", PostgresqlDateTimeFormatter.toChar(pattern + "th", ms0));
    assertEquals("002nd", PostgresqlDateTimeFormatter.toChar(pattern + "th", ms2));
    assertEquals("015th", PostgresqlDateTimeFormatter.toChar(pattern + "th", ms15));

    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FM" + pattern + "th", ms2));
    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FM" + pattern + "nd", ms2));
  }

  @Test void testUS() {
    final ZonedDateTime us0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime us2 = createDateTime(2024, 1, 1, 0, 0, 0, 2000);
    final ZonedDateTime us15 = createDateTime(2024, 1, 1, 0, 0, 0, 15000);
    final ZonedDateTime usWithMs = createDateTime(2024, 1, 1, 0, 0, 0, 2015000);

    assertEquals("000000", PostgresqlDateTimeFormatter.toChar("US", us0));
    assertEquals("000002", PostgresqlDateTimeFormatter.toChar("US", us2));
    assertEquals("000015", PostgresqlDateTimeFormatter.toChar("US", us15));
    assertEquals("002015", PostgresqlDateTimeFormatter.toChar("US", usWithMs));

    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMUS", us0));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("FMUS", us2));
    assertEquals("15", PostgresqlDateTimeFormatter.toChar("FMUS", us15));
    assertEquals("2015", PostgresqlDateTimeFormatter.toChar("FMUS", usWithMs));

    assertEquals("000000TH", PostgresqlDateTimeFormatter.toChar("USTH", us0));
    assertEquals("000002ND", PostgresqlDateTimeFormatter.toChar("USTH", us2));
    assertEquals("000015TH", PostgresqlDateTimeFormatter.toChar("USTH", us15));
    assertEquals("002015TH", PostgresqlDateTimeFormatter.toChar("USTH", usWithMs));
    assertEquals("000000th", PostgresqlDateTimeFormatter.toChar("USth", us0));
    assertEquals("000002nd", PostgresqlDateTimeFormatter.toChar("USth", us2));
    assertEquals("000015th", PostgresqlDateTimeFormatter.toChar("USth", us15));
    assertEquals("002015th", PostgresqlDateTimeFormatter.toChar("USth", usWithMs));

    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMUSth", us2));
    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMUSnd", us2));
  }

  @Test void testFF1() {
    final ZonedDateTime ms0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime ms200 = createDateTime(2024, 1, 1, 0, 0, 0, 200_000_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FF1", ms0));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("FF1", ms200));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FF1", ms150));

    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMFF1", ms0));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("FMFF1", ms200));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMFF1", ms150));

    assertEquals("0TH", PostgresqlDateTimeFormatter.toChar("FF1TH", ms0));
    assertEquals("2ND", PostgresqlDateTimeFormatter.toChar("FF1TH", ms200));
    assertEquals("1ST", PostgresqlDateTimeFormatter.toChar("FF1TH", ms150));
    assertEquals("0th", PostgresqlDateTimeFormatter.toChar("FF1th", ms0));
    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FF1th", ms200));
    assertEquals("1st", PostgresqlDateTimeFormatter.toChar("FF1th", ms150));

    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMFF1th", ms200));
    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMFF1nd", ms200));
  }

  @Test void testFF2() {
    final ZonedDateTime ms0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime ms20 = createDateTime(2024, 1, 1, 0, 0, 0, 20_000_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertEquals("00", PostgresqlDateTimeFormatter.toChar("FF2", ms0));
    assertEquals("02", PostgresqlDateTimeFormatter.toChar("FF2", ms20));
    assertEquals("15", PostgresqlDateTimeFormatter.toChar("FF2", ms150));

    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMFF2", ms0));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("FMFF2", ms20));
    assertEquals("15", PostgresqlDateTimeFormatter.toChar("FMFF2", ms150));

    assertEquals("00TH", PostgresqlDateTimeFormatter.toChar("FF2TH", ms0));
    assertEquals("02ND", PostgresqlDateTimeFormatter.toChar("FF2TH", ms20));
    assertEquals("15TH", PostgresqlDateTimeFormatter.toChar("FF2TH", ms150));
    assertEquals("00th", PostgresqlDateTimeFormatter.toChar("FF2th", ms0));
    assertEquals("02nd", PostgresqlDateTimeFormatter.toChar("FF2th", ms20));
    assertEquals("15th", PostgresqlDateTimeFormatter.toChar("FF2th", ms150));

    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMFF2th", ms20));
    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMFF2nd", ms20));
  }

  @Test void testFF4() {
    final ZonedDateTime us0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime us200 = createDateTime(2024, 1, 1, 0, 0, 0, 200_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertEquals("0000", PostgresqlDateTimeFormatter.toChar("FF4", us0));
    assertEquals("0002", PostgresqlDateTimeFormatter.toChar("FF4", us200));
    assertEquals("1500", PostgresqlDateTimeFormatter.toChar("FF4", ms150));

    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMFF4", us0));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("FMFF4", us200));
    assertEquals("1500", PostgresqlDateTimeFormatter.toChar("FMFF4", ms150));

    assertEquals("0000TH", PostgresqlDateTimeFormatter.toChar("FF4TH", us0));
    assertEquals("0002ND", PostgresqlDateTimeFormatter.toChar("FF4TH", us200));
    assertEquals("1500TH", PostgresqlDateTimeFormatter.toChar("FF4TH", ms150));
    assertEquals("0000th", PostgresqlDateTimeFormatter.toChar("FF4th", us0));
    assertEquals("0002nd", PostgresqlDateTimeFormatter.toChar("FF4th", us200));
    assertEquals("1500th", PostgresqlDateTimeFormatter.toChar("FF4th", ms150));

    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMFF4th", us200));
    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMFF4nd", us200));
  }

  @Test void testFF5() {
    final ZonedDateTime us0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime us20 = createDateTime(2024, 1, 1, 0, 0, 0, 20_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertEquals("00000", PostgresqlDateTimeFormatter.toChar("FF5", us0));
    assertEquals("00002", PostgresqlDateTimeFormatter.toChar("FF5", us20));
    assertEquals("15000", PostgresqlDateTimeFormatter.toChar("FF5", ms150));

    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMFF5", us0));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("FMFF5", us20));
    assertEquals("15000", PostgresqlDateTimeFormatter.toChar("FMFF5", ms150));

    assertEquals("00000TH", PostgresqlDateTimeFormatter.toChar("FF5TH", us0));
    assertEquals("00002ND", PostgresqlDateTimeFormatter.toChar("FF5TH", us20));
    assertEquals("15000TH", PostgresqlDateTimeFormatter.toChar("FF5TH", ms150));
    assertEquals("00000th", PostgresqlDateTimeFormatter.toChar("FF5th", us0));
    assertEquals("00002nd", PostgresqlDateTimeFormatter.toChar("FF5th", us20));
    assertEquals("15000th", PostgresqlDateTimeFormatter.toChar("FF5th", ms150));

    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMFF5th", us20));
    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMFF5nd", us20));
  }

  @Test void testFF6() {
    final ZonedDateTime us0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime us2 = createDateTime(2024, 1, 1, 0, 0, 0, 2_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertEquals("000000", PostgresqlDateTimeFormatter.toChar("FF6", us0));
    assertEquals("000002", PostgresqlDateTimeFormatter.toChar("FF6", us2));
    assertEquals("150000", PostgresqlDateTimeFormatter.toChar("FF6", ms150));

    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMFF6", us0));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("FMFF6", us2));
    assertEquals("150000", PostgresqlDateTimeFormatter.toChar("FMFF6", ms150));

    assertEquals("000000TH", PostgresqlDateTimeFormatter.toChar("FF6TH", us0));
    assertEquals("000002ND", PostgresqlDateTimeFormatter.toChar("FF6TH", us2));
    assertEquals("150000TH", PostgresqlDateTimeFormatter.toChar("FF6TH", ms150));
    assertEquals("000000th", PostgresqlDateTimeFormatter.toChar("FF6th", us0));
    assertEquals("000002nd", PostgresqlDateTimeFormatter.toChar("FF6th", us2));
    assertEquals("150000th", PostgresqlDateTimeFormatter.toChar("FF6th", ms150));

    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMFF6th", us2));
    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMFF6nd", us2));
  }

  @ParameterizedTest
  @ValueSource(strings = {"AM", "PM"})
  void testAMUpperCase(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertEquals("AM", PostgresqlDateTimeFormatter.toChar(pattern, midnight));
    assertEquals("AM", PostgresqlDateTimeFormatter.toChar(pattern, morning));
    assertEquals("PM", PostgresqlDateTimeFormatter.toChar(pattern, noon));
    assertEquals("PM", PostgresqlDateTimeFormatter.toChar(pattern, evening));
  }

  @ParameterizedTest
  @ValueSource(strings = {"am", "pm"})
  void testAMLowerCase(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertEquals("am", PostgresqlDateTimeFormatter.toChar(pattern, midnight));
    assertEquals("am", PostgresqlDateTimeFormatter.toChar(pattern, morning));
    assertEquals("pm", PostgresqlDateTimeFormatter.toChar(pattern, noon));
    assertEquals("pm", PostgresqlDateTimeFormatter.toChar(pattern, evening));
  }

  @ParameterizedTest
  @ValueSource(strings = {"A.M.", "P.M."})
  void testAMWithDotsUpperCase(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertEquals("A.M.", PostgresqlDateTimeFormatter.toChar(pattern, midnight));
    assertEquals("A.M.", PostgresqlDateTimeFormatter.toChar(pattern, morning));
    assertEquals("P.M.", PostgresqlDateTimeFormatter.toChar(pattern, noon));
    assertEquals("P.M.", PostgresqlDateTimeFormatter.toChar(pattern, evening));
  }

  @ParameterizedTest
  @ValueSource(strings = {"a.m.", "p.m."})
  void testAMWithDotsLowerCase(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertEquals("a.m.", PostgresqlDateTimeFormatter.toChar(pattern, midnight));
    assertEquals("a.m.", PostgresqlDateTimeFormatter.toChar(pattern, morning));
    assertEquals("p.m.", PostgresqlDateTimeFormatter.toChar(pattern, noon));
    assertEquals("p.m.", PostgresqlDateTimeFormatter.toChar(pattern, evening));
  }

  @Test void testYearWithCommas() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertEquals("2,024", PostgresqlDateTimeFormatter.toChar("Y,YYY", year1));
    assertEquals("0,100", PostgresqlDateTimeFormatter.toChar("Y,YYY", year2));
    assertEquals("0,001", PostgresqlDateTimeFormatter.toChar("Y,YYY", year3));
    assertEquals("32,136", PostgresqlDateTimeFormatter.toChar("Y,YYY", year4));
    assertEquals("2,024", PostgresqlDateTimeFormatter.toChar("FMY,YYY", year1));
    assertEquals("0,100", PostgresqlDateTimeFormatter.toChar("FMY,YYY", year2));
    assertEquals("0,001", PostgresqlDateTimeFormatter.toChar("FMY,YYY", year3));
    assertEquals("32,136", PostgresqlDateTimeFormatter.toChar("FMY,YYY", year4));

    assertEquals("2,024TH", PostgresqlDateTimeFormatter.toChar("Y,YYYTH", year1));
    assertEquals("0,100TH", PostgresqlDateTimeFormatter.toChar("Y,YYYTH", year2));
    assertEquals("0,001ST", PostgresqlDateTimeFormatter.toChar("Y,YYYTH", year3));
    assertEquals("32,136TH", PostgresqlDateTimeFormatter.toChar("Y,YYYTH", year4));
    assertEquals("2,024th", PostgresqlDateTimeFormatter.toChar("Y,YYYth", year1));
    assertEquals("0,100th", PostgresqlDateTimeFormatter.toChar("Y,YYYth", year2));
    assertEquals("0,001st", PostgresqlDateTimeFormatter.toChar("Y,YYYth", year3));
    assertEquals("32,136th", PostgresqlDateTimeFormatter.toChar("Y,YYYth", year4));

    assertEquals("2,024th", PostgresqlDateTimeFormatter.toChar("FMY,YYYth", year1));
  }

  @Test void testYYYY() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertEquals("2024", PostgresqlDateTimeFormatter.toChar("YYYY", year1));
    assertEquals("0100", PostgresqlDateTimeFormatter.toChar("YYYY", year2));
    assertEquals("0001", PostgresqlDateTimeFormatter.toChar("YYYY", year3));
    assertEquals("32136", PostgresqlDateTimeFormatter.toChar("YYYY", year4));
    assertEquals("2024", PostgresqlDateTimeFormatter.toChar("FMYYYY", year1));
    assertEquals("100", PostgresqlDateTimeFormatter.toChar("FMYYYY", year2));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMYYYY", year3));
    assertEquals("32136", PostgresqlDateTimeFormatter.toChar("FMYYYY", year4));

    assertEquals("2024TH", PostgresqlDateTimeFormatter.toChar("YYYYTH", year1));
    assertEquals("0100TH", PostgresqlDateTimeFormatter.toChar("YYYYTH", year2));
    assertEquals("0001ST", PostgresqlDateTimeFormatter.toChar("YYYYTH", year3));
    assertEquals("32136TH", PostgresqlDateTimeFormatter.toChar("YYYYTH", year4));
    assertEquals("2024th", PostgresqlDateTimeFormatter.toChar("YYYYth", year1));
    assertEquals("0100th", PostgresqlDateTimeFormatter.toChar("YYYYth", year2));
    assertEquals("0001st", PostgresqlDateTimeFormatter.toChar("YYYYth", year3));
    assertEquals("32136th", PostgresqlDateTimeFormatter.toChar("YYYYth", year4));

    assertEquals("2024th", PostgresqlDateTimeFormatter.toChar("FMYYYYth", year1));
  }

  @Test void testYYY() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertEquals("024", PostgresqlDateTimeFormatter.toChar("YYY", year1));
    assertEquals("100", PostgresqlDateTimeFormatter.toChar("YYY", year2));
    assertEquals("001", PostgresqlDateTimeFormatter.toChar("YYY", year3));
    assertEquals("136", PostgresqlDateTimeFormatter.toChar("YYY", year4));
    assertEquals("24", PostgresqlDateTimeFormatter.toChar("FMYYY", year1));
    assertEquals("100", PostgresqlDateTimeFormatter.toChar("FMYYY", year2));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMYYY", year3));
    assertEquals("136", PostgresqlDateTimeFormatter.toChar("FMYYY", year4));

    assertEquals("024TH", PostgresqlDateTimeFormatter.toChar("YYYTH", year1));
    assertEquals("100TH", PostgresqlDateTimeFormatter.toChar("YYYTH", year2));
    assertEquals("001ST", PostgresqlDateTimeFormatter.toChar("YYYTH", year3));
    assertEquals("136TH", PostgresqlDateTimeFormatter.toChar("YYYTH", year4));
    assertEquals("024th", PostgresqlDateTimeFormatter.toChar("YYYth", year1));
    assertEquals("100th", PostgresqlDateTimeFormatter.toChar("YYYth", year2));
    assertEquals("001st", PostgresqlDateTimeFormatter.toChar("YYYth", year3));
    assertEquals("136th", PostgresqlDateTimeFormatter.toChar("YYYth", year4));

    assertEquals("24th", PostgresqlDateTimeFormatter.toChar("FMYYYth", year1));
  }

  @Test void testYY() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertEquals("24", PostgresqlDateTimeFormatter.toChar("YY", year1));
    assertEquals("00", PostgresqlDateTimeFormatter.toChar("YY", year2));
    assertEquals("01", PostgresqlDateTimeFormatter.toChar("YY", year3));
    assertEquals("36", PostgresqlDateTimeFormatter.toChar("YY", year4));
    assertEquals("24", PostgresqlDateTimeFormatter.toChar("FMYY", year1));
    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMYY", year2));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMYY", year3));
    assertEquals("36", PostgresqlDateTimeFormatter.toChar("FMYY", year4));

    assertEquals("24TH", PostgresqlDateTimeFormatter.toChar("YYTH", year1));
    assertEquals("00TH", PostgresqlDateTimeFormatter.toChar("YYTH", year2));
    assertEquals("01ST", PostgresqlDateTimeFormatter.toChar("YYTH", year3));
    assertEquals("36TH", PostgresqlDateTimeFormatter.toChar("YYTH", year4));
    assertEquals("24th", PostgresqlDateTimeFormatter.toChar("YYth", year1));
    assertEquals("00th", PostgresqlDateTimeFormatter.toChar("YYth", year2));
    assertEquals("01st", PostgresqlDateTimeFormatter.toChar("YYth", year3));
    assertEquals("36th", PostgresqlDateTimeFormatter.toChar("YYth", year4));

    assertEquals("24th", PostgresqlDateTimeFormatter.toChar("FMYYth", year1));
  }

  @Test void testY() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertEquals("4", PostgresqlDateTimeFormatter.toChar("Y", year1));
    assertEquals("0", PostgresqlDateTimeFormatter.toChar("Y", year2));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("Y", year3));
    assertEquals("6", PostgresqlDateTimeFormatter.toChar("Y", year4));
    assertEquals("4", PostgresqlDateTimeFormatter.toChar("FMY", year1));
    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMY", year2));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMY", year3));
    assertEquals("6", PostgresqlDateTimeFormatter.toChar("FMY", year4));

    assertEquals("4TH", PostgresqlDateTimeFormatter.toChar("YTH", year1));
    assertEquals("0TH", PostgresqlDateTimeFormatter.toChar("YTH", year2));
    assertEquals("1ST", PostgresqlDateTimeFormatter.toChar("YTH", year3));
    assertEquals("6TH", PostgresqlDateTimeFormatter.toChar("YTH", year4));
    assertEquals("4th", PostgresqlDateTimeFormatter.toChar("Yth", year1));
    assertEquals("0th", PostgresqlDateTimeFormatter.toChar("Yth", year2));
    assertEquals("1st", PostgresqlDateTimeFormatter.toChar("Yth", year3));
    assertEquals("6th", PostgresqlDateTimeFormatter.toChar("Yth", year4));

    assertEquals("4th", PostgresqlDateTimeFormatter.toChar("FMYth", year1));
  }

  @Test void testIYYY() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(1);
    final ZonedDateTime date4 = date3.plusDays(1);
    final ZonedDateTime date5 = date4.plusDays(1);

    assertEquals("2019", PostgresqlDateTimeFormatter.toChar("IYYY", date1));
    assertEquals("2020", PostgresqlDateTimeFormatter.toChar("IYYY", date2));
    assertEquals("2020", PostgresqlDateTimeFormatter.toChar("IYYY", date3));
    assertEquals("2020", PostgresqlDateTimeFormatter.toChar("IYYY", date4));
    assertEquals("2020", PostgresqlDateTimeFormatter.toChar("IYYY", date5));
    assertEquals("2019", PostgresqlDateTimeFormatter.toChar("FMIYYY", date1));
    assertEquals("2020", PostgresqlDateTimeFormatter.toChar("FMIYYY", date2));
    assertEquals("2020", PostgresqlDateTimeFormatter.toChar("FMIYYY", date3));
    assertEquals("2020", PostgresqlDateTimeFormatter.toChar("FMIYYY", date4));
    assertEquals("2020", PostgresqlDateTimeFormatter.toChar("FMIYYY", date5));

    assertEquals("2019TH", PostgresqlDateTimeFormatter.toChar("IYYYTH", date1));
    assertEquals("2020TH", PostgresqlDateTimeFormatter.toChar("IYYYTH", date2));
    assertEquals("2020TH", PostgresqlDateTimeFormatter.toChar("IYYYTH", date3));
    assertEquals("2020TH", PostgresqlDateTimeFormatter.toChar("IYYYTH", date4));
    assertEquals("2020TH", PostgresqlDateTimeFormatter.toChar("IYYYTH", date5));
    assertEquals("2019th", PostgresqlDateTimeFormatter.toChar("IYYYth", date1));
    assertEquals("2020th", PostgresqlDateTimeFormatter.toChar("IYYYth", date2));
    assertEquals("2020th", PostgresqlDateTimeFormatter.toChar("IYYYth", date3));
    assertEquals("2020th", PostgresqlDateTimeFormatter.toChar("IYYYth", date4));
    assertEquals("2020th", PostgresqlDateTimeFormatter.toChar("IYYYth", date5));

    assertEquals("2020th", PostgresqlDateTimeFormatter.toChar("FMIYYYth", date5));
  }

  @Test void testIYY() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(1);
    final ZonedDateTime date4 = date3.plusDays(1);
    final ZonedDateTime date5 = date4.plusDays(1);

    assertEquals("019", PostgresqlDateTimeFormatter.toChar("IYY", date1));
    assertEquals("020", PostgresqlDateTimeFormatter.toChar("IYY", date2));
    assertEquals("020", PostgresqlDateTimeFormatter.toChar("IYY", date3));
    assertEquals("020", PostgresqlDateTimeFormatter.toChar("IYY", date4));
    assertEquals("020", PostgresqlDateTimeFormatter.toChar("IYY", date5));
    assertEquals("19", PostgresqlDateTimeFormatter.toChar("FMIYY", date1));
    assertEquals("20", PostgresqlDateTimeFormatter.toChar("FMIYY", date2));
    assertEquals("20", PostgresqlDateTimeFormatter.toChar("FMIYY", date3));
    assertEquals("20", PostgresqlDateTimeFormatter.toChar("FMIYY", date4));
    assertEquals("20", PostgresqlDateTimeFormatter.toChar("FMIYY", date5));

    assertEquals("019TH", PostgresqlDateTimeFormatter.toChar("IYYTH", date1));
    assertEquals("020TH", PostgresqlDateTimeFormatter.toChar("IYYTH", date2));
    assertEquals("020TH", PostgresqlDateTimeFormatter.toChar("IYYTH", date3));
    assertEquals("020TH", PostgresqlDateTimeFormatter.toChar("IYYTH", date4));
    assertEquals("020TH", PostgresqlDateTimeFormatter.toChar("IYYTH", date5));
    assertEquals("019th", PostgresqlDateTimeFormatter.toChar("IYYth", date1));
    assertEquals("020th", PostgresqlDateTimeFormatter.toChar("IYYth", date2));
    assertEquals("020th", PostgresqlDateTimeFormatter.toChar("IYYth", date3));
    assertEquals("020th", PostgresqlDateTimeFormatter.toChar("IYYth", date4));
    assertEquals("020th", PostgresqlDateTimeFormatter.toChar("IYYth", date5));

    assertEquals("20th", PostgresqlDateTimeFormatter.toChar("FMIYYth", date5));
  }

  @Test void testIY() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(1);
    final ZonedDateTime date4 = date3.plusDays(1);
    final ZonedDateTime date5 = date4.plusDays(1);

    assertEquals("19", PostgresqlDateTimeFormatter.toChar("IY", date1));
    assertEquals("20", PostgresqlDateTimeFormatter.toChar("IY", date2));
    assertEquals("20", PostgresqlDateTimeFormatter.toChar("IY", date3));
    assertEquals("20", PostgresqlDateTimeFormatter.toChar("IY", date4));
    assertEquals("20", PostgresqlDateTimeFormatter.toChar("IY", date5));
    assertEquals("19", PostgresqlDateTimeFormatter.toChar("FMIY", date1));
    assertEquals("20", PostgresqlDateTimeFormatter.toChar("FMIY", date2));
    assertEquals("20", PostgresqlDateTimeFormatter.toChar("FMIY", date3));
    assertEquals("20", PostgresqlDateTimeFormatter.toChar("FMIY", date4));
    assertEquals("20", PostgresqlDateTimeFormatter.toChar("FMIY", date5));

    assertEquals("19TH", PostgresqlDateTimeFormatter.toChar("IYTH", date1));
    assertEquals("20TH", PostgresqlDateTimeFormatter.toChar("IYTH", date2));
    assertEquals("20TH", PostgresqlDateTimeFormatter.toChar("IYTH", date3));
    assertEquals("20TH", PostgresqlDateTimeFormatter.toChar("IYTH", date4));
    assertEquals("20TH", PostgresqlDateTimeFormatter.toChar("IYTH", date5));
    assertEquals("19th", PostgresqlDateTimeFormatter.toChar("IYth", date1));
    assertEquals("20th", PostgresqlDateTimeFormatter.toChar("IYth", date2));
    assertEquals("20th", PostgresqlDateTimeFormatter.toChar("IYth", date3));
    assertEquals("20th", PostgresqlDateTimeFormatter.toChar("IYth", date4));
    assertEquals("20th", PostgresqlDateTimeFormatter.toChar("IYth", date5));

    assertEquals("20th", PostgresqlDateTimeFormatter.toChar("FMIYth", date5));
  }

  @Test void testI() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(1);
    final ZonedDateTime date4 = date3.plusDays(1);
    final ZonedDateTime date5 = date4.plusDays(1);

    assertEquals("9", PostgresqlDateTimeFormatter.toChar("I", date1));
    assertEquals("0", PostgresqlDateTimeFormatter.toChar("I", date2));
    assertEquals("0", PostgresqlDateTimeFormatter.toChar("I", date3));
    assertEquals("0", PostgresqlDateTimeFormatter.toChar("I", date4));
    assertEquals("0", PostgresqlDateTimeFormatter.toChar("I", date5));
    assertEquals("9", PostgresqlDateTimeFormatter.toChar("FMI", date1));
    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMI", date2));
    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMI", date3));
    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMI", date4));
    assertEquals("0", PostgresqlDateTimeFormatter.toChar("FMI", date5));

    assertEquals("9TH", PostgresqlDateTimeFormatter.toChar("ITH", date1));
    assertEquals("0TH", PostgresqlDateTimeFormatter.toChar("ITH", date2));
    assertEquals("0TH", PostgresqlDateTimeFormatter.toChar("ITH", date3));
    assertEquals("0TH", PostgresqlDateTimeFormatter.toChar("ITH", date4));
    assertEquals("0TH", PostgresqlDateTimeFormatter.toChar("ITH", date5));
    assertEquals("9th", PostgresqlDateTimeFormatter.toChar("Ith", date1));
    assertEquals("0th", PostgresqlDateTimeFormatter.toChar("Ith", date2));
    assertEquals("0th", PostgresqlDateTimeFormatter.toChar("Ith", date3));
    assertEquals("0th", PostgresqlDateTimeFormatter.toChar("Ith", date4));
    assertEquals("0th", PostgresqlDateTimeFormatter.toChar("Ith", date5));

    assertEquals("0th", PostgresqlDateTimeFormatter.toChar("FMIth", date5));
  }

  @Test void testIW() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(186);

    assertEquals("52", PostgresqlDateTimeFormatter.toChar("IW", date1));
    assertEquals("01", PostgresqlDateTimeFormatter.toChar("IW", date2));
    assertEquals("27", PostgresqlDateTimeFormatter.toChar("IW", date3));
    assertEquals("52", PostgresqlDateTimeFormatter.toChar("FMIW", date1));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMIW", date2));
    assertEquals("27", PostgresqlDateTimeFormatter.toChar("FMIW", date3));

    assertEquals("52ND", PostgresqlDateTimeFormatter.toChar("IWTH", date1));
    assertEquals("01ST", PostgresqlDateTimeFormatter.toChar("IWTH", date2));
    assertEquals("27TH", PostgresqlDateTimeFormatter.toChar("IWTH", date3));
    assertEquals("52nd", PostgresqlDateTimeFormatter.toChar("IWth", date1));
    assertEquals("01st", PostgresqlDateTimeFormatter.toChar("IWth", date2));
    assertEquals("27th", PostgresqlDateTimeFormatter.toChar("IWth", date3));

    assertEquals("27th", PostgresqlDateTimeFormatter.toChar("FMIWth", date3));
  }

  @Test void testIDDD() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(186);

    assertEquals("364", PostgresqlDateTimeFormatter.toChar("IDDD", date1));
    assertEquals("001", PostgresqlDateTimeFormatter.toChar("IDDD", date2));
    assertEquals("187", PostgresqlDateTimeFormatter.toChar("IDDD", date3));
    assertEquals("364", PostgresqlDateTimeFormatter.toChar("FMIDDD", date1));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMIDDD", date2));
    assertEquals("187", PostgresqlDateTimeFormatter.toChar("FMIDDD", date3));

    assertEquals("364TH", PostgresqlDateTimeFormatter.toChar("IDDDTH", date1));
    assertEquals("001ST", PostgresqlDateTimeFormatter.toChar("IDDDTH", date2));
    assertEquals("187TH", PostgresqlDateTimeFormatter.toChar("IDDDTH", date3));
    assertEquals("364th", PostgresqlDateTimeFormatter.toChar("IDDDth", date1));
    assertEquals("001st", PostgresqlDateTimeFormatter.toChar("IDDDth", date2));
    assertEquals("187th", PostgresqlDateTimeFormatter.toChar("IDDDth", date3));

    assertEquals("187th", PostgresqlDateTimeFormatter.toChar("FMIDDDth", date3));
  }

  @Test void testID() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(186);

    assertEquals("7", PostgresqlDateTimeFormatter.toChar("ID", date1));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("ID", date2));
    assertEquals("5", PostgresqlDateTimeFormatter.toChar("ID", date3));
    assertEquals("7", PostgresqlDateTimeFormatter.toChar("FMID", date1));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMID", date2));
    assertEquals("5", PostgresqlDateTimeFormatter.toChar("FMID", date3));

    assertEquals("7TH", PostgresqlDateTimeFormatter.toChar("IDTH", date1));
    assertEquals("1ST", PostgresqlDateTimeFormatter.toChar("IDTH", date2));
    assertEquals("5TH", PostgresqlDateTimeFormatter.toChar("IDTH", date3));
    assertEquals("7th", PostgresqlDateTimeFormatter.toChar("IDth", date1));
    assertEquals("1st", PostgresqlDateTimeFormatter.toChar("IDth", date2));
    assertEquals("5th", PostgresqlDateTimeFormatter.toChar("IDth", date3));

    assertEquals("5th", PostgresqlDateTimeFormatter.toChar("FMIDth", date3));
  }

  @ParameterizedTest
  @ValueSource(strings = {"AD", "BC"})
  void testEraUpperCaseNoDots(String pattern) {
    final ZonedDateTime date1 = createDateTime(2019, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2018);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertEquals("AD", PostgresqlDateTimeFormatter.toChar(pattern, date1));
    assertEquals("AD", PostgresqlDateTimeFormatter.toChar(pattern, date2));
    assertEquals("BC", PostgresqlDateTimeFormatter.toChar(pattern, date3));
    assertEquals("BC", PostgresqlDateTimeFormatter.toChar(pattern, date4));
  }

  @ParameterizedTest
  @ValueSource(strings = {"ad", "bc"})
  void testEraLowerCaseNoDots(String pattern) {
    final ZonedDateTime date1 = createDateTime(2019, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2018);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertEquals("ad", PostgresqlDateTimeFormatter.toChar(pattern, date1));
    assertEquals("ad", PostgresqlDateTimeFormatter.toChar(pattern, date2));
    assertEquals("bc", PostgresqlDateTimeFormatter.toChar(pattern, date3));
    assertEquals("bc", PostgresqlDateTimeFormatter.toChar(pattern, date4));
  }

  @ParameterizedTest
  @ValueSource(strings = {"A.D.", "B.C."})
  void testEraUpperCaseWithDots(String pattern) {
    final ZonedDateTime date1 = createDateTime(2019, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2018);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertEquals("A.D.", PostgresqlDateTimeFormatter.toChar(pattern, date1));
    assertEquals("A.D.", PostgresqlDateTimeFormatter.toChar(pattern, date2));
    assertEquals("B.C.", PostgresqlDateTimeFormatter.toChar(pattern, date3));
    assertEquals("B.C.", PostgresqlDateTimeFormatter.toChar(pattern, date4));
  }

  @ParameterizedTest
  @ValueSource(strings = {"a.d.", "b.c."})
  void testEraLowerCaseWithDots(String pattern) {
    final ZonedDateTime date1 = createDateTime(2019, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2018);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertEquals("a.d.", PostgresqlDateTimeFormatter.toChar(pattern, date1));
    assertEquals("a.d.", PostgresqlDateTimeFormatter.toChar(pattern, date2));
    assertEquals("b.c.", PostgresqlDateTimeFormatter.toChar(pattern, date3));
    assertEquals("b.c.", PostgresqlDateTimeFormatter.toChar(pattern, date4));
  }

  @Test void testMonthFullUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("JANUARY  ", PostgresqlDateTimeFormatter.toChar("MONTH", date1));
    assertEquals("MARCH    ", PostgresqlDateTimeFormatter.toChar("MONTH", date2));
    assertEquals("NOVEMBER ", PostgresqlDateTimeFormatter.toChar("MONTH", date3));
  }

  @Test void testMonthFullUpperCaseNoPadding() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("JANUARY", PostgresqlDateTimeFormatter.toChar("FMMONTH", date1));
    assertEquals("MARCH", PostgresqlDateTimeFormatter.toChar("FMMONTH", date2));
    assertEquals("NOVEMBER", PostgresqlDateTimeFormatter.toChar("FMMONTH", date3));
  }

  @Test void testMonthFullCapitalized() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("January  ", PostgresqlDateTimeFormatter.toChar("Month", date1));
    assertEquals("March    ", PostgresqlDateTimeFormatter.toChar("Month", date2));
    assertEquals("November ", PostgresqlDateTimeFormatter.toChar("Month", date3));
  }

  @Test void testMonthFullLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("january  ", PostgresqlDateTimeFormatter.toChar("month", date1));
    assertEquals("march    ", PostgresqlDateTimeFormatter.toChar("month", date2));
    assertEquals("november ", PostgresqlDateTimeFormatter.toChar("month", date3));
  }

  @Test void testMonthShortUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("JAN", PostgresqlDateTimeFormatter.toChar("MON", date1));
    assertEquals("MAR", PostgresqlDateTimeFormatter.toChar("MON", date2));
    assertEquals("NOV", PostgresqlDateTimeFormatter.toChar("MON", date3));
  }

  @Test void testMonthShortCapitalized() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("Jan", PostgresqlDateTimeFormatter.toChar("Mon", date1));
    assertEquals("Mar", PostgresqlDateTimeFormatter.toChar("Mon", date2));
    assertEquals("Nov", PostgresqlDateTimeFormatter.toChar("Mon", date3));
  }

  @Test void testMonthShortLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("jan", PostgresqlDateTimeFormatter.toChar("mon", date1));
    assertEquals("mar", PostgresqlDateTimeFormatter.toChar("mon", date2));
    assertEquals("nov", PostgresqlDateTimeFormatter.toChar("mon", date3));
  }

  @Test void testMM() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("01", PostgresqlDateTimeFormatter.toChar("MM", date1));
    assertEquals("03", PostgresqlDateTimeFormatter.toChar("MM", date2));
    assertEquals("11", PostgresqlDateTimeFormatter.toChar("MM", date3));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMMM", date1));
    assertEquals("3", PostgresqlDateTimeFormatter.toChar("FMMM", date2));
    assertEquals("11", PostgresqlDateTimeFormatter.toChar("FMMM", date3));

    assertEquals("01ST", PostgresqlDateTimeFormatter.toChar("MMTH", date1));
    assertEquals("03RD", PostgresqlDateTimeFormatter.toChar("MMTH", date2));
    assertEquals("11TH", PostgresqlDateTimeFormatter.toChar("MMTH", date3));
    assertEquals("01st", PostgresqlDateTimeFormatter.toChar("MMth", date1));
    assertEquals("03rd", PostgresqlDateTimeFormatter.toChar("MMth", date2));
    assertEquals("11th", PostgresqlDateTimeFormatter.toChar("MMth", date3));

    assertEquals("3rd", PostgresqlDateTimeFormatter.toChar("FMMMth", date2));
  }

  @Test void testDayFullUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("MONDAY   ", PostgresqlDateTimeFormatter.toChar("DAY", date1));
    assertEquals("FRIDAY   ", PostgresqlDateTimeFormatter.toChar("DAY", date2));
    assertEquals("TUESDAY  ", PostgresqlDateTimeFormatter.toChar("DAY", date3));
  }

  @Test void testDayFullCapitalized() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("Monday   ", PostgresqlDateTimeFormatter.toChar("Day", date1));
    assertEquals("Friday   ", PostgresqlDateTimeFormatter.toChar("Day", date2));
    assertEquals("Tuesday  ", PostgresqlDateTimeFormatter.toChar("Day", date3));
  }

  @Test void testDayFullLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("monday   ", PostgresqlDateTimeFormatter.toChar("day", date1));
    assertEquals("friday   ", PostgresqlDateTimeFormatter.toChar("day", date2));
    assertEquals("tuesday  ", PostgresqlDateTimeFormatter.toChar("day", date3));
  }

  @Test void testDayShortUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("MON", PostgresqlDateTimeFormatter.toChar("DY", date1));
    assertEquals("FRI", PostgresqlDateTimeFormatter.toChar("DY", date2));
    assertEquals("TUE", PostgresqlDateTimeFormatter.toChar("DY", date3));
  }

  @Test void testDayShortCapitalized() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("Mon", PostgresqlDateTimeFormatter.toChar("Dy", date1));
    assertEquals("Fri", PostgresqlDateTimeFormatter.toChar("Dy", date2));
    assertEquals("Tue", PostgresqlDateTimeFormatter.toChar("Dy", date3));
  }

  @Test void testDayShortLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("mon", PostgresqlDateTimeFormatter.toChar("dy", date1));
    assertEquals("fri", PostgresqlDateTimeFormatter.toChar("dy", date2));
    assertEquals("tue", PostgresqlDateTimeFormatter.toChar("dy", date3));
  }

  @Test void testDDD() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("001", PostgresqlDateTimeFormatter.toChar("DDD", date1));
    assertEquals("061", PostgresqlDateTimeFormatter.toChar("DDD", date2));
    assertEquals("306", PostgresqlDateTimeFormatter.toChar("DDD", date3));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMDDD", date1));
    assertEquals("61", PostgresqlDateTimeFormatter.toChar("FMDDD", date2));
    assertEquals("306", PostgresqlDateTimeFormatter.toChar("FMDDD", date3));

    assertEquals("001ST", PostgresqlDateTimeFormatter.toChar("DDDTH", date1));
    assertEquals("061ST", PostgresqlDateTimeFormatter.toChar("DDDTH", date2));
    assertEquals("306TH", PostgresqlDateTimeFormatter.toChar("DDDTH", date3));
    assertEquals("001st", PostgresqlDateTimeFormatter.toChar("DDDth", date1));
    assertEquals("061st", PostgresqlDateTimeFormatter.toChar("DDDth", date2));
    assertEquals("306th", PostgresqlDateTimeFormatter.toChar("DDDth", date3));

    assertEquals("1st", PostgresqlDateTimeFormatter.toChar("FMDDDth", date1));
  }

  @Test void testDD() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 1, 12, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 1, 29, 23, 0, 0, 0);

    assertEquals("01", PostgresqlDateTimeFormatter.toChar("DD", date1));
    assertEquals("12", PostgresqlDateTimeFormatter.toChar("DD", date2));
    assertEquals("29", PostgresqlDateTimeFormatter.toChar("DD", date3));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMDD", date1));
    assertEquals("12", PostgresqlDateTimeFormatter.toChar("FMDD", date2));
    assertEquals("29", PostgresqlDateTimeFormatter.toChar("FMDD", date3));

    assertEquals("01ST", PostgresqlDateTimeFormatter.toChar("DDTH", date1));
    assertEquals("12TH", PostgresqlDateTimeFormatter.toChar("DDTH", date2));
    assertEquals("29TH", PostgresqlDateTimeFormatter.toChar("DDTH", date3));
    assertEquals("01st", PostgresqlDateTimeFormatter.toChar("DDth", date1));
    assertEquals("12th", PostgresqlDateTimeFormatter.toChar("DDth", date2));
    assertEquals("29th", PostgresqlDateTimeFormatter.toChar("DDth", date3));

    assertEquals("1st", PostgresqlDateTimeFormatter.toChar("FMDDth", date1));
  }

  @Test void testD() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 1, 2, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 1, 27, 23, 0, 0, 0);

    assertEquals("2", PostgresqlDateTimeFormatter.toChar("D", date1));
    assertEquals("3", PostgresqlDateTimeFormatter.toChar("D", date2));
    assertEquals("7", PostgresqlDateTimeFormatter.toChar("D", date3));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("FMD", date1));
    assertEquals("3", PostgresqlDateTimeFormatter.toChar("FMD", date2));
    assertEquals("7", PostgresqlDateTimeFormatter.toChar("FMD", date3));

    assertEquals("2ND", PostgresqlDateTimeFormatter.toChar("DTH", date1));
    assertEquals("3RD", PostgresqlDateTimeFormatter.toChar("DTH", date2));
    assertEquals("7TH", PostgresqlDateTimeFormatter.toChar("DTH", date3));
    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("Dth", date1));
    assertEquals("3rd", PostgresqlDateTimeFormatter.toChar("Dth", date2));
    assertEquals("7th", PostgresqlDateTimeFormatter.toChar("Dth", date3));

    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("FMDth", date1));
  }

  @Test void testWW() {
    final ZonedDateTime date1 = createDateTime(2016, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2016, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2016, 10, 1, 23, 0, 0, 0);

    assertEquals("1", PostgresqlDateTimeFormatter.toChar("WW", date1));
    assertEquals("9", PostgresqlDateTimeFormatter.toChar("WW", date2));
    assertEquals("40", PostgresqlDateTimeFormatter.toChar("WW", date3));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMWW", date1));
    assertEquals("9", PostgresqlDateTimeFormatter.toChar("FMWW", date2));
    assertEquals("40", PostgresqlDateTimeFormatter.toChar("FMWW", date3));

    assertEquals("1ST", PostgresqlDateTimeFormatter.toChar("WWTH", date1));
    assertEquals("9TH", PostgresqlDateTimeFormatter.toChar("WWTH", date2));
    assertEquals("40TH", PostgresqlDateTimeFormatter.toChar("WWTH", date3));
    assertEquals("1st", PostgresqlDateTimeFormatter.toChar("WWth", date1));
    assertEquals("9th", PostgresqlDateTimeFormatter.toChar("WWth", date2));
    assertEquals("40th", PostgresqlDateTimeFormatter.toChar("WWth", date3));

    assertEquals("1st", PostgresqlDateTimeFormatter.toChar("FMWWth", date1));
  }

  @Test void testW() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 1, 15, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 31, 23, 0, 0, 0);

    assertEquals("1", PostgresqlDateTimeFormatter.toChar("W", date1));
    assertEquals("3", PostgresqlDateTimeFormatter.toChar("W", date2));
    assertEquals("5", PostgresqlDateTimeFormatter.toChar("W", date3));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMW", date1));
    assertEquals("3", PostgresqlDateTimeFormatter.toChar("FMW", date2));
    assertEquals("5", PostgresqlDateTimeFormatter.toChar("FMW", date3));

    assertEquals("1ST", PostgresqlDateTimeFormatter.toChar("WTH", date1));
    assertEquals("3RD", PostgresqlDateTimeFormatter.toChar("WTH", date2));
    assertEquals("5TH", PostgresqlDateTimeFormatter.toChar("WTH", date3));
    assertEquals("1st", PostgresqlDateTimeFormatter.toChar("Wth", date1));
    assertEquals("3rd", PostgresqlDateTimeFormatter.toChar("Wth", date2));
    assertEquals("5th", PostgresqlDateTimeFormatter.toChar("Wth", date3));

    assertEquals("1st", PostgresqlDateTimeFormatter.toChar("FMWth", date1));
  }

  @Test void testCC() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2023);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertEquals("21", PostgresqlDateTimeFormatter.toChar("CC", date1));
    assertEquals("01", PostgresqlDateTimeFormatter.toChar("CC", date2));
    assertEquals("-01", PostgresqlDateTimeFormatter.toChar("CC", date3));
    assertEquals("-03", PostgresqlDateTimeFormatter.toChar("CC", date4));
    assertEquals("21", PostgresqlDateTimeFormatter.toChar("FMCC", date1));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMCC", date2));
    assertEquals("-1", PostgresqlDateTimeFormatter.toChar("FMCC", date3));
    assertEquals("-3", PostgresqlDateTimeFormatter.toChar("FMCC", date4));

    assertEquals("21ST", PostgresqlDateTimeFormatter.toChar("CCTH", date1));
    assertEquals("01ST", PostgresqlDateTimeFormatter.toChar("CCTH", date2));
    assertEquals("-01ST", PostgresqlDateTimeFormatter.toChar("CCTH", date3));
    assertEquals("-03RD", PostgresqlDateTimeFormatter.toChar("CCTH", date4));
    assertEquals("21st", PostgresqlDateTimeFormatter.toChar("CCth", date1));
    assertEquals("01st", PostgresqlDateTimeFormatter.toChar("CCth", date2));
    assertEquals("-01st", PostgresqlDateTimeFormatter.toChar("CCth", date3));
    assertEquals("-03rd", PostgresqlDateTimeFormatter.toChar("CCth", date4));

    assertEquals("-1st", PostgresqlDateTimeFormatter.toChar("FMCCth", date3));
  }

  @Test void testJ() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2024);
    final ZonedDateTime date3 = date2.minusYears(1000);

    assertEquals("2460311", PostgresqlDateTimeFormatter.toChar("J", date1));
    assertEquals("1721060", PostgresqlDateTimeFormatter.toChar("J", date2));
    assertEquals("1356183", PostgresqlDateTimeFormatter.toChar("J", date3));
    assertEquals("2460311", PostgresqlDateTimeFormatter.toChar("FMJ", date1));
    assertEquals("1721060", PostgresqlDateTimeFormatter.toChar("FMJ", date2));
    assertEquals("1356183", PostgresqlDateTimeFormatter.toChar("FMJ", date3));

    assertEquals("2460311TH", PostgresqlDateTimeFormatter.toChar("JTH", date1));
    assertEquals("1721060TH", PostgresqlDateTimeFormatter.toChar("JTH", date2));
    assertEquals("1356183RD", PostgresqlDateTimeFormatter.toChar("JTH", date3));
    assertEquals("2460311th", PostgresqlDateTimeFormatter.toChar("Jth", date1));
    assertEquals("1721060th", PostgresqlDateTimeFormatter.toChar("Jth", date2));
    assertEquals("1356183rd", PostgresqlDateTimeFormatter.toChar("Jth", date3));
  }

  @Test void testQ() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 4, 9, 0, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 8, 23, 0, 0, 0, 0);
    final ZonedDateTime date4 = createDateTime(2024, 12, 31, 0, 0, 0, 0);

    assertEquals("1", PostgresqlDateTimeFormatter.toChar("Q", date1));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("Q", date2));
    assertEquals("3", PostgresqlDateTimeFormatter.toChar("Q", date3));
    assertEquals("4", PostgresqlDateTimeFormatter.toChar("Q", date4));
    assertEquals("1", PostgresqlDateTimeFormatter.toChar("FMQ", date1));
    assertEquals("2", PostgresqlDateTimeFormatter.toChar("FMQ", date2));
    assertEquals("3", PostgresqlDateTimeFormatter.toChar("FMQ", date3));
    assertEquals("4", PostgresqlDateTimeFormatter.toChar("FMQ", date4));

    assertEquals("1ST", PostgresqlDateTimeFormatter.toChar("QTH", date1));
    assertEquals("2ND", PostgresqlDateTimeFormatter.toChar("QTH", date2));
    assertEquals("3RD", PostgresqlDateTimeFormatter.toChar("QTH", date3));
    assertEquals("4TH", PostgresqlDateTimeFormatter.toChar("QTH", date4));
    assertEquals("1st", PostgresqlDateTimeFormatter.toChar("Qth", date1));
    assertEquals("2nd", PostgresqlDateTimeFormatter.toChar("Qth", date2));
    assertEquals("3rd", PostgresqlDateTimeFormatter.toChar("Qth", date3));
    assertEquals("4th", PostgresqlDateTimeFormatter.toChar("Qth", date4));
  }

  @Test void testRMUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 4, 9, 0, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 8, 23, 0, 0, 0, 0);
    final ZonedDateTime date4 = createDateTime(2024, 12, 31, 0, 0, 0, 0);

    assertEquals("I", PostgresqlDateTimeFormatter.toChar("RM", date1));
    assertEquals("IV", PostgresqlDateTimeFormatter.toChar("RM", date2));
    assertEquals("VIII", PostgresqlDateTimeFormatter.toChar("RM", date3));
    assertEquals("XII", PostgresqlDateTimeFormatter.toChar("RM", date4));
  }

  @Test void testRMLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 4, 9, 0, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 8, 23, 0, 0, 0, 0);
    final ZonedDateTime date4 = createDateTime(2024, 12, 31, 0, 0, 0, 0);

    assertEquals("i", PostgresqlDateTimeFormatter.toChar("rm", date1));
    assertEquals("iv", PostgresqlDateTimeFormatter.toChar("rm", date2));
    assertEquals("viii", PostgresqlDateTimeFormatter.toChar("rm", date3));
    assertEquals("xii", PostgresqlDateTimeFormatter.toChar("rm", date4));
  }

  @Test void testToTimestampHH() throws Exception {
    assertEquals(
        DAY_1_CE.plusHours(1),
        PostgresqlDateTimeFormatter.toTimestamp("01", "HH", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(1),
        PostgresqlDateTimeFormatter.toTimestamp("1", "HH", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(11),
        PostgresqlDateTimeFormatter.toTimestamp("11", "HH", TIME_ZONE));

    try {
      PostgresqlDateTimeFormatter.toTimestamp("72", "HH", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }

    try {
      PostgresqlDateTimeFormatter.toTimestamp("abc", "HH", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampHH12() throws Exception {
    assertEquals(
        DAY_1_CE.plusHours(1),
        PostgresqlDateTimeFormatter.toTimestamp("01", "HH12", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(1),
        PostgresqlDateTimeFormatter.toTimestamp("1", "HH12", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(11),
        PostgresqlDateTimeFormatter.toTimestamp("11", "HH12", TIME_ZONE));

    try {
      PostgresqlDateTimeFormatter.toTimestamp("72", "HH12", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }

    try {
      PostgresqlDateTimeFormatter.toTimestamp("abc", "HH12", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampHH24() throws Exception {
    assertEquals(
        DAY_1_CE.plusHours(1),
        PostgresqlDateTimeFormatter.toTimestamp("01", "HH24", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(1),
        PostgresqlDateTimeFormatter.toTimestamp("1", "HH24", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(18),
        PostgresqlDateTimeFormatter.toTimestamp("18", "HH24", TIME_ZONE));

    try {
      PostgresqlDateTimeFormatter.toTimestamp("72", "HH24", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }

    try {
      PostgresqlDateTimeFormatter.toTimestamp("abc", "HH24", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampMI() throws Exception {
    assertEquals(
        DAY_1_CE.plusMinutes(1),
        PostgresqlDateTimeFormatter.toTimestamp("01", "MI", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMinutes(1),
        PostgresqlDateTimeFormatter.toTimestamp("1", "MI", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMinutes(57),
        PostgresqlDateTimeFormatter.toTimestamp("57", "MI", TIME_ZONE));

    try {
      PostgresqlDateTimeFormatter.toTimestamp("72", "MI", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }

    try {
      PostgresqlDateTimeFormatter.toTimestamp("abc", "MI", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampSS() throws Exception {
    assertEquals(
        DAY_1_CE.plusSeconds(1),
        PostgresqlDateTimeFormatter.toTimestamp("01", "SS", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusSeconds(1),
        PostgresqlDateTimeFormatter.toTimestamp("1", "SS", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusSeconds(57),
        PostgresqlDateTimeFormatter.toTimestamp("57", "SS", TIME_ZONE));

    try {
      PostgresqlDateTimeFormatter.toTimestamp("72", "SS", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }

    try {
      PostgresqlDateTimeFormatter.toTimestamp("abc", "SS", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampMS() throws Exception {
    assertEquals(
        DAY_1_CE.plusNanos(1_000_000),
        PostgresqlDateTimeFormatter.toTimestamp("001", "MS", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(1_000_000),
        PostgresqlDateTimeFormatter.toTimestamp("1", "MS", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(999_000_000),
        PostgresqlDateTimeFormatter.toTimestamp("999", "MS", TIME_ZONE));

    try {
      PostgresqlDateTimeFormatter.toTimestamp("9999", "MS", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }

    try {
      PostgresqlDateTimeFormatter.toTimestamp("abc", "MS", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampUS() throws Exception {
    assertEquals(
        DAY_1_CE.plusNanos(1_000),
        PostgresqlDateTimeFormatter.toTimestamp("001", "US", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(1_000),
        PostgresqlDateTimeFormatter.toTimestamp("1", "US", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(999_000),
        PostgresqlDateTimeFormatter.toTimestamp("999", "US", TIME_ZONE));

    try {
      PostgresqlDateTimeFormatter.toTimestamp("9999999", "US", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }

    try {
      PostgresqlDateTimeFormatter.toTimestamp("abc", "US", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampFF1() throws Exception {
    assertEquals(
        DAY_1_CE.plusNanos(100_000_000),
        PostgresqlDateTimeFormatter.toTimestamp("1", "FF1", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(900_000_000),
        PostgresqlDateTimeFormatter.toTimestamp("9", "FF1", TIME_ZONE));

    try {
      PostgresqlDateTimeFormatter.toTimestamp("72", "FF1", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }

    try {
      PostgresqlDateTimeFormatter.toTimestamp("abc", "FF1", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampFF2() throws Exception {
    assertEquals(
        DAY_1_CE.plusNanos(10_000_000),
        PostgresqlDateTimeFormatter.toTimestamp("01", "FF2", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(10_000_000),
        PostgresqlDateTimeFormatter.toTimestamp("1", "FF2", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(970_000_000),
        PostgresqlDateTimeFormatter.toTimestamp("97", "FF2", TIME_ZONE));

    try {
      PostgresqlDateTimeFormatter.toTimestamp("999", "FF2", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }

    try {
      PostgresqlDateTimeFormatter.toTimestamp("abc", "FF2", TIME_ZONE);
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampFF3() throws Exception {
    assertEquals(
        DAY_1_CE.plusNanos(1_000_000),
        PostgresqlDateTimeFormatter.toTimestamp("001", "FF3", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(1_000_000),
        PostgresqlDateTimeFormatter.toTimestamp("1", "FF3", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(976_000_000),
        PostgresqlDateTimeFormatter.toTimestamp("976", "FF3", TIME_ZONE));
  }

  @Test void testToTimestampFF4() throws Exception {
    assertEquals(
        DAY_1_CE.plusNanos(100_000),
        PostgresqlDateTimeFormatter.toTimestamp("0001", "FF4", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(100_000),
        PostgresqlDateTimeFormatter.toTimestamp("1", "FF4", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(976_200_000),
        PostgresqlDateTimeFormatter.toTimestamp("9762", "FF4", TIME_ZONE));
  }

  @Test void testToTimestampFF5() throws Exception {
    assertEquals(
        DAY_1_CE.plusNanos(10_000),
        PostgresqlDateTimeFormatter.toTimestamp("00001", "FF5", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(10_000),
        PostgresqlDateTimeFormatter.toTimestamp("1", "FF5", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(976_210_000),
        PostgresqlDateTimeFormatter.toTimestamp("97621", "FF5", TIME_ZONE));
  }

  @Test void testToTimestampFF6() throws Exception {
    assertEquals(
        DAY_1_CE.plusNanos(1_000),
        PostgresqlDateTimeFormatter.toTimestamp("000001", "FF6", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(1_000),
        PostgresqlDateTimeFormatter.toTimestamp("1", "FF6", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusNanos(976_214_000),
        PostgresqlDateTimeFormatter.toTimestamp("976214", "FF6", TIME_ZONE));
  }

  @Test void testToTimestampAMPM() throws Exception {
    assertEquals(
        DAY_1_CE.plusHours(3),
        PostgresqlDateTimeFormatter.toTimestamp("03AM", "HH12AM", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(3),
        PostgresqlDateTimeFormatter.toTimestamp("03AM", "HH12PM", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(15),
        PostgresqlDateTimeFormatter.toTimestamp("03PM", "HH12AM", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(15),
        PostgresqlDateTimeFormatter.toTimestamp("03PM", "HH12PM", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(3),
        PostgresqlDateTimeFormatter.toTimestamp("03A.M.", "HH12A.M.", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(3),
        PostgresqlDateTimeFormatter.toTimestamp("03A.M.", "HH12P.M.", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(15),
        PostgresqlDateTimeFormatter.toTimestamp("03P.M.", "HH12A.M.", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(15),
        PostgresqlDateTimeFormatter.toTimestamp("03P.M.", "HH12P.M.", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(3),
        PostgresqlDateTimeFormatter.toTimestamp("03am", "HH12am", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(3),
        PostgresqlDateTimeFormatter.toTimestamp("03am", "HH12pm", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(15),
        PostgresqlDateTimeFormatter.toTimestamp("03pm", "HH12am", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(15),
        PostgresqlDateTimeFormatter.toTimestamp("03pm", "HH12pm", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(3),
        PostgresqlDateTimeFormatter.toTimestamp("03a.m.", "HH12a.m.", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(3),
        PostgresqlDateTimeFormatter.toTimestamp("03a.m.", "HH12p.m.", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(15),
        PostgresqlDateTimeFormatter.toTimestamp("03p.m.", "HH12a.m.", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusHours(15),
        PostgresqlDateTimeFormatter.toTimestamp("03p.m.", "HH12p.m.", TIME_ZONE));
  }

  @Test void testToTimestampYYYYWithCommas() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("0,001", "Y,YYY", TIME_ZONE));
    assertEquals(
        JAN_1_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2,024", "Y,YYY", TIME_ZONE));
  }

  @Test void testToTimestampYYYY() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("0001", "YYYY", TIME_ZONE));
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("1", "YYYY", TIME_ZONE));
    assertEquals(
        JAN_1_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2024", "YYYY", TIME_ZONE));
  }

  @Test void testToTimestampYYY() throws Exception {
    assertEquals(
        JAN_1_2001,
        PostgresqlDateTimeFormatter.toTimestamp("001", "YYY", TIME_ZONE));
    assertEquals(
        JAN_1_2001,
        PostgresqlDateTimeFormatter.toTimestamp("1", "YYY", TIME_ZONE));
    assertEquals(
        createDateTime(1987, 1, 1, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("987", "YYY", TIME_ZONE));
  }

  @Test void testToTimestampYY() throws Exception {
    assertEquals(
        JAN_1_2001,
        PostgresqlDateTimeFormatter.toTimestamp("01", "YY", TIME_ZONE));
    assertEquals(
        JAN_1_2001,
        PostgresqlDateTimeFormatter.toTimestamp("1", "YY", TIME_ZONE));
    assertEquals(
        JAN_1_2024,
        PostgresqlDateTimeFormatter.toTimestamp("24", "YY", TIME_ZONE));
  }

  @Test void testToTimestampY() throws Exception {
    assertEquals(
        JAN_1_2001,
        PostgresqlDateTimeFormatter.toTimestamp("1", "Y", TIME_ZONE));
    assertEquals(
        JAN_1_2001.plusYears(3),
        PostgresqlDateTimeFormatter.toTimestamp("4", "Y", TIME_ZONE));
  }

  @Test void testToTimestampIYYY() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("0001", "IYYY", TIME_ZONE));
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("1", "IYYY", TIME_ZONE));
    assertEquals(
        JAN_1_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2024", "IYYY", TIME_ZONE));
  }

  @Test void testToTimestampIYY() throws Exception {
    assertEquals(
        JAN_1_2001,
        PostgresqlDateTimeFormatter.toTimestamp("001", "IYY", TIME_ZONE));
    assertEquals(
        JAN_1_2001,
        PostgresqlDateTimeFormatter.toTimestamp("1", "IYY", TIME_ZONE));
    assertEquals(
        createDateTime(1987, 1, 1, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("987", "IYY", TIME_ZONE));
  }

  @Test void testToTimestampIY() throws Exception {
    assertEquals(
        JAN_1_2001,
        PostgresqlDateTimeFormatter.toTimestamp("01", "IY", TIME_ZONE));
    assertEquals(
        JAN_1_2001,
        PostgresqlDateTimeFormatter.toTimestamp("1", "IY", TIME_ZONE));
    assertEquals(
        JAN_1_2024,
        PostgresqlDateTimeFormatter.toTimestamp("24", "IY", TIME_ZONE));
  }

  @Test void testToTimestampI() throws Exception {
    assertEquals(
        JAN_1_2001,
        PostgresqlDateTimeFormatter.toTimestamp("1", "I", TIME_ZONE));
    assertEquals(
        JAN_1_2001,
        PostgresqlDateTimeFormatter.toTimestamp("1", "I", TIME_ZONE));
    assertEquals(
        JAN_1_2001.plusYears(3),
        PostgresqlDateTimeFormatter.toTimestamp("4", "I", TIME_ZONE));
  }

  @Test void testToTimestampBCAD() throws Exception {
    assertEquals(
        0,
        PostgresqlDateTimeFormatter.toTimestamp("1920BC", "YYYYBC", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        0,
        PostgresqlDateTimeFormatter.toTimestamp("1920BC", "YYYYAD", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        1,
        PostgresqlDateTimeFormatter.toTimestamp("1920AD", "YYYYBC", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        1,
        PostgresqlDateTimeFormatter.toTimestamp("1920AD", "YYYYAD", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        0,
        PostgresqlDateTimeFormatter.toTimestamp("1920B.C.", "YYYYB.C.", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        0,
        PostgresqlDateTimeFormatter.toTimestamp("1920B.C.", "YYYYA.D.", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        1,
        PostgresqlDateTimeFormatter.toTimestamp("1920A.D.", "YYYYB.C.", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        1,
        PostgresqlDateTimeFormatter.toTimestamp("1920A.D.", "YYYYA.D.", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        0,
        PostgresqlDateTimeFormatter.toTimestamp("1920bc", "YYYYbc", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        0,
        PostgresqlDateTimeFormatter.toTimestamp("1920bc", "YYYYad", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        1,
        PostgresqlDateTimeFormatter.toTimestamp("1920ad", "YYYYbc", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        1,
        PostgresqlDateTimeFormatter.toTimestamp("1920ad", "YYYYad", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        0,
        PostgresqlDateTimeFormatter.toTimestamp("1920b.c.", "YYYYb.c.", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        0,
        PostgresqlDateTimeFormatter.toTimestamp("1920b.c.", "YYYYa.d.", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        1,
        PostgresqlDateTimeFormatter.toTimestamp("1920a.d.", "YYYYb.c.", TIME_ZONE)
            .get(ChronoField.ERA));
    assertEquals(
        1,
        PostgresqlDateTimeFormatter.toTimestamp("1920a.d.", "YYYYa.d.", TIME_ZONE)
            .get(ChronoField.ERA));
  }

  @Test void testToTimestampMonthUpperCase() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("JANUARY", "MONTH", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(2),
        PostgresqlDateTimeFormatter.toTimestamp("MARCH", "MONTH", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(10),
        PostgresqlDateTimeFormatter.toTimestamp("NOVEMBER", "MONTH", TIME_ZONE));
  }

  @Test void testToTimestampMonthCapitalized() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("January", "Month", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(2),
        PostgresqlDateTimeFormatter.toTimestamp("March", "Month", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(10),
        PostgresqlDateTimeFormatter.toTimestamp("November", "Month", TIME_ZONE));
  }

  @Test void testToTimestampMonthLowerCase() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("january", "month", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(2),
        PostgresqlDateTimeFormatter.toTimestamp("march", "month", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(10),
        PostgresqlDateTimeFormatter.toTimestamp("november", "month", TIME_ZONE));
  }

  @Test void testToTimestampMonUpperCase() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("JAN", "MON", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(2),
        PostgresqlDateTimeFormatter.toTimestamp("MAR", "MON", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(10),
        PostgresqlDateTimeFormatter.toTimestamp("NOV", "MON", TIME_ZONE));
  }

  @Test void testToTimestampMonCapitalized() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("Jan", "Mon", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(2),
        PostgresqlDateTimeFormatter.toTimestamp("Mar", "Mon", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(10),
        PostgresqlDateTimeFormatter.toTimestamp("Nov", "Mon", TIME_ZONE));
  }

  @Test void testToTimestampMonLowerCase() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("jan", "mon", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(2),
        PostgresqlDateTimeFormatter.toTimestamp("mar", "mon", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(10),
        PostgresqlDateTimeFormatter.toTimestamp("nov", "mon", TIME_ZONE));
  }

  @Test void testToTimestampMM() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("01", "MM", TIME_ZONE));
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("1", "MM", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(10),
        PostgresqlDateTimeFormatter.toTimestamp("11", "MM", TIME_ZONE));
  }

  @Test void testToTimestampDayUpperCase() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 MONDAY", "IYYY IW DAY", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 THURSDAY", "IYYY IW DAY", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 FRIDAY", "IYYY IW DAY", TIME_ZONE));
  }

  @Test void testToTimestampDayCapitalized() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 Monday", "IYYY IW Day", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 Thursday", "IYYY IW Day", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 Friday", "IYYY IW Day", TIME_ZONE));
  }

  @Test void testToTimestampDayLowerCase() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 monday", "IYYY IW day", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 thursday", "IYYY IW day", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 friday", "IYYY IW day", TIME_ZONE));
  }

  @Test void testToTimestampDyUpperCase() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 MON", "IYYY IW DY", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 THU", "IYYY IW DY", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 FRI", "IYYY IW DY", TIME_ZONE));
  }

  @Test void testToTimestampDyCapitalized() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 Mon", "IYYY IW Dy", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 Thu", "IYYY IW Dy", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 Fri", "IYYY IW Dy", TIME_ZONE));
  }

  @Test void testToTimestampDyLowerCase() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 mon", "IYYY IW dy", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 thu", "IYYY IW dy", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 fri", "IYYY IW dy", TIME_ZONE));
  }

  @Test void testToTimestampDDD() throws Exception {
    assertEquals(
        JAN_1_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2024 001", "YYYY DDD", TIME_ZONE));
    assertEquals(
        JAN_1_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2024 1", "YYYY DDD", TIME_ZONE));
    assertEquals(
        createDateTime(2024, 5, 16, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("2024 137", "YYYY DDD", TIME_ZONE));
  }

  @Test void testToTimestampDD() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("01", "DD", TIME_ZONE));
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("1", "DD", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusDays(22),
        PostgresqlDateTimeFormatter.toTimestamp("23", "DD", TIME_ZONE));
  }

  @Test void testToTimestampIDDD() throws Exception {
    assertEquals(
        createDateTime(2019, 12, 30, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("2020 001", "IYYY IDDD", TIME_ZONE));
    assertEquals(
        createDateTime(2019, 12, 30, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("2020 1", "IYYY IDDD", TIME_ZONE));
    assertEquals(
        createDateTime(2020, 5, 14, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("2020 137", "IYYY IDDD", TIME_ZONE));
  }

  @Test void testToTimestampID() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 1", "IYYY IW ID", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 4", "IYYY IW ID", TIME_ZONE));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1982 23 5", "IYYY IW ID", TIME_ZONE));
  }

  @Test void testToTimestampW() throws Exception {
    assertEquals(
        JAN_1_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2024 1 1", "YYYY MM W", TIME_ZONE));
    assertEquals(
        createDateTime(2024, 4, 8, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("2024 4 2", "YYYY MM W", TIME_ZONE));
    assertEquals(
        createDateTime(2024, 11, 22, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("2024 11 4", "YYYY MM W", TIME_ZONE));
  }

  @Test void testToTimestampWW() throws Exception {
    assertEquals(
        JAN_1_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2024 01", "YYYY WW", TIME_ZONE));
    assertEquals(
        JAN_1_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2024 1", "YYYY WW", TIME_ZONE));
    assertEquals(
        createDateTime(2024, 12, 16, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("2024 51", "YYYY WW", TIME_ZONE));
  }

  @Test void testToTimestampIW() throws Exception {
    assertEquals(
        createDateTime(2019, 12, 30, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("2020 01", "IYYY IW", TIME_ZONE));
    assertEquals(
        createDateTime(2019, 12, 30, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("2020 1", "IYYY IW", TIME_ZONE));
    assertEquals(
        createDateTime(2020, 12, 14, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("2020 51", "IYYY IW", TIME_ZONE));
  }

  @Test void testToTimestampCC() throws Exception {
    assertEquals(
        JAN_1_2001,
        PostgresqlDateTimeFormatter.toTimestamp("21", "CC", TIME_ZONE));
    assertEquals(
        createDateTime(1501, 1, 1, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("16", "CC", TIME_ZONE));
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("1", "CC", TIME_ZONE));
  }

  @Test void testToTimestampJ() throws Exception {
    assertEquals(
        JAN_1_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2460311", "J", TIME_ZONE));
    assertEquals(
        createDateTime(1984, 7, 15, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("2445897", "J", TIME_ZONE));
    assertEquals(
        createDateTime(234, 3, 21, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("1806606", "J", TIME_ZONE));
  }

  @Test void testToTimestampRMUpperCase() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("I", "RM", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(3),
        PostgresqlDateTimeFormatter.toTimestamp("IV", "RM", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(8),
        PostgresqlDateTimeFormatter.toTimestamp("IX", "RM", TIME_ZONE));
  }

  @Test void testToTimestampRMLowerCase() throws Exception {
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("i", "rm", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(3),
        PostgresqlDateTimeFormatter.toTimestamp("iv", "rm", TIME_ZONE));
    assertEquals(
        DAY_1_CE.plusMonths(8),
        PostgresqlDateTimeFormatter.toTimestamp("ix", "rm", TIME_ZONE));
  }

  @Test void testToTimestampDateValidFormats() throws Exception {
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2024-04-17", "YYYY-MM-DD", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2,024-04-17", "Y,YYY-MM-DD", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("24-04-17", "YYY-MM-DD", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("24-04-17", "YY-MM-DD", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2124-04-17", "CCYY-MM-DD", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("20240417", "YYYYMMDD", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2,0240417", "Y,YYYMMDD", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2024-16-3", "IYYY-IW-ID", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2024-16 Wednesday", "IYYY-IW Day", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2024-108", "IYYY-IDDD", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("April 17, 2024", "Month DD, YYYY", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("IV 17, 2024", "RM DD, YYYY", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("APR 17, 2024", "MON DD, YYYY", TIME_ZONE));
    assertEquals(
        createDateTime(2024, 4, 15, 0, 0, 0, 0),
        PostgresqlDateTimeFormatter.toTimestamp("2024-16", "YYYY-WW", TIME_ZONE));
    assertEquals(
        APR_17_2024,
        PostgresqlDateTimeFormatter.toTimestamp("2024-108", "YYYY-DDD", TIME_ZONE));
    assertEquals(
        DAY_1_CE,
        PostgresqlDateTimeFormatter.toTimestamp("0000-01-01", "YYYY-MM-DD", TIME_ZONE));
  }

  @Test void testToTimestampWithTimezone() throws Exception {
    final ZoneId utcZone = ZoneId.of("UTC");
    assertEquals(
        APR_17_2024.plusHours(7).withZoneSameLocal(utcZone),
        PostgresqlDateTimeFormatter.toTimestamp("2024-04-17 00:00:00-07:00",
            "YYYY-MM-DD HH24:MI:SSTZH:TZM", utcZone));
  }

  protected static ZonedDateTime createDateTime(int year, int month, int dayOfMonth, int hour,
      int minute, int seconds, int nanoseconds) {
    return ZonedDateTime.of(
        LocalDateTime.of(year, month, dayOfMonth, hour, minute, seconds, nanoseconds),
        ZoneId.systemDefault());
  }
}
