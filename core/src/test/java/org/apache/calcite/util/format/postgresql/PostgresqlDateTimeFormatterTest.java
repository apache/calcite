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
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for {@link PostgresqlDateTimeFormatter}.
 */
public class PostgresqlDateTimeFormatterTest {
  private static final ZoneId TIME_ZONE;
  static {
    ZoneId timeZone;
    try {
      timeZone = ZoneId.of("America/Chicago");
    } catch (Exception e) {
      timeZone = ZoneId.systemDefault();
    }
    TIME_ZONE = timeZone;
  }

  private static final ZonedDateTime DAY_1_CE = createDateTime(1, 1, 1, 0, 0, 0, 0);
  private static final ZonedDateTime APR_17_2024 = createDateTime(2024, 4, 17, 0, 0, 0, 0);
  private static final ZonedDateTime JAN_1_2001 = createDateTime(2001, 1, 1, 0, 0, 0, 0);
  private static final ZonedDateTime JAN_1_2024 = createDateTime(2024, 1, 1, 0, 0, 0, 0);

  private String toCharUs(String pattern, ZonedDateTime dateTime) {
    return PostgresqlDateTimeFormatter.toChar(pattern, dateTime, Locale.US);
  }

  private String toCharFrench(String pattern, ZonedDateTime dateTime) {
    return PostgresqlDateTimeFormatter.toChar(pattern, dateTime, Locale.FRENCH);
  }

  private ZonedDateTime toTimestamp(String input, String format) throws Exception {
    return PostgresqlDateTimeFormatter.toTimestamp(input, format, TIME_ZONE, Locale.US);
  }

  @ParameterizedTest
  @ValueSource(strings = {"HH12", "HH"})
  void testHH12(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertEquals("12", toCharUs(pattern, midnight));
    assertEquals("06", toCharUs(pattern, morning));
    assertEquals("12", toCharUs(pattern, noon));
    assertEquals("06", toCharUs(pattern, evening));
    assertEquals("12", toCharUs("FM" + pattern, midnight));
    assertEquals("6", toCharUs("FM" + pattern, morning));
    assertEquals("12", toCharUs("FM" + pattern, noon));
    assertEquals("6", toCharUs("FM" + pattern, evening));

    final ZonedDateTime hourOne = createDateTime(2024, 1, 1, 1, 0, 0, 0);
    final ZonedDateTime hourTwo = createDateTime(2024, 1, 1, 2, 0, 0, 0);
    final ZonedDateTime hourThree = createDateTime(2024, 1, 1, 3, 0, 0, 0);
    assertEquals("12TH", toCharUs(pattern + "TH", midnight));
    assertEquals("01ST", toCharUs(pattern + "TH", hourOne));
    assertEquals("02ND", toCharUs(pattern + "TH", hourTwo));
    assertEquals("03RD", toCharUs(pattern + "TH", hourThree));
    assertEquals("12th", toCharUs(pattern + "th", midnight));
    assertEquals("01st", toCharUs(pattern + "th", hourOne));
    assertEquals("02nd", toCharUs(pattern + "th", hourTwo));
    assertEquals("03rd", toCharUs(pattern + "th", hourThree));

    assertEquals("2nd", toCharUs("FM" + pattern + "th", hourTwo));
  }

  @Test void testHH24() {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertEquals("00", toCharUs("HH24", midnight));
    assertEquals("06", toCharUs("HH24", morning));
    assertEquals("12", toCharUs("HH24", noon));
    assertEquals("18", toCharUs("HH24", evening));
    assertEquals("0", toCharUs("FMHH24", midnight));
    assertEquals("6", toCharUs("FMHH24", morning));
    assertEquals("12", toCharUs("FMHH24", noon));
    assertEquals("18", toCharUs("FMHH24", evening));

    final ZonedDateTime hourOne = createDateTime(2024, 1, 1, 1, 0, 0, 0);
    final ZonedDateTime hourTwo = createDateTime(2024, 1, 1, 2, 0, 0, 0);
    final ZonedDateTime hourThree = createDateTime(2024, 1, 1, 3, 0, 0, 0);
    assertEquals("00TH", toCharUs("HH24TH", midnight));
    assertEquals("01ST", toCharUs("HH24TH", hourOne));
    assertEquals("02ND", toCharUs("HH24TH", hourTwo));
    assertEquals("03RD", toCharUs("HH24TH", hourThree));
    assertEquals("00th", toCharUs("HH24th", midnight));
    assertEquals("01st", toCharUs("HH24th", hourOne));
    assertEquals("02nd", toCharUs("HH24th", hourTwo));
    assertEquals("03rd", toCharUs("HH24th", hourThree));

    assertEquals("2nd", toCharUs("FMHH24th", hourTwo));
  }

  @Test void testMI() {
    final ZonedDateTime minute0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime minute2 = createDateTime(2024, 1, 1, 0, 2, 0, 0);
    final ZonedDateTime minute15 = createDateTime(2024, 1, 1, 0, 15, 0, 0);

    assertEquals("00", toCharUs("MI", minute0));
    assertEquals("02", toCharUs("MI", minute2));
    assertEquals("15", toCharUs("MI", minute15));

    assertEquals("0", toCharUs("FMMI", minute0));
    assertEquals("2", toCharUs("FMMI", minute2));
    assertEquals("15", toCharUs("FMMI", minute15));

    assertEquals("00TH", toCharUs("MITH", minute0));
    assertEquals("02ND", toCharUs("MITH", minute2));
    assertEquals("15TH", toCharUs("MITH", minute15));
    assertEquals("00th", toCharUs("MIth", minute0));
    assertEquals("02nd", toCharUs("MIth", minute2));
    assertEquals("15th", toCharUs("MIth", minute15));

    assertEquals("2nd", toCharUs("FMMIth", minute2));
    assertEquals("2nd", toCharUs("FMMInd", minute2));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SSSSS", "SSSS"})
  void testSSSSS(String pattern) {
    final ZonedDateTime second0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime second1001 = createDateTime(2024, 1, 1, 0, 16, 41, 0);
    final ZonedDateTime endOfDay = createDateTime(2024, 1, 1, 23, 59, 59, 0);

    assertEquals("0", toCharUs(pattern, second0));
    assertEquals("1001", toCharUs(pattern, second1001));
    assertEquals("86399", toCharUs(pattern, endOfDay));

    assertEquals("0", toCharUs("FM" + pattern, second0));
    assertEquals("1001", toCharUs("FM" + pattern, second1001));
    assertEquals("86399", toCharUs("FM" + pattern, endOfDay));

    assertEquals("0TH", toCharUs(pattern + "TH", second0));
    assertEquals("1001ST", toCharUs(pattern + "TH", second1001));
    assertEquals("86399TH", toCharUs(pattern + "TH", endOfDay));
    assertEquals("0th", toCharUs(pattern + "th", second0));
    assertEquals("1001st", toCharUs(pattern + "th", second1001));
    assertEquals("86399th", toCharUs(pattern + "th", endOfDay));

    assertEquals("1001st", toCharUs("FM" + pattern + "th", second1001));
    assertEquals("1001nd", toCharUs("FM" + pattern + "nd", second1001));
  }

  @Test void testSS() {
    final ZonedDateTime second0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime second2 = createDateTime(2024, 1, 1, 0, 0, 2, 0);
    final ZonedDateTime second15 = createDateTime(2024, 1, 1, 0, 0, 15, 0);

    assertEquals("00", toCharUs("SS", second0));
    assertEquals("02", toCharUs("SS", second2));
    assertEquals("15", toCharUs("SS", second15));

    assertEquals("0", toCharUs("FMSS", second0));
    assertEquals("2", toCharUs("FMSS", second2));
    assertEquals("15", toCharUs("FMSS", second15));

    assertEquals("00TH", toCharUs("SSTH", second0));
    assertEquals("02ND", toCharUs("SSTH", second2));
    assertEquals("15TH", toCharUs("SSTH", second15));
    assertEquals("00th", toCharUs("SSth", second0));
    assertEquals("02nd", toCharUs("SSth", second2));
    assertEquals("15th", toCharUs("SSth", second15));

    assertEquals("2nd", toCharUs("FMSSth", second2));
    assertEquals("2nd", toCharUs("FMSSnd", second2));
  }

  @ParameterizedTest
  @ValueSource(strings = {"MS", "FF3"})
  void testMS(String pattern) {
    final ZonedDateTime ms0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime ms2 = createDateTime(2024, 1, 1, 0, 0, 2, 2000000);
    final ZonedDateTime ms15 = createDateTime(2024, 1, 1, 0, 0, 15, 15000000);

    assertEquals("000", toCharUs(pattern, ms0));
    assertEquals("002", toCharUs(pattern, ms2));
    assertEquals("015", toCharUs(pattern, ms15));

    assertEquals("0", toCharUs("FM" + pattern, ms0));
    assertEquals("2", toCharUs("FM" + pattern, ms2));
    assertEquals("15", toCharUs("FM" + pattern, ms15));

    assertEquals("000TH", toCharUs(pattern + "TH", ms0));
    assertEquals("002ND", toCharUs(pattern + "TH", ms2));
    assertEquals("015TH", toCharUs(pattern + "TH", ms15));
    assertEquals("000th", toCharUs(pattern + "th", ms0));
    assertEquals("002nd", toCharUs(pattern + "th", ms2));
    assertEquals("015th", toCharUs(pattern + "th", ms15));

    assertEquals("2nd", toCharUs("FM" + pattern + "th", ms2));
    assertEquals("2nd", toCharUs("FM" + pattern + "nd", ms2));
  }

  @Test void testUS() {
    final ZonedDateTime us0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime us2 = createDateTime(2024, 1, 1, 0, 0, 0, 2000);
    final ZonedDateTime us15 = createDateTime(2024, 1, 1, 0, 0, 0, 15000);
    final ZonedDateTime usWithMs = createDateTime(2024, 1, 1, 0, 0, 0, 2015000);

    assertEquals("000000", toCharUs("US", us0));
    assertEquals("000002", toCharUs("US", us2));
    assertEquals("000015", toCharUs("US", us15));
    assertEquals("002015", toCharUs("US", usWithMs));

    assertEquals("0", toCharUs("FMUS", us0));
    assertEquals("2", toCharUs("FMUS", us2));
    assertEquals("15", toCharUs("FMUS", us15));
    assertEquals("2015", toCharUs("FMUS", usWithMs));

    assertEquals("000000TH", toCharUs("USTH", us0));
    assertEquals("000002ND", toCharUs("USTH", us2));
    assertEquals("000015TH", toCharUs("USTH", us15));
    assertEquals("002015TH", toCharUs("USTH", usWithMs));
    assertEquals("000000th", toCharUs("USth", us0));
    assertEquals("000002nd", toCharUs("USth", us2));
    assertEquals("000015th", toCharUs("USth", us15));
    assertEquals("002015th", toCharUs("USth", usWithMs));

    assertEquals("2nd", toCharUs("FMUSth", us2));
    assertEquals("2nd", toCharUs("FMUSnd", us2));
  }

  @Test void testFF1() {
    final ZonedDateTime ms0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime ms200 = createDateTime(2024, 1, 1, 0, 0, 0, 200_000_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertEquals("0", toCharUs("FF1", ms0));
    assertEquals("2", toCharUs("FF1", ms200));
    assertEquals("1", toCharUs("FF1", ms150));

    assertEquals("0", toCharUs("FMFF1", ms0));
    assertEquals("2", toCharUs("FMFF1", ms200));
    assertEquals("1", toCharUs("FMFF1", ms150));

    assertEquals("0TH", toCharUs("FF1TH", ms0));
    assertEquals("2ND", toCharUs("FF1TH", ms200));
    assertEquals("1ST", toCharUs("FF1TH", ms150));
    assertEquals("0th", toCharUs("FF1th", ms0));
    assertEquals("2nd", toCharUs("FF1th", ms200));
    assertEquals("1st", toCharUs("FF1th", ms150));

    assertEquals("2nd", toCharUs("FMFF1th", ms200));
    assertEquals("2nd", toCharUs("FMFF1nd", ms200));
  }

  @Test void testFF2() {
    final ZonedDateTime ms0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime ms20 = createDateTime(2024, 1, 1, 0, 0, 0, 20_000_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertEquals("00", toCharUs("FF2", ms0));
    assertEquals("02", toCharUs("FF2", ms20));
    assertEquals("15", toCharUs("FF2", ms150));

    assertEquals("0", toCharUs("FMFF2", ms0));
    assertEquals("2", toCharUs("FMFF2", ms20));
    assertEquals("15", toCharUs("FMFF2", ms150));

    assertEquals("00TH", toCharUs("FF2TH", ms0));
    assertEquals("02ND", toCharUs("FF2TH", ms20));
    assertEquals("15TH", toCharUs("FF2TH", ms150));
    assertEquals("00th", toCharUs("FF2th", ms0));
    assertEquals("02nd", toCharUs("FF2th", ms20));
    assertEquals("15th", toCharUs("FF2th", ms150));

    assertEquals("2nd", toCharUs("FMFF2th", ms20));
    assertEquals("2nd", toCharUs("FMFF2nd", ms20));
  }

  @Test void testFF4() {
    final ZonedDateTime us0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime us200 = createDateTime(2024, 1, 1, 0, 0, 0, 200_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertEquals("0000", toCharUs("FF4", us0));
    assertEquals("0002", toCharUs("FF4", us200));
    assertEquals("1500", toCharUs("FF4", ms150));

    assertEquals("0", toCharUs("FMFF4", us0));
    assertEquals("2", toCharUs("FMFF4", us200));
    assertEquals("1500", toCharUs("FMFF4", ms150));

    assertEquals("0000TH", toCharUs("FF4TH", us0));
    assertEquals("0002ND", toCharUs("FF4TH", us200));
    assertEquals("1500TH", toCharUs("FF4TH", ms150));
    assertEquals("0000th", toCharUs("FF4th", us0));
    assertEquals("0002nd", toCharUs("FF4th", us200));
    assertEquals("1500th", toCharUs("FF4th", ms150));

    assertEquals("2nd", toCharUs("FMFF4th", us200));
    assertEquals("2nd", toCharUs("FMFF4nd", us200));
  }

  @Test void testFF5() {
    final ZonedDateTime us0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime us20 = createDateTime(2024, 1, 1, 0, 0, 0, 20_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertEquals("00000", toCharUs("FF5", us0));
    assertEquals("00002", toCharUs("FF5", us20));
    assertEquals("15000", toCharUs("FF5", ms150));

    assertEquals("0", toCharUs("FMFF5", us0));
    assertEquals("2", toCharUs("FMFF5", us20));
    assertEquals("15000", toCharUs("FMFF5", ms150));

    assertEquals("00000TH", toCharUs("FF5TH", us0));
    assertEquals("00002ND", toCharUs("FF5TH", us20));
    assertEquals("15000TH", toCharUs("FF5TH", ms150));
    assertEquals("00000th", toCharUs("FF5th", us0));
    assertEquals("00002nd", toCharUs("FF5th", us20));
    assertEquals("15000th", toCharUs("FF5th", ms150));

    assertEquals("2nd", toCharUs("FMFF5th", us20));
    assertEquals("2nd", toCharUs("FMFF5nd", us20));
  }

  @Test void testFF6() {
    final ZonedDateTime us0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime us2 = createDateTime(2024, 1, 1, 0, 0, 0, 2_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertEquals("000000", toCharUs("FF6", us0));
    assertEquals("000002", toCharUs("FF6", us2));
    assertEquals("150000", toCharUs("FF6", ms150));

    assertEquals("0", toCharUs("FMFF6", us0));
    assertEquals("2", toCharUs("FMFF6", us2));
    assertEquals("150000", toCharUs("FMFF6", ms150));

    assertEquals("000000TH", toCharUs("FF6TH", us0));
    assertEquals("000002ND", toCharUs("FF6TH", us2));
    assertEquals("150000TH", toCharUs("FF6TH", ms150));
    assertEquals("000000th", toCharUs("FF6th", us0));
    assertEquals("000002nd", toCharUs("FF6th", us2));
    assertEquals("150000th", toCharUs("FF6th", ms150));

    assertEquals("2nd", toCharUs("FMFF6th", us2));
    assertEquals("2nd", toCharUs("FMFF6nd", us2));
  }

  @ParameterizedTest
  @ValueSource(strings = {"AM", "PM"})
  void testAMUpperCase(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertEquals("AM", toCharUs(pattern, midnight));
    assertEquals("AM", toCharUs(pattern, morning));
    assertEquals("PM", toCharUs(pattern, noon));
    assertEquals("PM", toCharUs(pattern, evening));
  }

  @ParameterizedTest
  @ValueSource(strings = {"am", "pm"})
  void testAMLowerCase(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertEquals("am", toCharUs(pattern, midnight));
    assertEquals("am", toCharUs(pattern, morning));
    assertEquals("pm", toCharUs(pattern, noon));
    assertEquals("pm", toCharUs(pattern, evening));
  }

  @ParameterizedTest
  @ValueSource(strings = {"A.M.", "P.M."})
  void testAMWithDotsUpperCase(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertEquals("A.M.", toCharUs(pattern, midnight));
    assertEquals("A.M.", toCharUs(pattern, morning));
    assertEquals("P.M.", toCharUs(pattern, noon));
    assertEquals("P.M.", toCharUs(pattern, evening));
  }

  @ParameterizedTest
  @ValueSource(strings = {"a.m.", "p.m."})
  void testAMWithDotsLowerCase(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertEquals("a.m.", toCharUs(pattern, midnight));
    assertEquals("a.m.", toCharUs(pattern, morning));
    assertEquals("p.m.", toCharUs(pattern, noon));
    assertEquals("p.m.", toCharUs(pattern, evening));
  }

  @Test void testYearWithCommas() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertEquals("2,024", toCharUs("Y,YYY", year1));
    assertEquals("0,100", toCharUs("Y,YYY", year2));
    assertEquals("0,001", toCharUs("Y,YYY", year3));
    assertEquals("32,136", toCharUs("Y,YYY", year4));
    assertEquals("2,024", toCharUs("FMY,YYY", year1));
    assertEquals("0,100", toCharUs("FMY,YYY", year2));
    assertEquals("0,001", toCharUs("FMY,YYY", year3));
    assertEquals("32,136", toCharUs("FMY,YYY", year4));

    assertEquals("2,024TH", toCharUs("Y,YYYTH", year1));
    assertEquals("0,100TH", toCharUs("Y,YYYTH", year2));
    assertEquals("0,001ST", toCharUs("Y,YYYTH", year3));
    assertEquals("32,136TH", toCharUs("Y,YYYTH", year4));
    assertEquals("2,024th", toCharUs("Y,YYYth", year1));
    assertEquals("0,100th", toCharUs("Y,YYYth", year2));
    assertEquals("0,001st", toCharUs("Y,YYYth", year3));
    assertEquals("32,136th", toCharUs("Y,YYYth", year4));

    assertEquals("2,024th", toCharUs("FMY,YYYth", year1));
  }

  @Test void testYYYY() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertEquals("2024", toCharUs("YYYY", year1));
    assertEquals("0100", toCharUs("YYYY", year2));
    assertEquals("0001", toCharUs("YYYY", year3));
    assertEquals("32136", toCharUs("YYYY", year4));
    assertEquals("2024", toCharUs("FMYYYY", year1));
    assertEquals("100", toCharUs("FMYYYY", year2));
    assertEquals("1", toCharUs("FMYYYY", year3));
    assertEquals("32136", toCharUs("FMYYYY", year4));

    assertEquals("2024TH", toCharUs("YYYYTH", year1));
    assertEquals("0100TH", toCharUs("YYYYTH", year2));
    assertEquals("0001ST", toCharUs("YYYYTH", year3));
    assertEquals("32136TH", toCharUs("YYYYTH", year4));
    assertEquals("2024th", toCharUs("YYYYth", year1));
    assertEquals("0100th", toCharUs("YYYYth", year2));
    assertEquals("0001st", toCharUs("YYYYth", year3));
    assertEquals("32136th", toCharUs("YYYYth", year4));

    assertEquals("2024th", toCharUs("FMYYYYth", year1));
  }

  @Test void testYYY() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertEquals("024", toCharUs("YYY", year1));
    assertEquals("100", toCharUs("YYY", year2));
    assertEquals("001", toCharUs("YYY", year3));
    assertEquals("136", toCharUs("YYY", year4));
    assertEquals("24", toCharUs("FMYYY", year1));
    assertEquals("100", toCharUs("FMYYY", year2));
    assertEquals("1", toCharUs("FMYYY", year3));
    assertEquals("136", toCharUs("FMYYY", year4));

    assertEquals("024TH", toCharUs("YYYTH", year1));
    assertEquals("100TH", toCharUs("YYYTH", year2));
    assertEquals("001ST", toCharUs("YYYTH", year3));
    assertEquals("136TH", toCharUs("YYYTH", year4));
    assertEquals("024th", toCharUs("YYYth", year1));
    assertEquals("100th", toCharUs("YYYth", year2));
    assertEquals("001st", toCharUs("YYYth", year3));
    assertEquals("136th", toCharUs("YYYth", year4));

    assertEquals("24th", toCharUs("FMYYYth", year1));
  }

  @Test void testYY() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertEquals("24", toCharUs("YY", year1));
    assertEquals("00", toCharUs("YY", year2));
    assertEquals("01", toCharUs("YY", year3));
    assertEquals("36", toCharUs("YY", year4));
    assertEquals("24", toCharUs("FMYY", year1));
    assertEquals("0", toCharUs("FMYY", year2));
    assertEquals("1", toCharUs("FMYY", year3));
    assertEquals("36", toCharUs("FMYY", year4));

    assertEquals("24TH", toCharUs("YYTH", year1));
    assertEquals("00TH", toCharUs("YYTH", year2));
    assertEquals("01ST", toCharUs("YYTH", year3));
    assertEquals("36TH", toCharUs("YYTH", year4));
    assertEquals("24th", toCharUs("YYth", year1));
    assertEquals("00th", toCharUs("YYth", year2));
    assertEquals("01st", toCharUs("YYth", year3));
    assertEquals("36th", toCharUs("YYth", year4));

    assertEquals("24th", toCharUs("FMYYth", year1));
  }

  @Test void testY() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertEquals("4", toCharUs("Y", year1));
    assertEquals("0", toCharUs("Y", year2));
    assertEquals("1", toCharUs("Y", year3));
    assertEquals("6", toCharUs("Y", year4));
    assertEquals("4", toCharUs("FMY", year1));
    assertEquals("0", toCharUs("FMY", year2));
    assertEquals("1", toCharUs("FMY", year3));
    assertEquals("6", toCharUs("FMY", year4));

    assertEquals("4TH", toCharUs("YTH", year1));
    assertEquals("0TH", toCharUs("YTH", year2));
    assertEquals("1ST", toCharUs("YTH", year3));
    assertEquals("6TH", toCharUs("YTH", year4));
    assertEquals("4th", toCharUs("Yth", year1));
    assertEquals("0th", toCharUs("Yth", year2));
    assertEquals("1st", toCharUs("Yth", year3));
    assertEquals("6th", toCharUs("Yth", year4));

    assertEquals("4th", toCharUs("FMYth", year1));
  }

  @Test void testIYYY() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(1);
    final ZonedDateTime date4 = date3.plusDays(1);
    final ZonedDateTime date5 = date4.plusDays(1);

    assertEquals("2019", toCharUs("IYYY", date1));
    assertEquals("2020", toCharUs("IYYY", date2));
    assertEquals("2020", toCharUs("IYYY", date3));
    assertEquals("2020", toCharUs("IYYY", date4));
    assertEquals("2020", toCharUs("IYYY", date5));
    assertEquals("2019", toCharUs("FMIYYY", date1));
    assertEquals("2020", toCharUs("FMIYYY", date2));
    assertEquals("2020", toCharUs("FMIYYY", date3));
    assertEquals("2020", toCharUs("FMIYYY", date4));
    assertEquals("2020", toCharUs("FMIYYY", date5));

    assertEquals("2019TH", toCharUs("IYYYTH", date1));
    assertEquals("2020TH", toCharUs("IYYYTH", date2));
    assertEquals("2020TH", toCharUs("IYYYTH", date3));
    assertEquals("2020TH", toCharUs("IYYYTH", date4));
    assertEquals("2020TH", toCharUs("IYYYTH", date5));
    assertEquals("2019th", toCharUs("IYYYth", date1));
    assertEquals("2020th", toCharUs("IYYYth", date2));
    assertEquals("2020th", toCharUs("IYYYth", date3));
    assertEquals("2020th", toCharUs("IYYYth", date4));
    assertEquals("2020th", toCharUs("IYYYth", date5));

    assertEquals("2020th", toCharUs("FMIYYYth", date5));
  }

  @Test void testIYY() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(1);
    final ZonedDateTime date4 = date3.plusDays(1);
    final ZonedDateTime date5 = date4.plusDays(1);

    assertEquals("019", toCharUs("IYY", date1));
    assertEquals("020", toCharUs("IYY", date2));
    assertEquals("020", toCharUs("IYY", date3));
    assertEquals("020", toCharUs("IYY", date4));
    assertEquals("020", toCharUs("IYY", date5));
    assertEquals("19", toCharUs("FMIYY", date1));
    assertEquals("20", toCharUs("FMIYY", date2));
    assertEquals("20", toCharUs("FMIYY", date3));
    assertEquals("20", toCharUs("FMIYY", date4));
    assertEquals("20", toCharUs("FMIYY", date5));

    assertEquals("019TH", toCharUs("IYYTH", date1));
    assertEquals("020TH", toCharUs("IYYTH", date2));
    assertEquals("020TH", toCharUs("IYYTH", date3));
    assertEquals("020TH", toCharUs("IYYTH", date4));
    assertEquals("020TH", toCharUs("IYYTH", date5));
    assertEquals("019th", toCharUs("IYYth", date1));
    assertEquals("020th", toCharUs("IYYth", date2));
    assertEquals("020th", toCharUs("IYYth", date3));
    assertEquals("020th", toCharUs("IYYth", date4));
    assertEquals("020th", toCharUs("IYYth", date5));

    assertEquals("20th", toCharUs("FMIYYth", date5));
  }

  @Test void testIY() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(1);
    final ZonedDateTime date4 = date3.plusDays(1);
    final ZonedDateTime date5 = date4.plusDays(1);

    assertEquals("19", toCharUs("IY", date1));
    assertEquals("20", toCharUs("IY", date2));
    assertEquals("20", toCharUs("IY", date3));
    assertEquals("20", toCharUs("IY", date4));
    assertEquals("20", toCharUs("IY", date5));
    assertEquals("19", toCharUs("FMIY", date1));
    assertEquals("20", toCharUs("FMIY", date2));
    assertEquals("20", toCharUs("FMIY", date3));
    assertEquals("20", toCharUs("FMIY", date4));
    assertEquals("20", toCharUs("FMIY", date5));

    assertEquals("19TH", toCharUs("IYTH", date1));
    assertEquals("20TH", toCharUs("IYTH", date2));
    assertEquals("20TH", toCharUs("IYTH", date3));
    assertEquals("20TH", toCharUs("IYTH", date4));
    assertEquals("20TH", toCharUs("IYTH", date5));
    assertEquals("19th", toCharUs("IYth", date1));
    assertEquals("20th", toCharUs("IYth", date2));
    assertEquals("20th", toCharUs("IYth", date3));
    assertEquals("20th", toCharUs("IYth", date4));
    assertEquals("20th", toCharUs("IYth", date5));

    assertEquals("20th", toCharUs("FMIYth", date5));
  }

  @Test void testI() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(1);
    final ZonedDateTime date4 = date3.plusDays(1);
    final ZonedDateTime date5 = date4.plusDays(1);

    assertEquals("9", toCharUs("I", date1));
    assertEquals("0", toCharUs("I", date2));
    assertEquals("0", toCharUs("I", date3));
    assertEquals("0", toCharUs("I", date4));
    assertEquals("0", toCharUs("I", date5));
    assertEquals("9", toCharUs("FMI", date1));
    assertEquals("0", toCharUs("FMI", date2));
    assertEquals("0", toCharUs("FMI", date3));
    assertEquals("0", toCharUs("FMI", date4));
    assertEquals("0", toCharUs("FMI", date5));

    assertEquals("9TH", toCharUs("ITH", date1));
    assertEquals("0TH", toCharUs("ITH", date2));
    assertEquals("0TH", toCharUs("ITH", date3));
    assertEquals("0TH", toCharUs("ITH", date4));
    assertEquals("0TH", toCharUs("ITH", date5));
    assertEquals("9th", toCharUs("Ith", date1));
    assertEquals("0th", toCharUs("Ith", date2));
    assertEquals("0th", toCharUs("Ith", date3));
    assertEquals("0th", toCharUs("Ith", date4));
    assertEquals("0th", toCharUs("Ith", date5));

    assertEquals("0th", toCharUs("FMIth", date5));
  }

  @Test void testIW() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(186);

    assertEquals("52", toCharUs("IW", date1));
    assertEquals("01", toCharUs("IW", date2));
    assertEquals("27", toCharUs("IW", date3));
    assertEquals("52", toCharUs("FMIW", date1));
    assertEquals("1", toCharUs("FMIW", date2));
    assertEquals("27", toCharUs("FMIW", date3));

    assertEquals("52ND", toCharUs("IWTH", date1));
    assertEquals("01ST", toCharUs("IWTH", date2));
    assertEquals("27TH", toCharUs("IWTH", date3));
    assertEquals("52nd", toCharUs("IWth", date1));
    assertEquals("01st", toCharUs("IWth", date2));
    assertEquals("27th", toCharUs("IWth", date3));

    assertEquals("27th", toCharUs("FMIWth", date3));
  }

  @Test void testIDDD() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(186);

    assertEquals("364", toCharUs("IDDD", date1));
    assertEquals("001", toCharUs("IDDD", date2));
    assertEquals("187", toCharUs("IDDD", date3));
    assertEquals("364", toCharUs("FMIDDD", date1));
    assertEquals("1", toCharUs("FMIDDD", date2));
    assertEquals("187", toCharUs("FMIDDD", date3));

    assertEquals("364TH", toCharUs("IDDDTH", date1));
    assertEquals("001ST", toCharUs("IDDDTH", date2));
    assertEquals("187TH", toCharUs("IDDDTH", date3));
    assertEquals("364th", toCharUs("IDDDth", date1));
    assertEquals("001st", toCharUs("IDDDth", date2));
    assertEquals("187th", toCharUs("IDDDth", date3));

    assertEquals("187th", toCharUs("FMIDDDth", date3));
  }

  @Test void testID() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(186);

    assertEquals("7", toCharUs("ID", date1));
    assertEquals("1", toCharUs("ID", date2));
    assertEquals("5", toCharUs("ID", date3));
    assertEquals("7", toCharUs("FMID", date1));
    assertEquals("1", toCharUs("FMID", date2));
    assertEquals("5", toCharUs("FMID", date3));

    assertEquals("7TH", toCharUs("IDTH", date1));
    assertEquals("1ST", toCharUs("IDTH", date2));
    assertEquals("5TH", toCharUs("IDTH", date3));
    assertEquals("7th", toCharUs("IDth", date1));
    assertEquals("1st", toCharUs("IDth", date2));
    assertEquals("5th", toCharUs("IDth", date3));

    assertEquals("5th", toCharUs("FMIDth", date3));
  }

  @ParameterizedTest
  @ValueSource(strings = {"AD", "BC"})
  void testEraUpperCaseNoDots(String pattern) {
    final ZonedDateTime date1 = createDateTime(2019, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2018);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertEquals("AD", toCharUs(pattern, date1));
    assertEquals("AD", toCharUs(pattern, date2));
    assertEquals("BC", toCharUs(pattern, date3));
    assertEquals("BC", toCharUs(pattern, date4));
  }

  @ParameterizedTest
  @ValueSource(strings = {"ad", "bc"})
  void testEraLowerCaseNoDots(String pattern) {
    final ZonedDateTime date1 = createDateTime(2019, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2018);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertEquals("ad", toCharUs(pattern, date1));
    assertEquals("ad", toCharUs(pattern, date2));
    assertEquals("bc", toCharUs(pattern, date3));
    assertEquals("bc", toCharUs(pattern, date4));
  }

  @ParameterizedTest
  @ValueSource(strings = {"A.D.", "B.C."})
  void testEraUpperCaseWithDots(String pattern) {
    final ZonedDateTime date1 = createDateTime(2019, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2018);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertEquals("A.D.", toCharUs(pattern, date1));
    assertEquals("A.D.", toCharUs(pattern, date2));
    assertEquals("B.C.", toCharUs(pattern, date3));
    assertEquals("B.C.", toCharUs(pattern, date4));
  }

  @ParameterizedTest
  @ValueSource(strings = {"a.d.", "b.c."})
  void testEraLowerCaseWithDots(String pattern) {
    final ZonedDateTime date1 = createDateTime(2019, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2018);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertEquals("a.d.", toCharUs(pattern, date1));
    assertEquals("a.d.", toCharUs(pattern, date2));
    assertEquals("b.c.", toCharUs(pattern, date3));
    assertEquals("b.c.", toCharUs(pattern, date4));
  }

  @Test void testMonthFullUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("JANUARY  ", toCharUs("MONTH", date1));
    assertEquals("MARCH    ", toCharUs("MONTH", date2));
    assertEquals("NOVEMBER ", toCharUs("MONTH", date3));
  }

  @Test void testMonthFullUpperCaseNoTranslate() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("JANUARY  ", toCharFrench("MONTH", date1));
    assertEquals("MARCH    ", toCharFrench("MONTH", date2));
    assertEquals("NOVEMBER ", toCharFrench("MONTH", date3));
  }

  @Test void testMonthFullUpperCaseTranslate() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("JANVIER  ", toCharFrench("TMMONTH", date1));
    assertEquals("MARS     ", toCharFrench("TMMONTH", date2));
    assertEquals("NOVEMBRE ", toCharFrench("TMMONTH", date3));
  }

  @Test void testMonthFullUpperCaseNoPadding() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("JANUARY", toCharUs("FMMONTH", date1));
    assertEquals("MARCH", toCharUs("FMMONTH", date2));
    assertEquals("NOVEMBER", toCharUs("FMMONTH", date3));
  }

  @Test void testMonthFullCapitalized() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("January  ", toCharUs("Month", date1));
    assertEquals("March    ", toCharUs("Month", date2));
    assertEquals("November ", toCharUs("Month", date3));
  }

  @Test void testMonthFullLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("january  ", toCharUs("month", date1));
    assertEquals("march    ", toCharUs("month", date2));
    assertEquals("november ", toCharUs("month", date3));
  }

  @Test void testMonthShortUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("JAN", toCharUs("MON", date1));
    assertEquals("MAR", toCharUs("MON", date2));
    assertEquals("NOV", toCharUs("MON", date3));
  }

  @Test void testMonthShortCapitalized() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("Jan", toCharUs("Mon", date1));
    assertEquals("Mar", toCharUs("Mon", date2));
    assertEquals("Nov", toCharUs("Mon", date3));
  }

  @Test void testMonthShortLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("jan", toCharUs("mon", date1));
    assertEquals("mar", toCharUs("mon", date2));
    assertEquals("nov", toCharUs("mon", date3));
  }

  @Test void testMM() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("01", toCharUs("MM", date1));
    assertEquals("03", toCharUs("MM", date2));
    assertEquals("11", toCharUs("MM", date3));
    assertEquals("1", toCharUs("FMMM", date1));
    assertEquals("3", toCharUs("FMMM", date2));
    assertEquals("11", toCharUs("FMMM", date3));

    assertEquals("01ST", toCharUs("MMTH", date1));
    assertEquals("03RD", toCharUs("MMTH", date2));
    assertEquals("11TH", toCharUs("MMTH", date3));
    assertEquals("01st", toCharUs("MMth", date1));
    assertEquals("03rd", toCharUs("MMth", date2));
    assertEquals("11th", toCharUs("MMth", date3));

    assertEquals("3rd", toCharUs("FMMMth", date2));
  }

  @Test void testDayFullUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("MONDAY   ", toCharUs("DAY", date1));
    assertEquals("FRIDAY   ", toCharUs("DAY", date2));
    assertEquals("TUESDAY  ", toCharUs("DAY", date3));
  }

  @Test void testDayFullUpperNoTranslate() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("MONDAY   ", toCharFrench("DAY", date1));
    assertEquals("FRIDAY   ", toCharFrench("DAY", date2));
    assertEquals("TUESDAY  ", toCharFrench("DAY", date3));
  }

  @Test void testDayFullUpperTranslate() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("LUNDI    ", toCharFrench("TMDAY", date1));
    assertEquals("VENDREDI ", toCharFrench("TMDAY", date2));
    assertEquals("MARDI    ", toCharFrench("TMDAY", date3));
  }

  @Test void testDayFullCapitalized() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("Monday   ", toCharUs("Day", date1));
    assertEquals("Friday   ", toCharUs("Day", date2));
    assertEquals("Tuesday  ", toCharUs("Day", date3));
  }

  @Test void testDayFullLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("monday   ", toCharUs("day", date1));
    assertEquals("friday   ", toCharUs("day", date2));
    assertEquals("tuesday  ", toCharUs("day", date3));
  }

  @Test void testDayShortUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("MON", toCharUs("DY", date1));
    assertEquals("FRI", toCharUs("DY", date2));
    assertEquals("TUE", toCharUs("DY", date3));
  }

  @Test void testDayShortCapitalized() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("Mon", toCharUs("Dy", date1));
    assertEquals("Fri", toCharUs("Dy", date2));
    assertEquals("Tue", toCharUs("Dy", date3));
  }

  @Test void testDayShortLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertEquals("mon", toCharUs("dy", date1));
    assertEquals("fri", toCharUs("dy", date2));
    assertEquals("tue", toCharUs("dy", date3));
  }

  @Test void testDDD() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertEquals("001", toCharUs("DDD", date1));
    assertEquals("061", toCharUs("DDD", date2));
    assertEquals("306", toCharUs("DDD", date3));
    assertEquals("1", toCharUs("FMDDD", date1));
    assertEquals("61", toCharUs("FMDDD", date2));
    assertEquals("306", toCharUs("FMDDD", date3));

    assertEquals("001ST", toCharUs("DDDTH", date1));
    assertEquals("061ST", toCharUs("DDDTH", date2));
    assertEquals("306TH", toCharUs("DDDTH", date3));
    assertEquals("001st", toCharUs("DDDth", date1));
    assertEquals("061st", toCharUs("DDDth", date2));
    assertEquals("306th", toCharUs("DDDth", date3));

    assertEquals("1st", toCharUs("FMDDDth", date1));
  }

  @Test void testDD() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 1, 12, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 1, 29, 23, 0, 0, 0);

    assertEquals("01", toCharUs("DD", date1));
    assertEquals("12", toCharUs("DD", date2));
    assertEquals("29", toCharUs("DD", date3));
    assertEquals("1", toCharUs("FMDD", date1));
    assertEquals("12", toCharUs("FMDD", date2));
    assertEquals("29", toCharUs("FMDD", date3));

    assertEquals("01ST", toCharUs("DDTH", date1));
    assertEquals("12TH", toCharUs("DDTH", date2));
    assertEquals("29TH", toCharUs("DDTH", date3));
    assertEquals("01st", toCharUs("DDth", date1));
    assertEquals("12th", toCharUs("DDth", date2));
    assertEquals("29th", toCharUs("DDth", date3));

    assertEquals("1st", toCharUs("FMDDth", date1));
  }

  @Test void testD() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 1, 2, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 1, 27, 23, 0, 0, 0);

    assertEquals("2", toCharUs("D", date1));
    assertEquals("3", toCharUs("D", date2));
    assertEquals("7", toCharUs("D", date3));
    assertEquals("2", toCharUs("FMD", date1));
    assertEquals("3", toCharUs("FMD", date2));
    assertEquals("7", toCharUs("FMD", date3));

    assertEquals("2ND", toCharUs("DTH", date1));
    assertEquals("3RD", toCharUs("DTH", date2));
    assertEquals("7TH", toCharUs("DTH", date3));
    assertEquals("2nd", toCharUs("Dth", date1));
    assertEquals("3rd", toCharUs("Dth", date2));
    assertEquals("7th", toCharUs("Dth", date3));

    assertEquals("2nd", toCharUs("FMDth", date1));
  }

  @Test void testWW() {
    final ZonedDateTime date1 = createDateTime(2016, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2016, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2016, 10, 1, 23, 0, 0, 0);

    assertEquals("1", toCharUs("WW", date1));
    assertEquals("9", toCharUs("WW", date2));
    assertEquals("40", toCharUs("WW", date3));
    assertEquals("1", toCharUs("FMWW", date1));
    assertEquals("9", toCharUs("FMWW", date2));
    assertEquals("40", toCharUs("FMWW", date3));

    assertEquals("1ST", toCharUs("WWTH", date1));
    assertEquals("9TH", toCharUs("WWTH", date2));
    assertEquals("40TH", toCharUs("WWTH", date3));
    assertEquals("1st", toCharUs("WWth", date1));
    assertEquals("9th", toCharUs("WWth", date2));
    assertEquals("40th", toCharUs("WWth", date3));

    assertEquals("1st", toCharUs("FMWWth", date1));
  }

  @Test void testW() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 1, 15, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 31, 23, 0, 0, 0);

    assertEquals("1", toCharUs("W", date1));
    assertEquals("3", toCharUs("W", date2));
    assertEquals("5", toCharUs("W", date3));
    assertEquals("1", toCharUs("FMW", date1));
    assertEquals("3", toCharUs("FMW", date2));
    assertEquals("5", toCharUs("FMW", date3));

    assertEquals("1ST", toCharUs("WTH", date1));
    assertEquals("3RD", toCharUs("WTH", date2));
    assertEquals("5TH", toCharUs("WTH", date3));
    assertEquals("1st", toCharUs("Wth", date1));
    assertEquals("3rd", toCharUs("Wth", date2));
    assertEquals("5th", toCharUs("Wth", date3));

    assertEquals("1st", toCharUs("FMWth", date1));
  }

  @Test void testCC() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2023);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertEquals("21", toCharUs("CC", date1));
    assertEquals("01", toCharUs("CC", date2));
    assertEquals("-01", toCharUs("CC", date3));
    assertEquals("-03", toCharUs("CC", date4));
    assertEquals("21", toCharUs("FMCC", date1));
    assertEquals("1", toCharUs("FMCC", date2));
    assertEquals("-1", toCharUs("FMCC", date3));
    assertEquals("-3", toCharUs("FMCC", date4));

    assertEquals("21ST", toCharUs("CCTH", date1));
    assertEquals("01ST", toCharUs("CCTH", date2));
    assertEquals("-01ST", toCharUs("CCTH", date3));
    assertEquals("-03RD", toCharUs("CCTH", date4));
    assertEquals("21st", toCharUs("CCth", date1));
    assertEquals("01st", toCharUs("CCth", date2));
    assertEquals("-01st", toCharUs("CCth", date3));
    assertEquals("-03rd", toCharUs("CCth", date4));

    assertEquals("-1st", toCharUs("FMCCth", date3));
  }

  @Test void testJ() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2024);
    final ZonedDateTime date3 = date2.minusYears(1000);

    assertEquals("2460311", toCharUs("J", date1));
    assertEquals("1721060", toCharUs("J", date2));
    assertEquals("1356183", toCharUs("J", date3));
    assertEquals("2460311", toCharUs("FMJ", date1));
    assertEquals("1721060", toCharUs("FMJ", date2));
    assertEquals("1356183", toCharUs("FMJ", date3));

    assertEquals("2460311TH", toCharUs("JTH", date1));
    assertEquals("1721060TH", toCharUs("JTH", date2));
    assertEquals("1356183RD", toCharUs("JTH", date3));
    assertEquals("2460311th", toCharUs("Jth", date1));
    assertEquals("1721060th", toCharUs("Jth", date2));
    assertEquals("1356183rd", toCharUs("Jth", date3));
  }

  @Test void testQ() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 4, 9, 0, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 8, 23, 0, 0, 0, 0);
    final ZonedDateTime date4 = createDateTime(2024, 12, 31, 0, 0, 0, 0);

    assertEquals("1", toCharUs("Q", date1));
    assertEquals("2", toCharUs("Q", date2));
    assertEquals("3", toCharUs("Q", date3));
    assertEquals("4", toCharUs("Q", date4));
    assertEquals("1", toCharUs("FMQ", date1));
    assertEquals("2", toCharUs("FMQ", date2));
    assertEquals("3", toCharUs("FMQ", date3));
    assertEquals("4", toCharUs("FMQ", date4));

    assertEquals("1ST", toCharUs("QTH", date1));
    assertEquals("2ND", toCharUs("QTH", date2));
    assertEquals("3RD", toCharUs("QTH", date3));
    assertEquals("4TH", toCharUs("QTH", date4));
    assertEquals("1st", toCharUs("Qth", date1));
    assertEquals("2nd", toCharUs("Qth", date2));
    assertEquals("3rd", toCharUs("Qth", date3));
    assertEquals("4th", toCharUs("Qth", date4));
  }

  @Test void testRMUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 4, 9, 0, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 8, 23, 0, 0, 0, 0);
    final ZonedDateTime date4 = createDateTime(2024, 12, 31, 0, 0, 0, 0);

    assertEquals("I", toCharUs("RM", date1));
    assertEquals("IV", toCharUs("RM", date2));
    assertEquals("VIII", toCharUs("RM", date3));
    assertEquals("XII", toCharUs("RM", date4));
  }

  @Test void testRMLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 4, 9, 0, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 8, 23, 0, 0, 0, 0);
    final ZonedDateTime date4 = createDateTime(2024, 12, 31, 0, 0, 0, 0);

    assertEquals("i", toCharUs("rm", date1));
    assertEquals("iv", toCharUs("rm", date2));
    assertEquals("viii", toCharUs("rm", date3));
    assertEquals("xii", toCharUs("rm", date4));
  }

  @Test void testToTimestampHH() throws Exception {
    assertEquals(DAY_1_CE.plusHours(1), toTimestamp("01", "HH"));
    assertEquals(DAY_1_CE.plusHours(1), toTimestamp("1", "HH"));
    assertEquals(DAY_1_CE.plusHours(11), toTimestamp("11", "HH"));

    try {
      toTimestamp("72", "HH");
      fail();
    } catch (Exception e) {
    }

    try {
      toTimestamp("abc", "HH");
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampHH12() throws Exception {
    assertEquals(DAY_1_CE.plusHours(1), toTimestamp("01", "HH12"));
    assertEquals(DAY_1_CE.plusHours(1), toTimestamp("1", "HH12"));
    assertEquals(DAY_1_CE.plusHours(11), toTimestamp("11", "HH12"));

    try {
      toTimestamp("72", "HH12");
      fail();
    } catch (Exception e) {
    }

    try {
      toTimestamp("abc", "HH12");
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampHH24() throws Exception {
    assertEquals(DAY_1_CE.plusHours(1), toTimestamp("01", "HH24"));
    assertEquals(DAY_1_CE.plusHours(1), toTimestamp("1", "HH24"));
    assertEquals(DAY_1_CE.plusHours(18), toTimestamp("18", "HH24"));

    try {
      toTimestamp("72", "HH24");
      fail();
    } catch (Exception e) {
    }

    try {
      toTimestamp("abc", "HH24");
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampMI() throws Exception {
    assertEquals(DAY_1_CE.plusMinutes(1), toTimestamp("01", "MI"));
    assertEquals(DAY_1_CE.plusMinutes(1), toTimestamp("1", "MI"));
    assertEquals(DAY_1_CE.plusMinutes(57), toTimestamp("57", "MI"));

    try {
      toTimestamp("72", "MI");
      fail();
    } catch (Exception e) {
    }

    try {
      toTimestamp("abc", "MI");
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampSS() throws Exception {
    assertEquals(DAY_1_CE.plusSeconds(1), toTimestamp("01", "SS"));
    assertEquals(DAY_1_CE.plusSeconds(1), toTimestamp("1", "SS"));
    assertEquals(DAY_1_CE.plusSeconds(57), toTimestamp("57", "SS"));

    try {
      toTimestamp("72", "SS");
      fail();
    } catch (Exception e) {
    }

    try {
      toTimestamp("abc", "SS");
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampMS() throws Exception {
    assertEquals(DAY_1_CE.plusNanos(1_000_000), toTimestamp("001", "MS"));
    assertEquals(DAY_1_CE.plusNanos(1_000_000), toTimestamp("1", "MS"));
    assertEquals(DAY_1_CE.plusNanos(999_000_000), toTimestamp("999", "MS"));

    try {
      toTimestamp("9999", "MS");
      fail();
    } catch (Exception e) {
    }

    try {
      toTimestamp("abc", "MS");
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampUS() throws Exception {
    assertEquals(DAY_1_CE.plusNanos(1_000), toTimestamp("001", "US"));
    assertEquals(DAY_1_CE.plusNanos(1_000), toTimestamp("1", "US"));
    assertEquals(DAY_1_CE.plusNanos(999_000), toTimestamp("999", "US"));

    try {
      toTimestamp("9999999", "US");
      fail();
    } catch (Exception e) {
    }

    try {
      toTimestamp("abc", "US");
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampFF1() throws Exception {
    assertEquals(DAY_1_CE.plusNanos(100_000_000), toTimestamp("1", "FF1"));
    assertEquals(DAY_1_CE.plusNanos(900_000_000), toTimestamp("9", "FF1"));

    try {
      toTimestamp("72", "FF1");
      fail();
    } catch (Exception e) {
    }

    try {
      toTimestamp("abc", "FF1");
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampFF2() throws Exception {
    assertEquals(DAY_1_CE.plusNanos(10_000_000), toTimestamp("01", "FF2"));
    assertEquals(DAY_1_CE.plusNanos(10_000_000), toTimestamp("1", "FF2"));
    assertEquals(DAY_1_CE.plusNanos(970_000_000), toTimestamp("97", "FF2"));

    try {
      toTimestamp("999", "FF2");
      fail();
    } catch (Exception e) {
    }

    try {
      toTimestamp("abc", "FF2");
      fail();
    } catch (Exception e) {
    }
  }

  @Test void testToTimestampFF3() throws Exception {
    assertEquals(DAY_1_CE.plusNanos(1_000_000), toTimestamp("001", "FF3"));
    assertEquals(DAY_1_CE.plusNanos(1_000_000), toTimestamp("1", "FF3"));
    assertEquals(DAY_1_CE.plusNanos(976_000_000), toTimestamp("976", "FF3"));
  }

  @Test void testToTimestampFF4() throws Exception {
    assertEquals(DAY_1_CE.plusNanos(100_000), toTimestamp("0001", "FF4"));
    assertEquals(DAY_1_CE.plusNanos(100_000), toTimestamp("1", "FF4"));
    assertEquals(DAY_1_CE.plusNanos(976_200_000), toTimestamp("9762", "FF4"));
  }

  @Test void testToTimestampFF5() throws Exception {
    assertEquals(DAY_1_CE.plusNanos(10_000), toTimestamp("00001", "FF5"));
    assertEquals(DAY_1_CE.plusNanos(10_000), toTimestamp("1", "FF5"));
    assertEquals(DAY_1_CE.plusNanos(976_210_000), toTimestamp("97621", "FF5"));
  }

  @Test void testToTimestampFF6() throws Exception {
    assertEquals(DAY_1_CE.plusNanos(1_000), toTimestamp("000001", "FF6"));
    assertEquals(DAY_1_CE.plusNanos(1_000), toTimestamp("1", "FF6"));
    assertEquals(DAY_1_CE.plusNanos(976_214_000), toTimestamp("976214", "FF6"));
  }

  @Test void testToTimestampAMPM() throws Exception {
    assertEquals(DAY_1_CE.plusHours(3), toTimestamp("03AM", "HH12AM"));
    assertEquals(DAY_1_CE.plusHours(3), toTimestamp("03AM", "HH12PM"));
    assertEquals(DAY_1_CE.plusHours(15), toTimestamp("03PM", "HH12AM"));
    assertEquals(DAY_1_CE.plusHours(15), toTimestamp("03PM", "HH12PM"));
    assertEquals(DAY_1_CE.plusHours(3), toTimestamp("03A.M.", "HH12A.M."));
    assertEquals(DAY_1_CE.plusHours(3), toTimestamp("03A.M.", "HH12P.M."));
    assertEquals(DAY_1_CE.plusHours(15), toTimestamp("03P.M.", "HH12A.M."));
    assertEquals(DAY_1_CE.plusHours(15), toTimestamp("03P.M.", "HH12P.M."));
    assertEquals(DAY_1_CE.plusHours(3), toTimestamp("03am", "HH12am"));
    assertEquals(DAY_1_CE.plusHours(3), toTimestamp("03am", "HH12pm"));
    assertEquals(DAY_1_CE.plusHours(15), toTimestamp("03pm", "HH12am"));
    assertEquals(DAY_1_CE.plusHours(15), toTimestamp("03pm", "HH12pm"));
    assertEquals(DAY_1_CE.plusHours(3), toTimestamp("03a.m.", "HH12a.m."));
    assertEquals(DAY_1_CE.plusHours(3), toTimestamp("03a.m.", "HH12p.m."));
    assertEquals(DAY_1_CE.plusHours(15), toTimestamp("03p.m.", "HH12a.m."));
    assertEquals(DAY_1_CE.plusHours(15), toTimestamp("03p.m.", "HH12p.m."));
  }

  @Test void testToTimestampYYYYWithCommas() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("0,001", "Y,YYY"));
    assertEquals(JAN_1_2024, toTimestamp("2,024", "Y,YYY"));
  }

  @Test void testToTimestampYYYY() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("0001", "YYYY"));
    assertEquals(DAY_1_CE, toTimestamp("1", "YYYY"));
    assertEquals(JAN_1_2024, toTimestamp("2024", "YYYY"));
  }

  @Test void testToTimestampYYY() throws Exception {
    assertEquals(JAN_1_2001, toTimestamp("001", "YYY"));
    assertEquals(JAN_1_2001, toTimestamp("1", "YYY"));
    assertEquals(createDateTime(1987, 1, 1, 0, 0, 0, 0), toTimestamp("987", "YYY"));
  }

  @Test void testToTimestampYY() throws Exception {
    assertEquals(JAN_1_2001, toTimestamp("01", "YY"));
    assertEquals(JAN_1_2001, toTimestamp("1", "YY"));
    assertEquals(JAN_1_2024, toTimestamp("24", "YY"));
  }

  @Test void testToTimestampY() throws Exception {
    assertEquals(JAN_1_2001, toTimestamp("1", "Y"));
    assertEquals(JAN_1_2001.plusYears(3), toTimestamp("4", "Y"));
  }

  @Test void testToTimestampIYYY() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("0001", "IYYY"));
    assertEquals(DAY_1_CE, toTimestamp("1", "IYYY"));
    assertEquals(JAN_1_2024, toTimestamp("2024", "IYYY"));
  }

  @Test void testToTimestampIYY() throws Exception {
    assertEquals(JAN_1_2001, toTimestamp("001", "IYY"));
    assertEquals(JAN_1_2001, toTimestamp("1", "IYY"));
    assertEquals(createDateTime(1987, 1, 1, 0, 0, 0, 0), toTimestamp("987", "IYY"));
  }

  @Test void testToTimestampIY() throws Exception {
    assertEquals(JAN_1_2001, toTimestamp("01", "IY"));
    assertEquals(JAN_1_2001, toTimestamp("1", "IY"));
    assertEquals(JAN_1_2024, toTimestamp("24", "IY"));
  }

  @Test void testToTimestampI() throws Exception {
    assertEquals(JAN_1_2001, toTimestamp("1", "I"));
    assertEquals(JAN_1_2001, toTimestamp("1", "I"));
    assertEquals(JAN_1_2001.plusYears(3), toTimestamp("4", "I"));
  }

  @Test void testToTimestampBCAD() throws Exception {
    assertEquals(0, toTimestamp("1920BC", "YYYYBC").get(ChronoField.ERA));
    assertEquals(0, toTimestamp("1920BC", "YYYYAD").get(ChronoField.ERA));
    assertEquals(1, toTimestamp("1920AD", "YYYYBC").get(ChronoField.ERA));
    assertEquals(1, toTimestamp("1920AD", "YYYYAD").get(ChronoField.ERA));
    assertEquals(0, toTimestamp("1920B.C.", "YYYYB.C.").get(ChronoField.ERA));
    assertEquals(0, toTimestamp("1920B.C.", "YYYYA.D.").get(ChronoField.ERA));
    assertEquals(1, toTimestamp("1920A.D.", "YYYYB.C.").get(ChronoField.ERA));
    assertEquals(1, toTimestamp("1920A.D.", "YYYYA.D.").get(ChronoField.ERA));
    assertEquals(0, toTimestamp("1920bc", "YYYYbc").get(ChronoField.ERA));
    assertEquals(0, toTimestamp("1920bc", "YYYYad").get(ChronoField.ERA));
    assertEquals(1, toTimestamp("1920ad", "YYYYbc").get(ChronoField.ERA));
    assertEquals(1, toTimestamp("1920ad", "YYYYad").get(ChronoField.ERA));
    assertEquals(0, toTimestamp("1920b.c.", "YYYYb.c.").get(ChronoField.ERA));
    assertEquals(0, toTimestamp("1920b.c.", "YYYYa.d.").get(ChronoField.ERA));
    assertEquals(1, toTimestamp("1920a.d.", "YYYYb.c.").get(ChronoField.ERA));
    assertEquals(1, toTimestamp("1920a.d.", "YYYYa.d.").get(ChronoField.ERA));
  }

  @Test void testToTimestampMonthUpperCase() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("JANUARY", "MONTH"));
    assertEquals(DAY_1_CE.plusMonths(2), toTimestamp("MARCH", "MONTH"));
    assertEquals(DAY_1_CE.plusMonths(10), toTimestamp("NOVEMBER", "MONTH"));
  }

  @Test void testToTimestampMonthCapitalized() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("January", "Month"));
    assertEquals(DAY_1_CE.plusMonths(2), toTimestamp("March", "Month"));
    assertEquals(DAY_1_CE.plusMonths(10), toTimestamp("November", "Month"));
  }

  @Test void testToTimestampMonthLowerCase() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("january", "month"));
    assertEquals(DAY_1_CE.plusMonths(2), toTimestamp("march", "month"));
    assertEquals(DAY_1_CE.plusMonths(10), toTimestamp("november", "month"));
  }

  @Test void testToTimestampMonUpperCase() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("JAN", "MON"));
    assertEquals(DAY_1_CE.plusMonths(2), toTimestamp("MAR", "MON"));
    assertEquals(DAY_1_CE.plusMonths(10), toTimestamp("NOV", "MON"));
  }

  @Test void testToTimestampMonCapitalized() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("Jan", "Mon"));
    assertEquals(DAY_1_CE.plusMonths(2), toTimestamp("Mar", "Mon"));
    assertEquals(DAY_1_CE.plusMonths(10), toTimestamp("Nov", "Mon"));
  }

  @Test void testToTimestampMonLowerCase() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("jan", "mon"));
    assertEquals(DAY_1_CE.plusMonths(2), toTimestamp("mar", "mon"));
    assertEquals(DAY_1_CE.plusMonths(10), toTimestamp("nov", "mon"));
  }

  @Test void testToTimestampMM() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("01", "MM"));
    assertEquals(DAY_1_CE, toTimestamp("1", "MM"));
    assertEquals(DAY_1_CE.plusMonths(10), toTimestamp("11", "MM"));
  }

  @Test void testToTimestampDayUpperCase() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0), toTimestamp("1982 23 MONDAY", "IYYY IW DAY"));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0), toTimestamp("1982 23 THURSDAY", "IYYY IW DAY"));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0), toTimestamp("1982 23 FRIDAY", "IYYY IW DAY"));
  }

  @Test void testToTimestampDayCapitalized() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0), toTimestamp("1982 23 Monday", "IYYY IW Day"));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0), toTimestamp("1982 23 Thursday", "IYYY IW Day"));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0), toTimestamp("1982 23 Friday", "IYYY IW Day"));
  }

  @Test void testToTimestampDayLowerCase() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0), toTimestamp("1982 23 monday", "IYYY IW day"));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0), toTimestamp("1982 23 thursday", "IYYY IW day"));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0), toTimestamp("1982 23 friday", "IYYY IW day"));
  }

  @Test void testToTimestampDyUpperCase() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0), toTimestamp("1982 23 MON", "IYYY IW DY"));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0), toTimestamp("1982 23 THU", "IYYY IW DY"));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0), toTimestamp("1982 23 FRI", "IYYY IW DY"));
  }

  @Test void testToTimestampDyCapitalized() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0), toTimestamp("1982 23 Mon", "IYYY IW Dy"));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0), toTimestamp("1982 23 Thu", "IYYY IW Dy"));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0), toTimestamp("1982 23 Fri", "IYYY IW Dy"));
  }

  @Test void testToTimestampDyLowerCase() throws Exception {
    assertEquals(
        createDateTime(1982, 6, 7, 0, 0, 0, 0), toTimestamp("1982 23 mon", "IYYY IW dy"));
    assertEquals(
        createDateTime(1982, 6, 10, 0, 0, 0, 0), toTimestamp("1982 23 thu", "IYYY IW dy"));
    assertEquals(
        createDateTime(1982, 6, 11, 0, 0, 0, 0), toTimestamp("1982 23 fri", "IYYY IW dy"));
  }

  @Test void testToTimestampDDD() throws Exception {
    assertEquals(JAN_1_2024, toTimestamp("2024 001", "YYYY DDD"));
    assertEquals(JAN_1_2024, toTimestamp("2024 1", "YYYY DDD"));
    assertEquals(createDateTime(2024, 5, 16, 0, 0, 0, 0), toTimestamp("2024 137", "YYYY DDD"));
  }

  @Test void testToTimestampDD() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("01", "DD"));
    assertEquals(DAY_1_CE, toTimestamp("1", "DD"));
    assertEquals(DAY_1_CE.plusDays(22), toTimestamp("23", "DD"));
  }

  @Test void testToTimestampIDDD() throws Exception {
    assertEquals(createDateTime(2019, 12, 30, 0, 0, 0, 0), toTimestamp("2020 001", "IYYY IDDD"));
    assertEquals(createDateTime(2019, 12, 30, 0, 0, 0, 0), toTimestamp("2020 1", "IYYY IDDD"));
    assertEquals(createDateTime(2020, 5, 14, 0, 0, 0, 0), toTimestamp("2020 137", "IYYY IDDD"));
  }

  @Test void testToTimestampID() throws Exception {
    assertEquals(createDateTime(1982, 6, 7, 0, 0, 0, 0), toTimestamp("1982 23 1", "IYYY IW ID"));
    assertEquals(createDateTime(1982, 6, 10, 0, 0, 0, 0), toTimestamp("1982 23 4", "IYYY IW ID"));
    assertEquals(createDateTime(1982, 6, 11, 0, 0, 0, 0), toTimestamp("1982 23 5", "IYYY IW ID"));
  }

  @Test void testToTimestampW() throws Exception {
    assertEquals(JAN_1_2024, toTimestamp("2024 1 1", "YYYY MM W"));
    assertEquals(createDateTime(2024, 4, 8, 0, 0, 0, 0), toTimestamp("2024 4 2", "YYYY MM W"));
    assertEquals(createDateTime(2024, 11, 22, 0, 0, 0, 0), toTimestamp("2024 11 4", "YYYY MM W"));
  }

  @Test void testToTimestampWW() throws Exception {
    assertEquals(JAN_1_2024, toTimestamp("2024 01", "YYYY WW"));
    assertEquals(JAN_1_2024, toTimestamp("2024 1", "YYYY WW"));
    assertEquals(createDateTime(2024, 12, 16, 0, 0, 0, 0), toTimestamp("2024 51", "YYYY WW"));
  }

  @Test void testToTimestampIW() throws Exception {
    assertEquals(createDateTime(2019, 12, 30, 0, 0, 0, 0), toTimestamp("2020 01", "IYYY IW"));
    assertEquals(createDateTime(2019, 12, 30, 0, 0, 0, 0), toTimestamp("2020 1", "IYYY IW"));
    assertEquals(createDateTime(2020, 12, 14, 0, 0, 0, 0), toTimestamp("2020 51", "IYYY IW"));
  }

  @Test void testToTimestampCC() throws Exception {
    assertEquals(JAN_1_2001, toTimestamp("21", "CC"));
    assertEquals(createDateTime(1501, 1, 1, 0, 0, 0, 0), toTimestamp("16", "CC"));
    assertEquals(DAY_1_CE, toTimestamp("1", "CC"));
  }

  @Test void testToTimestampJ() throws Exception {
    assertEquals(JAN_1_2024, toTimestamp("2460311", "J"));
    assertEquals(createDateTime(1984, 7, 15, 0, 0, 0, 0), toTimestamp("2445897", "J"));
    assertEquals(createDateTime(234, 3, 21, 0, 0, 0, 0), toTimestamp("1806606", "J"));
  }

  @Test void testToTimestampRMUpperCase() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("I", "RM"));
    assertEquals(DAY_1_CE.plusMonths(3), toTimestamp("IV", "RM"));
    assertEquals(DAY_1_CE.plusMonths(8), toTimestamp("IX", "RM"));
  }

  @Test void testToTimestampRMLowerCase() throws Exception {
    assertEquals(DAY_1_CE, toTimestamp("i", "rm"));
    assertEquals(DAY_1_CE.plusMonths(3), toTimestamp("iv", "rm"));
    assertEquals(DAY_1_CE.plusMonths(8), toTimestamp("ix", "rm"));
  }

  @Test void testToTimestampDateValidFormats() throws Exception {
    assertEquals(APR_17_2024, toTimestamp("2024-04-17", "YYYY-MM-DD"));
    assertEquals(APR_17_2024, toTimestamp("2,024-04-17", "Y,YYY-MM-DD"));
    assertEquals(APR_17_2024, toTimestamp("24-04-17", "YYY-MM-DD"));
    assertEquals(APR_17_2024, toTimestamp("24-04-17", "YY-MM-DD"));
    assertEquals(APR_17_2024, toTimestamp("2124-04-17", "CCYY-MM-DD"));
    assertEquals(APR_17_2024, toTimestamp("20240417", "YYYYMMDD"));
    assertEquals(APR_17_2024, toTimestamp("2,0240417", "Y,YYYMMDD"));
    assertEquals(APR_17_2024, toTimestamp("2024-16-3", "IYYY-IW-ID"));
    assertEquals(APR_17_2024, toTimestamp("2024-16 Wednesday", "IYYY-IW Day"));
    assertEquals(APR_17_2024, toTimestamp("2024-108", "IYYY-IDDD"));
    assertEquals(APR_17_2024, toTimestamp("April 17, 2024", "Month DD, YYYY"));
    assertEquals(APR_17_2024, toTimestamp("IV 17, 2024", "RM DD, YYYY"));
    assertEquals(APR_17_2024, toTimestamp("APR 17, 2024", "MON DD, YYYY"));
    assertEquals(createDateTime(2024, 4, 15, 0, 0, 0, 0), toTimestamp("2024-16", "YYYY-WW"));
    assertEquals(APR_17_2024, toTimestamp("2024-108", "YYYY-DDD"));
    assertEquals(DAY_1_CE, toTimestamp("0000-01-01", "YYYY-MM-DD"));
  }

  @Test void testToTimestampWithTimezone() throws Exception {
    final ZoneId utcZone = ZoneId.of("UTC");
    assertEquals(
        APR_17_2024.plusHours(7).withZoneSameLocal(utcZone),
        PostgresqlDateTimeFormatter.toTimestamp("2024-04-17 00:00:00-07:00",
            "YYYY-MM-DD HH24:MI:SSTZH:TZM", utcZone, Locale.US));
  }

  protected static ZonedDateTime createDateTime(int year, int month, int dayOfMonth, int hour,
      int minute, int seconds, int nanoseconds) {
    return ZonedDateTime.of(
        LocalDateTime.of(year, month, dayOfMonth, hour, minute, seconds, nanoseconds),
        TIME_ZONE);
  }
}
