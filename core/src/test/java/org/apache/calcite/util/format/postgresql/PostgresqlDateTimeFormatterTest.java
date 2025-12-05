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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
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
    final CompiledDateTimeFormat dateTimeFormat =
        PostgresqlDateTimeFormatter.compilePattern(pattern);
    return dateTimeFormat.formatDateTime(dateTime, Locale.US);
  }

  private String toCharFrench(String pattern, ZonedDateTime dateTime) {
    final CompiledDateTimeFormat dateTimeFormat =
        PostgresqlDateTimeFormatter.compilePattern(pattern);
    return dateTimeFormat.formatDateTime(dateTime, Locale.FRENCH);
  }

  private ZonedDateTime toTimestamp(String input, String format) throws Exception {
    final CompiledDateTimeFormat dateTimeFormat =
        PostgresqlDateTimeFormatter.compilePattern(format);
    return dateTimeFormat.parseDateTime(input, TIME_ZONE, Locale.US);
  }

  @ParameterizedTest
  @ValueSource(strings = {"HH12", "HH"})
  void testHH12(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertThat(toCharUs(pattern, midnight), is("12"));
    assertThat(toCharUs(pattern, morning), is("06"));
    assertThat(toCharUs(pattern, noon), is("12"));
    assertThat(toCharUs(pattern, evening), is("06"));
    assertThat(toCharUs("FM" + pattern, midnight), is("12"));
    assertThat(toCharUs("FM" + pattern, morning), is("6"));
    assertThat(toCharUs("FM" + pattern, noon), is("12"));
    assertThat(toCharUs("FM" + pattern, evening), is("6"));

    final ZonedDateTime hourOne = createDateTime(2024, 1, 1, 1, 0, 0, 0);
    final ZonedDateTime hourTwo = createDateTime(2024, 1, 1, 2, 0, 0, 0);
    final ZonedDateTime hourThree = createDateTime(2024, 1, 1, 3, 0, 0, 0);
    assertThat(toCharUs(pattern + "TH", midnight), is("12TH"));
    assertThat(toCharUs(pattern + "TH", hourOne), is("01ST"));
    assertThat(toCharUs(pattern + "TH", hourTwo), is("02ND"));
    assertThat(toCharUs(pattern + "TH", hourThree), is("03RD"));
    assertThat(toCharUs(pattern + "th", midnight), is("12th"));
    assertThat(toCharUs(pattern + "th", hourOne), is("01st"));
    assertThat(toCharUs(pattern + "th", hourTwo), is("02nd"));
    assertThat(toCharUs(pattern + "th", hourThree), is("03rd"));

    assertThat(toCharUs("FM" + pattern + "th", hourTwo), is("2nd"));
  }

  @Test void testHH24() {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertThat(toCharUs("HH24", midnight), is("00"));
    assertThat(toCharUs("HH24", morning), is("06"));
    assertThat(toCharUs("HH24", noon), is("12"));
    assertThat(toCharUs("HH24", evening), is("18"));
    assertThat(toCharUs("FMHH24", midnight), is("0"));
    assertThat(toCharUs("FMHH24", morning), is("6"));
    assertThat(toCharUs("FMHH24", noon), is("12"));
    assertThat(toCharUs("FMHH24", evening), is("18"));

    final ZonedDateTime hourOne = createDateTime(2024, 1, 1, 1, 0, 0, 0);
    final ZonedDateTime hourTwo = createDateTime(2024, 1, 1, 2, 0, 0, 0);
    final ZonedDateTime hourThree = createDateTime(2024, 1, 1, 3, 0, 0, 0);
    assertThat(toCharUs("HH24TH", midnight), is("00TH"));
    assertThat(toCharUs("HH24TH", hourOne), is("01ST"));
    assertThat(toCharUs("HH24TH", hourTwo), is("02ND"));
    assertThat(toCharUs("HH24TH", hourThree), is("03RD"));
    assertThat(toCharUs("HH24th", midnight), is("00th"));
    assertThat(toCharUs("HH24th", hourOne), is("01st"));
    assertThat(toCharUs("HH24th", hourTwo), is("02nd"));
    assertThat(toCharUs("HH24th", hourThree), is("03rd"));

    assertThat(toCharUs("FMHH24th", hourTwo), is("2nd"));
  }

  @Test void testMI() {
    final ZonedDateTime minute0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime minute2 = createDateTime(2024, 1, 1, 0, 2, 0, 0);
    final ZonedDateTime minute15 = createDateTime(2024, 1, 1, 0, 15, 0, 0);

    assertThat(toCharUs("MI", minute0), is("00"));
    assertThat(toCharUs("MI", minute2), is("02"));
    assertThat(toCharUs("MI", minute15), is("15"));

    assertThat(toCharUs("FMMI", minute0), is("0"));
    assertThat(toCharUs("FMMI", minute2), is("2"));
    assertThat(toCharUs("FMMI", minute15), is("15"));

    assertThat(toCharUs("MITH", minute0), is("00TH"));
    assertThat(toCharUs("MITH", minute2), is("02ND"));
    assertThat(toCharUs("MITH", minute15), is("15TH"));
    assertThat(toCharUs("MIth", minute0), is("00th"));
    assertThat(toCharUs("MIth", minute2), is("02nd"));
    assertThat(toCharUs("MIth", minute15), is("15th"));

    assertThat(toCharUs("FMMIth", minute2), is("2nd"));
    assertThat(toCharUs("FMMInd", minute2), is("2nd"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SSSSS", "SSSS"})
  void testSSSSS(String pattern) {
    final ZonedDateTime second0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime second1001 = createDateTime(2024, 1, 1, 0, 16, 41, 0);
    final ZonedDateTime endOfDay = createDateTime(2024, 1, 1, 23, 59, 59, 0);

    assertThat(toCharUs(pattern, second0), is("0"));
    assertThat(toCharUs(pattern, second1001), is("1001"));
    assertThat(toCharUs(pattern, endOfDay), is("86399"));

    assertThat(toCharUs("FM" + pattern, second0), is("0"));
    assertThat(toCharUs("FM" + pattern, second1001), is("1001"));
    assertThat(toCharUs("FM" + pattern, endOfDay), is("86399"));

    assertThat(toCharUs(pattern + "TH", second0), is("0TH"));
    assertThat(toCharUs(pattern + "TH", second1001), is("1001ST"));
    assertThat(toCharUs(pattern + "TH", endOfDay), is("86399TH"));
    assertThat(toCharUs(pattern + "th", second0), is("0th"));
    assertThat(toCharUs(pattern + "th", second1001), is("1001st"));
    assertThat(toCharUs(pattern + "th", endOfDay), is("86399th"));

    assertThat(toCharUs("FM" + pattern + "th", second1001), is("1001st"));
    assertThat(toCharUs("FM" + pattern + "nd", second1001), is("1001nd"));
  }

  @Test void testSS() {
    final ZonedDateTime second0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime second2 = createDateTime(2024, 1, 1, 0, 0, 2, 0);
    final ZonedDateTime second15 = createDateTime(2024, 1, 1, 0, 0, 15, 0);

    assertThat(toCharUs("SS", second0), is("00"));
    assertThat(toCharUs("SS", second2), is("02"));
    assertThat(toCharUs("SS", second15), is("15"));

    assertThat(toCharUs("FMSS", second0), is("0"));
    assertThat(toCharUs("FMSS", second2), is("2"));
    assertThat(toCharUs("FMSS", second15), is("15"));

    assertThat(toCharUs("SSTH", second0), is("00TH"));
    assertThat(toCharUs("SSTH", second2), is("02ND"));
    assertThat(toCharUs("SSTH", second15), is("15TH"));
    assertThat(toCharUs("SSth", second0), is("00th"));
    assertThat(toCharUs("SSth", second2), is("02nd"));
    assertThat(toCharUs("SSth", second15), is("15th"));

    assertThat(toCharUs("FMSSth", second2), is("2nd"));
    assertThat(toCharUs("FMSSnd", second2), is("2nd"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"MS", "FF3"})
  void testMS(String pattern) {
    final ZonedDateTime ms0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime ms2 = createDateTime(2024, 1, 1, 0, 0, 2, 2000000);
    final ZonedDateTime ms15 = createDateTime(2024, 1, 1, 0, 0, 15, 15000000);

    assertThat(toCharUs(pattern, ms0), is("000"));
    assertThat(toCharUs(pattern, ms2), is("002"));
    assertThat(toCharUs(pattern, ms15), is("015"));

    assertThat(toCharUs("FM" + pattern, ms0), is("0"));
    assertThat(toCharUs("FM" + pattern, ms2), is("2"));
    assertThat(toCharUs("FM" + pattern, ms15), is("15"));

    assertThat(toCharUs(pattern + "TH", ms0), is("000TH"));
    assertThat(toCharUs(pattern + "TH", ms2), is("002ND"));
    assertThat(toCharUs(pattern + "TH", ms15), is("015TH"));
    assertThat(toCharUs(pattern + "th", ms0), is("000th"));
    assertThat(toCharUs(pattern + "th", ms2), is("002nd"));
    assertThat(toCharUs(pattern + "th", ms15), is("015th"));

    assertThat(toCharUs("FM" + pattern + "th", ms2), is("2nd"));
    assertThat(toCharUs("FM" + pattern + "nd", ms2), is("2nd"));
  }

  @Test void testUS() {
    final ZonedDateTime us0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime us2 = createDateTime(2024, 1, 1, 0, 0, 0, 2000);
    final ZonedDateTime us15 = createDateTime(2024, 1, 1, 0, 0, 0, 15000);
    final ZonedDateTime usWithMs = createDateTime(2024, 1, 1, 0, 0, 0, 2015000);

    assertThat(toCharUs("US", us0), is("000000"));
    assertThat(toCharUs("US", us2), is("000002"));
    assertThat(toCharUs("US", us15), is("000015"));
    assertThat(toCharUs("US", usWithMs), is("002015"));

    assertThat(toCharUs("FMUS", us0), is("0"));
    assertThat(toCharUs("FMUS", us2), is("2"));
    assertThat(toCharUs("FMUS", us15), is("15"));
    assertThat(toCharUs("FMUS", usWithMs), is("2015"));

    assertThat(toCharUs("USTH", us0), is("000000TH"));
    assertThat(toCharUs("USTH", us2), is("000002ND"));
    assertThat(toCharUs("USTH", us15), is("000015TH"));
    assertThat(toCharUs("USTH", usWithMs), is("002015TH"));
    assertThat(toCharUs("USth", us0), is("000000th"));
    assertThat(toCharUs("USth", us2), is("000002nd"));
    assertThat(toCharUs("USth", us15), is("000015th"));
    assertThat(toCharUs("USth", usWithMs), is("002015th"));

    assertThat(toCharUs("FMUSth", us2), is("2nd"));
    assertThat(toCharUs("FMUSnd", us2), is("2nd"));
  }

  @Test void testFF1() {
    final ZonedDateTime ms0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime ms200 = createDateTime(2024, 1, 1, 0, 0, 0, 200_000_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertThat(toCharUs("FF1", ms0), is("0"));
    assertThat(toCharUs("FF1", ms200), is("2"));
    assertThat(toCharUs("FF1", ms150), is("1"));

    assertThat(toCharUs("FMFF1", ms0), is("0"));
    assertThat(toCharUs("FMFF1", ms200), is("2"));
    assertThat(toCharUs("FMFF1", ms150), is("1"));

    assertThat(toCharUs("FF1TH", ms0), is("0TH"));
    assertThat(toCharUs("FF1TH", ms200), is("2ND"));
    assertThat(toCharUs("FF1TH", ms150), is("1ST"));
    assertThat(toCharUs("FF1th", ms0), is("0th"));
    assertThat(toCharUs("FF1th", ms200), is("2nd"));
    assertThat(toCharUs("FF1th", ms150), is("1st"));

    assertThat(toCharUs("FMFF1th", ms200), is("2nd"));
    assertThat(toCharUs("FMFF1nd", ms200), is("2nd"));
  }

  @Test void testFF2() {
    final ZonedDateTime ms0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime ms20 = createDateTime(2024, 1, 1, 0, 0, 0, 20_000_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertThat(toCharUs("FF2", ms0), is("00"));
    assertThat(toCharUs("FF2", ms20), is("02"));
    assertThat(toCharUs("FF2", ms150), is("15"));

    assertThat(toCharUs("FMFF2", ms0), is("0"));
    assertThat(toCharUs("FMFF2", ms20), is("2"));
    assertThat(toCharUs("FMFF2", ms150), is("15"));

    assertThat(toCharUs("FF2TH", ms0), is("00TH"));
    assertThat(toCharUs("FF2TH", ms20), is("02ND"));
    assertThat(toCharUs("FF2TH", ms150), is("15TH"));
    assertThat(toCharUs("FF2th", ms0), is("00th"));
    assertThat(toCharUs("FF2th", ms20), is("02nd"));
    assertThat(toCharUs("FF2th", ms150), is("15th"));

    assertThat(toCharUs("FMFF2th", ms20), is("2nd"));
    assertThat(toCharUs("FMFF2nd", ms20), is("2nd"));
  }

  @Test void testFF4() {
    final ZonedDateTime us0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime us200 = createDateTime(2024, 1, 1, 0, 0, 0, 200_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertThat(toCharUs("FF4", us0), is("0000"));
    assertThat(toCharUs("FF4", us200), is("0002"));
    assertThat(toCharUs("FF4", ms150), is("1500"));

    assertThat(toCharUs("FMFF4", us0), is("0"));
    assertThat(toCharUs("FMFF4", us200), is("2"));
    assertThat(toCharUs("FMFF4", ms150), is("1500"));

    assertThat(toCharUs("FF4TH", us0), is("0000TH"));
    assertThat(toCharUs("FF4TH", us200), is("0002ND"));
    assertThat(toCharUs("FF4TH", ms150), is("1500TH"));
    assertThat(toCharUs("FF4th", us0), is("0000th"));
    assertThat(toCharUs("FF4th", us200), is("0002nd"));
    assertThat(toCharUs("FF4th", ms150), is("1500th"));

    assertThat(toCharUs("FMFF4th", us200), is("2nd"));
    assertThat(toCharUs("FMFF4nd", us200), is("2nd"));
  }

  @Test void testFF5() {
    final ZonedDateTime us0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime us20 = createDateTime(2024, 1, 1, 0, 0, 0, 20_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertThat(toCharUs("FF5", us0), is("00000"));
    assertThat(toCharUs("FF5", us20), is("00002"));
    assertThat(toCharUs("FF5", ms150), is("15000"));

    assertThat(toCharUs("FMFF5", us0), is("0"));
    assertThat(toCharUs("FMFF5", us20), is("2"));
    assertThat(toCharUs("FMFF5", ms150), is("15000"));

    assertThat(toCharUs("FF5TH", us0), is("00000TH"));
    assertThat(toCharUs("FF5TH", us20), is("00002ND"));
    assertThat(toCharUs("FF5TH", ms150), is("15000TH"));
    assertThat(toCharUs("FF5th", us0), is("00000th"));
    assertThat(toCharUs("FF5th", us20), is("00002nd"));
    assertThat(toCharUs("FF5th", ms150), is("15000th"));

    assertThat(toCharUs("FMFF5th", us20), is("2nd"));
    assertThat(toCharUs("FMFF5nd", us20), is("2nd"));
  }

  @Test void testFF6() {
    final ZonedDateTime us0 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime us2 = createDateTime(2024, 1, 1, 0, 0, 0, 2_000);
    final ZonedDateTime ms150 = createDateTime(2024, 1, 1, 0, 0, 0, 150_000_000);

    assertThat(toCharUs("FF6", us0), is("000000"));
    assertThat(toCharUs("FF6", us2), is("000002"));
    assertThat(toCharUs("FF6", ms150), is("150000"));

    assertThat(toCharUs("FMFF6", us0), is("0"));
    assertThat(toCharUs("FMFF6", us2), is("2"));
    assertThat(toCharUs("FMFF6", ms150), is("150000"));

    assertThat(toCharUs("FF6TH", us0), is("000000TH"));
    assertThat(toCharUs("FF6TH", us2), is("000002ND"));
    assertThat(toCharUs("FF6TH", ms150), is("150000TH"));
    assertThat(toCharUs("FF6th", us0), is("000000th"));
    assertThat(toCharUs("FF6th", us2), is("000002nd"));
    assertThat(toCharUs("FF6th", ms150), is("150000th"));

    assertThat(toCharUs("FMFF6th", us2), is("2nd"));
    assertThat(toCharUs("FMFF6nd", us2), is("2nd"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"AM", "PM"})
  void testAMUpperCase(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertThat(toCharUs(pattern, midnight), is("AM"));
    assertThat(toCharUs(pattern, morning), is("AM"));
    assertThat(toCharUs(pattern, noon), is("PM"));
    assertThat(toCharUs(pattern, evening), is("PM"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"am", "pm"})
  void testAMLowerCase(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertThat(toCharUs(pattern, midnight), is("am"));
    assertThat(toCharUs(pattern, morning), is("am"));
    assertThat(toCharUs(pattern, noon), is("pm"));
    assertThat(toCharUs(pattern, evening), is("pm"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"A.M.", "P.M."})
  void testAMWithDotsUpperCase(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertThat(toCharUs(pattern, midnight), is("A.M."));
    assertThat(toCharUs(pattern, morning), is("A.M."));
    assertThat(toCharUs(pattern, noon), is("P.M."));
    assertThat(toCharUs(pattern, evening), is("P.M."));
  }

  @ParameterizedTest
  @ValueSource(strings = {"a.m.", "p.m."})
  void testAMWithDotsLowerCase(String pattern) {
    final ZonedDateTime midnight = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime morning = createDateTime(2024, 1, 1, 6, 0, 0, 0);
    final ZonedDateTime noon = createDateTime(2024, 1, 1, 12, 0, 0, 0);
    final ZonedDateTime evening = createDateTime(2024, 1, 1, 18, 0, 0, 0);

    assertThat(toCharUs(pattern, midnight), is("a.m."));
    assertThat(toCharUs(pattern, morning), is("a.m."));
    assertThat(toCharUs(pattern, noon), is("p.m."));
    assertThat(toCharUs(pattern, evening), is("p.m."));
  }

  @Test void testYearWithCommas() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertThat(toCharUs("Y,YYY", year1), is("2,024"));
    assertThat(toCharUs("Y,YYY", year2), is("0,100"));
    assertThat(toCharUs("Y,YYY", year3), is("0,001"));
    assertThat(toCharUs("Y,YYY", year4), is("32,136"));
    assertThat(toCharUs("FMY,YYY", year1), is("2,024"));
    assertThat(toCharUs("FMY,YYY", year2), is("0,100"));
    assertThat(toCharUs("FMY,YYY", year3), is("0,001"));
    assertThat(toCharUs("FMY,YYY", year4), is("32,136"));

    assertThat(toCharUs("Y,YYYTH", year1), is("2,024TH"));
    assertThat(toCharUs("Y,YYYTH", year2), is("0,100TH"));
    assertThat(toCharUs("Y,YYYTH", year3), is("0,001ST"));
    assertThat(toCharUs("Y,YYYTH", year4), is("32,136TH"));
    assertThat(toCharUs("Y,YYYth", year1), is("2,024th"));
    assertThat(toCharUs("Y,YYYth", year2), is("0,100th"));
    assertThat(toCharUs("Y,YYYth", year3), is("0,001st"));
    assertThat(toCharUs("Y,YYYth", year4), is("32,136th"));

    assertThat(toCharUs("FMY,YYYth", year1), is("2,024th"));
  }

  @Test void testYYYY() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertThat(toCharUs("YYYY", year1), is("2024"));
    assertThat(toCharUs("YYYY", year2), is("0100"));
    assertThat(toCharUs("YYYY", year3), is("0001"));
    assertThat(toCharUs("YYYY", year4), is("32136"));
    assertThat(toCharUs("FMYYYY", year1), is("2024"));
    assertThat(toCharUs("FMYYYY", year2), is("100"));
    assertThat(toCharUs("FMYYYY", year3), is("1"));
    assertThat(toCharUs("FMYYYY", year4), is("32136"));

    assertThat(toCharUs("YYYYTH", year1), is("2024TH"));
    assertThat(toCharUs("YYYYTH", year2), is("0100TH"));
    assertThat(toCharUs("YYYYTH", year3), is("0001ST"));
    assertThat(toCharUs("YYYYTH", year4), is("32136TH"));
    assertThat(toCharUs("YYYYth", year1), is("2024th"));
    assertThat(toCharUs("YYYYth", year2), is("0100th"));
    assertThat(toCharUs("YYYYth", year3), is("0001st"));
    assertThat(toCharUs("YYYYth", year4), is("32136th"));

    assertThat(toCharUs("FMYYYYth", year1), is("2024th"));
  }

  @Test void testYYY() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertThat(toCharUs("YYY", year1), is("024"));
    assertThat(toCharUs("YYY", year2), is("100"));
    assertThat(toCharUs("YYY", year3), is("001"));
    assertThat(toCharUs("YYY", year4), is("136"));
    assertThat(toCharUs("FMYYY", year1), is("24"));
    assertThat(toCharUs("FMYYY", year2), is("100"));
    assertThat(toCharUs("FMYYY", year3), is("1"));
    assertThat(toCharUs("FMYYY", year4), is("136"));

    assertThat(toCharUs("YYYTH", year1), is("024TH"));
    assertThat(toCharUs("YYYTH", year2), is("100TH"));
    assertThat(toCharUs("YYYTH", year3), is("001ST"));
    assertThat(toCharUs("YYYTH", year4), is("136TH"));
    assertThat(toCharUs("YYYth", year1), is("024th"));
    assertThat(toCharUs("YYYth", year2), is("100th"));
    assertThat(toCharUs("YYYth", year3), is("001st"));
    assertThat(toCharUs("YYYth", year4), is("136th"));

    assertThat(toCharUs("FMYYYth", year1), is("24th"));
  }

  @Test void testYY() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertThat(toCharUs("YY", year1), is("24"));
    assertThat(toCharUs("YY", year2), is("00"));
    assertThat(toCharUs("YY", year3), is("01"));
    assertThat(toCharUs("YY", year4), is("36"));
    assertThat(toCharUs("FMYY", year1), is("24"));
    assertThat(toCharUs("FMYY", year2), is("0"));
    assertThat(toCharUs("FMYY", year3), is("1"));
    assertThat(toCharUs("FMYY", year4), is("36"));

    assertThat(toCharUs("YYTH", year1), is("24TH"));
    assertThat(toCharUs("YYTH", year2), is("00TH"));
    assertThat(toCharUs("YYTH", year3), is("01ST"));
    assertThat(toCharUs("YYTH", year4), is("36TH"));
    assertThat(toCharUs("YYth", year1), is("24th"));
    assertThat(toCharUs("YYth", year2), is("00th"));
    assertThat(toCharUs("YYth", year3), is("01st"));
    assertThat(toCharUs("YYth", year4), is("36th"));

    assertThat(toCharUs("FMYYth", year1), is("24th"));
  }

  @Test void testY() {
    final ZonedDateTime year1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year2 = createDateTime(100, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year3 = createDateTime(1, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime year4 = createDateTime(32136, 1, 1, 0, 0, 0, 0);

    assertThat(toCharUs("Y", year1), is("4"));
    assertThat(toCharUs("Y", year2), is("0"));
    assertThat(toCharUs("Y", year3), is("1"));
    assertThat(toCharUs("Y", year4), is("6"));
    assertThat(toCharUs("FMY", year1), is("4"));
    assertThat(toCharUs("FMY", year2), is("0"));
    assertThat(toCharUs("FMY", year3), is("1"));
    assertThat(toCharUs("FMY", year4), is("6"));

    assertThat(toCharUs("YTH", year1), is("4TH"));
    assertThat(toCharUs("YTH", year2), is("0TH"));
    assertThat(toCharUs("YTH", year3), is("1ST"));
    assertThat(toCharUs("YTH", year4), is("6TH"));
    assertThat(toCharUs("Yth", year1), is("4th"));
    assertThat(toCharUs("Yth", year2), is("0th"));
    assertThat(toCharUs("Yth", year3), is("1st"));
    assertThat(toCharUs("Yth", year4), is("6th"));

    assertThat(toCharUs("FMYth", year1), is("4th"));
  }

  @Test void testIYYY() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(1);
    final ZonedDateTime date4 = date3.plusDays(1);
    final ZonedDateTime date5 = date4.plusDays(1);

    assertThat(toCharUs("IYYY", date1), is("2019"));
    assertThat(toCharUs("IYYY", date2), is("2020"));
    assertThat(toCharUs("IYYY", date3), is("2020"));
    assertThat(toCharUs("IYYY", date4), is("2020"));
    assertThat(toCharUs("IYYY", date5), is("2020"));
    assertThat(toCharUs("FMIYYY", date1), is("2019"));
    assertThat(toCharUs("FMIYYY", date2), is("2020"));
    assertThat(toCharUs("FMIYYY", date3), is("2020"));
    assertThat(toCharUs("FMIYYY", date4), is("2020"));
    assertThat(toCharUs("FMIYYY", date5), is("2020"));

    assertThat(toCharUs("IYYYTH", date1), is("2019TH"));
    assertThat(toCharUs("IYYYTH", date2), is("2020TH"));
    assertThat(toCharUs("IYYYTH", date3), is("2020TH"));
    assertThat(toCharUs("IYYYTH", date4), is("2020TH"));
    assertThat(toCharUs("IYYYTH", date5), is("2020TH"));
    assertThat(toCharUs("IYYYth", date1), is("2019th"));
    assertThat(toCharUs("IYYYth", date2), is("2020th"));
    assertThat(toCharUs("IYYYth", date3), is("2020th"));
    assertThat(toCharUs("IYYYth", date4), is("2020th"));
    assertThat(toCharUs("IYYYth", date5), is("2020th"));

    assertThat(toCharUs("FMIYYYth", date5), is("2020th"));
  }

  @Test void testIYY() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(1);
    final ZonedDateTime date4 = date3.plusDays(1);
    final ZonedDateTime date5 = date4.plusDays(1);

    assertThat(toCharUs("IYY", date1), is("019"));
    assertThat(toCharUs("IYY", date2), is("020"));
    assertThat(toCharUs("IYY", date3), is("020"));
    assertThat(toCharUs("IYY", date4), is("020"));
    assertThat(toCharUs("IYY", date5), is("020"));
    assertThat(toCharUs("FMIYY", date1), is("19"));
    assertThat(toCharUs("FMIYY", date2), is("20"));
    assertThat(toCharUs("FMIYY", date3), is("20"));
    assertThat(toCharUs("FMIYY", date4), is("20"));
    assertThat(toCharUs("FMIYY", date5), is("20"));

    assertThat(toCharUs("IYYTH", date1), is("019TH"));
    assertThat(toCharUs("IYYTH", date2), is("020TH"));
    assertThat(toCharUs("IYYTH", date3), is("020TH"));
    assertThat(toCharUs("IYYTH", date4), is("020TH"));
    assertThat(toCharUs("IYYTH", date5), is("020TH"));
    assertThat(toCharUs("IYYth", date1), is("019th"));
    assertThat(toCharUs("IYYth", date2), is("020th"));
    assertThat(toCharUs("IYYth", date3), is("020th"));
    assertThat(toCharUs("IYYth", date4), is("020th"));
    assertThat(toCharUs("IYYth", date5), is("020th"));

    assertThat(toCharUs("FMIYYth", date5), is("20th"));
  }

  @Test void testIY() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(1);
    final ZonedDateTime date4 = date3.plusDays(1);
    final ZonedDateTime date5 = date4.plusDays(1);

    assertThat(toCharUs("IY", date1), is("19"));
    assertThat(toCharUs("IY", date2), is("20"));
    assertThat(toCharUs("IY", date3), is("20"));
    assertThat(toCharUs("IY", date4), is("20"));
    assertThat(toCharUs("IY", date5), is("20"));
    assertThat(toCharUs("FMIY", date1), is("19"));
    assertThat(toCharUs("FMIY", date2), is("20"));
    assertThat(toCharUs("FMIY", date3), is("20"));
    assertThat(toCharUs("FMIY", date4), is("20"));
    assertThat(toCharUs("FMIY", date5), is("20"));

    assertThat(toCharUs("IYTH", date1), is("19TH"));
    assertThat(toCharUs("IYTH", date2), is("20TH"));
    assertThat(toCharUs("IYTH", date3), is("20TH"));
    assertThat(toCharUs("IYTH", date4), is("20TH"));
    assertThat(toCharUs("IYTH", date5), is("20TH"));
    assertThat(toCharUs("IYth", date1), is("19th"));
    assertThat(toCharUs("IYth", date2), is("20th"));
    assertThat(toCharUs("IYth", date3), is("20th"));
    assertThat(toCharUs("IYth", date4), is("20th"));
    assertThat(toCharUs("IYth", date5), is("20th"));

    assertThat(toCharUs("FMIYth", date5), is("20th"));
  }

  @Test void testI() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(1);
    final ZonedDateTime date4 = date3.plusDays(1);
    final ZonedDateTime date5 = date4.plusDays(1);

    assertThat(toCharUs("I", date1), is("9"));
    assertThat(toCharUs("I", date2), is("0"));
    assertThat(toCharUs("I", date3), is("0"));
    assertThat(toCharUs("I", date4), is("0"));
    assertThat(toCharUs("I", date5), is("0"));
    assertThat(toCharUs("FMI", date1), is("9"));
    assertThat(toCharUs("FMI", date2), is("0"));
    assertThat(toCharUs("FMI", date3), is("0"));
    assertThat(toCharUs("FMI", date4), is("0"));
    assertThat(toCharUs("FMI", date5), is("0"));

    assertThat(toCharUs("ITH", date1), is("9TH"));
    assertThat(toCharUs("ITH", date2), is("0TH"));
    assertThat(toCharUs("ITH", date3), is("0TH"));
    assertThat(toCharUs("ITH", date4), is("0TH"));
    assertThat(toCharUs("ITH", date5), is("0TH"));
    assertThat(toCharUs("Ith", date1), is("9th"));
    assertThat(toCharUs("Ith", date2), is("0th"));
    assertThat(toCharUs("Ith", date3), is("0th"));
    assertThat(toCharUs("Ith", date4), is("0th"));
    assertThat(toCharUs("Ith", date5), is("0th"));

    assertThat(toCharUs("FMIth", date5), is("0th"));
  }

  @Test void testIW() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(186);

    assertThat(toCharUs("IW", date1), is("52"));
    assertThat(toCharUs("IW", date2), is("01"));
    assertThat(toCharUs("IW", date3), is("27"));
    assertThat(toCharUs("FMIW", date1), is("52"));
    assertThat(toCharUs("FMIW", date2), is("1"));
    assertThat(toCharUs("FMIW", date3), is("27"));

    assertThat(toCharUs("IWTH", date1), is("52ND"));
    assertThat(toCharUs("IWTH", date2), is("01ST"));
    assertThat(toCharUs("IWTH", date3), is("27TH"));
    assertThat(toCharUs("IWth", date1), is("52nd"));
    assertThat(toCharUs("IWth", date2), is("01st"));
    assertThat(toCharUs("IWth", date3), is("27th"));

    assertThat(toCharUs("FMIWth", date3), is("27th"));
  }

  @Test void testIDDD() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(186);

    assertThat(toCharUs("IDDD", date1), is("364"));
    assertThat(toCharUs("IDDD", date2), is("001"));
    assertThat(toCharUs("IDDD", date3), is("187"));
    assertThat(toCharUs("FMIDDD", date1), is("364"));
    assertThat(toCharUs("FMIDDD", date2), is("1"));
    assertThat(toCharUs("FMIDDD", date3), is("187"));

    assertThat(toCharUs("IDDDTH", date1), is("364TH"));
    assertThat(toCharUs("IDDDTH", date2), is("001ST"));
    assertThat(toCharUs("IDDDTH", date3), is("187TH"));
    assertThat(toCharUs("IDDDth", date1), is("364th"));
    assertThat(toCharUs("IDDDth", date2), is("001st"));
    assertThat(toCharUs("IDDDth", date3), is("187th"));

    assertThat(toCharUs("FMIDDDth", date3), is("187th"));
  }

  @Test void testID() {
    final ZonedDateTime date1 = createDateTime(2019, 12, 29, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.plusDays(1);
    final ZonedDateTime date3 = date2.plusDays(186);

    assertThat(toCharUs("ID", date1), is("7"));
    assertThat(toCharUs("ID", date2), is("1"));
    assertThat(toCharUs("ID", date3), is("5"));
    assertThat(toCharUs("FMID", date1), is("7"));
    assertThat(toCharUs("FMID", date2), is("1"));
    assertThat(toCharUs("FMID", date3), is("5"));

    assertThat(toCharUs("IDTH", date1), is("7TH"));
    assertThat(toCharUs("IDTH", date2), is("1ST"));
    assertThat(toCharUs("IDTH", date3), is("5TH"));
    assertThat(toCharUs("IDth", date1), is("7th"));
    assertThat(toCharUs("IDth", date2), is("1st"));
    assertThat(toCharUs("IDth", date3), is("5th"));

    assertThat(toCharUs("FMIDth", date3), is("5th"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"AD", "BC"})
  void testEraUpperCaseNoDots(String pattern) {
    final ZonedDateTime date1 = createDateTime(2019, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2018);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertThat(toCharUs(pattern, date1), is("AD"));
    assertThat(toCharUs(pattern, date2), is("AD"));
    assertThat(toCharUs(pattern, date3), is("BC"));
    assertThat(toCharUs(pattern, date4), is("BC"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"ad", "bc"})
  void testEraLowerCaseNoDots(String pattern) {
    final ZonedDateTime date1 = createDateTime(2019, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2018);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertThat(toCharUs(pattern, date1), is("ad"));
    assertThat(toCharUs(pattern, date2), is("ad"));
    assertThat(toCharUs(pattern, date3), is("bc"));
    assertThat(toCharUs(pattern, date4), is("bc"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"A.D.", "B.C."})
  void testEraUpperCaseWithDots(String pattern) {
    final ZonedDateTime date1 = createDateTime(2019, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2018);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertThat(toCharUs(pattern, date1), is("A.D."));
    assertThat(toCharUs(pattern, date2), is("A.D."));
    assertThat(toCharUs(pattern, date3), is("B.C."));
    assertThat(toCharUs(pattern, date4), is("B.C."));
  }

  @ParameterizedTest
  @ValueSource(strings = {"a.d.", "b.c."})
  void testEraLowerCaseWithDots(String pattern) {
    final ZonedDateTime date1 = createDateTime(2019, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2018);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertThat(toCharUs(pattern, date1), is("a.d."));
    assertThat(toCharUs(pattern, date2), is("a.d."));
    assertThat(toCharUs(pattern, date3), is("b.c."));
    assertThat(toCharUs(pattern, date4), is("b.c."));
  }

  @Test void testMonthFullUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertThat(toCharUs("MONTH", date1), is("JANUARY  "));
    assertThat(toCharUs("MONTH", date2), is("MARCH    "));
    assertThat(toCharUs("MONTH", date3), is("NOVEMBER "));
  }

  @Test void testMonthFullUpperCaseNoTranslate() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertThat(toCharFrench("MONTH", date1), is("JANUARY  "));
    assertThat(toCharFrench("MONTH", date2), is("MARCH    "));
    assertThat(toCharFrench("MONTH", date3), is("NOVEMBER "));
  }

  @Test void testMonthFullUpperCaseTranslate() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertThat(toCharFrench("TMMONTH", date1), is("JANVIER"));
    assertThat(toCharFrench("TMMONTH", date2), is("MARS"));
    assertThat(toCharFrench("TMMONTH", date3), is("NOVEMBRE"));
  }

  @Test void testMonthFullUpperCaseNoPadding() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertThat(toCharUs("FMMONTH", date1), is("JANUARY"));
    assertThat(toCharUs("FMMONTH", date2), is("MARCH"));
    assertThat(toCharUs("FMMONTH", date3), is("NOVEMBER"));
  }

  @Test void testMonthFullCapitalized() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertThat(toCharUs("Month", date1), is("January  "));
    assertThat(toCharUs("Month", date2), is("March    "));
    assertThat(toCharUs("Month", date3), is("November "));
  }

  @Test void testMonthFullLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertThat(toCharUs("month", date1), is("january  "));
    assertThat(toCharUs("month", date2), is("march    "));
    assertThat(toCharUs("month", date3), is("november "));
  }

  @Test void testMonthShortUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertThat(toCharUs("MON", date1), is("JAN"));
    assertThat(toCharUs("MON", date2), is("MAR"));
    assertThat(toCharUs("MON", date3), is("NOV"));
  }

  @Test void testMonthShortCapitalized() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertThat(toCharUs("Mon", date1), is("Jan"));
    assertThat(toCharUs("Mon", date2), is("Mar"));
    assertThat(toCharUs("Mon", date3), is("Nov"));
  }

  @Test void testMonthShortLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertThat(toCharUs("mon", date1), is("jan"));
    assertThat(toCharUs("mon", date2), is("mar"));
    assertThat(toCharUs("mon", date3), is("nov"));
  }

  @Test void testMM() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertThat(toCharUs("MM", date1), is("01"));
    assertThat(toCharUs("MM", date2), is("03"));
    assertThat(toCharUs("MM", date3), is("11"));
    assertThat(toCharUs("FMMM", date1), is("1"));
    assertThat(toCharUs("FMMM", date2), is("3"));
    assertThat(toCharUs("FMMM", date3), is("11"));

    assertThat(toCharUs("MMTH", date1), is("01ST"));
    assertThat(toCharUs("MMTH", date2), is("03RD"));
    assertThat(toCharUs("MMTH", date3), is("11TH"));
    assertThat(toCharUs("MMth", date1), is("01st"));
    assertThat(toCharUs("MMth", date2), is("03rd"));
    assertThat(toCharUs("MMth", date3), is("11th"));

    assertThat(toCharUs("FMMMth", date2), is("3rd"));
  }

  @Test void testDayFullUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertThat(toCharUs("DAY", date1), is("MONDAY   "));
    assertThat(toCharUs("DAY", date2), is("FRIDAY   "));
    assertThat(toCharUs("DAY", date3), is("TUESDAY  "));
  }

  @Test void testDayFullUpperNoTranslate() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertThat(toCharFrench("DAY", date1), is("MONDAY   "));
    assertThat(toCharFrench("DAY", date2), is("FRIDAY   "));
    assertThat(toCharFrench("DAY", date3), is("TUESDAY  "));
  }

  @Test void testDayFullUpperTranslate() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertThat(toCharFrench("TMDAY", date1), is("LUNDI"));
    assertThat(toCharFrench("TMDAY", date2), is("VENDREDI"));
    assertThat(toCharFrench("TMDAY", date3), is("MARDI"));
  }

  @Test void testDayFullCapitalized() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertThat(toCharUs("Day", date1), is("Monday   "));
    assertThat(toCharUs("Day", date2), is("Friday   "));
    assertThat(toCharUs("Day", date3), is("Tuesday  "));
  }

  @Test void testDayFullLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertThat(toCharUs("day", date1), is("monday   "));
    assertThat(toCharUs("day", date2), is("friday   "));
    assertThat(toCharUs("day", date3), is("tuesday  "));
  }

  @Test void testDayShortUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertThat(toCharUs("DY", date1), is("MON"));
    assertThat(toCharUs("DY", date2), is("FRI"));
    assertThat(toCharUs("DY", date3), is("TUE"));
  }

  @Test void testDayShortCapitalized() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertThat(toCharUs("Dy", date1), is("Mon"));
    assertThat(toCharUs("Dy", date2), is("Fri"));
    assertThat(toCharUs("Dy", date3), is("Tue"));
  }

  @Test void testDayShortLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    assertThat(toCharUs("dy", date1), is("mon"));
    assertThat(toCharUs("dy", date2), is("fri"));
    assertThat(toCharUs("dy", date3), is("tue"));
  }

  @Test void testDDD() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    assertThat(toCharUs("DDD", date1), is("001"));
    assertThat(toCharUs("DDD", date2), is("061"));
    assertThat(toCharUs("DDD", date3), is("306"));
    assertThat(toCharUs("FMDDD", date1), is("1"));
    assertThat(toCharUs("FMDDD", date2), is("61"));
    assertThat(toCharUs("FMDDD", date3), is("306"));

    assertThat(toCharUs("DDDTH", date1), is("001ST"));
    assertThat(toCharUs("DDDTH", date2), is("061ST"));
    assertThat(toCharUs("DDDTH", date3), is("306TH"));
    assertThat(toCharUs("DDDth", date1), is("001st"));
    assertThat(toCharUs("DDDth", date2), is("061st"));
    assertThat(toCharUs("DDDth", date3), is("306th"));

    assertThat(toCharUs("FMDDDth", date1), is("1st"));
  }

  @Test void testDD() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 1, 12, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 1, 29, 23, 0, 0, 0);

    assertThat(toCharUs("DD", date1), is("01"));
    assertThat(toCharUs("DD", date2), is("12"));
    assertThat(toCharUs("DD", date3), is("29"));
    assertThat(toCharUs("FMDD", date1), is("1"));
    assertThat(toCharUs("FMDD", date2), is("12"));
    assertThat(toCharUs("FMDD", date3), is("29"));

    assertThat(toCharUs("DDTH", date1), is("01ST"));
    assertThat(toCharUs("DDTH", date2), is("12TH"));
    assertThat(toCharUs("DDTH", date3), is("29TH"));
    assertThat(toCharUs("DDth", date1), is("01st"));
    assertThat(toCharUs("DDth", date2), is("12th"));
    assertThat(toCharUs("DDth", date3), is("29th"));

    assertThat(toCharUs("FMDDth", date1), is("1st"));
  }

  @Test void testD() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 1, 2, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 1, 27, 23, 0, 0, 0);

    assertThat(toCharUs("D", date1), is("2"));
    assertThat(toCharUs("D", date2), is("3"));
    assertThat(toCharUs("D", date3), is("7"));
    assertThat(toCharUs("FMD", date1), is("2"));
    assertThat(toCharUs("FMD", date2), is("3"));
    assertThat(toCharUs("FMD", date3), is("7"));

    assertThat(toCharUs("DTH", date1), is("2ND"));
    assertThat(toCharUs("DTH", date2), is("3RD"));
    assertThat(toCharUs("DTH", date3), is("7TH"));
    assertThat(toCharUs("Dth", date1), is("2nd"));
    assertThat(toCharUs("Dth", date2), is("3rd"));
    assertThat(toCharUs("Dth", date3), is("7th"));

    assertThat(toCharUs("FMDth", date1), is("2nd"));
  }

  @Test void testWW() {
    final ZonedDateTime date1 = createDateTime(2016, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2016, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2016, 10, 1, 23, 0, 0, 0);

    assertThat(toCharUs("WW", date1), is("1"));
    assertThat(toCharUs("WW", date2), is("9"));
    assertThat(toCharUs("WW", date3), is("40"));
    assertThat(toCharUs("FMWW", date1), is("1"));
    assertThat(toCharUs("FMWW", date2), is("9"));
    assertThat(toCharUs("FMWW", date3), is("40"));

    assertThat(toCharUs("WWTH", date1), is("1ST"));
    assertThat(toCharUs("WWTH", date2), is("9TH"));
    assertThat(toCharUs("WWTH", date3), is("40TH"));
    assertThat(toCharUs("WWth", date1), is("1st"));
    assertThat(toCharUs("WWth", date2), is("9th"));
    assertThat(toCharUs("WWth", date3), is("40th"));

    assertThat(toCharUs("FMWWth", date1), is("1st"));
  }

  @Test void testW() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 1, 15, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 31, 23, 0, 0, 0);

    assertThat(toCharUs("W", date1), is("1"));
    assertThat(toCharUs("W", date2), is("3"));
    assertThat(toCharUs("W", date3), is("5"));
    assertThat(toCharUs("FMW", date1), is("1"));
    assertThat(toCharUs("FMW", date2), is("3"));
    assertThat(toCharUs("FMW", date3), is("5"));

    assertThat(toCharUs("WTH", date1), is("1ST"));
    assertThat(toCharUs("WTH", date2), is("3RD"));
    assertThat(toCharUs("WTH", date3), is("5TH"));
    assertThat(toCharUs("Wth", date1), is("1st"));
    assertThat(toCharUs("Wth", date2), is("3rd"));
    assertThat(toCharUs("Wth", date3), is("5th"));

    assertThat(toCharUs("FMWth", date1), is("1st"));
  }

  @Test void testCC() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2023);
    final ZonedDateTime date3 = date2.minusYears(1);
    final ZonedDateTime date4 = date3.minusYears(200);

    assertThat(toCharUs("CC", date1), is("21"));
    assertThat(toCharUs("CC", date2), is("01"));
    assertThat(toCharUs("CC", date3), is("-01"));
    assertThat(toCharUs("CC", date4), is("-03"));
    assertThat(toCharUs("FMCC", date1), is("21"));
    assertThat(toCharUs("FMCC", date2), is("1"));
    assertThat(toCharUs("FMCC", date3), is("-1"));
    assertThat(toCharUs("FMCC", date4), is("-3"));

    assertThat(toCharUs("CCTH", date1), is("21ST"));
    assertThat(toCharUs("CCTH", date2), is("01ST"));
    assertThat(toCharUs("CCTH", date3), is("-01ST"));
    assertThat(toCharUs("CCTH", date4), is("-03RD"));
    assertThat(toCharUs("CCth", date1), is("21st"));
    assertThat(toCharUs("CCth", date2), is("01st"));
    assertThat(toCharUs("CCth", date3), is("-01st"));
    assertThat(toCharUs("CCth", date4), is("-03rd"));

    assertThat(toCharUs("FMCCth", date3), is("-1st"));
  }

  @Test void testJ() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime date2 = date1.minusYears(2024);
    final ZonedDateTime date3 = date2.minusYears(1000);

    assertThat(toCharUs("J", date1), is("2460311"));
    assertThat(toCharUs("J", date2), is("1721060"));
    assertThat(toCharUs("J", date3), is("1356183"));
    assertThat(toCharUs("FMJ", date1), is("2460311"));
    assertThat(toCharUs("FMJ", date2), is("1721060"));
    assertThat(toCharUs("FMJ", date3), is("1356183"));

    assertThat(toCharUs("JTH", date1), is("2460311TH"));
    assertThat(toCharUs("JTH", date2), is("1721060TH"));
    assertThat(toCharUs("JTH", date3), is("1356183RD"));
    assertThat(toCharUs("Jth", date1), is("2460311th"));
    assertThat(toCharUs("Jth", date2), is("1721060th"));
    assertThat(toCharUs("Jth", date3), is("1356183rd"));
  }

  @Test void testQ() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 4, 9, 0, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 8, 23, 0, 0, 0, 0);
    final ZonedDateTime date4 = createDateTime(2024, 12, 31, 0, 0, 0, 0);

    assertThat(toCharUs("Q", date1), is("1"));
    assertThat(toCharUs("Q", date2), is("2"));
    assertThat(toCharUs("Q", date3), is("3"));
    assertThat(toCharUs("Q", date4), is("4"));
    assertThat(toCharUs("FMQ", date1), is("1"));
    assertThat(toCharUs("FMQ", date2), is("2"));
    assertThat(toCharUs("FMQ", date3), is("3"));
    assertThat(toCharUs("FMQ", date4), is("4"));

    assertThat(toCharUs("QTH", date1), is("1ST"));
    assertThat(toCharUs("QTH", date2), is("2ND"));
    assertThat(toCharUs("QTH", date3), is("3RD"));
    assertThat(toCharUs("QTH", date4), is("4TH"));
    assertThat(toCharUs("Qth", date1), is("1st"));
    assertThat(toCharUs("Qth", date2), is("2nd"));
    assertThat(toCharUs("Qth", date3), is("3rd"));
    assertThat(toCharUs("Qth", date4), is("4th"));
  }

  @Test void testRMUpperCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 4, 9, 0, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 8, 23, 0, 0, 0, 0);
    final ZonedDateTime date4 = createDateTime(2024, 12, 31, 0, 0, 0, 0);

    assertThat(toCharUs("RM", date1), is("I"));
    assertThat(toCharUs("RM", date2), is("IV"));
    assertThat(toCharUs("RM", date3), is("VIII"));
    assertThat(toCharUs("RM", date4), is("XII"));
  }

  @Test void testRMLowerCase() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 0, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 4, 9, 0, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 8, 23, 0, 0, 0, 0);
    final ZonedDateTime date4 = createDateTime(2024, 12, 31, 0, 0, 0, 0);

    assertThat(toCharUs("rm", date1), is("i"));
    assertThat(toCharUs("rm", date2), is("iv"));
    assertThat(toCharUs("rm", date3), is("viii"));
    assertThat(toCharUs("rm", date4), is("xii"));
  }

  @Test void testToCharReuseFormat() throws Exception {
    final CompiledDateTimeFormat compiledFormat =
        PostgresqlDateTimeFormatter.compilePattern("YYYY-MM-DD HH24:MI:SS.MS");
    final ZonedDateTime expected1 = createDateTime(2019, 3, 7, 15, 46, 23, 521000000);
    final ZonedDateTime expected2 = createDateTime(1983, 11, 29, 4, 21, 16, 45000000);
    final ZonedDateTime expected3 = createDateTime(2024, 9, 24, 14, 53, 37, 891000000);
    assertThat(
        compiledFormat.parseDateTime("2019-03-07 15:46:23.521", TIME_ZONE,
            Locale.US),
        is(expected1));
    assertThat(
        compiledFormat.parseDateTime("1983-11-29 04:21:16.045", TIME_ZONE,
            Locale.US),
        is(expected2));
    assertThat(
        compiledFormat.parseDateTime("2024-09-24 14:53:37.891", TIME_ZONE,
            Locale.US),
        is(expected3));
    assertThat(
        compiledFormat.parseDateTime("2024x09x24x14x53x37x891", TIME_ZONE,
            Locale.US),
        is(expected3));
  }

  @Test void testToTimestampHH() throws Exception {
    assertThat(toTimestamp("01", "HH"), is(DAY_1_CE.plusHours(1)));
    assertThat(toTimestamp("1", "HH"), is(DAY_1_CE.plusHours(1)));
    assertThat(toTimestamp("11", "HH"), is(DAY_1_CE.plusHours(11)));

    try {
      ZonedDateTime x = toTimestamp("72", "HH");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Parsed value outside of valid range"));
    }

    try {
      ZonedDateTime x = toTimestamp("abc", "HH");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Unable to parse value"));
    }
  }

  @Test void testToTimestampHH12() throws Exception {
    assertThat(toTimestamp("01", "HH12"), is(DAY_1_CE.plusHours(1)));
    assertThat(toTimestamp("1", "HH12"), is(DAY_1_CE.plusHours(1)));
    assertThat(toTimestamp("11", "HH12"), is(DAY_1_CE.plusHours(11)));

    try {
      ZonedDateTime x = toTimestamp("72", "HH12");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Parsed value outside of valid range"));
    }

    try {
      ZonedDateTime x = toTimestamp("abc", "HH12");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Unable to parse value"));
    }
  }

  @Test void testToTimestampHH24() throws Exception {
    assertThat(toTimestamp("01", "HH24"), is(DAY_1_CE.plusHours(1)));
    assertThat(toTimestamp("1", "HH24"), is(DAY_1_CE.plusHours(1)));
    assertThat(toTimestamp("18", "HH24"), is(DAY_1_CE.plusHours(18)));

    try {
      ZonedDateTime x = toTimestamp("72", "HH24");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Parsed value outside of valid range"));
    }

    try {
      ZonedDateTime x = toTimestamp("abc", "HH24");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Unable to parse value"));
    }
  }

  @Test void testToTimestampMI() throws Exception {
    assertThat(toTimestamp("01", "MI"), is(DAY_1_CE.plusMinutes(1)));
    assertThat(toTimestamp("1", "MI"), is(DAY_1_CE.plusMinutes(1)));
    assertThat(toTimestamp("57", "MI"), is(DAY_1_CE.plusMinutes(57)));

    try {
      ZonedDateTime x = toTimestamp("72", "MI");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Parsed value outside of valid range"));
    }

    try {
      ZonedDateTime x = toTimestamp("abc", "MI");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Unable to parse value"));
    }
  }

  @Test void testToTimestampSS() throws Exception {
    assertThat(toTimestamp("01", "SS"), is(DAY_1_CE.plusSeconds(1)));
    assertThat(toTimestamp("1", "SS"), is(DAY_1_CE.plusSeconds(1)));
    assertThat(toTimestamp("57", "SS"), is(DAY_1_CE.plusSeconds(57)));

    try {
      ZonedDateTime x = toTimestamp("72", "SS");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Parsed value outside of valid range"));
    }

    try {
      ZonedDateTime x = toTimestamp("abc", "SS");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Unable to parse value"));
    }
  }

  @Test void testToTimestampMS() throws Exception {
    assertThat(toTimestamp("001", "MS"), is(DAY_1_CE.plusNanos(1_000_000)));
    assertThat(toTimestamp("1", "MS"), is(DAY_1_CE.plusNanos(1_000_000)));
    assertThat(toTimestamp("999", "MS"), is(DAY_1_CE.plusNanos(999_000_000)));

    try {
      ZonedDateTime x = toTimestamp("9999", "MS");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Parsed value outside of valid range"));
    }

    try {
      ZonedDateTime x = toTimestamp("abc", "MS");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Unable to parse value"));
    }
  }

  @Test void testToTimestampUS() throws Exception {
    assertThat(toTimestamp("001", "US"), is(DAY_1_CE.plusNanos(1_000)));
    assertThat(toTimestamp("1", "US"), is(DAY_1_CE.plusNanos(1_000)));
    assertThat(toTimestamp("999", "US"), is(DAY_1_CE.plusNanos(999_000)));

    try {
      ZonedDateTime x = toTimestamp("9999999", "US");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Parsed value outside of valid range"));
    }

    try {
      ZonedDateTime x = toTimestamp("abc", "US");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Unable to parse value"));
    }
  }

  @Test void testToTimestampFF1() throws Exception {
    assertThat(toTimestamp("1", "FF1"), is(DAY_1_CE.plusNanos(100_000_000)));
    assertThat(toTimestamp("9", "FF1"), is(DAY_1_CE.plusNanos(900_000_000)));

    try {
      ZonedDateTime x = toTimestamp("72", "FF1");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Parsed value outside of valid range"));
    }

    try {
      ZonedDateTime x = toTimestamp("abc", "FF1");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Unable to parse value"));
    }
  }

  @Test void testToTimestampFF2() throws Exception {
    assertThat(toTimestamp("01", "FF2"), is(DAY_1_CE.plusNanos(10_000_000)));
    assertThat(toTimestamp("1", "FF2"), is(DAY_1_CE.plusNanos(10_000_000)));
    assertThat(toTimestamp("97", "FF2"), is(DAY_1_CE.plusNanos(970_000_000)));

    try {
      ZonedDateTime x = toTimestamp("999", "FF2");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Parsed value outside of valid range"));
    }

    try {
      ZonedDateTime x = toTimestamp("abc", "FF2");
      fail("expected error, got " + x);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Unable to parse value"));
    }
  }

  @Test void testToTimestampFF3() throws Exception {
    assertThat(toTimestamp("001", "FF3"), is(DAY_1_CE.plusNanos(1_000_000)));
    assertThat(toTimestamp("1", "FF3"), is(DAY_1_CE.plusNanos(1_000_000)));
    assertThat(toTimestamp("976", "FF3"), is(DAY_1_CE.plusNanos(976_000_000)));
  }

  @Test void testToTimestampFF4() throws Exception {
    assertThat(toTimestamp("0001", "FF4"), is(DAY_1_CE.plusNanos(100_000)));
    assertThat(toTimestamp("1", "FF4"), is(DAY_1_CE.plusNanos(100_000)));
    assertThat(toTimestamp("9762", "FF4"), is(DAY_1_CE.plusNanos(976_200_000)));
  }

  @Test void testToTimestampFF5() throws Exception {
    assertThat(toTimestamp("00001", "FF5"), is(DAY_1_CE.plusNanos(10_000)));
    assertThat(toTimestamp("1", "FF5"), is(DAY_1_CE.plusNanos(10_000)));
    assertThat(toTimestamp("97621", "FF5"), is(DAY_1_CE.plusNanos(976_210_000)));
  }

  @Test void testToTimestampFF6() throws Exception {
    assertThat(toTimestamp("000001", "FF6"), is(DAY_1_CE.plusNanos(1_000)));
    assertThat(toTimestamp("1", "FF6"), is(DAY_1_CE.plusNanos(1_000)));
    assertThat(toTimestamp("976214", "FF6"), is(DAY_1_CE.plusNanos(976_214_000)));
  }

  @Test void testToTimestampAMPM() throws Exception {
    assertThat(toTimestamp("03AM", "HH12AM"), is(DAY_1_CE.plusHours(3)));
    assertThat(toTimestamp("03AM", "HH12PM"), is(DAY_1_CE.plusHours(3)));
    assertThat(toTimestamp("03PM", "HH12AM"), is(DAY_1_CE.plusHours(15)));
    assertThat(toTimestamp("03PM", "HH12PM"), is(DAY_1_CE.plusHours(15)));
    assertThat(toTimestamp("03A.M.", "HH12A.M."), is(DAY_1_CE.plusHours(3)));
    assertThat(toTimestamp("03A.M.", "HH12P.M."), is(DAY_1_CE.plusHours(3)));
    assertThat(toTimestamp("03P.M.", "HH12A.M."), is(DAY_1_CE.plusHours(15)));
    assertThat(toTimestamp("03P.M.", "HH12P.M."), is(DAY_1_CE.plusHours(15)));
    assertThat(toTimestamp("03am", "HH12am"), is(DAY_1_CE.plusHours(3)));
    assertThat(toTimestamp("03am", "HH12pm"), is(DAY_1_CE.plusHours(3)));
    assertThat(toTimestamp("03pm", "HH12am"), is(DAY_1_CE.plusHours(15)));
    assertThat(toTimestamp("03pm", "HH12pm"), is(DAY_1_CE.plusHours(15)));
    assertThat(toTimestamp("03a.m.", "HH12a.m."), is(DAY_1_CE.plusHours(3)));
    assertThat(toTimestamp("03a.m.", "HH12p.m."), is(DAY_1_CE.plusHours(3)));
    assertThat(toTimestamp("03p.m.", "HH12a.m."), is(DAY_1_CE.plusHours(15)));
    assertThat(toTimestamp("03p.m.", "HH12p.m."), is(DAY_1_CE.plusHours(15)));
  }

  @Test void testToTimestampYYYYWithCommas() throws Exception {
    assertThat(toTimestamp("0,001", "Y,YYY"), is(DAY_1_CE));
    assertThat(toTimestamp("2,024", "Y,YYY"), is(JAN_1_2024));
  }

  @Test void testToTimestampYYYY() throws Exception {
    assertThat(toTimestamp("0001", "YYYY"), is(DAY_1_CE));
    assertThat(toTimestamp("1", "YYYY"), is(DAY_1_CE));
    assertThat(toTimestamp("2024", "YYYY"), is(JAN_1_2024));
  }

  @Test void testToTimestampYYY() throws Exception {
    assertThat(toTimestamp("001", "YYY"), is(JAN_1_2001));
    assertThat(toTimestamp("1", "YYY"), is(JAN_1_2001));
    assertThat(toTimestamp("987", "YYY"),
        is(createDateTime(1987, 1, 1, 0, 0, 0, 0)));
  }

  @Test void testToTimestampYY() throws Exception {
    assertThat(toTimestamp("01", "YY"), is(JAN_1_2001));
    assertThat(toTimestamp("1", "YY"), is(JAN_1_2001));
    assertThat(toTimestamp("24", "YY"), is(JAN_1_2024));
  }

  @Test void testToTimestampY() throws Exception {
    assertThat(toTimestamp("1", "Y"), is(JAN_1_2001));
    assertThat(toTimestamp("4", "Y"), is(JAN_1_2001.plusYears(3)));
  }

  @Test void testToTimestampIYYY() throws Exception {
    assertThat(toTimestamp("0001", "IYYY"), is(DAY_1_CE));
    assertThat(toTimestamp("1", "IYYY"), is(DAY_1_CE));
    assertThat(toTimestamp("2024", "IYYY"), is(JAN_1_2024));
  }

  @Test void testToTimestampIYY() throws Exception {
    assertThat(toTimestamp("001", "IYY"), is(JAN_1_2001));
    assertThat(toTimestamp("1", "IYY"), is(JAN_1_2001));
    assertThat(toTimestamp("987", "IYY"),
        is(createDateTime(1987, 1, 1, 0, 0, 0, 0)));
  }

  @Test void testToTimestampIY() throws Exception {
    assertThat(toTimestamp("01", "IY"), is(JAN_1_2001));
    assertThat(toTimestamp("1", "IY"), is(JAN_1_2001));
    assertThat(toTimestamp("24", "IY"), is(JAN_1_2024));
  }

  @Test void testToTimestampI() throws Exception {
    assertThat(toTimestamp("1", "I"), is(JAN_1_2001));
    assertThat(toTimestamp("1", "I"), is(JAN_1_2001));
    assertThat(toTimestamp("4", "I"), is(JAN_1_2001.plusYears(3)));
  }

  @Test void testToTimestampBCAD() throws Exception {
    assertThat(toTimestamp("1920BC", "YYYYBC").get(ChronoField.ERA), is(0));
    assertThat(toTimestamp("1920BC", "YYYYAD").get(ChronoField.ERA), is(0));
    assertThat(toTimestamp("1920AD", "YYYYBC").get(ChronoField.ERA), is(1));
    assertThat(toTimestamp("1920AD", "YYYYAD").get(ChronoField.ERA), is(1));
    assertThat(toTimestamp("1920B.C.", "YYYYB.C.").get(ChronoField.ERA), is(0));
    assertThat(toTimestamp("1920B.C.", "YYYYA.D.").get(ChronoField.ERA), is(0));
    assertThat(toTimestamp("1920A.D.", "YYYYB.C.").get(ChronoField.ERA), is(1));
    assertThat(toTimestamp("1920A.D.", "YYYYA.D.").get(ChronoField.ERA), is(1));
    assertThat(toTimestamp("1920bc", "YYYYbc").get(ChronoField.ERA), is(0));
    assertThat(toTimestamp("1920bc", "YYYYad").get(ChronoField.ERA), is(0));
    assertThat(toTimestamp("1920ad", "YYYYbc").get(ChronoField.ERA), is(1));
    assertThat(toTimestamp("1920ad", "YYYYad").get(ChronoField.ERA), is(1));
    assertThat(toTimestamp("1920b.c.", "YYYYb.c.").get(ChronoField.ERA), is(0));
    assertThat(toTimestamp("1920b.c.", "YYYYa.d.").get(ChronoField.ERA), is(0));
    assertThat(toTimestamp("1920a.d.", "YYYYb.c.").get(ChronoField.ERA), is(1));
    assertThat(toTimestamp("1920a.d.", "YYYYa.d.").get(ChronoField.ERA), is(1));
  }

  @Test void testToTimestampMonthUpperCase() throws Exception {
    assertThat(toTimestamp("JANUARY", "MONTH"), is(DAY_1_CE));
    assertThat(toTimestamp("MARCH", "MONTH"), is(DAY_1_CE.plusMonths(2)));
    assertThat(toTimestamp("NOVEMBER", "MONTH"), is(DAY_1_CE.plusMonths(10)));
  }

  @Test void testToTimestampMonthCapitalized() throws Exception {
    assertThat(toTimestamp("January", "Month"), is(DAY_1_CE));
    assertThat(toTimestamp("March", "Month"), is(DAY_1_CE.plusMonths(2)));
    assertThat(toTimestamp("November", "Month"), is(DAY_1_CE.plusMonths(10)));
  }

  @Test void testToTimestampMonthLowerCase() throws Exception {
    assertThat(toTimestamp("january", "month"), is(DAY_1_CE));
    assertThat(toTimestamp("march", "month"), is(DAY_1_CE.plusMonths(2)));
    assertThat(toTimestamp("november", "month"), is(DAY_1_CE.plusMonths(10)));
  }

  @Test void testToTimestampMonUpperCase() throws Exception {
    assertThat(toTimestamp("JAN", "MON"), is(DAY_1_CE));
    assertThat(toTimestamp("MAR", "MON"), is(DAY_1_CE.plusMonths(2)));
    assertThat(toTimestamp("NOV", "MON"), is(DAY_1_CE.plusMonths(10)));
  }

  @Test void testToTimestampMonCapitalized() throws Exception {
    assertThat(toTimestamp("Jan", "Mon"), is(DAY_1_CE));
    assertThat(toTimestamp("Mar", "Mon"), is(DAY_1_CE.plusMonths(2)));
    assertThat(toTimestamp("Nov", "Mon"), is(DAY_1_CE.plusMonths(10)));
  }

  @Test void testToTimestampMonLowerCase() throws Exception {
    assertThat(toTimestamp("jan", "mon"), is(DAY_1_CE));
    assertThat(toTimestamp("mar", "mon"), is(DAY_1_CE.plusMonths(2)));
    assertThat(toTimestamp("nov", "mon"), is(DAY_1_CE.plusMonths(10)));
  }

  @Test void testToTimestampMM() throws Exception {
    assertThat(toTimestamp("01", "MM"), is(DAY_1_CE));
    assertThat(toTimestamp("1", "MM"), is(DAY_1_CE));
    assertThat(toTimestamp("11", "MM"), is(DAY_1_CE.plusMonths(10)));
  }

  @Test void testToTimestampDayUpperCase() throws Exception {
    assertThat(toTimestamp("1982 23 MONDAY", "IYYY IW DAY"),
        is(createDateTime(1982, 6, 7, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 THURSDAY", "IYYY IW DAY"),
        is(createDateTime(1982, 6, 10, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 FRIDAY", "IYYY IW DAY"),
        is(createDateTime(1982, 6, 11, 0, 0, 0, 0)));
  }

  @Test void testToTimestampDayCapitalized() throws Exception {
    assertThat(toTimestamp("1982 23 Monday", "IYYY IW Day"),
        is(createDateTime(1982, 6, 7, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 Thursday", "IYYY IW Day"),
        is(createDateTime(1982, 6, 10, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 Friday", "IYYY IW Day"),
        is(createDateTime(1982, 6, 11, 0, 0, 0, 0)));
  }

  @Test void testToTimestampDayLowerCase() throws Exception {
    assertThat(toTimestamp("1982 23 monday", "IYYY IW day"),
        is(createDateTime(1982, 6, 7, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 thursday", "IYYY IW day"),
        is(createDateTime(1982, 6, 10, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 friday", "IYYY IW day"),
        is(createDateTime(1982, 6, 11, 0, 0, 0, 0)));
  }

  @Test void testToTimestampDyUpperCase() throws Exception {
    assertThat(toTimestamp("1982 23 MON", "IYYY IW DY"),
        is(createDateTime(1982, 6, 7, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 THU", "IYYY IW DY"),
        is(createDateTime(1982, 6, 10, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 FRI", "IYYY IW DY"),
        is(createDateTime(1982, 6, 11, 0, 0, 0, 0)));
  }

  @Test void testToTimestampDyCapitalized() throws Exception {
    assertThat(toTimestamp("1982 23 Mon", "IYYY IW Dy"),
        is(createDateTime(1982, 6, 7, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 Thu", "IYYY IW Dy"),
        is(createDateTime(1982, 6, 10, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 Fri", "IYYY IW Dy"),
        is(createDateTime(1982, 6, 11, 0, 0, 0, 0)));
  }

  @Test void testToTimestampDyLowerCase() throws Exception {
    assertThat(toTimestamp("1982 23 mon", "IYYY IW dy"),
        is(createDateTime(1982, 6, 7, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 thu", "IYYY IW dy"),
        is(createDateTime(1982, 6, 10, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 fri", "IYYY IW dy"),
        is(createDateTime(1982, 6, 11, 0, 0, 0, 0)));
  }

  @Test void testToTimestampDDD() throws Exception {
    assertThat(toTimestamp("2024 001", "YYYY DDD"), is(JAN_1_2024));
    assertThat(toTimestamp("2024 1", "YYYY DDD"), is(JAN_1_2024));
    assertThat(toTimestamp("2024 137", "YYYY DDD"),
        is(createDateTime(2024, 5, 16, 0, 0, 0, 0)));
  }

  @Test void testToTimestampDD() throws Exception {
    assertThat(toTimestamp("01", "DD"), is(DAY_1_CE));
    assertThat(toTimestamp("1", "DD"), is(DAY_1_CE));
    assertThat(toTimestamp("23", "DD"), is(DAY_1_CE.plusDays(22)));
  }

  @Test void testToTimestampIDDD() throws Exception {
    assertThat(toTimestamp("2020 001", "IYYY IDDD"),
        is(createDateTime(2019, 12, 30, 0, 0, 0, 0)));
    assertThat(toTimestamp("2020 1", "IYYY IDDD"),
        is(createDateTime(2019, 12, 30, 0, 0, 0, 0)));
    assertThat(toTimestamp("2020 137", "IYYY IDDD"),
        is(createDateTime(2020, 5, 14, 0, 0, 0, 0)));
  }

  @Test void testToTimestampID() throws Exception {
    assertThat(toTimestamp("1982 23 1", "IYYY IW ID"),
        is(createDateTime(1982, 6, 7, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 4", "IYYY IW ID"),
        is(createDateTime(1982, 6, 10, 0, 0, 0, 0)));
    assertThat(toTimestamp("1982 23 5", "IYYY IW ID"),
        is(createDateTime(1982, 6, 11, 0, 0, 0, 0)));
  }

  @Test void testToTimestampW() throws Exception {
    assertThat(toTimestamp("2024 1 1", "YYYY MM W"), is(JAN_1_2024));
    assertThat(toTimestamp("2024 4 2", "YYYY MM W"),
        is(createDateTime(2024, 4, 8, 0, 0, 0, 0)));
    assertThat(toTimestamp("2024 11 4", "YYYY MM W"),
        is(createDateTime(2024, 11, 22, 0, 0, 0, 0)));
  }

  @Test void testToTimestampWW() throws Exception {
    assertThat(toTimestamp("2024 01", "YYYY WW"), is(JAN_1_2024));
    assertThat(toTimestamp("2024 1", "YYYY WW"), is(JAN_1_2024));
    assertThat(toTimestamp("2024 51", "YYYY WW"),
        is(createDateTime(2024, 12, 16, 0, 0, 0, 0)));
  }

  @Test void testToTimestampIW() throws Exception {
    assertThat(toTimestamp("2020 01", "IYYY IW"),
        is(createDateTime(2019, 12, 30, 0, 0, 0, 0)));
    assertThat(toTimestamp("2020 1", "IYYY IW"),
        is(createDateTime(2019, 12, 30, 0, 0, 0, 0)));
    assertThat(toTimestamp("2020 51", "IYYY IW"),
        is(createDateTime(2020, 12, 14, 0, 0, 0, 0)));
  }

  @Test void testToTimestampCC() throws Exception {
    assertThat(toTimestamp("21", "CC"), is(JAN_1_2001));
    assertThat(toTimestamp("16", "CC"), is(createDateTime(1501, 1, 1, 0, 0, 0, 0)));
    assertThat(toTimestamp("1", "CC"), is(DAY_1_CE));
  }

  @Test void testToTimestampJ() throws Exception {
    assertThat(toTimestamp("2460311", "J"), is(JAN_1_2024));
    assertThat(toTimestamp("2445897", "J"),
        is(createDateTime(1984, 7, 15, 0, 0, 0, 0)));
    assertThat(toTimestamp("1806606", "J"),
        is(createDateTime(234, 3, 21, 0, 0, 0, 0)));
  }

  @Test void testToTimestampRMUpperCase() throws Exception {
    assertThat(toTimestamp("I", "RM"), is(DAY_1_CE));
    assertThat(toTimestamp("IV", "RM"), is(DAY_1_CE.plusMonths(3)));
    assertThat(toTimestamp("IX", "RM"), is(DAY_1_CE.plusMonths(8)));
  }

  @Test void testToTimestampRMLowerCase() throws Exception {
    assertThat(toTimestamp("i", "rm"), is(DAY_1_CE));
    assertThat(toTimestamp("iv", "rm"), is(DAY_1_CE.plusMonths(3)));
    assertThat(toTimestamp("ix", "rm"), is(DAY_1_CE.plusMonths(8)));
  }

  @Test void testToTimestampDateValidFormats() throws Exception {
    assertThat(toTimestamp("2024-04-17", "YYYY-MM-DD"), is(APR_17_2024));
    assertThat(toTimestamp("2,024-04-17", "Y,YYY-MM-DD"), is(APR_17_2024));
    assertThat(toTimestamp("24-04-17", "YYY-MM-DD"), is(APR_17_2024));
    assertThat(toTimestamp("24-04-17", "YY-MM-DD"), is(APR_17_2024));
    assertThat(toTimestamp("2124-04-17", "CCYY-MM-DD"), is(APR_17_2024));
    assertThat(toTimestamp("20240417", "YYYYMMDD"), is(APR_17_2024));
    assertThat(toTimestamp("2,0240417", "Y,YYYMMDD"), is(APR_17_2024));
    assertThat(toTimestamp("2024-16-3", "IYYY-IW-ID"), is(APR_17_2024));
    assertThat(toTimestamp("2024-16 Wednesday", "IYYY-IW Day"), is(APR_17_2024));
    assertThat(toTimestamp("2024-108", "IYYY-IDDD"), is(APR_17_2024));
    assertThat(toTimestamp("April 17, 2024", "Month DD, YYYY"), is(APR_17_2024));
    assertThat(toTimestamp("IV 17, 2024", "RM DD, YYYY"), is(APR_17_2024));
    assertThat(toTimestamp("APR 17, 2024", "MON DD, YYYY"), is(APR_17_2024));
    assertThat(toTimestamp("2024-16", "YYYY-WW"),
        is(createDateTime(2024, 4, 15, 0, 0, 0, 0)));
    assertThat(toTimestamp("2024-108", "YYYY-DDD"), is(APR_17_2024));
    assertThat(toTimestamp("0000-01-01", "YYYY-MM-DD"), is(DAY_1_CE));
  }

  @Test void testToTimestampWithTimezone() throws Exception {
    final ZoneId utcZone = ZoneId.of("UTC");
    final CompiledDateTimeFormat dateTimeFormat =
        PostgresqlDateTimeFormatter.compilePattern("YYYY-MM-DD HH24:MI:SSTZH:TZM");
    assertThat(
        dateTimeFormat.parseDateTime("2024-04-17 00:00:00-07:00", utcZone,
            Locale.US),
        is(APR_17_2024.plusHours(7).withZoneSameLocal(utcZone)));
  }

  @Test void testToTimestampReuseFormat() {
    final CompiledDateTimeFormat compiledFormat =
        PostgresqlDateTimeFormatter.compilePattern("YYYY-MM-DD HH24:MI:SS.MS");
    final String expected1 = "2019-03-07 15:46:23.521";
    final String expected2 = "1983-11-29 04:21:16.045";
    final String expected3 = "2024-09-24 14:53:37.891";
    final ZonedDateTime timestamp1 =
        createDateTime(2019, 3, 7, 15, 46, 23, 521000000);
    final ZonedDateTime timestamp2 =
        createDateTime(1983, 11, 29, 4, 21, 16, 45000000);
    final ZonedDateTime timestamp3 =
        createDateTime(2024, 9, 24, 14, 53, 37, 891000000);
    assertThat(compiledFormat.formatDateTime(timestamp1, Locale.US),
        is(expected1));
    assertThat(compiledFormat.formatDateTime(timestamp2, Locale.US),
        is(expected2));
    assertThat(compiledFormat.formatDateTime(timestamp3, Locale.US),
        is(expected3));
  }

  protected static ZonedDateTime createDateTime(int year, int month, int dayOfMonth, int hour,
      int minute, int seconds, int nanoseconds) {
    return ZonedDateTime.of(
        LocalDateTime.of(year, month, dayOfMonth, hour, minute, seconds, nanoseconds),
        TIME_ZONE);
  }
}
