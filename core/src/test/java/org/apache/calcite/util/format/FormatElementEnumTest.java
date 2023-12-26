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
package org.apache.calcite.util.format;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;

/**
 * Unit test for {@link FormatElementEnum}.
 */
class FormatElementEnumTest {
  @Test void testDay() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.DAY.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("Tuesday"));
  }

  @Test void testD() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.D.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("3"));
  }

  @Test void testDD() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.DD.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("30"));
  }

  @Test void testDDD() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.DDD.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("273"));
  }

  @Test void testDY() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.DY.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("Tue"));
  }

  @Test void testFF1() {
    final String date = "2014-09-30T10:00:00.123456Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.FF1.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("123"));
  }

  @Test void testFF2() {
    final String date = "2014-09-30T10:00:00.123456Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.FF2.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("123"));
  }

  @Test void testFF3() {
    final String date = "2014-09-30T10:00:00.123456Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.FF3.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("123"));
  }

  @Test void testFF4() {
    final String date = "2014-09-30T10:00:00.123456Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.FF4.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("0123"));
  }

  @Test void testFF5() {
    final String date = "2014-09-30T10:00:00.123456Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.FF5.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("00123"));
  }

  @Test void testFF6() {
    final String date = "2014-09-30T10:00:00.123456Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.FF6.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("000123"));
  }

  @Test void testIW() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.IW.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("40"));
  }

  @Test void testMM() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.MM.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("09"));
  }

  @Test void testMON() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.MON.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("Sep"));
  }

  @Test void testQ() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.Q.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("3"));
  }

  @Test void testMS() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.MS.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("000"));
  }

  @Test void testSS() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.SS.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("00"));
  }

  @Test void testWM() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.W.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("5"));
  }

  @Test void testWW() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.WW.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("40"));
  }

  @Test void testYY() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.YY.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("14"));
  }

  @Test void testYYYY() {
    final String date = "2014-09-30T10:00:00Z";
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.YYYY.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString("2014"));
  }
}
