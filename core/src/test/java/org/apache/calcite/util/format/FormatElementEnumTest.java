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

  @Test void testCC() {
    assertFormatElement(FormatElementEnum.CC, "2014-09-30T10:00:00Z", "21");
  }

  @Test void testDay() {
    assertFormatElement(FormatElementEnum.DAY, "2014-09-30T10:00:00Z", "Tuesday");
  }

  @Test void testD() {
    assertFormatElement(FormatElementEnum.D, "2014-09-30T10:00:00Z", "3");
  }

  @Test void testDD() {
    assertFormatElement(FormatElementEnum.DD, "2014-09-30T10:00:00Z", "30");
  }

  @Test void testDDD() {
    assertFormatElement(FormatElementEnum.DDD, "2014-09-30T10:00:00Z", "273");
  }

  @Test void testDY() {
    assertFormatElement(FormatElementEnum.DY, "2014-09-30T10:00:00Z", "Tue");
  }

  @Test void testFF1() {
    assertFormatElement(FormatElementEnum.FF1, "2014-09-30T10:00:00.123456Z", "123");
  }

  @Test void testFF2() {
    assertFormatElement(FormatElementEnum.FF2, "2014-09-30T10:00:00.123456Z", "123");
  }

  @Test void testFF3() {
    assertFormatElement(FormatElementEnum.FF3, "2014-09-30T10:00:00.123456Z", "123");
  }

  @Test void testFF4() {
    assertFormatElement(FormatElementEnum.FF4, "2014-09-30T10:00:00.123456Z", "0123");
  }

  @Test void testFF5() {
    assertFormatElement(FormatElementEnum.FF5, "2014-09-30T10:00:00.123456Z", "00123");
  }

  @Test void testFF6() {
    assertFormatElement(FormatElementEnum.FF6, "2014-09-30T10:00:00.123456Z", "000123");
  }

  @Test void testIW() {
    assertFormatElement(FormatElementEnum.IW, "2014-09-30T10:00:00Z", "40");
  }

  @Test void testMM() {
    assertFormatElement(FormatElementEnum.MM, "2014-09-30T10:00:00Z", "09");
  }

  @Test void testMON() {
    assertFormatElement(FormatElementEnum.MON, "2014-09-30T10:00:00Z", "Sep");
  }

  @Test void testQ() {
    assertFormatElement(FormatElementEnum.Q, "2014-09-30T10:00:00Z", "3");
  }

  @Test void testMS() {
    assertFormatElement(FormatElementEnum.MS, "2014-09-30T10:00:00Z", "000");
  }

  @Test void testSS() {
    assertFormatElement(FormatElementEnum.SS, "2014-09-30T10:00:00Z", "00");
  }

  @Test void testWM() {
    assertFormatElement(FormatElementEnum.W, "2014-09-30T10:00:00Z", "5");
  }

  @Test void testWW() {
    assertFormatElement(FormatElementEnum.WW, "2014-09-30T10:00:00Z", "40");
  }

  @Test void testYY() {
    assertFormatElement(FormatElementEnum.YY, "2014-09-30T10:00:00Z", "14");
  }

  @Test void testYYYY() {
    assertFormatElement(FormatElementEnum.YYYY, "2014-09-30T10:00:00Z", "2014");
  }

  private void assertFormatElement(FormatElementEnum formatElement, String date, String expected) {
    StringBuilder ts = new StringBuilder();
    formatElement.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString(expected));
  }
}
