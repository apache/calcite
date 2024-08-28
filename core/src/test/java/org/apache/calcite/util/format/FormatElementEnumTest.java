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

  @Test void testDAY() {
    assertFormatElement(FormatElementEnum.DAY, "2014-09-30T10:00:00Z", "TUESDAY");
  }
  @Test void testDay() {
    assertFormatElement(FormatElementEnum.Day, "2014-09-30T10:00:00Z", "Tuesday");
  }
  @Test void testday() {
    assertFormatElement(FormatElementEnum.day, "2014-09-30T10:00:00Z", "tuesday");
  }

  @Test void testD() {
    assertFormatElement(FormatElementEnum.D, "2014-09-30T10:00:00Z", "3");
  }

  @Test void testD0() {
    assertFormatElement(FormatElementEnum.D0, "2014-09-30T10:00:00Z", "2");
  }

  @Test void testDS() {
    assertFormatElement(FormatElementEnum.DS, "2014-09-01T10:00:00Z", "1st");
    assertFormatElement(FormatElementEnum.DS, "2014-09-02T10:00:00Z", "2nd");
    assertFormatElement(FormatElementEnum.DS, "2014-09-03T10:00:00Z", "3rd");
    assertFormatElement(FormatElementEnum.DS, "2014-09-04T10:00:00Z", "4th");
    assertFormatElement(FormatElementEnum.DS, "2014-09-30T10:00:00Z", "30th");
  }

  @Test void testMCS() {
    assertFormatElement(FormatElementEnum.MCS, "2014-09-01T10:00:00Z", "000000");
    assertFormatElement(FormatElementEnum.MCS, "2014-09-01T10:00:13.123456Z", "123000");
    assertFormatElement(FormatElementEnum.MCS, "2014-09-01T10:00:13.123000Z", "123000");
  }

  @Test void testDD() {
    assertFormatElement(FormatElementEnum.DD, "2014-09-30T10:00:00Z", "30");
  }

  @Test void testDDD() {
    assertFormatElement(FormatElementEnum.DDD, "2014-09-30T10:00:00Z", "273");
  }

  @Test void testDY() {
    assertFormatElement(FormatElementEnum.DY, "2014-09-30T10:00:00Z", "TUE");
  }
  @Test void testDy() {
    assertFormatElement(FormatElementEnum.Dy, "2014-09-30T10:00:00Z", "Tue");
  }
  @Test void testdy() {
    assertFormatElement(FormatElementEnum.dy, "2014-09-30T10:00:00Z", "tue");
  }

  @Test void testFF1() {
    assertFormatElement(FormatElementEnum.FF1, "2014-09-30T10:00:00.123456Z", "1");
  }

  @Test void testFF2() {
    assertFormatElement(FormatElementEnum.FF2, "2014-09-30T10:00:00.123456Z", "12");
  }

  @Test void testFF3() {
    assertFormatElement(FormatElementEnum.FF3, "2014-09-30T10:00:00.123456Z", "123");
  }

  @Test void testFF4() {
    assertFormatElement(FormatElementEnum.FF4, "2014-09-30T10:00:00.123456Z", "1230");
  }

  @Test void testFF5() {
    assertFormatElement(FormatElementEnum.FF5, "2014-09-30T10:00:00.123456Z", "12300");
  }

  @Test void testFF6() {
    assertFormatElement(FormatElementEnum.FF6, "2014-09-30T10:00:00.123456Z", "123000");
  }
  @Test void testFF7() {
    assertFormatElement(FormatElementEnum.FF7, "2014-09-30T10:00:00.123456Z", "1230000");
  }
  @Test void testFF8() {
    assertFormatElement(FormatElementEnum.FF8, "2014-09-30T10:00:00.123456Z", "12300000");
  }
  @Test void testFF9() {
    assertFormatElement(FormatElementEnum.FF9, "2014-09-30T10:00:00.123456Z", "123000000");
  }

  @Test void testIW() {
    assertFormatElement(FormatElementEnum.IW, "2014-09-30T10:00:00Z", "40");
  }

  @Test void testMM() {
    assertFormatElement(FormatElementEnum.MM, "2014-09-30T10:00:00Z", "09");
  }

  @Test void testMON() {
    assertFormatElement(FormatElementEnum.MON, "2014-09-30T10:00:00Z", "SEP");
  }
  @Test void testMon() {
    assertFormatElement(FormatElementEnum.Mon, "2014-09-30T10:00:00Z", "Sep");
  }
  @Test void testmon() {
    assertFormatElement(FormatElementEnum.mon, "2014-09-30T10:00:00Z", "sep");
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

  @Test void testV() {
    assertFormatElement(FormatElementEnum.V, "1997-01-01T10:23:00Z", "52");
  }

  @Test void testv() {
    assertFormatElement(FormatElementEnum.v, "1997-01-01T10:23:00Z", "01");
  }

  @Test void testWW1() {
    assertFormatElement(FormatElementEnum.WW1, "1997-01-01T10:23:00Z", "00");
  }

  @Test void testWW2() {
    assertFormatElement(FormatElementEnum.WW2, "1997-01-01T10:23:00Z", "01");
  }

  @Test void testWFY() {
    assertFormatElement(FormatElementEnum.WFY, "1997-01-04T10:23:00Z", "1996");
  }

  @Test void testWFY0() {
    assertFormatElement(FormatElementEnum.WFY0, "1997-01-04T10:23:00Z", "1997");
  }

  private void assertFormatElement(FormatElementEnum formatElement, String date, String expected) {
    StringBuilder ts = new StringBuilder();
    formatElement.format(ts, Date.from(Instant.parse(date)));
    assertThat(ts, hasToString(expected));
  }
}
