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
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.DAY.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("Tuesday"));
  }

  @Test void TestD(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.D.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("3"));
  }

  @Test void TestDD(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.DD.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("30"));
  }

  @Test void TestDDD(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.DDD.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("273"));
  }

  @Test void TestDY(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.DY.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("Tue"));
  }

  @Test void TestFF1(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.FF1.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("0"));
  }

  @Test void TestFF2(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.FF2.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("00"));
  }

  @Test void TestFF3(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.FF3.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("000"));
  }

  @Test void TestFF4(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.FF4.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("0000"));
  }

  @Test void TestFF5(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.FF5.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("00000"));
  }

  @Test void TestFF6(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.FF6.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("000000"));
  }

  @Test void TestHH12(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.HH12.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("06"));
  }

  @Test void TestHH24(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.HH24.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("18"));
  }

  @Test void TestIW(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.IW.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("40"));
  }

  @Test void TestM1(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.MI.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("00"));
  }

  @Test void TestMM(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.MM.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("09"));
  }

  @Test void TestMON(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.MON.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("Sep"));
  }

  @Test void TestMONTH(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.MONTH.format(ts, Date.from(Instant.parse("2014-06-30T10:00:00Z")));
    assertThat(ts, hasToString("Sep"));
  }

  @Test void TestPM(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.PM.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("PM"));
  }

  @Test void TestQ(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.Q.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("3"));
  }

  @Test void TestMS(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.MS.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("000"));
  }

  @Test void TestSS(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.SS.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("00"));
  }

  @Test void TestWW(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.WW.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("40"));
  }

  @Test void TestYY(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.YY.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("14"));
  }

  @Test void TestYYYY(){
    StringBuilder ts = new StringBuilder();
    FormatElementEnum.YYYY.format(ts, Date.from(Instant.parse("2014-09-30T10:00:00Z")));
    assertThat(ts, hasToString("2014"));
  }
}
