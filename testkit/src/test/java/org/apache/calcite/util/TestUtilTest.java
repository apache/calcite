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
package org.apache.calcite.util;

import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static org.apache.calcite.util.TestUtil.roundGeom;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for TestUtil.
 */
class TestUtilTest {

  /** Tests {@link TestUtil#roundGeom}. */
  @Test void testRoundGeom() {
    assertThat(roundGeom("1", 3), is("1"));
    assertThat(roundGeom("1.28", 3), is("1.28"));
    assertThat(roundGeom("1.278", 3), is("1.278"));
    assertThat(roundGeom("1.2784", 3), is("1.278")); // < 5 rounds down
    assertThat(roundGeom("1.2785", 3), is("1.278")); // = 5 rounds down
    assertThat(roundGeom("1.27850001", 3), is("1.279")); // > 5 rounds up
    assertThat(roundGeom("1.27950001", 3), is("1.280"));
    assertThat(roundGeom("1.29950001", 3), is("1.300"));
    assertThat(roundGeom("19.99950001", 3), is("20.000"));
    assertThat(roundGeom("2 9.99950001", 3), is("2 10.000"));
    assertThat(roundGeom("23 9.99950001", 3), is("23 10.000"));
    assertThat(roundGeom("234 9.99950001", 3), is("234 10.000"));
    assertThat(roundGeom("POINT(-1.23456, 9.87654)", 3),
        is("POINT(-1.235, 9.877)"));
  }

  @Test void javaMajorVersionExceeds6() {
    // shouldn't throw any exceptions (for current JDK)
    int majorVersion = TestUtil.getJavaMajorVersion();
    assertTrue(majorVersion > 6,
        "current JavaMajorVersion == " + majorVersion + " is expected to exceed 6");
  }

  @Test void majorVersionFromString() {
    testJavaVersion(4, "1.4.2_03");
    testJavaVersion(5, "1.5.0_16");
    testJavaVersion(6, "1.6.0_22");
    testJavaVersion(7, "1.7.0_65-b20");
    testJavaVersion(8, "1.8.0_72-internal");
    testJavaVersion(8, "1.8.0_151");
    testJavaVersion(8, "1.8.0_141");
    testJavaVersion(9, "1.9.0_20-b62");
    testJavaVersion(9, "1.9.0-ea-b19");
    testJavaVersion(9, "9");
    testJavaVersion(9, "9.0");
    testJavaVersion(9, "9.0.1");
    testJavaVersion(9, "9-ea");
    testJavaVersion(9, "9.0.1");
    testJavaVersion(9, "9.1-ea");
    testJavaVersion(9, "9.1.1-ea");
    testJavaVersion(9, "9.1.1-ea+123");
    testJavaVersion(10, "10");
    testJavaVersion(10, "10+456");
    testJavaVersion(10, "10-ea");
    testJavaVersion(10, "10-ea42");
    testJavaVersion(10, "10-ea+555");
    testJavaVersion(10, "10-ea42+555");
    testJavaVersion(10, "10.0");
    testJavaVersion(10, "10.0.0");
    testJavaVersion(10, "10.0.0.0.0");
    testJavaVersion(10, "10.1.2.3.4.5.6.7.8");
    testJavaVersion(10, "10.0.1");
    testJavaVersion(10, "10.1.1-foo");
    testJavaVersion(11, "11");
    testJavaVersion(11, "11+111");
    testJavaVersion(11, "11-ea");
    testJavaVersion(11, "11.0");
    testJavaVersion(12, "12.0");
    testJavaVersion(20, "20.0");
    testJavaVersion(42, "42");
    testJavaVersion(100, "100");
    testJavaVersion(100, "100.0");
    testJavaVersion(1000, "1000");
    testJavaVersion(2000, "2000");
    testJavaVersion(205, "205.0");
    testJavaVersion(2017, "2017");
    testJavaVersion(2017, "2017.0");
    testJavaVersion(2017, "2017.12");
    testJavaVersion(2017, "2017.12-pre");
    testJavaVersion(2017, "2017.12.31");
  }

  private void testJavaVersion(int expectedMajorVersion, String versionString) {
    assertEquals(expectedMajorVersion,
        TestUtil.majorVersionFromString(versionString),
        versionString);
  }

  @Test void testGuavaMajorVersion() {
    int majorVersion = TestUtil.getGuavaMajorVersion();
    assertTrue(majorVersion >= 2,
        "current GuavaMajorVersion is " + majorVersion + "; should exceed 2");
  }

  /** Tests {@link TestUtil#correctRoundedFloat(String)}. */
  @Test void testCorrectRoundedFloat() {
    // unchanged; no '.'
    assertThat(TestUtil.correctRoundedFloat("1230000006"), is("1230000006"));
    assertThat(TestUtil.correctRoundedFloat("12.300000006"), is("12.3"));
    assertThat(TestUtil.correctRoundedFloat("53.742500000000014"),
        is("53.7425"));
    // unchanged; too few zeros
    assertThat(TestUtil.correctRoundedFloat("12.30006"), is("12.30006"));
    assertThat(TestUtil.correctRoundedFloat("12.300000"), is("12.3"));
    assertThat(TestUtil.correctRoundedFloat("-12.30000006"), is("-12.3"));
    assertThat(TestUtil.correctRoundedFloat("-12.349999991"), is("-12.35"));
    assertThat(TestUtil.correctRoundedFloat("-12.349999999"), is("-12.35"));
    assertThat(TestUtil.correctRoundedFloat("-12.3499999911"), is("-12.35"));
    // unchanged; too many non-nines at the end
    assertThat(TestUtil.correctRoundedFloat("-12.34999999118"),
        is("-12.34999999118"));
    // unchanged; too few nines
    assertThat(TestUtil.correctRoundedFloat("-12.349991"), is("-12.349991"));
    assertThat(TestUtil.correctRoundedFloat("95637.41489999992"),
        is("95637.4149"));
    assertThat(TestUtil.correctRoundedFloat("14181.569999999989"),
        is("14181.57"));
    // can't handle nines that start right after the point very well. oh well.
    assertThat(TestUtil.correctRoundedFloat("12.999999"), is("12."));
  }

  /** Tests {@link TestUtil#outOfOrderItems(List)}. */
  @Test void testOutOfOrderItems() {
    final List<String> list =
        new ArrayList<>(Arrays.asList("a", "g", "b", "c", "e", "d"));
    final SortedSet<String> distance = TestUtil.outOfOrderItems(list);
    assertThat(distance.toString(), is("[b, d]"));

    list.add("f");
    final SortedSet<String> distance2 = TestUtil.outOfOrderItems(list);
    assertThat(distance2.toString(), is("[b, d]"));

    list.add(1, "b");
    final SortedSet<String> distance3 = TestUtil.outOfOrderItems(list);
    assertThat(distance3.toString(), is("[b, d]"));

    list.add(1, "c");
    final SortedSet<String> distance4 = TestUtil.outOfOrderItems(list);
    assertThat(distance4.toString(), is("[b, d]"));

    list.add(0, "z");
    final SortedSet<String> distance5 = TestUtil.outOfOrderItems(list);
    assertThat(distance5.toString(), is("[a, b, d]"));
  }

  private long totalDistance(ImmutableMap<String, IntPair> map) {
    return map.entrySet().stream()
        .collect(Collectors.summarizingInt(e -> e.getValue().target)).getSum();
  }
}
