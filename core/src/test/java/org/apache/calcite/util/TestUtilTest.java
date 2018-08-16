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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for TestUtil
 */
public class TestUtilTest {

  @Test
  public void javaMajorVersionExceeds6() {
    // shouldn't throw any exceptions (for current JDK)
    int majorVersion = TestUtil.getJavaMajorVersion();
    assertTrue("current JavaMajorVersion == " + majorVersion + " is expected to exceed 6",
        majorVersion > 6);
  }

  @Test
  public void majorVersionFromString() {
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
    assertEquals(versionString, expectedMajorVersion,
        TestUtil.majorVersionFromString(versionString));
  }

}

// End TestUtilTest.java
