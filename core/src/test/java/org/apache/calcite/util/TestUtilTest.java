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
  public void current() {
    // shouldn't throw any exceptions (for current JDK)
    assertTrue(TestUtil.getJavaMajorVersion() > 6);
  }

  @Test
  public void majorVersionFromString() {
    assertEquals(4, TestUtil.majorVersionFromString("1.4.2_03"));
    assertEquals(5, TestUtil.majorVersionFromString("1.5.0_16"));
    assertEquals(6, TestUtil.majorVersionFromString("1.6.0_22"));
    assertEquals(7, TestUtil.majorVersionFromString("1.7.0_65-b20"));
    assertEquals(8, TestUtil.majorVersionFromString("1.8.0_72-internal"));
    assertEquals(8, TestUtil.majorVersionFromString("1.8.0_151"));
    assertEquals(8, TestUtil.majorVersionFromString("1.8.0_141"));
    assertEquals(9, TestUtil.majorVersionFromString("1.9.0_20-b62"));
    assertEquals(9, TestUtil.majorVersionFromString("1.9.0-ea-b19"));
    assertEquals(9, TestUtil.majorVersionFromString("9"));
    assertEquals(9, TestUtil.majorVersionFromString("9.0"));
    assertEquals(9, TestUtil.majorVersionFromString("9.0.1"));
    assertEquals(9, TestUtil.majorVersionFromString("9-ea"));
    assertEquals(9, TestUtil.majorVersionFromString("9.0.1"));
    assertEquals(9, TestUtil.majorVersionFromString("9.1-ea"));
    assertEquals(9, TestUtil.majorVersionFromString("9.1.1-ea"));
    assertEquals(9, TestUtil.majorVersionFromString("9.1.1-ea+123"));
    assertEquals(10, TestUtil.majorVersionFromString("10"));
    assertEquals(10, TestUtil.majorVersionFromString("10+456"));
    assertEquals(10, TestUtil.majorVersionFromString("10-ea"));
    assertEquals(10, TestUtil.majorVersionFromString("10-ea42"));
    assertEquals(10, TestUtil.majorVersionFromString("10-ea+555"));
    assertEquals(10, TestUtil.majorVersionFromString("10-ea42+555"));
    assertEquals(10, TestUtil.majorVersionFromString("10.0"));
    assertEquals(10, TestUtil.majorVersionFromString("10.0.0"));
    assertEquals(10, TestUtil.majorVersionFromString("10.0.0.0.0"));
    assertEquals(10, TestUtil.majorVersionFromString("10.1.2.3.4.5.6.7.8"));
    assertEquals(10, TestUtil.majorVersionFromString("10.0.1"));
    assertEquals(10, TestUtil.majorVersionFromString("10.1.1-foo"));
    assertEquals(11, TestUtil.majorVersionFromString("11"));
    assertEquals(11, TestUtil.majorVersionFromString("11+111"));
    assertEquals(11, TestUtil.majorVersionFromString("11-ea"));
    assertEquals(11, TestUtil.majorVersionFromString("11.0"));
    assertEquals(12, TestUtil.majorVersionFromString("12.0"));
    assertEquals(20, TestUtil.majorVersionFromString("20.0"));
    assertEquals(42, TestUtil.majorVersionFromString("42"));
    assertEquals(100, TestUtil.majorVersionFromString("100"));
    assertEquals(100, TestUtil.majorVersionFromString("100.0"));
    assertEquals(1000, TestUtil.majorVersionFromString("1000"));
    assertEquals(2000, TestUtil.majorVersionFromString("2000"));
    assertEquals(205, TestUtil.majorVersionFromString("205.0"));
    assertEquals(2017, TestUtil.majorVersionFromString("2017"));
    assertEquals(2017, TestUtil.majorVersionFromString("2017.0"));
    assertEquals(2017, TestUtil.majorVersionFromString("2017.12"));
    assertEquals(2017, TestUtil.majorVersionFromString("2017.12-pre"));
    assertEquals(2017, TestUtil.majorVersionFromString("2017.12.31"));
  }

}

// End TestUtilTest.java
