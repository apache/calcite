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
package org.apache.calcite.test;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link DiffRepository} out-of-order test case detection.
 *
 * <p>This test is in a separate class to avoid interfering with other
 * DiffRepository tests, since the test's reference XML file intentionally
 * contains out-of-order test cases.
 */
public class DiffRepositoryOutOfOrderTest {

  @Test void testOutOfOrderTestCases() {
    // testOutOfOrderTestCases is out-of-order in the XML (comes after testZ)
    DiffRepository r = DiffRepository.lookup(DiffRepositoryOutOfOrderTest.class);

    IllegalArgumentException thrown = null;
    try {
      r.assertEquals("data", "data", "data");
    } catch (IllegalArgumentException e) {
      thrown = e;
    }

    // Verify that the out-of-order error was thrown
    assertThat("Expected IllegalArgumentException for out-of-order test",
        thrown, notNullValue());
    assertThat("Error should mention out of order",
        thrown.getMessage(), containsString("out of order"));
  }
}
