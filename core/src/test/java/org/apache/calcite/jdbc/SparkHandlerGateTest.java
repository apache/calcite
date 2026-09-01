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
package org.apache.calcite.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that the Spark engine requires the operator-level opt-in
 * {@code calcite.enable.spark}.
 *
 * <p>These tests run without {@code -Dcalcite.enable.spark=true} (the
 * default), so the gate is closed. The Spark module's own tests run with
 * the opt-in set (see {@code spark/build.gradle.kts}).
 */
public class SparkHandlerGateTest {
  @Test void testSparkHandlerRequiresOperatorOptIn() {
    SecurityException e =
        assertThrows(SecurityException.class, () ->
            CalcitePrepare.Dummy.getSparkHandler(true));
    assertThat(e.getMessage(), containsString("calcite.enable.spark"));
  }

  @Test void testSparkConnectionPropertyGatedBeforePrepare()
      throws Exception {
    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:spark=true");
         Statement statement = connection.createStatement()) {
      Throwable e =
          assertThrows(Throwable.class, () ->
              statement.executeQuery("values (1, 2, 3, 4, 5, 6)"));
      while (e != null && !(e instanceof SecurityException)) {
        e = e.getCause();
      }
      assertThat("expected a SecurityException in the chain", e,
          notNullValue());
      assertThat(e.getMessage(), containsString("calcite.enable.spark"));
    }
  }

  @Test void testTrivialHandlerStillAvailableWithoutOptIn() {
    assertThat(CalcitePrepare.Dummy.getSparkHandler(false).enabled(),
        is(false));
  }
}
