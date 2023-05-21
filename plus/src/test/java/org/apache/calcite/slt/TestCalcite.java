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
package org.apache.calcite.slt;

import org.apache.calcite.slt.executors.CalciteExecutor;

import net.hydromatic.sqllogictest.OptionsParser;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Tests using sql-logic-test suite.
 */
public class TestCalcite {
  private static final String UTF_8 = StandardCharsets.UTF_8.name();

  /** Test that runs all scripts with no executor. */
  @Test void testRunNoExecutor() throws IOException {
    Output res = launchSqlLogicTest("-e", "none", "select1.test");
    String[] outLines = res.out.split("\n");
    assertThat(res.err, is(""));
    assertThat(res.out, outLines.length, is(4));
    assertThat(res.out, outLines[1], is("Passed: 0"));
    assertThat(res.out, outLines[2], is("Failed: 0"));
    assertThat(res.out, outLines[3], is("Ignored: 1,000"));
  }

  private static Output launchSqlLogicTest(String... args) throws IOException {
    try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
         ByteArrayOutputStream berr = new ByteArrayOutputStream()) {
      final PrintStream out = new PrintStream(bout, false, UTF_8);
      final PrintStream err = new PrintStream(berr, false, UTF_8);
      OptionsParser options = new OptionsParser(false, out, err);
      CalciteExecutor.register(options);
      net.hydromatic.sqllogictest.Main.execute(options, args);
      out.flush();
      err.flush();
      return new Output(bout.toString(UTF_8), berr.toString(UTF_8));
    }
  }

  /** Test that runs one script against Calcite + HSQLDB. */
  @Test void testRunCalcite() throws IOException {
    Output res = launchSqlLogicTest("-x", "-v", "-e", "calcite", "select1.test");
    // This test uncovers Calcite bugs.
    String[] outLines = res.out.split("\n");
    assertThat("Bugs exist", outLines.length > 6);
    // Always 1 failure, since this test stops at the first failure
    assertThat(res.out, outLines[6], is("Failed: 1"));
  }

  /**
   * Wrapper around the output of the sql-logic-test CLI.
   */
  private static class Output {
    final String out;
    final String err;

    Output(String out, String err) {
      this.out = out;
      this.err = err;
    }
  }
}
