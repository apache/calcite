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
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableSet;

import net.hydromatic.sqllogictest.OptionsParser;
import net.hydromatic.sqllogictest.TestStatistics;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Tests using sql-logic-test suite.
 *
 * <p>For each test file the number of failed tests is saved in a "golden" file.
 * These results are checked in as part of the `sltttestfailures.txt` resource file.
 * Currently, there are quite a few errors, so this tool does not track of the actual
 * errors that were encountered; we expect that, as bugs are fixed in Calcite,
 * the number of errors will shrink, and a more precise accounting method will be used.
 *
 * <p>The tests will fail if any test script generates
 * *more* errors than the number from the golden file.
 */
public class SqlLogicTests {
  private static final Logger LOGGER =
      CalciteTrace.getTestTracer(SqlLogicTests.class);

  /**
   * Short summary of the results of a test execution.
   */
  public static class TestSummary {
    /**
     * File containing tests.
     */
    final String file;
    /**
     * Number of tests that have failed.
     */
    final int failed;

    TestSummary(String file, int failed) {
      this.file = file;
      this.failed = failed;
    }

    /**
     * Parses a TestSummary from a string.
     * The inverse of 'toString'.
     *
     * @return The parsed TestSummary or null on failure.
     */
    public static TestSummary parse(String line) {
      String[] parts = line.split(":");
      if (parts.length != 2) {
        return null;
      }
      try {
        int failed = Integer.parseInt(parts[1]);
        return new TestSummary(parts[0], failed);
      } catch (NumberFormatException ex) {
        return null;
      }
    }

    @Override public String toString() {
      return this.file + ":" + this.failed;
    }

    /**
     * Check if the 'other' TestSummaries indicate a regressions
     * when compared to 'this'.
     *
     * @param other TestSummary to compare against.
     * @return 'true' if 'other' is a regression from 'this'.
     */
    public boolean isRegression(TestSummary other) {
      return other.failed > this.failed;
    }
  }

  /**
   * Summary for all tests executed.
   */
  public static class AllTestSummaries {
    /**
     * Map test summary name to test summary.
     */
    final Map<String, TestSummary> testResults;

    AllTestSummaries() {
      this.testResults = new HashMap<>();
    }

    void add(TestSummary summary) {
      this.testResults.put(summary.file, summary);
    }

    AllTestSummaries read(InputStream stream) {
      try (BufferedReader reader =
               new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
        reader.lines().forEach(line -> {
          TestSummary summary = TestSummary.parse(line);
          if (summary != null) {
            this.add(summary);
          } else {
            LOGGER.warn("Could not parse line " + line);
          }
        });
        return this;
      } catch (IOException ex) {
        // Wrapping the IOException makes it easier to use this method in the
        // initializer of a static variable.
        throw new RuntimeException(ex);
      }
    }

    boolean regression(TestSummary summary) {
      TestSummary original = this.testResults.get(summary.file);
      if (original == null) {
        LOGGER.warn("No historical data for test " + summary.file);
        return false;
      }
      if (original.isRegression(summary)) {
        LOGGER.error("Regression: " + original.file
            + " had " + original.failed + " failures, now has " + summary.failed);
        return true;
      }
      return false;
    }

    /**
     * Check if 'other' summaries have regressions compared to `this`.
     *
     * @return 'true' if other contains regressions.
     * @param other  Test results to compare with.
     *               'other' can contain only a subset of the tests.
     */
    boolean regression(AllTestSummaries other) {
      boolean regression = false;
      for (TestSummary summary: other.testResults.values()) {
        regression = regression || this.regression(summary);
      }
      return regression;
    }

    @Override public String toString() {
      List<TestSummary> results = new ArrayList<>(this.testResults.values());
      results.sort(Comparator.comparing(left -> left.file));
      StringBuilder result = new StringBuilder();
      for (TestSummary summary: results) {
        result.append(summary.toString());
        result.append(System.lineSeparator());
      }
      return result.toString();
    }

    /**
     * Write the test results to the specified file.
     */
    public void writeToFile(File file) throws IOException {
      try (BufferedWriter writer =
               new BufferedWriter(
                   new OutputStreamWriter(
                       Files.newOutputStream(file.toPath()), StandardCharsets.UTF_8))) {
        writer.write(this.toString());
      }
    }

    /**
     * True if there is an entry for the specified test file.
     *
     * @param test Test file name.
     */
    public boolean contains(String test) {
      return this.testResults.containsKey(test);
    }
  }

  /**
   * Summaries produced for the current run.
   * Must be static since it is written by the `findRegressions`
   * static method.
   */
  static AllTestSummaries testSummaries = new AllTestSummaries();

  static final String GOLDEN_FILE = "/slttestfailures.txt";
  /**
   * Summaries checked-in as resources that we compare against.
   */
  static AllTestSummaries goldenTestSummaries =
      new AllTestSummaries()
          .read(SqlLogicTests.class.getResourceAsStream(GOLDEN_FILE));

  private static TestStatistics launchSqlLogicTest(String... args) throws IOException {
    OptionsParser options = new OptionsParser(false, System.out, System.err);
    CalciteExecutor.register(options);
    return net.hydromatic.sqllogictest.Main.execute(options, args);
  }

  TestSummary shortSummary(String file, TestStatistics statistics) {
    return new TestSummary(file, statistics.getFailedTestCount());
  }

  /**
   * The following tests currently timeout during execution.
   * Technically these are Calcite bugs.
   */
  Set<String> timeout =
      ImmutableSet.of("test/select5.test",
          "test/random/groupby/slt_good_10.test");

  /**
   * The following tests contain SQL statements that are not supported by HSQLDB.
   */
  Set<String> unsupported =
      ImmutableSet.of("test/evidence/slt_lang_replace.test",
          "test/evidence/slt_lang_createtrigger.test",
          "test/evidence/slt_lang_droptrigger.test",
          "test/evidence/slt_lang_update.test",
          "test/evidence/slt_lang_reindex.test");

  void runOneTestFile(String testFile) throws IOException {
    if (timeout.contains(testFile)) {
      return;
    }
    if (unsupported.contains(testFile)) {
      return;
    }

    // The arguments below are command-line arguments for the sql-logic-test
    // executable from the hydromatic project.  The verbosity of the
    // output can be increased by adding more "-v" flags to the command-line.
    // By increasing verbosity even more you can get in the output a complete stack trace
    // for each error caused by an exception.
    TestStatistics res = launchSqlLogicTest("-v", "-e", "calcite", testFile);
    assertThat(res, notNullValue());
    assertThat(res.getParseFailureCount(), is(0));
    assertThat(res.getIgnoredTestCount(), is(0));
    assertThat(res.getTestFileCount(), is(1));
    res.printStatistics(System.err);  // Print errors found
    TestSummary summary = this.shortSummary(testFile, res);
    boolean regression = goldenTestSummaries.regression(summary);
    Assumptions.assumeFalse(regression, "Regression in " + summary.file);
    // The following is only useful if a new golden file need to be created
    testSummaries.add(summary);
  }

  @TestFactory @Tag("slow")
  List<DynamicTest> testSlow() {
    return generateTests(ImmutableSet.of("select1.test"));
  }

  @TestFactory @Disabled("This takes very long, should be run manually")
  List<DynamicTest> testAll() {
    // Run in parallel each test file.  There are 622 of these, each taking
    // a few minutes.
    return generateTests(net.hydromatic.sqllogictest.Main.getTestList());
  }

  /**
   * Generate a list of all the tests that can be executed.
   *
   * @param testFiles Names of files containing tests.
   */
  List<DynamicTest> generateTests(Set<String> testFiles) {
    List<DynamicTest> result = new ArrayList<>();
    for (String test: testFiles) {
      Executable executable = new Executable() {
        @Override public void execute() {
          assertTimeoutPreemptively(Duration.ofMinutes(10), () -> runOneTestFile(test));
        }
      };
      DynamicTest dynamicTest = DynamicTest.dynamicTest(test, executable);
      result.add(dynamicTest);
    }
    return result;
  }

  /**
   * Create the golden reference file with test results.
   */
  public static void createGoldenFile() throws IOException {
    // Currently this method is not invoked.
    // It can be used to create a new version of the GOLDEN_FILE
    // when bugs in Calcite are fixed.  It should be called after
    // all tests have executed and passed.
    File file = new File(GOLDEN_FILE);
    if (!file.exists()) {
      testSummaries.writeToFile(file);
    }
  }
}
