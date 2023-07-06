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
import net.hydromatic.sqllogictest.TestStatistics;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Tests using sql-logic-test suite.
 */
public class SqlLogicTestsForCalciteTests {
  /**
   * Short summary of the results of a test execution.
   */
  static class TestSummary {
    /**
     * File containing tests.
     */
    final String file;
    /**
     * Number of tests that have passed.
     */
    final int passed;
    /**
     * Number of tests that have failed.
     */
    final int failed;

    TestSummary(String file, int passed, int failed) {
      this.file = file;
      this.passed = passed;
      this.failed = failed;
    }

    /**
     * Parse a TestSummary from a string.
     * The inverse of 'toString'.
     *
     * @return The parsed TestSummary or null on failure.
     */
    static TestSummary parse(String line) {
      String[] parts = line.split(":");
      if (parts.length != 3) {
        return null;
      }
      try {
        int passed = Integer.parseInt(parts[1]);
        int failed = Integer.parseInt(parts[2]);
        return new TestSummary(parts[0], passed, failed);
      } catch (NumberFormatException ex) {
        return null;
      }
    }

    @Override public String toString() {
      return this.file + ":" + this.passed + ":" + this.failed;
    }

    /**
     * Check if the 'other' TestSummaries are a regressions
     * when compared to 'this'.
     *
     * @param other TestSummary to compare against.
     * @return 'true' if 'other' is a regression from 'this'.
     */
    boolean regression(TestSummary other) {
      return other.failed > this.failed;
    }
  }

  /**
   * Summary for all tests executed.
   */
  static class AllTestSummaries {
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

    void read(InputStream stream) throws IOException {
      try (BufferedReader reader =
               new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
        reader.lines().forEach(line -> {
          TestSummary summary = TestSummary.parse(line);
          if (summary != null) {
            this.add(summary);
          } else {
            System.err.println("Could not parse line " + line);
          }
        });
      }
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
        TestSummary original = this.testResults.get(summary.file);
        if (original == null) {
          System.err.println("No historical data for test " + summary.file);
          continue;
        }
        if (original.regression(summary)) {
          System.err.println("Regression: " + original.file
              + " had " + original.failed + " failures, now has " + summary.failed);
          regression = true;
        }
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
  /**
   * Summaries checked-in as resources that we compare against.
   */
  static AllTestSummaries goldenTestSummaries = new AllTestSummaries();

  static final String GOLDENFILE = "/slttestfailures.txt";

  private static TestStatistics launchSqlLogicTest(String... args) throws IOException {
    OptionsParser options = new OptionsParser(false, System.out, System.err);
    CalciteExecutor.register(options);
    return net.hydromatic.sqllogictest.Main.execute(options, args);
  }

  TestSummary shortSummary(String file, TestStatistics statistics) {
    return new TestSummary(file, statistics.getPassedTestCount(), statistics.getFailedTestCount());
  }

  // The following tests currently timeout during execution.
  // Technically these are Calcite bugs.
  Set<String> timeout = new HashSet<String>() {
    {
      add("test/select5.test");
      add("test/random/groupby/slt_good_10.test");
    }
  };

  // The following tests contain SQL statements that are not supported by HSQLDB
  Set<String> unsupported = new HashSet<String>() {
    {
      add("test/evidence/slt_lang_replace.test");
      add("test/evidence/slt_lang_createtrigger.test");
      add("test/evidence/slt_lang_droptrigger.test");
      add("test/evidence/slt_lang_update.test");
      add("test/evidence/slt_lang_reindex.test");
    }
  };

  void runOneTestFile(String testFile) throws IOException {
    if (timeout.contains(testFile)) {
      return;
    }
    if (unsupported.contains(testFile)) {
      return;
    }

    TestStatistics res = launchSqlLogicTest("-v", "-e", "calcite", testFile);
    assertThat(res, notNullValue());
    assertThat(res.getParseFailureCount(), is(0));
    assertThat(res.getIgnoredTestCount(), is(0));
    assertThat(res.getTestFileCount(), is(1));
    res.printStatistics(System.err);  // Print errors found
    TestSummary summary = this.shortSummary(testFile, res);
    testSummaries.add(summary);
  }

  @Test @Tag("slow")
  public void runOneTestFile() throws IOException {
    runOneTestFile("select1.test");
  }

  @TestFactory @Disabled("This takes very long, should be run manually")
  List<DynamicTest> runAllTests() {
    // Run in parallel each test file.
    Set<String> tests = net.hydromatic.sqllogictest.Main.getTestList();
    List<DynamicTest> result = new ArrayList<>();
    for (String test: tests) {
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

  @BeforeAll
  public static void readGoldenFile() throws IOException {
    // Read the statistics of the previously-failing tests
    try (InputStream stream = SqlLogicTestsForCalciteTests.class.getResourceAsStream(GOLDENFILE)) {
      goldenTestSummaries.read(stream);
    }
  }

  @AfterAll
  public static void findRegressions() throws IOException {
    // Compare with failures produced by a previous execution

    // Code used to create the golden file originally
    // File file = new File(goldenFile);
    // if (!file.exists()) {
    //   testSummaries.writeToFile(file);
    //   return;
    // }
    boolean regression = goldenTestSummaries.regression(testSummaries);
    Assertions.assertFalse(regression, "Regression discovered");
  }
}
