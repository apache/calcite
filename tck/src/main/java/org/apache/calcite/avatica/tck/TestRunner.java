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
package org.apache.calcite.avatica.tck;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Entry point for running an Avatica cross-version compatibility test.
 */
public class TestRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TestRunner.class);
  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ANSI_RED = "\u001B[31m";
  private static final String ANSI_GREEN = "\u001B[32m";

  private static Driver driver;
  private static String driverUrl;

  @Parameter(names = { "-u", "--jdbcUrl" }, description = "JDBC URL for Avatica Driver")
  private String jdbcUrl;

  private JUnitCore junitCore;

  /**
   * Returns the {@link Connection} for tests to use.
   *
   * @return A JDBC Connection.
   */
  public static Connection getConnection() throws SQLException {
    if (null == driver) {
      throw new IllegalStateException("JDBC Driver is not initialized");
    }

    return driver.connect(driverUrl, new Properties());
  }

  @Override public void run() {
    // Construct the Connection
    initializeDriver();

    if (null == driver) {
      LOG.error("Failed to find driver for {}", jdbcUrl);
      Unsafe.systemExit(TestRunnerExitCodes.NO_SUCH_DRIVER.ordinal());
      return;
    }

    // Initialize JUnit
    initializeJUnit();

    // Enumerate available test cases
    final List<Class<?>> testClasses = getAllTestClasses();

    final TestResults globalResults = new TestResults();

    // Run each test case
    for (Class<?> testClass : testClasses) {
      runSingleTest(globalResults, testClass);
    }

    System.out.println(globalResults.summarize());

    if (globalResults.numFailed > 0) {
      // Tests failed, don't exit normally
      Unsafe.systemExit(TestRunnerExitCodes.FAILED_TESTS.ordinal());
    } else {
      // Exited normally
      Unsafe.systemExit(TestRunnerExitCodes.NORMAL.ordinal());
    }
  }

  /**
   * Finds all tests to run for the TCK.
   *
   * @return A list of test classes to run.
   */
  List<Class<?>> getAllTestClasses() {
    try {
      ClassPath cp = ClassPath.from(getClass().getClassLoader());
      ImmutableSet<ClassInfo> classes =
          cp.getTopLevelClasses("org.apache.calcite.avatica.tck.tests");

      List<Class<?>> testClasses = new ArrayList<>(classes.size());
      for (ClassInfo classInfo : classes) {
        if (classInfo.getSimpleName().equals("package-info")) {
          continue;
        }
        Class<?> clz = Class.forName(classInfo.getName());
        if (Modifier.isAbstract(clz.getModifiers())) {
          // Ignore abstract classes
          continue;
        }
        testClasses.add(clz);
      }

      return testClasses;
    } catch (Exception e) {
      LOG.error("Failed to instantiate test classes", e);
      Unsafe.systemExit(TestRunnerExitCodes.TEST_CASE_INSTANTIATION.ordinal());
      // Unreachable..
      return null;
    }
  }

  void initializeDriver() {
    try {
      // Make sure the Avatica Driver gets loaded
      Class.forName("org.apache.calcite.avatica.remote.Driver");
      driverUrl = jdbcUrl;
      driver = DriverManager.getDriver(driverUrl);
    } catch (SQLException e) {
      LOG.error("Could not instantiate JDBC Driver with URL: '{}'", jdbcUrl, e);
      Unsafe.systemExit(TestRunnerExitCodes.BAD_JDBC_URL.ordinal());
    } catch (ClassNotFoundException e) {
      LOG.error("Could not load Avatica Driver class", e);
      Unsafe.systemExit(TestRunnerExitCodes.MISSING_DRIVER_CLASS.ordinal());
    }
  }

  /**
   * Sets up JUnit to run the tests for us.
   */
  void initializeJUnit() {
    junitCore = new JUnitCore();

    junitCore.addListener(new RunListener() {
      @Override public void testStarted(Description description) throws Exception {
        LOG.debug("Starting {}", description);
      }

      @Override public void testFinished(Description description) throws Exception {
        LOG.debug("{}Finished {}{}", ANSI_GREEN, description, ANSI_RESET);
      }

      @Override public void testFailure(Failure failure) throws Exception {
        LOG.info("{}Failed {}{}", ANSI_RED, failure.getDescription(), ANSI_RESET,
            failure.getException());
      }
    });
  }

  /**
   * Runs a single test class, adding its results to <code>globalResults</code>.
   *
   * @param globalResults A global record of test results.
   * @param testClass The test class to run.
   */
  void runSingleTest(final TestResults globalResults, final Class<?> testClass) {
    final String className = Objects.requireNonNull(testClass).getName();
    LOG.info("{}Running {}{}", ANSI_GREEN, className, ANSI_RESET);

    try {
      Result result = junitCore.run(testClass);
      globalResults.merge(testClass, result);
    } catch (Exception e) {
      // most likely JUnit issues, like no tests to run
      LOG.error("{}Test failed: {}{}", ANSI_RED, className, ANSI_RESET, e);
    }
  }

  public static void main(String[] args) {
    TestRunner runner = new TestRunner();

    // Parse the args, sets it on runner.
    JCommander jc = new JCommander(runner);
    jc.parse(args);

    // Run the tests.
    runner.run();
  }

  /**
   * A container to track results from all tests executed.
   */
  private static class TestResults {
    private int numRun = 0;
    private int numFailed = 0;
    private int numIgnored = 0;
    private List<Failure> failures = new ArrayList<>();

    /**
     * Updates the current state of <code>this</code> with the <code>result</code>.
     *
     * @param testClass The test class executed.
     * @param result The results of the test class execution.
     * @return <code>this</code>
     */
    public TestResults merge(Class<?> testClass, Result result) {
      LOG.info("Tests run: {}, Failures: {}, Skipped: {}, Time elapsed: {} - in {}",
          result.getRunCount(), result.getFailureCount(), result.getIgnoreCount(),
          TimeUnit.SECONDS.convert(result.getRunTime(), TimeUnit.MILLISECONDS),
          testClass.getName());

      numRun += result.getRunCount();
      numFailed += result.getFailureCount();
      numIgnored += result.getIgnoreCount();

      // Collect the failures
      if (!result.wasSuccessful()) {
        failures.addAll(result.getFailures());
      }

      return this;
    }

    /**
     * Constructs a human-readable summary of the success/failure of the tests executed.
     *
     * @return A summary in the form of a String.
     */
    public String summarize() {
      StringBuilder sb = new StringBuilder(64);
      sb.append("\nTest Summary: Run: ").append(numRun).append(", Failed: ").append(numFailed);
      sb.append(", Skipped: ").append(numIgnored);
      if (numFailed > 0) {
        sb.append(ANSI_RED).append("\n\nFailures:").append(ANSI_RESET).append("\n\n");
        for (Failure failure : failures) {
          sb.append(failure.getTestHeader()).append(" ").append(failure.getMessage()).append("\n");
        }
      }
      return sb.toString();
    }
  }

  /**
   * ExitCodes set by {@link TestRunner}.
   */
  private enum TestRunnerExitCodes {
    // Position is important, ordinal() is used!
    NORMAL,                  // 0, all tests passed
    BAD_JDBC_URL,            // 1
    TEST_CASE_INSTANTIATION, // 2
    NO_SUCH_DRIVER,          // 3
    FAILED_TESTS,            // 4
    MISSING_DRIVER_CLASS;    // 5
  }
}

// End TestRunner.java
