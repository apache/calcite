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

import net.hydromatic.sqllogictest.TestStatistics;

import org.apache.calcite.slt.executors.CalciteExecutor;

import net.hydromatic.sqllogictest.OptionsParser;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

/**
 * Tests using sql-logic-test suite.
 */
public class SqlLogicTestsForCalciteTests {
  private static TestStatistics launchSqlLogicTest(String... args) throws IOException {
    OptionsParser options = new OptionsParser(false, System.out, System.err);
    CalciteExecutor.register(options);
    return net.hydromatic.sqllogictest.Main.execute(options, args);
  }

  void runOneTestFile(String testFile) throws IOException {
    TestStatistics res = launchSqlLogicTest("-v", "-e", "calcite", testFile);
    res.printStatistics(System.err);
    assertThat(res, notNullValue());
    assertThat(res.getParseFailureCount(), is(0));
    assertThat(res.getIgnoredTestCount(), is(0));
    assertThat(res.getTestFileCount(), is(1));
  }

  @TestFactory
  List<DynamicTest> runAllTests() {
    Set<String> tests = net.hydromatic.sqllogictest.Main.getTestList();
    return tests.stream().map(s -> DynamicTest.dynamicTest(s,
        () -> runOneTestFile(s))).collect(Collectors.toList());
  }
}
