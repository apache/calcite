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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class TestStatistics {
  static final DecimalFormat df = new DecimalFormat("#,###");

  public static class FailedTestDescription {
    public final SqlTestQuery query;
    public final String error;

    public FailedTestDescription(SqlTestQuery query, String error) {
      this.query = query;
      this.error = error;
    }

    @Override
    public String toString() {
      return "ERROR: " + this.error + "\n\t" + this.query.file + ":" + this.query.line +
          "\n\t" + this.query + "\n";
    }
  }

  public int failed;
  public int passed;
  public int ignored;

  public void add(TestStatistics stats) {
    this.failed += stats.failed;
    this.passed += stats.passed;
    this.ignored += stats.ignored;
    this.failures.addAll(stats.failures);
  }

  List<FailedTestDescription> failures = new ArrayList<>();
  final boolean stopAtFirstErrror;

  public TestStatistics(boolean stopAtFirstError) {
    this.stopAtFirstErrror = stopAtFirstError;
  }

  public void addFailure(FailedTestDescription failure) {
    this.failures.add(failure);
    this.failed++;
  }

  public int testsRun() {
    return this.passed + this.ignored + this.failed;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (this.failures.size() > 0)
      result.append(this.failures.size())
          .append(" failures:\n");
    for (FailedTestDescription failure : this.failures)
      result.append(failure.toString());
    return "Passed: " + TestStatistics.df.format(this.passed) +
        "\nFailed: " + TestStatistics.df.format(this.failed) +
        "\nIgnored: " + TestStatistics.df.format(this.ignored) +
        "\n" +
        result;
  }

  public int totalTests() {
    return this.failed + this.passed + this.ignored;
  }
}
