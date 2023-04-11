/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.apache.calcite.slt.executors;

import org.apache.calcite.slt.ExecutionOptions;
import org.apache.calcite.slt.SLTTestFile;
import org.apache.calcite.slt.TestStatistics;

/**
 * This executor does not execute the tests at all.
 * It is still useful to validate that the test parsing works.
 */
public class NoExecutor extends SqlSLTTestExecutor {
  @Override
  public TestStatistics execute(SLTTestFile testFile, ExecutionOptions options) {
    TestStatistics result = new TestStatistics(options.stopAtFirstError);
    this.startTest();
    result.failed = 0;
    result.ignored = testFile.getTestCount();
    result.passed = 0;
    this.reportTime(testFile.getTestCount());
    return result;
  }
}
