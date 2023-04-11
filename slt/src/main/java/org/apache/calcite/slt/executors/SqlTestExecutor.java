/*
 * Copyright 2023 VMware, Inc.
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

import org.apache.calcite.slt.ICastable;

import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Set;

/**
 * Base class for classes that can run tests.
 */
public class SqlTestExecutor implements ICastable {
  static final DecimalFormat df = new DecimalFormat("#,###");
  protected final Set<String> buggyOperations;
  /**
   * If true validate the status of the SQL statements executed.
   */
  protected boolean validateStatus = true;
  private static long startTime = -1;
  private static int totalTests = 0;
  private long lastTestStartTime;
  protected long statementsExecuted = 0;

  protected SqlTestExecutor() {
    this.buggyOperations = new HashSet<>();
  }

  static long seconds(long end, long start) {
    return (end - start) / 1000000000;
  }

  public void avoid(HashSet<String> statementsToSkip) {
    this.buggyOperations.addAll(statementsToSkip);
  }

  public void setValidateStatus(boolean validate) {
    this.validateStatus = validate;
  }

  protected void reportTime(int tests) {
    long end = System.nanoTime();
    totalTests += tests;
    System.out.println(df.format(tests) + " tests took " +
        df.format(seconds(end, this.lastTestStartTime)) + "s, "
        + df.format(totalTests) + " took " +
        df.format(seconds(end, startTime)) + "s");
  }

  protected void startTest() {
    this.lastTestStartTime = System.nanoTime();
    if (startTime == -1)
      startTime = lastTestStartTime;
  }
}
