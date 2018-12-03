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

import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.util.TryThreadLocal;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * Test that runs every Quidem file in the "core" module as a test.
 */
@RunWith(Parameterized.class)
public class CoreQuidemTest extends QuidemTest {
  public CoreQuidemTest(String path) {
    super(path);
  }

  /** Runs a test from the command line.
   *
   * <p>For example:
   *
   * <blockquote>
   *   <code>java CoreQuidemTest sql/dummy.iq</code>
   * </blockquote> */
  public static void main(String[] args) throws Exception {
    for (String arg : args) {
      new CoreQuidemTest(arg).test();
    }
  }

  /** For {@link Parameterized} runner. */
  @Parameterized.Parameters(name = "{index}: quidem({0})")
  public static Collection<Object[]> data() {
    // Start with a test file we know exists, then find the directory and list
    // its files.
    final String first = "sql/agg.iq";
    return data(first);
  }

  /** Override settings for "sql/misc.iq". */
  public void testSqlMisc() throws Exception {
    switch (CalciteAssert.DB) {
    case ORACLE:
      // There are formatting differences (e.g. "4.000" vs "4") when using
      // Oracle as the JDBC data source.
      return;
    }
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push(true)) {
      checkRun(path);
    }
  }

  /** Override settings for "sql/scalar.iq". */
  public void testSqlScalar() throws Exception {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push(true)) {
      checkRun(path);
    }
  }

  /** Runs the dummy script "sql/dummy.iq", which is checked in empty but
   * which you may use as scratch space during development. */

  // Do not disable this test; just remember not to commit changes to dummy.iq
  public void testSqlDummy() throws Exception {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push(true)) {
      checkRun(path);
    }
  }

}

// End CoreQuidemTest.java
