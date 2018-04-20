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
package org.apache.calcite.chinook;

import org.apache.calcite.test.QuidemTest;

import net.hydromatic.quidem.Quidem;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * Entry point for all end-to-end tests based on Chinook data in HSQLDB wrapped
 * by Calcite schema.
 */
@RunWith(Parameterized.class)
public class EndToEndTest extends QuidemTest {
  public EndToEndTest(String path) {
    super(path);
  }

  /** Runs a test from the command line.
   *
   * <p>For example:
   *
   * <blockquote>
   *   <code>java EndToEndTest sql/basic.iq</code>
   * </blockquote> */
  public static void main(String[] args) throws Exception {
    for (String arg : args) {
      new EndToEndTest(arg).test();
    }
  }

  /** For {@link Parameterized} runner. */
  @Parameterized.Parameters(name = "{index}: quidem({0})")
  public static Collection<Object[]> data() {
    // Start with a test file we know exists, then find the directory and list
    // its files.
    final String first = "sql/basic.iq";
    return data(first);
  }

  @Override protected Quidem.ConnectionFactory createConnectionFactory() {
    return new ConnectionFactory();
  }
}

// End EndToEndTest.java
