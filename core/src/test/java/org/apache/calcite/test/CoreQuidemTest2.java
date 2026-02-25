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

import org.apache.calcite.config.CalciteConnectionProperty;

/**
 * Test that runs Quidem files with the top-down decorrelator enabled.
 */
public class CoreQuidemTest2 extends CoreQuidemTest {
  /** Runs a test from the command line.
   *
   * <p>For example:
   *
   * <blockquote>
   *   <code>java CoreQuidemTest2 sql/dummy.iq</code>
   * </blockquote> */
  public static void main(String[] args) throws Exception {
    for (String arg : args) {
      new CoreQuidemTest2().test(arg);
    }
  }

  @Override protected CalciteAssert.AssertThat customize(CalciteAssert.AssertThat assertThat) {
    return super.customize(assertThat)
        .with(CalciteConnectionProperty.TOPDOWN_GENERAL_DECORRELATION_ENABLED, true);
  }

  @Override protected boolean useTopDownGeneralDecorrelator() {
    return true;
  }
}
