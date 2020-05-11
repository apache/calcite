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

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Test that runs every Quidem file in the "core" module with CascadePlanner
 */
class CoreCascadeQuidemTest extends CoreQuidemTest {

  /** Runs a test from the command line.
   *
   * <p>For example:
   *
   * <blockquote>
   *   <code>java CoreQuidemTest sql/dummy.iq</code>
   * </blockquote> */
  public static void main(String[] args) throws Exception {
    for (String arg : args) {
      new CoreCascadeQuidemTest().test(arg);
    }
  }

  public static Collection<Object[]> data() {
    // skip sql/sequence.iq as its result is not stable running multiple times
    return CoreQuidemTest.data().stream().filter(
        objs -> !objs[0].equals("sql/sequence.iq")).collect(Collectors.toList());
  }

  @Override protected boolean cascade() {
    return true;
  }
}
