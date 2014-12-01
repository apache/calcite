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

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.Map;

import static org.apache.calcite.util.Static.RESOURCE;

import static org.junit.Assert.assertThat;

/**
 * Tests the generated implementation of
 * {@link org.apache.calcite.runtime.CalciteResource} (mostly a sanity check for
 * the resource-generation infrastructure).
 */
public class CalciteResourceTest {
  //~ Constructors -----------------------------------------------------------

  public CalciteResourceTest() {
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Verifies that resource properties such as SQLSTATE are available at
   * runtime.
   */
  @Test public void testSqlstateProperty() {
    Map<String, String> props =
        RESOURCE.illegalIntervalLiteral("", "").getProperties();
    assertThat(props.get("SQLSTATE"), CoreMatchers.equalTo("42000"));
  }
}

// End CalciteResourceTest.java
