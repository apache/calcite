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
package org.apache.calcite.avatica.metrics;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link MetricsUtil}.
 */
public class MetricsUtilTest {

  private MetricsUtil metricsUtil;

  @Before public void setup() {
    this.metricsUtil = MetricsUtil.getInstance();
  }

  @Test public void testSingletonInstance() {
    assertTrue("Expected to find a singleton instance of MetricsUtil",
        this.metricsUtil == MetricsUtil.getInstance());
  }

  @Test public void testNullHandling() {
    assertNull("Expected a null Histogram",
        metricsUtil.getHistogram(null, getClass(), "histogram"));
    assertNull("Expected a null Timer",
        metricsUtil.getTimer(null, getClass(), "timer"));
    assertNull("Expected a null Context", metricsUtil.startTimer(null));
  }

}

// End MetricsUtilTest.java
