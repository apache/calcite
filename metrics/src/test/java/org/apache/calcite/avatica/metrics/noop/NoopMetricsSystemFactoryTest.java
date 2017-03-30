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
package org.apache.calcite.avatica.metrics.noop;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link NoopMetricsSystemFactory}.
 */
public class NoopMetricsSystemFactoryTest {

  @Test public void testSingleton() {
    NoopMetricsSystemFactory factory = new NoopMetricsSystemFactory();
    NoopMetricsSystemConfiguration config = NoopMetricsSystemConfiguration.getInstance();
    assertTrue("The factory should only return one NoopMetricsSystem instance",
        factory.create(config) == factory.create(config));
  }

}

// End NoopMetricsSystemFactoryTest.java
