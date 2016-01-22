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
package org.apache.calcite.avatica.metrics.dropwizard3;

import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystemConfiguration;

import com.codahale.metrics.MetricRegistry;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Test class for {@link DropwizardMetricsSystemFactory}.
 */
public class DropwizardMetricsSystemFactoryTest {

  private DropwizardMetricsSystemFactory factory;

  @Before public void setup() {
    factory = new DropwizardMetricsSystemFactory();
  }

  @Test(expected = IllegalStateException.class) public void testNullConfigurationFails() {
    factory.create(null);
  }

  @Test(expected = IllegalStateException.class) public void testUnhandledConfigurationType() {
    factory.create(NoopMetricsSystemConfiguration.getInstance());
  }

  @Test public void testHandledConfigurationType() {
    DropwizardMetricsSystem metrics =
        factory.create(new DropwizardMetricsSystemConfiguration(new MetricRegistry()));
    assertNotNull("Expected DropwizardMetricsSystem to be non-null", metrics);
  }
}

// End DropwizardMetricsSystemFactoryTest.java
