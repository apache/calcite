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

import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystemConfiguration;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link MetricsSystemLoader}.
 */
public class MetricsSystemLoaderTest {

  @Test public void testSingleInstance() {
    final List<MetricsSystemFactory> factories =
        Collections.<MetricsSystemFactory>singletonList(new MarkedNoopMetricsSystemFactory());
    MetricsSystemLoader loader = Mockito.mock(MetricsSystemLoader.class);

    Mockito.when(loader.getFactories()).thenReturn(factories);
    Mockito.when(loader._load(Mockito.any(MetricsSystemConfiguration.class))).thenCallRealMethod();

    // One MetricsSystemFactory should return the MetricsSystem it creates
    MetricsSystem system = loader._load(NoopMetricsSystemConfiguration.getInstance());
    assertEquals(MarkedMetricsSystem.INSTANCE, system);
  }

  @Test public void testMultipleInstances() {
    // The type of the factories doesn't matter (we can send duplicates for testing purposes)
    final List<MetricsSystemFactory> factories =
        Arrays.<MetricsSystemFactory>asList(new MarkedNoopMetricsSystemFactory(),
            new MarkedNoopMetricsSystemFactory());
    MetricsSystemLoader loader = Mockito.mock(MetricsSystemLoader.class);

    Mockito.when(loader.getFactories()).thenReturn(factories);
    Mockito.when(loader._load(Mockito.any(MetricsSystemConfiguration.class))).thenCallRealMethod();

    // We had two factories loaded, therefore we'll fall back to the NoopMetricsSystem
    MetricsSystem system = loader._load(NoopMetricsSystemConfiguration.getInstance());
    assertEquals(NoopMetricsSystem.getInstance(), system);
  }

  @Test public void testNoInstances() {
    // The type of the factories doesn't matter (we can send duplicates for testing purposes)
    final List<MetricsSystemFactory> factories = Collections.emptyList();
    MetricsSystemLoader loader = Mockito.mock(MetricsSystemLoader.class);

    Mockito.when(loader.getFactories()).thenReturn(factories);
    Mockito.when(loader._load(Mockito.any(MetricsSystemConfiguration.class))).thenCallRealMethod();

    // We had no factories loaded, therefore we'll fall back to the NoopMetricsSystem
    MetricsSystem system = loader._load(NoopMetricsSystemConfiguration.getInstance());
    assertEquals(NoopMetricsSystem.getInstance(), system);
  }

  /**
   * A test factory implementation which can return a recognized MetricsSystem implementation.
   */
  private static class MarkedNoopMetricsSystemFactory implements MetricsSystemFactory {
    public MarkedMetricsSystem create(MetricsSystemConfiguration<?> config) {
      return MarkedMetricsSystem.INSTANCE;
    }
  }

  /**
   * A metrics system implementation that is identifiable for testing.
   */
  private static class MarkedMetricsSystem implements MetricsSystem {
    private static final MarkedMetricsSystem INSTANCE = new MarkedMetricsSystem();

    private MarkedMetricsSystem() {}

    @Override public Timer getTimer(String name) {
      return null;
    }

    @Override public Histogram getHistogram(String name) {
      return null;
    }

    @Override public Meter getMeter(String name) {
      return null;
    }

    @Override public Counter getCounter(String name) {
      return null;
    }

    @Override public <T> void register(String name, Gauge<T> gauge) {}
  }
}

// End MetricsSystemLoaderTest.java
