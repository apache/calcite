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
package org.apache.calcite.avatica.metrics.dropwizard;

import org.apache.calcite.avatica.metrics.Counter;
import org.apache.calcite.avatica.metrics.Gauge;
import org.apache.calcite.avatica.metrics.Histogram;
import org.apache.calcite.avatica.metrics.Meter;
import org.apache.calcite.avatica.metrics.Timer;
import org.apache.calcite.avatica.metrics.Timer.Context;

import com.codahale.metrics.MetricRegistry;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DropwizardMetricsSystem}.
 */
public class DropwizardMetricsSystemTest {

  private MetricRegistry mockRegistry;
  private DropwizardMetricsSystem metrics;

  @Before public void setup() {
    mockRegistry = mock(MetricRegistry.class);
    metrics = new DropwizardMetricsSystem(mockRegistry);
  }

  @Test public void testGauge() {
    final long gaugeValue = 42L;
    final String name = "gauge";
    metrics.register(name, new Gauge<Long>() {
      @Override public Long getValue() {
        return gaugeValue;
      }
    });

    verify(mockRegistry, times(1)).register(eq(name), any(com.codahale.metrics.Gauge.class));
  }

  @Test public void testMeter() {
    final String name = "meter";
    final com.codahale.metrics.Meter mockMeter = mock(com.codahale.metrics.Meter.class);

    when(mockRegistry.meter(name)).thenReturn(mockMeter);

    Meter meter = metrics.getMeter(name);

    final long count = 5;
    meter.mark(count);

    verify(mockMeter, times(1)).mark(count);

    meter.mark();

    verify(mockMeter, times(1)).mark();
  }

  @Test public void testHistogram() {
    final String name = "histogram";
    final com.codahale.metrics.Histogram mockHistogram = mock(com.codahale.metrics.Histogram.class);

    when(mockRegistry.histogram(name)).thenReturn(mockHistogram);

    Histogram histogram = metrics.getHistogram(name);

    long[] long_values = new long[] {1L, 5L, 15L, 30L, 60L};
    for (long value : long_values) {
      histogram.update(value);
    }

    for (long value : long_values) {
      verify(mockHistogram).update(value);
    }

    int[] int_values = new int[] {2, 6, 16, 31, 61};
    for (int value : int_values) {
      histogram.update(value);
    }

    for (int value : int_values) {
      verify(mockHistogram).update(value);
    }
  }

  @Test public void testCounter() {
    final String name = "counter";
    final com.codahale.metrics.Counter mockCounter = mock(com.codahale.metrics.Counter.class);

    when(mockRegistry.counter(name)).thenReturn(mockCounter);

    Counter counter = metrics.getCounter(name);

    long[] updates = new long[] {1L, 5L, -2L, 4L, -8L, 0};
    for (long update : updates) {
      if (update < 0) {
        counter.decrement(Math.abs(update));
      } else {
        counter.increment(update);
      }
    }

    for (long update : updates) {
      if (update < 0) {
        verify(mockCounter).dec(Math.abs(update));
      } else {
        verify(mockCounter).inc(update);
      }
    }

    int numSingleUpdates = 3;
    for (int i = 0; i < numSingleUpdates; i++) {
      counter.increment();
      counter.decrement();
    }

    verify(mockCounter, times(numSingleUpdates)).inc();
    verify(mockCounter, times(numSingleUpdates)).dec();
  }

  @Test public void testTimer() {
    final String name = "timer";
    final com.codahale.metrics.Timer mockTimer = mock(com.codahale.metrics.Timer.class);
    final com.codahale.metrics.Timer.Context mockContext =
        mock(com.codahale.metrics.Timer.Context.class);

    when(mockRegistry.timer(name)).thenReturn(mockTimer);
    when(mockTimer.time()).thenReturn(mockContext);

    Timer timer = metrics.getTimer(name);
    Context context = timer.start();
    context.close();

    verify(mockTimer).time();
    verify(mockContext).stop();
  }
}

// End DropwizardMetricsSystemTest.java
