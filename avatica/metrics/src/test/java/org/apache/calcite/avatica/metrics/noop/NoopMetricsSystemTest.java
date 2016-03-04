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

import org.apache.calcite.avatica.metrics.Counter;
import org.apache.calcite.avatica.metrics.Gauge;
import org.apache.calcite.avatica.metrics.Histogram;
import org.apache.calcite.avatica.metrics.Meter;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.Timer;
import org.apache.calcite.avatica.metrics.Timer.Context;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link NoopMetricsSystem}.
 */
public class NoopMetricsSystemTest {

  @Test public void testNoNulls() {
    // The NOOP implementation should act as a real implementation, no "nulls" allowed.
    MetricsSystem metrics = NoopMetricsSystem.getInstance();

    Counter counter = metrics.getCounter("counter");
    counter.decrement();
    counter.increment();
    counter.decrement(1L);
    counter.increment(1L);

    Histogram histogram = metrics.getHistogram("histogram");
    histogram.update(1);
    histogram.update(1L);

    Timer timer = metrics.getTimer("timer");
    Context context = timer.start();
    context.close();
    Context contextTwo = timer.start();
    assertTrue("Timer's context should be a singleton", context == contextTwo);

    Meter meter = metrics.getMeter("meter");
    meter.mark();
    meter.mark(5L);

    metrics.register("gauge", new Gauge<Long>() {
      @Override public Long getValue() {
        return 42L;
      }
    });
  }

  @Test public void testSingleton() {
    assertTrue("Should be a singleton",
        NoopMetricsSystem.getInstance() == NoopMetricsSystem.getInstance());
  }
}

// End NoopMetricsSystemTest.java
