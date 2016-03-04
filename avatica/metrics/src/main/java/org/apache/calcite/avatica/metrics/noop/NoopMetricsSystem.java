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
import org.apache.calcite.avatica.metrics.Metric;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.Timer;

/**
 * {@link MetricsSystem} implementation which does nothing. Returns {@link Metric} implementations
 * which also does nothing (avoiding null instances).
 */
public class NoopMetricsSystem implements MetricsSystem {

  private static final NoopMetricsSystem NOOP_METRICS = new NoopMetricsSystem();

  private static final Timer TIMER = new NoopTimer();
  private static final Histogram HISTOGRAM = new NoopHistogram();
  private static final Meter METER = new NoopMeter();
  private static final Counter COUNTER = new NoopCounter();

  /**
   * @return A {@link NoopMetricsSystem} instance.
   */
  public static NoopMetricsSystem getInstance() {
    return NOOP_METRICS;
  }

  private NoopMetricsSystem() {}

  @Override public Timer getTimer(String name) {
    return TIMER;
  }

  @Override public Histogram getHistogram(String name) {
    return HISTOGRAM;
  }

  @Override public Meter getMeter(String name) {
    return METER;
  }

  @Override public Counter getCounter(String name) {
    return COUNTER;
  }

  @Override public <T> void register(String name, Gauge<T> gauge) {}

}

// End NoopMetricsSystem.java
