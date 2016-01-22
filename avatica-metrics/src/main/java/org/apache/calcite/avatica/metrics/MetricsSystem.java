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

/**
 * General purpose factory for creating various metrics. Modeled off of the Dropwizard Metrics API.
 */
public interface MetricsSystem {

  /**
   * Get or construct a {@link Timer} used to measure durations and report rates.
   *
   * @param name The name of the Timer.
   * @return An instance of {@link Timer}.
   */
  Timer getTimer(String name);

  /**
   * Get or construct a {@link Histogram} used to measure a distribution of values.
   *
   * @param name The name of the Histogram.
   * @return An instance of {@link Histogram}.
   */
  Histogram getHistogram(String name);

  /**
   * Get or construct a {@link Meter} used to measure durations and report distributions (a
   * combination of a {@link Timer} and a {@link Histogram}.
   *
   * @param name The name of the Meter.
   * @return An instance of {@link Meter}.
   */
  Meter getMeter(String name);

  /**
   * Get or construct a {@link Counter} used to track a mutable number.
   *
   * @param name The name of the Counter
   * @return An instance of {@link Counter}.
   */
  Counter getCounter(String name);

  /**
   * Register a {@link Gauge}. The Gauge will be invoked at a period defined by the implementation
   * of {@link MetricsSystem}.
   *
   * @param name The name of the Gauge.
   * @param gauge A callback to compute the current value.
   */
  <T> void register(String name, Gauge<T> gauge);

}

// End MetricsSystem.java
