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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.Objects;

/**
 * A utility class to encapsulate common logic in use of Dropwizard metrics implementation.
 */
public class MetricsUtil {

  private static final MetricsUtil INSTANCE = new MetricsUtil();

  private MetricsUtil() {}

  /**
   * Returns an instance of {@link MetricsUtil}. This class is thread-safe, therefore is may be
   * a singleton instance.
   *
   * @return A (singleton) instance of {@link MetricsUtil}.
   */
  public static MetricsUtil getInstance() {
    return INSTANCE;
  }

  /**
   * Create a {@link Timer} if a {@link MetricRegistry} instance was provided. The instance of
   * the timer may be cached if the {@code caller} and {@code timerName} were previously
   * used to create a {@link Timer} with the given {@code metrics}.
   *
   * @param metrics A {@link MetricRegistry} instance or null.
   * @param caller A class to identify the {@link Timer}'s owner.
   * @param timerName A name to identify what the {@link Timer} is measuring.
   * @return A {@link Timer}, or null.
   */
  public Timer getTimer(MetricRegistry metrics, Class<?> caller, String timerName) {
    if (null != metrics) {
      return metrics.timer(name(Objects.requireNonNull(caller), Objects.requireNonNull(timerName)));
    }
    return null;
  }

  /**
   * Create a {@link Histogram} if a {@link MetricRegistry} instance was provided. The instance of
   * the histogram may be cached if the {@code caller} and {@code histogramName} were previously
   * used to create a {@link Histogram} with the given {@code metrics}.
   *
   * @param metrics A {@link MetricRegistry} instance or null.
   * @param caller A class to identify the {@link Histogram}'s owner.
   * @param histogramName A name to identify what the {@link Histogram} is measuring.
   * @return A {@link Histogram}, or null.
   */
  public Histogram getHistogram(MetricRegistry metrics, Class<?> caller, String histogramName) {
    if (null != metrics) {
      String name = name(Objects.requireNonNull(caller), Objects.requireNonNull(histogramName));
      return metrics.histogram(name);
    }
    return null;
  }

  /**
   * Start the provided timer if it is non-null.
   *
   * @param timer The {@link Timer} to start, or null.
   * @return The Context from starting the timer or null if the Timer was null.
   */
  public Context startTimer(Timer timer) {
    if (null != timer) {
      return timer.time();
    }
    return null;
  }

}

// End MetricsUtil.java
