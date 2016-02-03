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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * A utility encapsulating use of {@link ServiceLoader} to instantiate a {@link MetricsSystem}.
 */
public class MetricsSystemLoader {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsSystemLoader.class);
  private static final MetricsSystemLoader INSTANCE = new MetricsSystemLoader();

  private MetricsSystemLoader() {}

  /**
   * Creates a {@link MetricsSystem} instance using the corresponding {@link MetricsSystemFactory}
   * available to {@link ServiceLoader} on the classpath. If there is not exactly one instance of
   * a {@link MetricsSystemFactory}, an instance of {@link NoopMetricsSystem} will be returned.
   *
   * @param config State to pass to the {@link MetricsSystemFactory}.
   * @return A {@link MetricsSystem} implementation.
   */
  public static MetricsSystem load(MetricsSystemConfiguration<?> config) {
    return INSTANCE._load(Objects.requireNonNull(config));
  }

  MetricsSystem _load(MetricsSystemConfiguration<?> config) {
    List<MetricsSystemFactory> availableFactories = getFactories();

    if (1 == availableFactories.size()) {
      // One and only one instance -- what we want/expect
      MetricsSystemFactory factory = availableFactories.get(0);
      LOG.info("Loaded MetricsSystem {}", factory.getClass());
      return factory.create(config);
    } else if (availableFactories.isEmpty()) {
      // None-provided default to no metrics
      LOG.info("No metrics implementation available on classpath. Using No-op implementation");
      return NoopMetricsSystem.getInstance();
    } else {
      // Tell the user they're doing something wrong, and choose the first impl.
      StringBuilder sb = new StringBuilder();
      for (MetricsSystemFactory factory : availableFactories) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append(factory.getClass());
      }
      LOG.warn("Found multiple MetricsSystemFactory implementations: {}."
          + " Using No-op implementation", sb);
      return NoopMetricsSystem.getInstance();
    }
  }

  List<MetricsSystemFactory> getFactories() {
    ServiceLoader<MetricsSystemFactory> loader = ServiceLoader.load(MetricsSystemFactory.class);
    List<MetricsSystemFactory> availableFactories = new ArrayList<>();
    for (MetricsSystemFactory factory : loader) {
      availableFactories.add(factory);
    }
    return availableFactories;
  }
}

// End MetricsSystemLoader.java
