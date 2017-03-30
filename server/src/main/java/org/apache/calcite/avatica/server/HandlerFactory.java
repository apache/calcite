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
package org.apache.calcite.avatica.server;

import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.MetricsSystemConfiguration;
import org.apache.calcite.avatica.metrics.MetricsSystemFactory;
import org.apache.calcite.avatica.metrics.MetricsSystemLoader;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystemConfiguration;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Factory that instantiates the desired implementation, typically differing on the method
 * used to serialize messages, for use in the Avatica server.
 */
public class HandlerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HandlerFactory.class);

  /**
   * Constructs the desired implementation for the given serialization method with metrics.
   *
   * @param service The underlying {@link Service}.
   * @param serialization The desired message serialization.
   * @return The {@link AvaticaHandler}.
   */
  public AvaticaHandler getHandler(Service service, Driver.Serialization serialization) {
    return getHandler(service, serialization, NoopMetricsSystemConfiguration.getInstance());
  }

  /**
   * Constructs the desired implementation for the given serialization method and server
   * configuration with metrics.
   *
   * @param service The underlying {@link Service}.
   * @param serialization The desired message serialization.
   * @param serverConfig Avatica server configuration or null.
   * @return The {@link AvaticaHandler}.
   */
  public AvaticaHandler getHandler(Service service, Driver.Serialization serialization,
      AvaticaServerConfiguration serverConfig) {
    return getHandler(service, serialization, NoopMetricsSystemConfiguration.getInstance(),
        serverConfig);
  }

  /**
   * Constructs the desired implementation for the given serialization method with metrics.
   *
   * @param service The underlying {@link Service}.
   * @param serialization The desired message serialization.
   * @param metricsConfig Configuration for the {@link MetricsSystem}.
   * @return The {@link AvaticaHandler}.
   */
  public AvaticaHandler getHandler(Service service, Driver.Serialization serialization,
      MetricsSystemConfiguration<?> metricsConfig) {
    return getHandler(service, serialization, metricsConfig, null);
  }

  /**
   * Constructs the desired implementation for the given serialization method and server
   * configuration with metrics.
   *
   * @param service The underlying {@link Service}
   * @param serialization The serializatio mechanism to use
   * @param metricsConfig Configuration for the {@link MetricsSystem}.
   * @param serverConfig Avatica server configuration or null
   * @return An {@link AvaticaHandler}
   */
  public AvaticaHandler getHandler(Service service, Driver.Serialization serialization,
      MetricsSystemConfiguration<?> metricsConfig, AvaticaServerConfiguration serverConfig) {
    if (null == metricsConfig) {
      metricsConfig = NoopMetricsSystemConfiguration.getInstance();
    }
    MetricsSystem metrics = MetricsSystemLoader.load(metricsConfig);

    switch (serialization) {
    case JSON:
      return new AvaticaJsonHandler(service, metrics, serverConfig);
    case PROTOBUF:
      return new AvaticaProtobufHandler(service, metrics, serverConfig);
    default:
      throw new IllegalArgumentException("Unknown Avatica handler for " + serialization.name());
    }
  }

  /**
   * Load a {@link MetricsSystem} using ServiceLoader to create a {@link MetricsSystemFactory}.
   *
   * @param config State to pass to the factory for initialization.
   * @return A {@link MetricsSystem} instance.
   */
  MetricsSystem loadMetricsSystem(MetricsSystemConfiguration<?> config) {
    ServiceLoader<MetricsSystemFactory> loader = ServiceLoader.load(MetricsSystemFactory.class);
    List<MetricsSystemFactory> availableFactories = new ArrayList<>();
    for (MetricsSystemFactory factory : loader) {
      availableFactories.add(factory);
    }

    if (1 == availableFactories.size()) {
      // One and only one instance -- what we want
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
}

// End HandlerFactory.java
