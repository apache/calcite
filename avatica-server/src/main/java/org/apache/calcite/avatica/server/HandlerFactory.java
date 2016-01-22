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

import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.Service;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;

import org.eclipse.jetty.server.Handler;

/**
 * Factory that instantiates the desired implementation, typically differing on the method
 * used to serialize messages, for use in the Avatica server.
 */
public class HandlerFactory {

  /**
   * Constructs the desired implementation for the given serialization method without any
   * metrics support.
   *
   * @param service The underlying {@link Service}.
   * @param serialization The type of serialization to use.
   * @return The {@link Handler}.
   */
  public Handler getHandler(Service service, Driver.Serialization serialization) {
    return getHandler(service, serialization, Optional.<MetricRegistry>absent());
  }

  /**
   * Constructs the desired implementation for the given serialization method with metrics.
   *
   * @param service The underlying {@link Service}.
   * @param serialization The desired message serialization.
   * @param metrics Optionally, a {@link MetricRegistry} instance.
   * @return The {@link Handler}.
   */
  public Handler getHandler(Service service, Driver.Serialization serialization,
      Optional<MetricRegistry> metrics) {
    switch (serialization) {
    case JSON:
      return new AvaticaJsonHandler(service, metrics);
    case PROTOBUF:
      return new AvaticaProtobufHandler(service, metrics);
    default:
      throw new IllegalArgumentException("Unknown Avatica handler for " + serialization.name());
    }
  }

}

// End HandlerFactory.java
