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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.ConnectionConfig;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Objects;

/**
 * Default implementation of {@link AvaticaHttpClientFactory} which chooses an implementation
 * from a property.
 */
public class AvaticaHttpClientFactoryImpl implements AvaticaHttpClientFactory {
  public static final String HTTP_CLIENT_IMPL_DEFAULT =
      AvaticaCommonsHttpClientImpl.class.getName();

  // Public for Type.PLUGIN
  public static final AvaticaHttpClientFactoryImpl INSTANCE = new AvaticaHttpClientFactoryImpl();

  // Public for Type.PLUGIN
  public AvaticaHttpClientFactoryImpl() {}

  /**
   * Returns a singleton instance of {@link AvaticaHttpClientFactoryImpl}.
   *
   * @return A singleton instance.
   */
  public static AvaticaHttpClientFactoryImpl getInstance() {
    return INSTANCE;
  }

  @Override public AvaticaHttpClient getClient(URL url, ConnectionConfig config) {
    String className = config.httpClientClass();
    if (null == className) {
      className = HTTP_CLIENT_IMPL_DEFAULT;
    }

    try {
      Class<?> clz = Class.forName(className);
      Constructor<?> constructor = clz.getConstructor(URL.class);
      Object instance = constructor.newInstance(Objects.requireNonNull(url));
      return AvaticaHttpClient.class.cast(instance);
    } catch (Exception e) {
      throw new RuntimeException("Failed to construct AvaticaHttpClient implementation "
          + className, e);
    }
  }
}

// End AvaticaHttpClientFactoryImpl.java
