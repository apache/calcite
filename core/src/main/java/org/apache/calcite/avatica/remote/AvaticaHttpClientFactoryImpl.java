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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Objects;

/**
 * Default implementation of {@link AvaticaHttpClientFactory} which chooses an implementation
 * from a property.
 */
public class AvaticaHttpClientFactoryImpl implements AvaticaHttpClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(AvaticaHttpClientFactoryImpl.class);

  public static final String HTTP_CLIENT_IMPL_DEFAULT =
      AvaticaCommonsHttpClientImpl.class.getName();
  public static final String SPNEGO_HTTP_CLIENT_IMPL_DEFAULT =
      AvaticaCommonsHttpClientSpnegoImpl.class.getName();

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

  @Override public AvaticaHttpClient getClient(URL url, ConnectionConfig config,
      KerberosConnection kerberosUtil) {
    String className = config.httpClientClass();
    if (null == className) {
      // Provide an implementation that works with SPNEGO if that's the authentication is use.
      if ("SPNEGO".equalsIgnoreCase(config.authentication())) {
        className = SPNEGO_HTTP_CLIENT_IMPL_DEFAULT;
      } else {
        className = HTTP_CLIENT_IMPL_DEFAULT;
      }
    }

    AvaticaHttpClient client = instantiateClient(className, url);
    if (null != kerberosUtil) {
      client = new DoAsAvaticaHttpClient(client, kerberosUtil);
    }

    if (client instanceof TrustStoreConfigurable) {
      File truststore = config.truststore();
      String truststorePassword = config.truststorePassword();
      if (null != truststore && null != truststorePassword) {
        ((TrustStoreConfigurable) client).setTrustStore(truststore, truststorePassword);
      }
    } else {
      LOG.debug("{} is not capable of SSL/TLS communication", client.getClass().getName());
    }

    if (client instanceof UsernamePasswordAuthenticateable) {
      // Shortcircuit quickly if authentication wasn't provided (implies NONE)
      final String authString = config.authentication();
      if (null == authString) {
        return client;
      }

      final AuthenticationType authType = AuthenticationType.valueOf(authString);
      final String username = config.avaticaUser();
      final String password = config.avaticaPassword();

      // Can't authenticate with NONE or w/o username and password
      if (isUserPasswordAuth(authType)) {
        if (null != username && null != password) {
          ((UsernamePasswordAuthenticateable) client)
              .setUsernamePassword(authType, username, password);
        } else {
          LOG.debug("Username or password was null");
        }
      } else {
        LOG.debug("{} is not capable of username/password authentication.", authType);
      }
    }

    return client;
  }

  private AvaticaHttpClient instantiateClient(String className, URL url) {
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

  private boolean isUserPasswordAuth(AuthenticationType authType) {
    return AuthenticationType.BASIC == authType || AuthenticationType.DIGEST == authType;
  }
}

// End AvaticaHttpClientFactoryImpl.java
