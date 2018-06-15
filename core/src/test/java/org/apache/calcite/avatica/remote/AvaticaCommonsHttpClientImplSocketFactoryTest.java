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

import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.ssl.SSLContextBuilder;

import org.junit.Test;

import java.io.File;
import java.net.URL;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests to verify loading of truststore/keystore in AvaticaCommonsHttpClientImpl
 */
public class AvaticaCommonsHttpClientImplSocketFactoryTest {

  private static final String HTTP_REGISTRY = "http";
  private static final String HTTPS_REGISTRY = "https";

  private URL url;
  private AvaticaCommonsHttpClientImpl client;
  private File storeFile;
  private String password;

  @Test public void testPlainSocketFactory() throws Exception {
    configureHttpClient();
    verifyFactoryInstance(client, HTTP_REGISTRY, PlainConnectionSocketFactory.class);
    verifyFactoryInstance(client, HTTPS_REGISTRY, null);
    verify(client, times(0)).loadTrustStore(any(SSLContextBuilder.class));
    verify(client, times(0)).loadKeyStore(any(SSLContextBuilder.class));
  }

  @Test public void testTrustStoreLoadedInFactory() throws Exception {
    configureHttpsClient();
    client.setTrustStore(storeFile, password);
    verifyFactoryInstance(client, HTTP_REGISTRY, null);
    verifyFactoryInstance(client, HTTPS_REGISTRY, SSLConnectionSocketFactory.class);
    verify(client, times(1)).configureSocketFactories();
    verify(client, times(1)).loadTrustStore(any(SSLContextBuilder.class));
    verify(client, times(0)).loadKeyStore(any(SSLContextBuilder.class));
  }

  @Test public void testKeyStoreLoadedInFactory() throws Exception {
    configureHttpsClient();
    client.setKeyStore(storeFile, password, password);
    verifyFactoryInstance(client, HTTP_REGISTRY, null);
    verifyFactoryInstance(client, HTTPS_REGISTRY, SSLConnectionSocketFactory.class);
    verify(client, times(1)).configureSocketFactories();
    verify(client, times(0)).loadTrustStore(any(SSLContextBuilder.class));
    verify(client, times(1)).loadKeyStore(any(SSLContextBuilder.class));
  }

  private void configureHttpClient() throws Exception {
    url = new URL("http://fake_url.com");
    configureClient();
  }

  private void configureHttpsClient() throws Exception {
    url = new URL("https://fake_url.com");
    configureClient();
  }

  private void configureClient() throws Exception {
    client = spy(new AvaticaCommonsHttpClientImpl(url));
    // storeFile can be used as either Keystore/Truststore
    storeFile = mock(File.class);
    when(storeFile.exists()).thenReturn(true);
    when(storeFile.isFile()).thenReturn(true);
    password = "";

    doNothing().when(client).loadTrustStore(any(SSLContextBuilder.class));
    doNothing().when(client).loadKeyStore(any(SSLContextBuilder.class));
  }

  <T> void verifyFactoryInstance(AvaticaCommonsHttpClientImpl client,
      String registry, Class<T> expected) {
    ConnectionSocketFactory factory = client.socketFactoryRegistry.lookup(registry);
    if (expected == null) {
      assertTrue("Factory for registry " + registry + " expected as null", factory == null);
    } else {
      assertTrue("Factory for registry " + registry + " expected of type " + expected.getName(),
              expected.equals(factory.getClass()));
    }
  }
}

// End AvaticaCommonsHttpClientImplSocketFactoryTest.java
