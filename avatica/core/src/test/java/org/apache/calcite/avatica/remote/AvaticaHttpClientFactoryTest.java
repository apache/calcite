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

import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.ConnectionConfigImpl;

import org.junit.Test;

import java.net.URL;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the factory that creates http clients.
 */
public class AvaticaHttpClientFactoryTest {

  @Test public void testDefaultHttpClient() throws Exception {
    Properties props = new Properties();
    URL url = new URL("http://localhost:8765");
    ConnectionConfig config = new ConnectionConfigImpl(props);
    AvaticaHttpClientFactory httpClientFactory = new AvaticaHttpClientFactoryImpl();

    AvaticaHttpClient client = httpClientFactory.getClient(url, config, null);
    assertTrue("Client was an instance of " + client.getClass(),
        client instanceof AvaticaCommonsHttpClientImpl);
  }

  @Test public void testOverridenHttpClient() throws Exception {
    Properties props = new Properties();
    props.setProperty(BuiltInConnectionProperty.HTTP_CLIENT_IMPL.name(),
        AvaticaHttpClientImpl.class.getName());
    URL url = new URL("http://localhost:8765");
    ConnectionConfig config = new ConnectionConfigImpl(props);
    AvaticaHttpClientFactory httpClientFactory = new AvaticaHttpClientFactoryImpl();

    AvaticaHttpClient client = httpClientFactory.getClient(url, config, null);
    assertTrue("Client was an instance of " + client.getClass(),
        client instanceof AvaticaHttpClientImpl);
  }
}

// End AvaticaHttpClientFactoryTest.java
