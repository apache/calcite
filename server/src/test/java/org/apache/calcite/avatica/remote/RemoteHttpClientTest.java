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
import org.apache.calcite.avatica.ConnectionSpec;
import org.apache.calcite.avatica.server.HttpServer;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;

/**
 * Tests for the HttpClient which manipulate things at the HTTP layer instead of Avatica's
 * "application" layer.
 */
@RunWith(Parameterized.class)
public class RemoteHttpClientTest {
  private static Map<String, String> getHeaders() {
    return HEADERS_REF.get();
  }

  private static final AtomicReference<Map<String, String>> HEADERS_REF = new AtomicReference<>();
  private static final AvaticaServersForTest SERVERS = new AvaticaServersForTest();

  @Parameters(name = "{0}")
  public static List<Object[]> parameters() throws Exception {
    SERVERS.startServers();
    return SERVERS.getJUnitParameters();
  }

  @AfterClass public static void afterClass() throws Exception {
    if (null != SERVERS) {
      SERVERS.stopServers();
    }
  }

  private final HttpServer server;
  private final String url;
  private final int port;
  private final Driver.Serialization serialization;

  public RemoteHttpClientTest(Driver.Serialization serialization, HttpServer server) {
    this.server = server;
    this.port = this.server.getPort();
    this.serialization = serialization;
    this.url = SERVERS.getJdbcUrl(port, serialization);
  }

  static String generateString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append('a');
    }
    return sb.toString();
  }

  @Test public void testLargeHeaders() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    // 32K header. Relies on the default value of 64K from HttpServer.Builder
    HEADERS_REF.set(Collections.singletonMap("MyLargeHeader", generateString(1024 * 32)));
    String modifiedUrl = url + ";httpclient_factory=" + HttpClientFactoryForTest.class.getName();
    try (Connection conn = DriverManager.getConnection(modifiedUrl);
        Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("CREATE TABLE largeHeaders(pk integer not null primary key)"));
      assertFalse(stmt.execute("DROP TABLE largeHeaders"));
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  /**
   * Factory class to inject a custom HttpClient
   */
  public static class HttpClientFactoryForTest implements AvaticaHttpClientFactory {
    @Override public AvaticaHttpClient getClient(URL url, ConnectionConfig config,
        KerberosConnection kerberosUtil) {
      Map<String, String> headers = getHeaders();
      return new HeaderInjectingHttpClient(headers, url);
    }
  }

  /**
   * HttpClient implementation which supports injecting custom HTTP headers
   */
  public static class HeaderInjectingHttpClient extends AvaticaHttpClientImpl {
    private final Map<String, String> headers;

    public HeaderInjectingHttpClient(Map<String, String> headers, URL url) {
      super(url);
      this.headers = Objects.requireNonNull(headers);
    }

    @Override HttpURLConnection openConnection() throws IOException {
      HttpURLConnection connection = super.openConnection();
      for (Entry<String, String> entry : headers.entrySet()) {
        connection.setRequestProperty(entry.getKey(), entry.getValue());
      }
      return connection;
    }
  }
}
// End RemoteHttpClientTest.java
