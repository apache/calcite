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

import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for the HTTP transport.
 */
public class AvaticaHttpClientTest {
  private static final String REQUEST =
      "{\"request\":\"createStatement\",\"connectionId\":\"8f3f28ee-d0bb-4cdb-a4b1-8f6e8476c534\"}";
  private static final String RESPONSE =
      "{\"response\":\"createStatement\",\"connectionId\":"
          + "\"8f3f28ee-d0bb-4cdb-a4b1-8f6e8476c534\",\"statementId\":1608176856}";

  @Test
  public void testRetryOnUnavailable() throws Exception {
    // HTTP-503, try again
    URL url = new URL("http://127.0.0.1:8765");
    final HttpURLConnection cnxn = Mockito.mock(HttpURLConnection.class);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ByteArrayInputStream bais = new ByteArrayInputStream(RESPONSE.getBytes(StandardCharsets.UTF_8));

    // Create the HTTP client
    AvaticaHttpClientImpl client = new AvaticaHttpClientImpl(url) {
      @Override HttpURLConnection openConnection() throws IOException {
        return cnxn;
      }
    };

    // HTTP 503 then 200
    Mockito.when(cnxn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_UNAVAILABLE,
        HttpURLConnection.HTTP_OK);

    Mockito.when(cnxn.getOutputStream()).thenReturn(baos);
    Mockito.when(cnxn.getInputStream()).thenReturn(bais);

    byte[] response = client.send(REQUEST.getBytes(StandardCharsets.UTF_8));

    assertArrayEquals(RESPONSE.getBytes(StandardCharsets.UTF_8), response);
  }

  @Test(expected = RuntimeException.class)
  public void testServerError() throws Exception {
    // HTTP 500 should error out
    URL url = new URL("http://127.0.0.1:8765");
    final HttpURLConnection cnxn = Mockito.mock(HttpURLConnection.class);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Create the HTTP client
    AvaticaHttpClientImpl client = new AvaticaHttpClientImpl(url) {
      @Override HttpURLConnection openConnection() throws IOException {
        return cnxn;
      }
    };

    // HTTP 500
    Mockito.when(cnxn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_INTERNAL_ERROR);

    Mockito.when(cnxn.getOutputStream()).thenReturn(baos);

    // Should throw an RTE
    client.send(REQUEST.getBytes(StandardCharsets.UTF_8));
  }

}

// End AvaticaHttpClientTest.java
