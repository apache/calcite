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

import org.apache.calcite.avatica.AvaticaUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * A common class to invoke HTTP requests against the Avatica server agnostic of the data being
 * sent and received across the wire.
 */
public class AvaticaHttpClientImpl implements AvaticaHttpClient {
  protected final URL url;

  public AvaticaHttpClientImpl(URL url) {
    this.url = url;
  }

  public byte[] send(byte[] request) {
    // TODO back-off policy?
    while (true) {
      try {
        final HttpURLConnection connection = openConnection();
        connection.setRequestMethod("POST");
        connection.setDoInput(true);
        connection.setDoOutput(true);
        try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
          wr.write(request);
          wr.flush();
          wr.close();
        }
        final int responseCode = connection.getResponseCode();
        final InputStream inputStream;
        if (responseCode == HttpURLConnection.HTTP_UNAVAILABLE) {
          // Could be sitting behind a load-balancer, try again.
          continue;
        } else if (responseCode != HttpURLConnection.HTTP_OK) {
          inputStream = connection.getErrorStream();
        } else {
          inputStream = connection.getInputStream();
        }
        return AvaticaUtils.readFullyToBytes(inputStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  HttpURLConnection openConnection() throws IOException {
    return (HttpURLConnection) url.openConnection();
  }
}

// End AvaticaHttpClientImpl.java
