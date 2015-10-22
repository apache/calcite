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
 * Implementation of {@link org.apache.calcite.avatica.remote.Service}
 * that translates requests into JSON and sends them to a remote server,
 * usually an HTTP server.
 */
public class RemoteService extends JsonService {
  private final URL url;

  public RemoteService(URL url) {
    this.url = url;
  }

  @Override public String apply(String request) {
    try {
      final HttpURLConnection connection =
          (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setDoInput(true);
      connection.setDoOutput(true);
      if (request.length() < 256) {
        connection.setRequestProperty("request", request);
      } else {
        try (DataOutputStream wr
            = new DataOutputStream(connection.getOutputStream())) {
          wr.writeBytes(request);
          wr.flush();
          wr.close();
        }
      }
      final int responseCode = connection.getResponseCode();
      final InputStream inputStream;
      if (responseCode != HttpURLConnection.HTTP_OK) {
        inputStream = connection.getErrorStream();
      } else {
        inputStream = connection.getInputStream();
      }

      return AvaticaUtils.readFully(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
}

// End RemoteService.java
