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
 * ProtobufService implementation that queries against a remote implementation, using
 * protocol buffers as the serialized form.
 */
public class RemoteProtobufService extends ProtobufService {
  private final URL url;
  private final ProtobufTranslation translation;

  public RemoteProtobufService(URL url, ProtobufTranslation translation) {
    this.url = url;
    this.translation = translation;
  }

  @Override public Response _apply(Request request) {
    try {
      final HttpURLConnection connection =
          (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setDoInput(true);
      connection.setDoOutput(true);
      try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
        // Convert the Request to a protobuf and send it over the wire
        wr.write(translation.serializeRequest(request));
        wr.flush();
        wr.close();
      }
      final int responseCode = connection.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        InputStream errorStream = connection.getErrorStream();
        if (errorStream != null) {
          byte[] errorResponse = AvaticaUtils.readFullyToBytes(errorStream);
          ErrorResponse response = (ErrorResponse) translation.parseResponse(errorResponse);
          throw new RuntimeException("Remote driver error: " + response.message);
        } else {
          throw new RuntimeException("response code " + responseCode);
        }
      }
      final InputStream inputStream = connection.getInputStream();
      // Read the (serialized protobuf) response off the wire and convert it back to a Response
      return translation.parseResponse(AvaticaUtils.readFullyToBytes(inputStream));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

// End RemoteProtobufService.java
