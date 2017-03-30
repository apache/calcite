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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


/**
 * ProtobufService implementation that queries against a remote implementation, using
 * protocol buffers as the serialized form.
 */
public class RemoteProtobufService extends ProtobufService {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteProtobufService.class);

  private final AvaticaHttpClient client;
  private final ProtobufTranslation translation;

  public RemoteProtobufService(AvaticaHttpClient client, ProtobufTranslation translation) {
    this.client = client;
    this.translation = translation;
  }

  @Override public Response _apply(Request request) {
    final Response resp;
    byte[] response = null;
    try {
      response = client.send(translation.serializeRequest(request));
    } catch (IOException e) {
      LOG.debug("Failed to execute remote request: {}", request);
      // Failed to get a response from the server for the request.
      throw new RuntimeException(e);
    }

    try {
      resp = translation.parseResponse(response);
    } catch (IOException e) {
      LOG.debug("Failed to deserialize reponse to {}. '{}'", request,
          new String(response, StandardCharsets.UTF_8));
      // Not a protobuf that we could parse.
      throw new RuntimeException(e);
    }

    // The server had an error, throw an Exception for that.
    if (resp instanceof ErrorResponse) {
      throw ((ErrorResponse) resp).toException();
    }

    return resp;
  }
}

// End RemoteProtobufService.java
