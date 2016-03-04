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

import java.io.IOException;

/**
 * ProtobufService implementation that queries against a remote implementation, using
 * protocol buffers as the serialized form.
 */
public class RemoteProtobufService extends ProtobufService {
  private final AvaticaHttpClient client;
  private final ProtobufTranslation translation;

  public RemoteProtobufService(AvaticaHttpClient client, ProtobufTranslation translation) {
    this.client = client;
    this.translation = translation;
  }

  @Override public Response _apply(Request request) {
    final Response resp;
    try {
      byte[] response = client.send(translation.serializeRequest(request));
      resp = translation.parseResponse(response);
    } catch (IOException e) {
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
