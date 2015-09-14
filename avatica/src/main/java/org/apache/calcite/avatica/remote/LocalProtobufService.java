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
 * A Service implementation that performs protocol buffer serialization on request and responses
 * on either side of computing a response from a request to mimic some transport to a server which
 * would normally perform such computation.
 */
public class LocalProtobufService extends ProtobufService {
  private final Service service;
  private final ProtobufTranslation translation;

  public LocalProtobufService(Service service, ProtobufTranslation translation) {
    this.service = service;
    this.translation = translation;
  }

  @Override public Response _apply(Request request) {
    try {
      // Serialize the request to "send to the server"
      byte[] serializedRequest = translation.serializeRequest(request);

      // *some transport would normally happen here*

      // Fake deserializing that request somewhere else
      Request request2 = translation.parseRequest(serializedRequest);

      // Serialize the response from the service to "send to the client"
      byte[] serializedResponse = translation.serializeResponse(request2.accept(service));

      // *some transport would normally happen here*

      // Deserialize the response on "the client"
      return translation.parseResponse(serializedResponse);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

// End LocalProtobufService.java
