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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Implementation of {@link org.apache.calcite.avatica.remote.Handler}
 * that decodes JSON requests, sends them to a {@link Service},
 * and encodes the responses into JSON.
 *
 * @see org.apache.calcite.avatica.remote.JsonService
 */
public class JsonHandler implements Handler<String> {
  private final Service service;

  protected static final ObjectMapper MAPPER = JsonService.MAPPER;

  public JsonHandler(Service service) {
    this.service = service;
  }

  public String apply(String jsonRequest) {
    try {
      Service.Request request = decode(jsonRequest, Service.Request.class);
      final Service.Response response = request.accept(service);
      return encode(response);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  private <T> T decode(String request, Class<T> valueType) throws IOException {
    return MAPPER.readValue(request, valueType);
  }

  /**
   * Serializes the provided object as JSON.
   *
   * @param response The object to serialize.
   * @return A JSON string.
   */
  public <T> String encode(T response) throws IOException {
    final StringWriter w = new StringWriter();
    MAPPER.writeValue(w, response);
    return w.toString();
  }

  protected RuntimeException handle(IOException e) {
    return new RuntimeException(e);
  }
}

// End JsonHandler.java
