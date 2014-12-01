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

import com.fasterxml.jackson.core.JsonParser;
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
public class JsonHandler implements Handler {
  private final Service service;
  protected final ObjectMapper mapper;
  protected final StringWriter w = new StringWriter();

  public JsonHandler(Service service) {
    super();
    this.service = service;
    mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
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
    return mapper.readValue(request, valueType);
  }

  private <T> String encode(T response) throws IOException {
    assert w.getBuffer().length() == 0;
    mapper.writeValue(w, response);
    final String s = w.toString();
    w.getBuffer().setLength(0);
    return s;
  }

  protected RuntimeException handle(IOException e) {
    return new RuntimeException(e);
  }
}

// End JsonHandler.java
