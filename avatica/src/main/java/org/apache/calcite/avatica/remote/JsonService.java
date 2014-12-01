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
 * Implementation of {@link org.apache.calcite.avatica.remote.Service}
 * that encodes requests and responses as JSON.
 */
public abstract class JsonService implements Service {
  protected final ObjectMapper mapper;
  protected final StringWriter w = new StringWriter();

  public JsonService() {
    mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
  }

  /** Derived class should implement this method to transport requests and
   * responses to and from the peer service. */
  public abstract String apply(String request);

  private <T> T decode(String response, Class<T> valueType) throws IOException {
    return mapper.readValue(response, valueType);
  }

  private <T> String encode(T request) throws IOException {
    assert w.getBuffer().length() == 0;
    mapper.writeValue(w, request);
    final String s = w.toString();
    w.getBuffer().setLength(0);
    return s;
  }

  protected RuntimeException handle(IOException e) {
    return new RuntimeException(e);
  }

  public ResultSetResponse apply(CatalogsRequest request) {
    try {
      return decode(apply(encode(request)), ResultSetResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ResultSetResponse apply(SchemasRequest request) {
    try {
      return decode(apply(encode(request)), ResultSetResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public PrepareResponse apply(PrepareRequest request) {
    try {
      return decode(apply(encode(request)), PrepareResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ResultSetResponse apply(PrepareAndExecuteRequest request) {
    try {
      return decode(apply(encode(request)), ResultSetResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public CreateStatementResponse apply(CreateStatementRequest request) {
    try {
      return decode(apply(encode(request)), CreateStatementResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }
}

// End JsonService.java
