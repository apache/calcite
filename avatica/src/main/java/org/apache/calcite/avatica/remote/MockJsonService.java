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

import org.apache.calcite.avatica.AvaticaConnection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Mock implementation of {@link Service}
 * that encodes its requests and responses as JSON
 * and looks up responses from a pre-defined map.
 */
public class MockJsonService extends JsonService {
  private final Map<String, String> map;
  private final ObjectMapper mapper = new ObjectMapper();

  public MockJsonService(Map<String, String> map) {
    this.map = map;
  }

  @Override public String apply(String request) {
    String response = map.get(canonicalizeConnectionId(request));
    if (response == null) {
      throw new RuntimeException("No response for " + request);
    }
    return response;
  }

  /**
   * The connection id is always different, therefore when present in the request,
   * set it to 0.
   */
  private String canonicalizeConnectionId(String request) {
    ObjectNode jsonRequest;
    try {
      jsonRequest = (ObjectNode) mapper.readTree(request);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (jsonRequest.get("connectionId") != null) {
      jsonRequest.put("connectionId", "0");
      return jsonRequest.toString();
    } else {
      return request;
    }
  }

  /** Factory that creates a {@code MockJsonService}. */
  public static class Factory implements Service.Factory {
    public Service create(AvaticaConnection connection) {
      final Map<String, String> map1 = new HashMap<>();
      try {
        map1.put(
            "{\"request\":\"openConnection\",\"connectionId\":\"0\",\"info\":{}}",
            "{\"response\":\"openConnection\"}");
        map1.put(
            "{\"request\":\"closeConnection\",\"connectionId\":\"0\"}",
            "{\"response\":\"closeConnection\"}");
        map1.put(
            "{\"request\":\"getSchemas\",\"catalog\":null,\"schemaPattern\":{\"s\":null}}",
            "{\"response\":\"resultSet\", updateCount: -1, firstFrame: {offset: 0, done: true, rows: []}}");
        map1.put(
            JsonService.encode(new SchemasRequest("0", null, null)),
            "{\"response\":\"resultSet\", updateCount: -1, firstFrame: {offset: 0, done: true, rows: []}}");
        map1.put(
            JsonService.encode(
                new TablesRequest("0", null, null, null, Arrays.<String>asList())),
            "{\"response\":\"resultSet\", updateCount: -1, firstFrame: {offset: 0, done: true, rows: []}}");
        map1.put(
            "{\"request\":\"createStatement\",\"connectionId\":0}",
            "{\"response\":\"createStatement\",\"id\":0}");
        map1.put(
            "{\"request\":\"prepareAndExecute\",\"statementId\":0,"
                + "\"sql\":\"select * from (\\n  values (1, 'a'), (null, 'b'), (3, 'c')) as t (c1, c2)\",\"maxRowCount\":-1}",
            "{\"response\":\"resultSet\", updateCount: -1, \"signature\": {\n"
                + " \"columns\": [\n"
                + "   {\"columnName\": \"C1\", \"type\": {type: \"scalar\", id: 4, rep: \"INTEGER\"}},\n"
                + "   {\"columnName\": \"C2\", \"type\": {type: \"scalar\", id: 12, rep: \"STRING\"}}\n"
                + " ], \"cursorFactory\": {\"style\": \"ARRAY\"}\n"
                + "}, \"rows\": [[1, \"a\"], [null, \"b\"], [3, \"c\"]]}");
        map1.put(
            "{\"request\":\"prepare\",\"statementId\":0,"
                + "\"sql\":\"select * from (\\n  values (1, 'a'), (null, 'b'), (3, 'c')) as t (c1, c2)\",\"maxRowCount\":-1}",
            "{\"response\":\"prepare\",\"signature\": {\n"
                + " \"columns\": [\n"
                + "   {\"columnName\": \"C1\", \"type\": {type: \"scalar\", id: 4, rep: \"INTEGER\"}},\n"
                + "   {\"columnName\": \"C2\", \"type\": {type: \"scalar\", id: 12, rep: \"STRING\"}}\n"
                + " ],\n"
                + " \"parameters\": [],\n"
                + " \"cursorFactory\": {\"style\": \"ARRAY\"}\n"
                + "}}");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return new MockJsonService(map1);
    }
  }
}

// End MockJsonService.java
