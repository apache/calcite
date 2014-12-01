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

import java.util.HashMap;
import java.util.Map;

/**
 * Mock implementation of {@link Service}
 * that encodes its requests and responses as JSON
 * and looks up responses from a pre-defined map.
 */
public class MockJsonService extends JsonService {
  private final Map<String, String> map;

  public MockJsonService(Map<String, String> map) {
    this.map = map;
  }

  @Override public String apply(String request) {
    final String response = map.get(request);
    if (response == null) {
      throw new RuntimeException("No response for " + request);
    }
    return response;
  }

  /** Factory that creates a {@code MockJsonService}. */
  public static class Factory implements Service.Factory {
    public Service create(AvaticaConnection connection) {
      final Map<String, String> map1 = new HashMap<String, String>();
      map1.put(
          "{\"request\":\"getSchemas\",\"catalog\":null,\"schemaPattern\":{\"s\":null}}",
          "{\"response\":\"resultSet\", rows: []}");
      return new MockJsonService(map1);
    }
  }
}

// End MockJsonService.java
