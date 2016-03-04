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
import java.io.StringWriter;

/**
 * Implementation of {@link org.apache.calcite.avatica.remote.Service}
 * that goes to an in-process instance of {@code Service}.
 */
public class LocalJsonService extends JsonService {
  private final Service service;

  public LocalJsonService(Service service) {
    this.service = service;
  }

  @Override public String apply(String request) {
    try {
      Request request2 = MAPPER.readValue(request, Request.class);
      Response response2 = request2.accept(service);
      final StringWriter w = new StringWriter();
      MAPPER.writeValue(w, response2);
      return w.toString();
    } catch (IOException e) {
      throw handle(e);
    }
  }
}

// End LocalJsonService.java
