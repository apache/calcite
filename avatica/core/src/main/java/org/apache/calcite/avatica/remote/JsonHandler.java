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

import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.Timer;
import org.apache.calcite.avatica.metrics.Timer.Context;
import org.apache.calcite.avatica.remote.Service.Request;
import org.apache.calcite.avatica.remote.Service.Response;

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
public class JsonHandler extends AbstractHandler<String> {

  protected static final ObjectMapper MAPPER = JsonService.MAPPER;

  final MetricsSystem metrics;
  final Timer serializationTimer;

  public JsonHandler(Service service, MetricsSystem metrics) {
    super(service);
    this.metrics = metrics;
    this.serializationTimer = this.metrics.getTimer(
        MetricsHelper.concat(JsonHandler.class, HANDLER_SERIALIZATION_METRICS_NAME));
  }

  public HandlerResponse<String> apply(String jsonRequest) {
    return super.apply(jsonRequest);
  }

  @Override Request decode(String request) throws IOException {
    try (final Context ctx = serializationTimer.start()) {
      return MAPPER.readValue(request, Service.Request.class);
    }
  }

  /**
   * Serializes the provided object as JSON.
   *
   * @param response The object to serialize.
   * @return A JSON string.
   */
  @Override String encode(Response response) throws IOException {
    try (final Context ctx = serializationTimer.start()) {
      final StringWriter w = new StringWriter();
      MAPPER.writeValue(w, response);
      return w.toString();
    }
  }
}

// End JsonHandler.java
