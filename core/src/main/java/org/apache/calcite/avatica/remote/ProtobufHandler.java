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
import org.apache.calcite.avatica.remote.Service.Response;

import java.io.IOException;

/**
 * Dispatches serialized protocol buffer messages to the provided {@link Service}
 * by converting them to the POJO Request. Returns back the serialized protocol
 * buffer response.
 */
public class ProtobufHandler extends AbstractHandler<byte[]> {

  private final ProtobufTranslation translation;
  private final MetricsSystem metrics;
  private final Timer serializationTimer;

  public ProtobufHandler(Service service, ProtobufTranslation translation, MetricsSystem metrics) {
    super(service);
    this.translation = translation;
    this.metrics = metrics;
    this.serializationTimer = this.metrics.getTimer(
        MetricsHelper.concat(ProtobufHandler.class, HANDLER_SERIALIZATION_METRICS_NAME));
  }

  @Override public HandlerResponse<byte[]> apply(byte[] requestBytes) {
    return super.apply(requestBytes);
  }

  @Override Service.Request decode(byte[] serializedRequest) throws IOException {
    try (final Context ctx = serializationTimer.start()) {
      return translation.parseRequest(serializedRequest);
    }
  }

  @Override byte[] encode(Response response) throws IOException {
    try (final Context ctx = serializationTimer.start()) {
      return translation.serializeResponse(response);
    }
  }
}

// End ProtobufHandler.java
