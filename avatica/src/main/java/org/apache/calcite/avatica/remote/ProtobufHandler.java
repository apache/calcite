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

import org.apache.calcite.avatica.metrics.MetricsUtil;
import org.apache.calcite.avatica.remote.Service.Response;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import java.io.IOException;

/**
 * Dispatches serialized protocol buffer messages to the provided {@link Service}
 * by converting them to the POJO Request. Returns back the serialized protocol
 * buffer response.
 */
public class ProtobufHandler extends AbstractHandler<byte[]> {

  private final ProtobufTranslation translation;
  private final MetricRegistry metrics;
  private final MetricsUtil metricsUtil;
  private final Timer serializationTimer;

  public ProtobufHandler(Service service, ProtobufTranslation translation, MetricRegistry metrics) {
    super(service);
    this.translation = translation;
    this.metrics = metrics;
    this.metricsUtil = MetricsUtil.getInstance();
    this.serializationTimer = metricsUtil.getTimer(this.metrics, ProtobufHandler.class,
        HANDLER_SERIALIZATION_METRICS_NAME);
  }

  @Override public HandlerResponse<byte[]> apply(byte[] requestBytes) {
    return super.apply(requestBytes);
  }

  @Override Service.Request decode(byte[] serializedRequest) throws IOException {
    Context ctx = metricsUtil.startTimer(serializationTimer);
    try {
      return translation.parseRequest(serializedRequest);
    } finally {
      if (null != ctx) {
        ctx.stop();
      }
    }
  }

  @Override byte[] encode(Response response) throws IOException {
    Context ctx = metricsUtil.startTimer(serializationTimer);
    try {
      return translation.serializeResponse(response);
    } finally {
      if (null != ctx) {
        ctx.stop();
      }
    }
  }
}

// End ProtobufHandler.java
