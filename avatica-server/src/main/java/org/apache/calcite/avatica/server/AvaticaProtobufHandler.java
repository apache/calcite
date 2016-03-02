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
package org.apache.calcite.avatica.server;

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.Timer;
import org.apache.calcite.avatica.metrics.Timer.Context;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.remote.Handler.HandlerResponse;
import org.apache.calcite.avatica.remote.MetricsHelper;
import org.apache.calcite.avatica.remote.ProtobufHandler;
import org.apache.calcite.avatica.remote.ProtobufTranslation;
import org.apache.calcite.avatica.remote.ProtobufTranslationImpl;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;
import org.apache.calcite.avatica.util.UnsynchronizedBuffer;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Jetty handler that executes Avatica JSON request-responses.
 */
public class AvaticaProtobufHandler extends AbstractHandler implements MetricsAwareAvaticaHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AvaticaJsonHandler.class);

  private final Service service;
  private final ProtobufHandler pbHandler;
  private final ProtobufTranslation protobufTranslation;
  private final MetricsSystem metrics;
  private final Timer requestTimer;

  final ThreadLocal<UnsynchronizedBuffer> threadLocalBuffer;

  public AvaticaProtobufHandler(Service service) {
    this(service, NoopMetricsSystem.getInstance());
  }

  public AvaticaProtobufHandler(Service service, MetricsSystem metrics) {
    this.service = Objects.requireNonNull(service);
    this.metrics = Objects.requireNonNull(metrics);

    this.requestTimer = this.metrics.getTimer(
        MetricsHelper.concat(AvaticaProtobufHandler.class,
            MetricsAwareAvaticaHandler.REQUEST_TIMER_NAME));

    this.protobufTranslation = new ProtobufTranslationImpl();
    this.pbHandler = new ProtobufHandler(service, protobufTranslation, metrics);

    this.threadLocalBuffer = new ThreadLocal<UnsynchronizedBuffer>() {
      @Override public UnsynchronizedBuffer initialValue() {
        return new UnsynchronizedBuffer();
      }
    };
  }

  public void handle(String target, Request baseRequest,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    try (final Context ctx = this.requestTimer.start()) {
      response.setContentType("application/octet-stream;charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);
      if (request.getMethod().equals("POST")) {
        byte[] requestBytes;
        // Avoid a new buffer creation for every HTTP request
        final UnsynchronizedBuffer buffer = threadLocalBuffer.get();
        try (ServletInputStream inputStream = request.getInputStream()) {
          requestBytes = AvaticaUtils.readFullyToBytes(inputStream, buffer);
        } finally {
          buffer.reset();
        }

        HandlerResponse<byte[]> handlerResponse = pbHandler.apply(requestBytes);

        baseRequest.setHandled(true);
        response.setStatus(handlerResponse.getStatusCode());
        response.getOutputStream().write(handlerResponse.getResponse());
      }
    }
  }

  @Override public void setServerRpcMetadata(RpcMetadataResponse metadata) {
    // Set the metadata for the normal service calls
    service.setRpcMetadata(metadata);
    // Also add it to the handler to include with exceptions
    pbHandler.setRpcMetadata(metadata);
  }

  @Override public MetricsSystem getMetrics() {
    return this.metrics;
  }

}

// End AvaticaProtobufHandler.java
