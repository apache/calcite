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
import org.apache.calcite.avatica.metrics.MetricsUtil;
import org.apache.calcite.avatica.remote.Handler.HandlerResponse;
import org.apache.calcite.avatica.remote.JsonHandler;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.base.Optional;

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
public class AvaticaJsonHandler extends AbstractHandler implements MetricsAwareAvaticaHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AvaticaJsonHandler.class);

  final Service service;
  final JsonHandler jsonHandler;

  final Optional<MetricRegistry> metrics;
  final Timer requestTimer;
  final MetricsUtil metricsUtil;

  public AvaticaJsonHandler(Service service) {
    this(service, Optional.<MetricRegistry>absent());
  }

  public AvaticaJsonHandler(Service service, Optional<MetricRegistry> metrics) {
    this.service = Objects.requireNonNull(service);
    this.metrics = Objects.requireNonNull(metrics);
    // Avatica doesn't have a Guava dependency
    this.jsonHandler = new JsonHandler(service, this.metrics.orNull());

    // Metrics
    this.metricsUtil = MetricsUtil.getInstance();
    this.requestTimer = metricsUtil.getTimer(this.metrics.orNull(), AvaticaJsonHandler.class,
        MetricsAwareAvaticaHandler.REQUEST_TIMER_NAME);
  }

  public void handle(String target, Request baseRequest,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    final Context ctx = metricsUtil.startTimer(requestTimer);
    try {
      response.setContentType("application/json;charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);
      if (request.getMethod().equals("POST")) {
        // First look for a request in the header, then look in the body.
        // The latter allows very large requests without hitting HTTP 413.
        String rawRequest = request.getHeader("request");
        if (rawRequest == null) {
          try (ServletInputStream inputStream = request.getInputStream()) {
            rawRequest = AvaticaUtils.readFully(inputStream);
          }
        }
        final String jsonRequest =
            new String(rawRequest.getBytes("ISO-8859-1"), "UTF-8");
        LOG.trace("request: {}", jsonRequest);

        final HandlerResponse<String> jsonResponse = jsonHandler.apply(jsonRequest);
        LOG.trace("response: {}", jsonResponse);
        baseRequest.setHandled(true);
        // Set the status code and write out the response.
        response.setStatus(jsonResponse.getStatusCode());
        response.getWriter().println(jsonResponse.getResponse());
      }
    } finally {
      if (null != ctx) {
        ctx.stop();
      }
    }
  }

  @Override public void setServerRpcMetadata(RpcMetadataResponse metadata) {
    // Set the metadata for the normal service calls
    service.setRpcMetadata(metadata);
    // Also add it to the handler to include with exceptions
    jsonHandler.setRpcMetadata(metadata);
  }

  @Override public MetricRegistry getMetrics() {
    return metrics.orNull();
  }
}

// End AvaticaJsonHandler.java
