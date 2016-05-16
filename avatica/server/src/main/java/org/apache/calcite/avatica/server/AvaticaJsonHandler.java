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
import org.apache.calcite.avatica.remote.JsonHandler;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;
import org.apache.calcite.avatica.util.UnsynchronizedBuffer;

import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Callable;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.apache.calcite.avatica.remote.MetricsHelper.concat;

/**
 * Jetty handler that executes Avatica JSON request-responses.
 */
public class AvaticaJsonHandler extends AbstractAvaticaHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AvaticaJsonHandler.class);

  final Service service;
  final JsonHandler jsonHandler;

  final MetricsSystem metrics;
  final Timer requestTimer;

  final ThreadLocal<UnsynchronizedBuffer> threadLocalBuffer;

  final AvaticaServerConfiguration serverConfig;

  public AvaticaJsonHandler(Service service) {
    this(service, NoopMetricsSystem.getInstance(), null);
  }

  public AvaticaJsonHandler(Service service, MetricsSystem metrics) {
    this(service, metrics, null);
  }

  public AvaticaJsonHandler(Service service, MetricsSystem metrics,
      AvaticaServerConfiguration serverConfig) {
    this.service = Objects.requireNonNull(service);
    this.metrics = Objects.requireNonNull(metrics);
    // Avatica doesn't have a Guava dependency
    this.jsonHandler = new JsonHandler(service, this.metrics);

    // Metrics
    this.requestTimer = this.metrics.getTimer(
        concat(AvaticaJsonHandler.class, MetricsAwareAvaticaHandler.REQUEST_TIMER_NAME));

    this.threadLocalBuffer = new ThreadLocal<UnsynchronizedBuffer>() {
      @Override public UnsynchronizedBuffer initialValue() {
        return new UnsynchronizedBuffer();
      }
    };

    this.serverConfig = serverConfig;
  }

  public void handle(String target, Request baseRequest,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    try (final Context ctx = requestTimer.start()) {
      if (!isUserPermitted(serverConfig, request, response)) {
        LOG.debug("HTTP request from {} is unauthenticated and authentication is required",
            request.getRemoteAddr());
        return;
      }

      response.setContentType("application/json;charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);
      if (request.getMethod().equals("POST")) {
        // First look for a request in the header, then look in the body.
        // The latter allows very large requests without hitting HTTP 413.
        String rawRequest = request.getHeader("request");
        if (rawRequest == null) {
          // Avoid a new buffer creation for every HTTP request
          final UnsynchronizedBuffer buffer = threadLocalBuffer.get();
          try (ServletInputStream inputStream = request.getInputStream()) {
            rawRequest = AvaticaUtils.readFully(inputStream, buffer);
          } finally {
            // Reset the offset into the buffer after we're done
            buffer.reset();
          }
        }
        final String jsonRequest =
            new String(rawRequest.getBytes("ISO-8859-1"), "UTF-8");
        LOG.trace("request: {}", jsonRequest);

        HandlerResponse<String> jsonResponse;
        try {
          if (null != serverConfig && serverConfig.supportsImpersonation()) {
            jsonResponse = serverConfig.doAsRemoteUser(request.getRemoteUser(),
                request.getRemoteAddr(), new Callable<HandlerResponse<String>>() {
                  @Override public HandlerResponse<String> call() {
                    return jsonHandler.apply(jsonRequest);
                  }
                });
          } else {
            jsonResponse = jsonHandler.apply(jsonRequest);
          }
        } catch (Exception e) {
          LOG.debug("Error invoking request from {}", baseRequest.getRemoteAddr(), e);
          jsonResponse = jsonHandler.convertToErrorResponse(e);
        }

        LOG.trace("response: {}", jsonResponse);
        baseRequest.setHandled(true);
        // Set the status code and write out the response.
        response.setStatus(jsonResponse.getStatusCode());
        response.getWriter().println(jsonResponse.getResponse());
      }
    }
  }

  @Override public void setServerRpcMetadata(RpcMetadataResponse metadata) {
    // Set the metadata for the normal service calls
    service.setRpcMetadata(metadata);
    // Also add it to the handler to include with exceptions
    jsonHandler.setRpcMetadata(metadata);
  }

  @Override public MetricsSystem getMetrics() {
    return metrics;
  }
}

// End AvaticaJsonHandler.java
