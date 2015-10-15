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
import org.apache.calcite.avatica.remote.JsonHandler;
import org.apache.calcite.avatica.remote.Service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Jetty handler that executes Avatica JSON request-responses.
 */
public class AvaticaHandler extends AbstractHandler {
  private static final Log LOG = LogFactory.getLog(AvaticaHandler.class);

  final JsonHandler jsonHandler;

  public AvaticaHandler(Service service) {
    this.jsonHandler = new JsonHandler(service);
  }

  public void handle(String target, Request baseRequest,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
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
      if (LOG.isTraceEnabled()) {
        LOG.trace("request: " + jsonRequest);
      }
      String jsonResponse;
      try {
        jsonResponse = jsonHandler.apply(jsonRequest);
      } catch (Throwable t) {
        LOG.error("Error handling request: " + jsonRequest, t);
        response.setStatus(500);
        jsonResponse = createErrorResponse(t);
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("response: " + jsonResponse);
      }
      baseRequest.setHandled(true);
      response.getWriter().println(jsonResponse);
    }
  }

  private String createErrorResponse(Throwable t) throws IOException {
    return jsonHandler.encode(new Service.ErrorResponse(getErrorMessage(t)));
  }

  public static String getErrorMessage(Throwable t) {
    return Joiner.on(" -> ").join(
        Iterables.transform(Throwables.getCausalChain(t), new Function<Throwable, String>() {
          @Override
          public String apply(Throwable input) {
            return input.getMessage() == null
                ? "(null exception message)" : input.getMessage();
          }
        }));
  }
}

// End AvaticaHandler.java
