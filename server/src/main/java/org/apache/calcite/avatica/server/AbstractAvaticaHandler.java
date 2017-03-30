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

import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.remote.AuthenticationType;
import org.apache.calcite.avatica.remote.Service.ErrorResponse;

import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Base-class for Avatica implemented Jetty Handlers.
 */
public abstract class AbstractAvaticaHandler extends AbstractHandler
    implements MetricsAwareAvaticaHandler {

  private static final ErrorResponse UNAUTHORIZED_ERROR = new ErrorResponse(
      Collections.<String>emptyList(), "User is not authenticated",
      ErrorResponse.UNAUTHORIZED_ERROR_CODE, ErrorResponse.UNAUTHORIZED_SQL_STATE,
      AvaticaSeverity.ERROR, null);

  /**
   * Determines if a request is permitted to be executed. The server may require authentication
   * and the login mechanism might have failed. This check verifies that only authenticated
   * users are permitted through when the server is requiring authentication. When a user
   * is disallowed, a status code and response will be automatically written to the provided
   * <code>response</code> and the caller should return immediately.
   *
   * @param serverConfig The server's configuration
   * @param request The user's request
   * @param response The response to the user's request
   * @return True if request can proceed, false otherwise.
   */
  public boolean isUserPermitted(AvaticaServerConfiguration serverConfig,
      HttpServletRequest request, HttpServletResponse response) throws IOException {
    // Make sure that we drop any unauthenticated users out first.
    if (null != serverConfig) {
      if (AuthenticationType.SPNEGO == serverConfig.getAuthenticationType()) {
        String remoteUser = request.getRemoteUser();
        if (null == remoteUser) {
          response.setStatus(HttpURLConnection.HTTP_UNAUTHORIZED);
          response.getOutputStream().write(UNAUTHORIZED_ERROR.serialize().toByteArray());
          return false;
        }
      }
    }

    return true;
  }
}

// End AbstractAvaticaHandler.java
