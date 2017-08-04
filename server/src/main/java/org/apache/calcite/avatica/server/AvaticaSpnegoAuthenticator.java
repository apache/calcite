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

import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.authentication.DeferredAuthentication;
import org.eclipse.jetty.security.authentication.SpnegoAuthenticator;
import org.eclipse.jetty.server.Authentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Custom SpnegoAuthenticator which will still reponse with a WWW-Authentication: Negotiate
 * header if the client provides some other kind of authentication header.
 */
public class AvaticaSpnegoAuthenticator extends SpnegoAuthenticator {
  private static final Logger LOG = LoggerFactory.getLogger(AvaticaSpnegoAuthenticator.class);

  @Override public Authentication validateRequest(ServletRequest request,
      ServletResponse response, boolean mandatory) throws ServerAuthException {
    final Authentication computedAuth = super.validateRequest(request, response, mandatory);
    try {
      return sendChallengeIfNecessary(computedAuth, request, response);
    } catch (IOException e) {
      throw new ServerAuthException(e);
    }
  }

  /**
   * Jetty has a bug in which if there is an Authorization header sent by a client which is
   * not of the Negotiate type, Jetty does not send the challenge to negotiate. This works
   * around that issue, forcing the challenge to be sent. Will require investigation on
   * upgrade to a newer version of Jetty.
   */
  Authentication sendChallengeIfNecessary(Authentication computedAuth, ServletRequest request,
      ServletResponse response) throws IOException {
    if (computedAuth == Authentication.UNAUTHENTICATED) {
      HttpServletRequest req = (HttpServletRequest) request;
      HttpServletResponse res = (HttpServletResponse) response;

      String header = req.getHeader(HttpHeader.AUTHORIZATION.asString());
      // We have an authorization header, but it's not Negotiate
      if (header != null && !header.startsWith(HttpHeader.NEGOTIATE.asString())) {
        LOG.debug("Client sent Authorization header that was not for Negotiate,"
            + " sending challenge anyways.");
        if (DeferredAuthentication.isDeferred(res)) {
          return Authentication.UNAUTHENTICATED;
        }

        res.setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), HttpHeader.NEGOTIATE.asString());
        res.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return Authentication.SEND_CONTINUE;
      }
    }
    return computedAuth;
  }
}

// End AvaticaSpnegoAuthenticator.java
