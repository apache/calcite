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
import org.eclipse.jetty.server.Authentication;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Test class for AvaticaSpnegoAuthenticator.
 */
public class AvaticaSpnegoAuthenticatorTest {

  private HttpServletRequest request;
  private HttpServletResponse response;
  private AvaticaSpnegoAuthenticator authenticator;

  @Before public void setup() {
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    authenticator = new AvaticaSpnegoAuthenticator();
  }

  @Test public void testAuthenticatedDoesNothingExtra() throws IOException {
    List<Authentication> authsNotRequiringUpdate = Arrays.asList(Authentication.NOT_CHECKED,
        Authentication.SEND_CONTINUE, Authentication.SEND_FAILURE, Authentication.SEND_SUCCESS);
    for (Authentication auth : authsNotRequiringUpdate) {
      assertEquals(auth, authenticator.sendChallengeIfNecessary(auth, request, response));
      verifyZeroInteractions(request);
      verifyZeroInteractions(response);
    }
  }

  @Test public void testChallengeSendOnBasicAuthorization() throws IOException {
    when(request.getHeader("Authorization")).thenReturn("Basic asdf");
    assertEquals(Authentication.SEND_CONTINUE,
        authenticator.sendChallengeIfNecessary(Authentication.UNAUTHENTICATED, request,
            response));
    verify(response).setHeader(HttpHeader.WWW_AUTHENTICATE.toString(),
        HttpHeader.NEGOTIATE.asString());
    verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED);
  }
}

// End AvaticaSpnegoAuthenticatorTest.java
