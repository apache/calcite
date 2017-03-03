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

import org.apache.calcite.avatica.remote.AuthenticationType;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * Test class for logic common to all {@link AvaticaHandler}'s.
 */
public class AbstractAvaticaHandlerTest {

  private AbstractAvaticaHandler handler;
  private AvaticaServerConfiguration config;
  private HttpServletRequest request;
  private HttpServletResponse response;

  @Before public void setup() throws Exception {
    handler = mock(AbstractAvaticaHandler.class);
    config = mock(AvaticaServerConfiguration.class);
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    when(handler.isUserPermitted(config, request, response)).thenCallRealMethod();
  }

  @Test public void disallowUnauthenticatedUsers() throws Exception {
    ServletOutputStream os = mock(ServletOutputStream.class);

    when(config.getAuthenticationType()).thenReturn(AuthenticationType.SPNEGO);
    when(request.getRemoteUser()).thenReturn(null);
    when(response.getOutputStream()).thenReturn(os);

    assertFalse(handler.isUserPermitted(config, request, response));

    verify(response).setStatus(HttpURLConnection.HTTP_UNAUTHORIZED);
    // Make sure that the serialized ErrorMessage looks reasonable
    verify(os).write(argThat(new BaseMatcher<byte[]>() {
      @Override public void describeTo(Description description) {
        String desc = "A serialized ErrorMessage which contains 'User is not authenticated'";
        description.appendText(desc);
      }

      @Override public boolean matches(Object item) {
        String msg = new String((byte[]) item, StandardCharsets.UTF_8);
        return msg.contains("User is not authenticated");
      }

      @Override public void describeMismatch(Object item, Description mismatchDescription) {
        mismatchDescription.appendText("The message should contain 'User is not authenticated'");
      }
    }));
  }

  @Test public void allowAuthenticatedUsers() throws Exception {
    when(config.getAuthenticationType()).thenReturn(AuthenticationType.SPNEGO);
    when(request.getRemoteUser()).thenReturn("user1");
    assertTrue(handler.isUserPermitted(config, request, response));
  }

  @Test public void allowAllUsersWhenNoAuthenticationIsNeeded() throws Exception {
    when(config.getAuthenticationType()).thenReturn(AuthenticationType.NONE);
    when(request.getRemoteUser()).thenReturn(null);
    assertTrue(handler.isUserPermitted(config, request, response));

    when(request.getRemoteUser()).thenReturn("user1");
    assertTrue(handler.isUserPermitted(config, request, response));
  }
}

// End AbstractAvaticaHandlerTest.java
