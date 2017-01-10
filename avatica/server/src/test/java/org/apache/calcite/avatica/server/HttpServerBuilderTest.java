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

import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.Service;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

/**
 * Test class for {@link HttpServer}.
 */
public class HttpServerBuilderTest {

  @Test public void extraAllowedRolesConfigured() {
    final String[] extraRoles = new String[] {"BAR.COM"};
    final Service mockService = Mockito.mock(Service.class);
    HttpServer server = new HttpServer.Builder()
        .withSpnego("HTTP/localhost.localdomain@EXAMPLE.COM", extraRoles)
        .withHandler(mockService, Serialization.JSON)
        .build();

    assertArrayEquals(extraRoles, server.getConfig().getAllowedRoles());

    assertArrayEquals(new String[] {"EXAMPLE.COM", "BAR.COM"},
        server.getAllowedRealms("EXAMPLE.COM", server.getConfig()));
  }

  @Test public void lotsOfExtraRoles() {
    final String[] extraRoles = new String[] {"BAR.COM", "BAZ.COM", "FOO.COM"};
    final Service mockService = Mockito.mock(Service.class);
    HttpServer server = new HttpServer.Builder()
        .withSpnego("HTTP/localhost.localdomain@EXAMPLE.COM", extraRoles)
        .withHandler(mockService, Serialization.JSON)
        .build();

    assertArrayEquals(extraRoles, server.getConfig().getAllowedRoles());

    assertArrayEquals(new String[] {"EXAMPLE.COM", "BAR.COM", "BAZ.COM", "FOO.COM"},
        server.getAllowedRealms("EXAMPLE.COM", server.getConfig()));
  }

  @Test public void nullExtraRoles() {
    final String[] extraRoles = null;
    final Service mockService = Mockito.mock(Service.class);
    HttpServer server = new HttpServer.Builder()
        .withSpnego("HTTP/localhost.localdomain@EXAMPLE.COM", extraRoles)
        .withHandler(mockService, Serialization.JSON)
        .build();

    assertNull(server.getConfig().getAllowedRoles());

    assertArrayEquals(new String[] {"EXAMPLE.COM"},
        server.getAllowedRealms("EXAMPLE.COM", server.getConfig()));
  }

  @Test public void emptyExtraRoles() {
    final String[] extraRoles = new String[0];
    final Service mockService = Mockito.mock(Service.class);
    HttpServer server = new HttpServer.Builder()
        .withSpnego("HTTP/localhost.localdomain@EXAMPLE.COM", extraRoles)
        .withHandler(mockService, Serialization.JSON)
        .build();

    assertArrayEquals(extraRoles, server.getConfig().getAllowedRoles());

    assertArrayEquals(new String[] {"EXAMPLE.COM"},
        server.getAllowedRealms("EXAMPLE.COM", server.getConfig()));
  }

  @Test public void extraAllowedRolesConfiguredWithExplitRealm() {
    final String[] extraRoles = new String[] {"BAR.COM"};
    final Service mockService = Mockito.mock(Service.class);
    HttpServer server = new HttpServer.Builder()
        .withSpnego("HTTP/localhost.localdomain@EXAMPLE.COM", "EXAMPLE.COM", extraRoles)
        .withHandler(mockService, Serialization.JSON)
        .build();

    assertArrayEquals(extraRoles, server.getConfig().getAllowedRoles());

    assertArrayEquals(new String[] {"EXAMPLE.COM", "BAR.COM"},
        server.getAllowedRealms("EXAMPLE.COM", server.getConfig()));
  }

  @Test public void lotsOfExtraRolesWithExplitRealm() {
    final String[] extraRoles = new String[] {"BAR.COM", "BAZ.COM", "FOO.COM"};
    final Service mockService = Mockito.mock(Service.class);
    HttpServer server = new HttpServer.Builder()
        .withSpnego("HTTP/localhost.localdomain@EXAMPLE.COM", "EXAMPLE.COM", extraRoles)
        .withHandler(mockService, Serialization.JSON)
        .build();

    assertArrayEquals(extraRoles, server.getConfig().getAllowedRoles());

    assertArrayEquals(new String[] {"EXAMPLE.COM", "BAR.COM", "BAZ.COM", "FOO.COM"},
        server.getAllowedRealms("EXAMPLE.COM", server.getConfig()));
  }

  @Test public void nullExtraRolesWithExplitRealm() {
    final String[] extraRoles = null;
    final Service mockService = Mockito.mock(Service.class);
    HttpServer server = new HttpServer.Builder()
        .withSpnego("HTTP/localhost.localdomain@EXAMPLE.COM", "EXAMPLE.COM", extraRoles)
        .withHandler(mockService, Serialization.JSON)
        .build();

    assertNull(server.getConfig().getAllowedRoles());

    assertArrayEquals(new String[] {"EXAMPLE.COM"},
        server.getAllowedRealms("EXAMPLE.COM", server.getConfig()));
  }

  @Test public void emptyExtraRolesWithExplitRealm() {
    final String[] extraRoles = new String[0];
    final Service mockService = Mockito.mock(Service.class);
    HttpServer server = new HttpServer.Builder()
        .withSpnego("HTTP/localhost.localdomain@EXAMPLE.COM", "EXAMPLE.COM", extraRoles)
        .withHandler(mockService, Serialization.JSON)
        .build();

    assertArrayEquals(extraRoles, server.getConfig().getAllowedRoles());

    assertArrayEquals(new String[] {"EXAMPLE.COM"},
        server.getAllowedRealms("EXAMPLE.COM", server.getConfig()));
  }
}

// End HttpServerBuilderTest.java
