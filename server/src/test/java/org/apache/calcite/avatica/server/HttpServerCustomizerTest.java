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

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;

import org.eclipse.jetty.server.Server;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * HTTP server customizer tests
 */
public class HttpServerCustomizerTest {

  private static Meta mockMeta = mock(Meta.class);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @SuppressWarnings("unchecked") // needed for the mocked customizers, not the builder
  @Test public void serverCustomizersInvoked() {
    ServerCustomizer<Server> mockCustomizer1 =
        (ServerCustomizer<Server>) mock(ServerCustomizer.class);
    ServerCustomizer<Server> mockCustomizer2 =
        (ServerCustomizer<Server>) mock(ServerCustomizer.class);
    Service service = new LocalService(mockMeta);
    HttpServer server =
        HttpServer.Builder.<Server>newBuilder().withHandler(service, Driver.Serialization.PROTOBUF)
            .withServerCustomizers(Arrays.asList(mockCustomizer1, mockCustomizer2), Server.class)
            .withPort(0).build();
    try {
      server.start();
      verify(mockCustomizer2).customize(any(Server.class));
      verify(mockCustomizer1).customize(any(Server.class));
    } finally {
      server.stop();
    }
  }

  @Test public void onlyJettyCustomizersAllowed() {
    Service service = new LocalService(mockMeta);
    List<ServerCustomizer<UnsupportedServer>> unsupportedCustomizers = new ArrayList<>();
    unsupportedCustomizers.add(new ServerCustomizer<UnsupportedServer>() {
      @Override public void customize(UnsupportedServer server) {
      }
    });
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Only Jetty Server customizers are supported");
    HttpServer.Builder.<UnsupportedServer>newBuilder()
        .withHandler(service, Driver.Serialization.PROTOBUF)
        .withServerCustomizers(unsupportedCustomizers, UnsupportedServer.class).withPort(0).build();
  }

  /**
   * A server type that cannot be customized
   */
  private static class UnsupportedServer {
  }
}

// End HttpServerCustomizerTest.java
