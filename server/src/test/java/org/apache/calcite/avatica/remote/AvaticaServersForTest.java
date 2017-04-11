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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.ConnectionSpec;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.AvaticaProtobufHandler;
import org.apache.calcite.avatica.server.AvaticaServerConfiguration;
import org.apache.calcite.avatica.server.HandlerFactory;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Utility class which encapsulates the setup required to write Avatica tests that run against
 * servers using each serialization approach.
 */
public class AvaticaServersForTest {
  private static final ConnectionSpec CONNECTION_SPEC = ConnectionSpec.HSQLDB;
  private static final String[] SERVER_ARGS = { FullyRemoteJdbcMetaFactory.class.getName() };

  private final Map<Serialization, HttpServer> serversBySerialization;

  public AvaticaServersForTest() {
    serversBySerialization = new HashMap<>();
  }

  /**
   * Starts an Avatica server for each serialization type.
   */
  public void startServers() throws Exception {
    // Bind to '0' to pluck an ephemeral port instead of expecting a certain one to be free
    final HttpServer jsonServer = Main.start(SERVER_ARGS, 0, new Main.HandlerFactory() {
      @Override public AvaticaJsonHandler createHandler(Service service) {
        return new AvaticaJsonHandler(service);
      }
    });
    serversBySerialization.put(Serialization.JSON, jsonServer);

    final HttpServer protobufServer = Main.start(SERVER_ARGS, 0, new Main.HandlerFactory() {
      @Override public AvaticaProtobufHandler createHandler(Service service) {
        return new AvaticaProtobufHandler(service);
      }
    });
    serversBySerialization.put(Serialization.PROTOBUF, protobufServer);
  }

  /**
   * Starts Avatica servers for each serialization type with the provided {@code serverConfig}.
   */
  public void startServers(AvaticaServerConfiguration serverConfig) {
    final HandlerFactory factory = new HandlerFactory();

    // Construct the JSON server
    Service jsonService = new LocalService(FullyRemoteJdbcMetaFactory.getInstance());
    AvaticaHandler jsonHandler = factory.getHandler(jsonService, Serialization.JSON, null,
        serverConfig);
    final HttpServer jsonServer = new HttpServer.Builder().withHandler(jsonHandler)
        .withPort(0).build();
    jsonServer.start();
    serversBySerialization.put(Serialization.JSON, jsonServer);

    // Construct the Protobuf server
    Service protobufService = new LocalService(FullyRemoteJdbcMetaFactory.getInstance());
    AvaticaHandler protobufHandler = factory.getHandler(protobufService, Serialization.PROTOBUF,
        null, serverConfig);
    final HttpServer protobufServer = new HttpServer.Builder().withHandler(protobufHandler)
        .withPort(0).build();
    protobufServer.start();
    serversBySerialization.put(Serialization.PROTOBUF, protobufServer);
  }

  /**
   * Stops the servers currently running.
   *
   * @throws Exception If there is an error stopping a server
   */
  public void stopServers() throws Exception {
    Iterator<Entry<Serialization, HttpServer>> servers =
        serversBySerialization.entrySet().iterator();
    while (servers.hasNext()) {
      try {
        servers.next().getValue().stop();
      } finally {
        // Still remove it if we failed to stop it
        servers.remove();
      }
    }
  }

  /**
   * Computes an array of parameters to support JUnit's parameterized tests. The Object array
   * actually contains a {@link Serialization} and the {@link HttpServer} instance in that order.
   *
   * @return A list of arrays of Serialization and HttpServer pairs.
   */
  public List<Object[]> getJUnitParameters() {
    List<Object[]> params = new ArrayList<>(serversBySerialization.size());

    for (Entry<Serialization, HttpServer> servers : serversBySerialization.entrySet()) {
      params.add(new Object[]{ servers.getKey(), servers.getValue() });
    }

    return params;
  }

  /**
   * Computes the JDBC url for the Avatica server running on localhost, bound to the given port,
   * and using the given serialization.
   *
   * @param port The port the Avatica server is listening on.
   * @param serialization The serialization the Avatica server is using.
   * @return A JDBC URL to the local Avatica server.
   */
  public String getJdbcUrl(int port, Serialization serialization) {
    return getJdbcUrl(port, serialization, "");
  }

  /**
   * Computes the JDBC url for the Avatica server running on localhost, bound to the given port,
   * using the given serialization, with the user-provided suffix for the HTTP URL to the server.
   *
   * @param port The port the Avatica server is listening on.
   * @param serialization The serialization the Avatica server is using.
   * @param urlSuffix Extra information to apend to the HTTP URL to the Avatica server (optional).
   * @return A JDBC URL to the local Avatica server.
   */
  public String getJdbcUrl(int port, Serialization serialization, String urlSuffix) {
    return "jdbc:avatica:remote:url=http://localhost:" + port + urlSuffix + ";serialization="
        + serialization.name();
  }

  /** Factory that provides a {@link JdbcMeta}. */
  public static class FullyRemoteJdbcMetaFactory implements Meta.Factory {

    private static JdbcMeta instance = null;

    static JdbcMeta getInstance() {
      if (instance == null) {
        try {
          instance = new JdbcMeta(CONNECTION_SPEC.url, CONNECTION_SPEC.username,
              CONNECTION_SPEC.password);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
      return instance;
    }

    @Override public Meta create(List<String> args) {
      return getInstance();
    }
  }
}

// End AvaticaServersForTest.java
