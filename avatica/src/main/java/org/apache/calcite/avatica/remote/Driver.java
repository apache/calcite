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

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Avatica Remote JDBC driver.
 */
public class Driver extends UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:avatica:remote:";

  static {
    new Driver().register();
  }

  public Driver() {
    super();
  }

  /**
   * Defines the method of message serialization used by the Driver
   */
  public static enum Serialization {
    JSON,
    PROTOBUF;
  }

  @Override protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        Driver.class,
        "org-apache-calcite-jdbc.properties",
        "Avatica Remote JDBC Driver",
        "unknown version",
        "Avatica",
        "unknown version");
  }

  @Override protected Collection<ConnectionProperty> getConnectionProperties() {
    final List<ConnectionProperty> list = new ArrayList<ConnectionProperty>();
    Collections.addAll(list, BuiltInConnectionProperty.values());
    Collections.addAll(list, AvaticaRemoteConnectionProperty.values());
    return list;
  }

  @Override public Meta createMeta(AvaticaConnection connection) {
    final ConnectionConfig config = connection.config();
    final Service service = createService(connection, config);
    return new RemoteMeta(connection, service);
  }

  private Service createService(AvaticaConnection connection, ConnectionConfig config) {
    final Service.Factory metaFactory = config.factory();
    final Service service;
    if (metaFactory != null) {
      service = metaFactory.create(connection);
    } else if (config.url() != null) {
      final URL url;
      try {
        url = new URL(config.url());
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }

      Serialization serializationType = getSerialization(config);

      switch (serializationType) {
      case JSON:
        service = new RemoteService(url);
        break;
      case PROTOBUF:
        service = new RemoteProtobufService(url, new ProtobufTranslationImpl());
        break;
      default:
        throw new IllegalArgumentException("Unhandled serialization type: " + serializationType);
      }
    } else {
      service = new MockJsonService(Collections.<String, String>emptyMap());
    }
    return service;
  }

  @Override public Connection connect(String url, Properties info)
      throws SQLException {
    AvaticaConnection conn = (AvaticaConnection) super.connect(url, info);
    if (conn == null) {
      // It's not an url for our driver
      return null;
    }

    // Create the corresponding remote connection
    ConnectionConfig config = conn.config();
    Service service = createService(conn, config);

    Map<String, String> infoAsString = new HashMap<>();
    for (Map.Entry<Object, Object> entry : info.entrySet()) {
      // Determine if this is a property we want to forward to the server
      boolean localProperty = false;
      for (BuiltInConnectionProperty prop : BuiltInConnectionProperty.values()) {
        if (prop.camelName().equals(entry.getKey())) {
          localProperty = true;
          break;
        }
      }

      if (!localProperty) {
        infoAsString.put(entry.getKey().toString(), entry.getValue().toString());
      }
    }

    service.apply(new Service.OpenConnectionRequest(conn.id, infoAsString));

    return conn;
  }

  private Serialization getSerialization(ConnectionConfig config) {
    final String serializationStr = config.serialization();
    Serialization serializationType = Serialization.JSON;
    if (null != serializationStr) {
      try {
        serializationType = Serialization.valueOf(serializationStr.toUpperCase());
      } catch (Exception e) {
        // Log a warning instead of failing harshly? Intentionally no loggers available?
        throw new RuntimeException(e);
      }
    }

    return serializationType;
  }
}

// End Driver.java
