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
package org.apache.calcite.jdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.avatica.HandlerImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.model.ModelHandler;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Calcite JDBC driver.
 */
public class Driver extends UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:calcite:";

  final Function0<CalcitePrepare> prepareFactory;

  static {
    new Driver().register();
  }

  public Driver() {
    super();
    this.prepareFactory = createPrepareFactory();
  }

  protected Function0<CalcitePrepare> createPrepareFactory() {
    return CalcitePrepare.DEFAULT_FACTORY;
  }

  @Override protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
    case JDBC_30:
    case JDBC_40:
      throw new IllegalArgumentException("JDBC version not supported: "
          + jdbcVersion);
    case JDBC_41:
    default:
      return "org.apache.calcite.jdbc.CalciteJdbc41Factory";
    }
  }

  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        Driver.class,
        "org-apache-calcite-jdbc.properties",
        "Calcite JDBC Driver",
        "unknown version",
        "Calcite",
        "unknown version");
  }

  @Override protected Handler createHandler() {
    return new HandlerImpl() {
      @Override public void onConnectionInit(AvaticaConnection connection_)
          throws SQLException {
        final CalciteConnectionImpl connection =
            (CalciteConnectionImpl) connection_;
        super.onConnectionInit(connection);
        final String model = connection.config().model();
        if (model != null) {
          try {
            new ModelHandler(connection, model);
          } catch (IOException e) {
            throw new SQLException(e);
          }
        }
        connection.init();
      }
    };
  }

  @Override protected Collection<ConnectionProperty> getConnectionProperties() {
    final List<ConnectionProperty> list = new ArrayList<ConnectionProperty>();
    Collections.addAll(list, BuiltInConnectionProperty.values());
    Collections.addAll(list, CalciteConnectionProperty.values());
    return list;
  }

  @Override public Meta createMeta(AvaticaConnection connection) {
    return new CalciteMetaImpl((CalciteConnectionImpl) connection);
  }

  /** Creates an internal connection. */
  CalciteConnection connect(CalciteRootSchema rootSchema,
      JavaTypeFactory typeFactory) {
    return (CalciteConnection) ((CalciteFactory) factory)
        .newConnection(this, factory, CONNECT_STRING_PREFIX, new Properties(),
            rootSchema, typeFactory);
  }
}

// End Driver.java
