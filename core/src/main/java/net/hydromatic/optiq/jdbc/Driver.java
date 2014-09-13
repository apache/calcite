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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.avatica.*;

import net.hydromatic.linq4j.function.Function0;

import net.hydromatic.optiq.config.OptiqConnectionProperty;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.model.ModelHandler;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * Optiq JDBC driver.
 */
public class Driver extends UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:optiq:";

  final Function0<OptiqPrepare> prepareFactory;

  static {
    new Driver().register();
  }

  public Driver() {
    super();
    this.prepareFactory = createPrepareFactory();
  }

  protected Function0<OptiqPrepare> createPrepareFactory() {
    return OptiqPrepare.DEFAULT_FACTORY;
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
    case JDBC_30:
      return "net.hydromatic.optiq.jdbc.OptiqJdbc3Factory";
    case JDBC_40:
      return "net.hydromatic.optiq.jdbc.OptiqJdbc40Factory";
    case JDBC_41:
    default:
      return "net.hydromatic.optiq.jdbc.OptiqJdbc41Factory";
    }
  }

  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        Driver.class,
        "net-hydromatic-optiq-jdbc.properties",
        "Optiq JDBC Driver",
        "unknown version",
        "Optiq",
        "unknown version");
  }

  @Override
  protected Handler createHandler() {
    return new HandlerImpl() {
      @Override
      public void onConnectionInit(AvaticaConnection connection_)
          throws SQLException {
        final OptiqConnectionImpl connection =
            (OptiqConnectionImpl) connection_;
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

  @Override
  protected Collection<ConnectionProperty> getConnectionProperties() {
    final List<ConnectionProperty> list = new ArrayList<ConnectionProperty>();
    Collections.addAll(list, BuiltInConnectionProperty.values());
    Collections.addAll(list, OptiqConnectionProperty.values());
    return list;
  }

  /** Creates an internal connection. */
  OptiqConnection connect(OptiqRootSchema rootSchema,
      JavaTypeFactory typeFactory) {
    return (OptiqConnection) ((OptiqFactory) factory)
        .newConnection(this, factory, CONNECT_STRING_PREFIX, new Properties(),
            rootSchema, typeFactory);
  }
}

// End Driver.java
