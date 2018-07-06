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
import org.apache.calcite.adapter.jdbc.JdbcSchema;
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
import org.apache.calcite.model.JsonSchema;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
        final String model = model(connection);
        if (model != null) {
          try {
            new ModelHandler(connection, model);
          } catch (IOException e) {
            throw new SQLException(e);
          }
        }
        connection.init();
      }

      String model(CalciteConnectionImpl connection) {
        String model = connection.config().model();
        if (model != null) {
          return model;
        }
        SchemaFactory schemaFactory =
            connection.config().schemaFactory(SchemaFactory.class, null);
        final Properties info = connection.getProperties();
        final String schemaName = Util.first(connection.config().schema(), "adhoc");
        if (schemaFactory == null) {
          final JsonSchema.Type schemaType = connection.config().schemaType();
          if (schemaType != null) {
            switch (schemaType) {
            case JDBC:
              schemaFactory = JdbcSchema.Factory.INSTANCE;
              break;
            case MAP:
              schemaFactory = AbstractSchema.Factory.INSTANCE;
              break;
            }
          }
        }
        if (schemaFactory != null) {
          final JsonBuilder json = new JsonBuilder();
          final Map<String, Object> root = json.map();
          root.put("version", "1.0");
          root.put("defaultSchema", schemaName);
          final List<Object> schemaList = json.list();
          root.put("schemas", schemaList);
          final Map<String, Object> schema = json.map();
          schemaList.add(schema);
          schema.put("type", "custom");
          schema.put("name", schemaName);
          schema.put("factory", schemaFactory.getClass().getName());
          final Map<String, Object> operandMap = json.map();
          schema.put("operand", operandMap);
          for (Map.Entry<String, String> entry : Util.toMap(info).entrySet()) {
            if (entry.getKey().startsWith("schema.")) {
              operandMap.put(entry.getKey().substring("schema.".length()),
                  entry.getValue());
            }
          }
          return "inline:" + json.toJsonString(root);
        }
        return null;
      }
    };
  }

  @Override protected Collection<ConnectionProperty> getConnectionProperties() {
    final List<ConnectionProperty> list = new ArrayList<>();
    Collections.addAll(list, BuiltInConnectionProperty.values());
    Collections.addAll(list, CalciteConnectionProperty.values());
    return list;
  }

  @Override public Meta createMeta(AvaticaConnection connection) {
    return new CalciteMetaImpl((CalciteConnectionImpl) connection);
  }

  /** Creates an internal connection. */
  CalciteConnection connect(CalciteSchema rootSchema,
      JavaTypeFactory typeFactory) {
    return (CalciteConnection) ((CalciteFactory) factory)
        .newConnection(this, factory, CONNECT_STRING_PREFIX, new Properties(),
            rootSchema, typeFactory);
  }

  /** Creates an internal connection. */
  CalciteConnection connect(CalciteSchema rootSchema,
      JavaTypeFactory typeFactory, Properties properties) {
    return (CalciteConnection) ((CalciteFactory) factory)
        .newConnection(this, factory, CONNECT_STRING_PREFIX, properties,
            rootSchema, typeFactory);
  }
}

// End Driver.java
