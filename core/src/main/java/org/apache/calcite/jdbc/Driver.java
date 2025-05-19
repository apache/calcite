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
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.model.JsonSchema;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Calcite JDBC driver.
 */
public class Driver extends UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:calcite:";

  protected final @Nullable Supplier<CalcitePrepare> prepareFactory;

  static {
    new Driver().register();
  }

  /** Creates a Driver. */
  public Driver() {
    this(null);
  }

  /** Creates a Driver with a factory for {@code CalcitePrepare} objects;
   * if the factory is null, the driver will call
   * {@link CalcitePrepare#DEFAULT_FACTORY}. */
  protected Driver(@Nullable Supplier<CalcitePrepare> prepareFactory) {
    this.prepareFactory = prepareFactory;
  }

  /** Creates a copy of this Driver with a new factory for creating
   * {@link CalcitePrepare}.
   *
   * <p>Allows users of the Driver to change the factory without subclassing
   * the Driver. But subclasses of the driver should override this method to
   * create an instance of their subclass.
   *
   * @param prepareFactory Supplier of a {@code CalcitePrepare}
   * @return Driver with the provided prepareFactory
   */
  public Driver withPrepareFactory(Supplier<CalcitePrepare> prepareFactory) {
    requireNonNull(prepareFactory, "prepareFactory");
    if (this.prepareFactory == prepareFactory) {
      return this;
    }
    return new Driver(prepareFactory);
  }

  /** Creates a {@link CalcitePrepare} to be used to prepare a statement for
   * execution.
   *
   * <p>If you wish to use a custom prepare, either override this method or
   * call {@link #withPrepareFactory(Supplier)}. */
  public CalcitePrepare createPrepare() {
    if (prepareFactory != null) {
      return prepareFactory.get();
    }
    return CalcitePrepare.DEFAULT_FACTORY.apply();
  }

  /** Returns a factory with which to create a {@link CalcitePrepare}.
   *
   * <p>Now deprecated; if you wish to use a custom prepare, please call
   * {@link #withPrepareFactory(Supplier)}
   * or override {@link #createPrepare()}. */
  @Deprecated // to be removed before 2.0
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

  @Override protected DriverVersion createDriverVersion() {
    return CalciteDriverVersion.INSTANCE;
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
            ModelHandler h = new ModelHandler(connection.getRootSchema(), model);
            String defaultName = h.defaultSchemaName();
            if (defaultName != null) {
              connection.setSchema(defaultName);
            }
          } catch (IOException e) {
            throw new SQLException(e);
          }
        }
        connection.init();
      }

      @Nullable String model(CalciteConnectionImpl connection) {
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
            default:
              break;
            }
          }
        }
        if (schemaFactory != null) {
          final JsonBuilder json = new JsonBuilder();
          final Map<String, @Nullable Object> root = json.map();
          root.put("version", "1.0");
          root.put("defaultSchema", schemaName);
          final List<@Nullable Object> schemaList = json.list();
          root.put("schemas", schemaList);
          final Map<String, @Nullable Object> schema = json.map();
          schemaList.add(schema);
          schema.put("type", "custom");
          schema.put("name", schemaName);
          schema.put("factory", schemaFactory.getClass().getName());
          final Map<String, @Nullable Object> operandMap = json.map();
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
    final CalciteConnectionConfig config =
        (CalciteConnectionConfig) connection.config();
    CalciteMetaTableFactory metaTableFactory =
        config.metaTableFactory(CalciteMetaTableFactory.class,
            CalciteMetaTableFactoryImpl.INSTANCE);
    CalciteMetaColumnFactory metaColumnFactory =
        config.metaColumnFactory(CalciteMetaColumnFactory.class,
            CalciteMetaColumnFactoryImpl.INSTANCE);
    return CalciteMetaImpl.create((CalciteConnectionImpl) connection,
        metaTableFactory, metaColumnFactory);
  }

  /** Creates an internal connection. */
  CalciteConnection connect(CalciteSchema rootSchema,
      @Nullable JavaTypeFactory typeFactory) {
    return (CalciteConnection) ((CalciteFactory) factory)
        .newConnection(this, factory, CONNECT_STRING_PREFIX, new Properties(),
            rootSchema, typeFactory);
  }

  /** Creates an internal connection. */
  CalciteConnection connect(CalciteSchema rootSchema,
      @Nullable JavaTypeFactory typeFactory, Properties properties) {
    return (CalciteConnection) ((CalciteFactory) factory)
        .newConnection(this, factory, CONNECT_STRING_PREFIX, properties,
            rootSchema, typeFactory);
  }
}
