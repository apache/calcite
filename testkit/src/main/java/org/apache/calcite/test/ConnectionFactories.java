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
package org.apache.calcite.test;

import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/** Utilities for {@link ConnectionFactory} and
 * {@link org.apache.calcite.test.CalciteAssert.ConnectionPostProcessor}. */
public abstract class ConnectionFactories {
  /** The empty connection factory. */
  private static final ConnectionFactory EMPTY =
      new MapConnectionFactory(ImmutableMap.of(), ImmutableList.of());

  /** Prevent instantiation of utility class. */
  private ConnectionFactories() {
  }

  /** Returns an empty connection factory. */
  public static ConnectionFactory empty() {
    return EMPTY;
  }

  /** Creates a connection factory that uses a single pooled connection,
   * as opposed to creating a new connection on each invocation. */
  public static ConnectionFactory pool(ConnectionFactory connectionFactory) {
    return connectionFactory instanceof PoolingConnectionFactory
        ? connectionFactory
        : new PoolingConnectionFactory(connectionFactory);
  }

  /** Returns a post-processor that adds a {@link CalciteAssert.SchemaSpec}
   * (set of schemes) to a connection. */
  public static CalciteAssert.ConnectionPostProcessor add(
      CalciteAssert.SchemaSpec schemaSpec) {
    return new AddSchemaSpecPostProcessor(schemaSpec);
  }

  /** Returns a post-processor that adds {@link Schema} and sets it as
   * default. */
  public static CalciteAssert.ConnectionPostProcessor add(String name,
      Schema schema) {
    return new AddSchemaPostProcessor(name, schema);
  }

  /** Returns a post-processor that sets a default schema name. */
  public static CalciteAssert.ConnectionPostProcessor setDefault(
      String schema) {
    return new DefaultSchemaPostProcessor(schema);
  }

  /** Returns a post-processor that adds a type. */
  public static CalciteAssert.ConnectionPostProcessor addType(String name,
      RelProtoDataType protoDataType) {
    return new AddTypePostProcessor(name, protoDataType);
  }

  /** Connection factory that uses a given map of (name, value) pairs and
   * optionally an initial schema. */
  private static class MapConnectionFactory implements ConnectionFactory {
    private final ImmutableMap<String, String> map;
    private final ImmutableList<CalciteAssert.ConnectionPostProcessor> postProcessors;

    MapConnectionFactory(ImmutableMap<String, String> map,
        ImmutableList<CalciteAssert.ConnectionPostProcessor> postProcessors) {
      this.map = requireNonNull(map, "map");
      this.postProcessors = requireNonNull(postProcessors, "postProcessors");
    }

    @Override public boolean equals(Object obj) {
      return this == obj
          || obj.getClass() == MapConnectionFactory.class
          && ((MapConnectionFactory) obj).map.equals(map)
          && ((MapConnectionFactory) obj).postProcessors.equals(postProcessors);
    }

    @Override public int hashCode() {
      return Objects.hash(map, postProcessors);
    }

    @Override public Connection createConnection() throws SQLException {
      final Properties info = new Properties();
      for (Map.Entry<String, String> entry : map.entrySet()) {
        info.setProperty(entry.getKey(), entry.getValue());
      }
      Connection connection =
          DriverManager.getConnection("jdbc:calcite:", info);
      for (CalciteAssert.ConnectionPostProcessor postProcessor : postProcessors) {
        connection = postProcessor.apply(connection);
      }
      return connection;
    }

    @Override public ConnectionFactory with(String property, Object value) {
      return new MapConnectionFactory(
          FlatLists.append(this.map, property, value.toString()),
          postProcessors);
    }

    @Override public ConnectionFactory with(ConnectionProperty property, Object value) {
      if (!property.type().valid(value, property.valueClass())) {
        throw new IllegalArgumentException();
      }
      return with(property.camelName(), value.toString());
    }

    @Override public ConnectionFactory with(
        CalciteAssert.ConnectionPostProcessor postProcessor) {
      ImmutableList.Builder<CalciteAssert.ConnectionPostProcessor> builder =
          ImmutableList.builder();
      builder.addAll(postProcessors);
      builder.add(postProcessor);
      return new MapConnectionFactory(map, builder.build());
    }
  }

  /** Post-processor that adds a {@link Schema} and sets it as default. */
  private static class AddSchemaPostProcessor
      implements CalciteAssert.ConnectionPostProcessor {
    private final String name;
    private final Schema schema;

    AddSchemaPostProcessor(String name, Schema schema) {
      this.name = requireNonNull(name, "name");
      this.schema = requireNonNull(schema, "schema");
    }

    @Override public Connection apply(Connection connection) throws SQLException {
      CalciteConnection con = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = con.getRootSchema();
      rootSchema.add(name, schema);
      connection.setSchema(name);
      return connection;
    }
  }

  /** Post-processor that adds a type. */
  private static class AddTypePostProcessor
      implements CalciteAssert.ConnectionPostProcessor {
    private final String name;
    private final RelProtoDataType protoDataType;

    AddTypePostProcessor(String name, RelProtoDataType protoDataType) {
      this.name = requireNonNull(name, "name");
      this.protoDataType = requireNonNull(protoDataType, "protoDataType");
    }

    @Override public Connection apply(Connection connection) throws SQLException {
      CalciteConnection con = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = con.getRootSchema();
      rootSchema.add(name, protoDataType);
      return connection;
    }
  }

  /** Post-processor that sets a default schema name. */
  private static class DefaultSchemaPostProcessor
      implements CalciteAssert.ConnectionPostProcessor {
    private final String name;

    DefaultSchemaPostProcessor(String name) {
      this.name = name;
    }

    @Override public Connection apply(Connection connection) throws SQLException {
      connection.setSchema(name);
      return connection;
    }
  }

  /** Post-processor that adds a {@link CalciteAssert.SchemaSpec}
   * (set of schemes) to a connection. */
  private static class AddSchemaSpecPostProcessor
      implements CalciteAssert.ConnectionPostProcessor {
    private final CalciteAssert.SchemaSpec schemaSpec;

    AddSchemaSpecPostProcessor(CalciteAssert.SchemaSpec schemaSpec) {
      this.schemaSpec = schemaSpec;
    }

    @Override public Connection apply(Connection connection) throws SQLException {
      CalciteConnection con = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = con.getRootSchema();
      switch (schemaSpec) {
      case CLONE_FOODMART:
      case JDBC_FOODMART_WITH_LATTICE:
        CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.JDBC_FOODMART);
        // fall through
      default:
        CalciteAssert.addSchema(rootSchema, schemaSpec);
      }
      con.setSchema(schemaSpec.schemaName);
      return connection;
    }
  }

  /** Connection factory that uses the same instance of connections. */
  private static class PoolingConnectionFactory implements ConnectionFactory {
    private final PoolingDataSource<PoolableConnection> dataSource;

    PoolingConnectionFactory(final ConnectionFactory factory) {
      final PoolableConnectionFactory connectionFactory =
          new PoolableConnectionFactory(factory::createConnection, null);
      connectionFactory.setRollbackOnReturn(false);
      this.dataSource =
          new PoolingDataSource<>(new GenericObjectPool<>(connectionFactory));
    }

    @Override public Connection createConnection() throws SQLException {
      return dataSource.getConnection();
    }
  }
}
