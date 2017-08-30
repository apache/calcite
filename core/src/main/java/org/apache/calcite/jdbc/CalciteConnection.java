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
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalcitePrepare.Context;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Extension to Calcite's implementation of
 * {@link java.sql.Connection JDBC connection} allows schemas to be defined
 * dynamically.
 *
 * <p>You can start off with an empty connection (no schemas), define one
 * or two schemas, and start querying them.</p>
 *
 * <p>Since a {@code CalciteConnection} implements the linq4j
 * {@link QueryProvider} interface, you can use a connection to execute
 * expression trees as queries.</p>
 */
public interface CalciteConnection extends Connection, QueryProvider {
  /**
   * Returns the root schema.
   *
   * <p>You can define objects (such as relations) in this schema, and
   * also nested schemas.</p>
   *
   * @return Root schema
   */
  SchemaPlus getRootSchema();

  /**
   * Returns the type factory.
   *
   * @return Type factory
   */
  JavaTypeFactory getTypeFactory();

  /**
   * Returns an instance of the connection properties.
   *
   * <p>NOTE: The resulting collection of properties is same collection used
   * by the connection, and is writable, but behavior if you modify the
   * collection is undefined. Some implementations might, for example, see
   * a modified property, but only if you set it before you create a
   * statement. We will remove this method when there are better
   * implementations of stateful connections and configuration.</p>
   *
   * @return properties
   */
  Properties getProperties();

  // in java.sql.Connection from JDK 1.7, but declare here to allow other JDKs
  void setSchema(String schema) throws SQLException;

  // in java.sql.Connection from JDK 1.7, but declare here to allow other JDKs
  String getSchema() throws SQLException;

  CalciteConnectionConfig config();

  /** Creates a context for preparing a statement for execution. */
  Context createPrepareContext();
}

// End CalciteConnection.java
