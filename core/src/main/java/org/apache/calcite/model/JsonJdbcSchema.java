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
package org.apache.calcite.model;

/**
 * JSON object representing a schema that maps to a JDBC database.
 *
 * <p>Like the base class {@link JsonSchema},
 * occurs within {@link JsonRoot#schemas}.
 *
 * @see JsonRoot Description of JSON schema elements
 */
public class JsonJdbcSchema extends JsonSchema {
  /** The name of the JDBC driver class.
   *
   * <p>Optional. If not specified, uses whichever class the JDBC
   * {@link java.sql.DriverManager} chooses.
   */
  public String jdbcDriver;

  /** The FQN of the {@link org.apache.calcite.sql.SqlDialectFactory} implementation.
   *
   * <p>Optional. If not specified, uses whichever class the JDBC
   * {@link java.sql.DriverManager} chooses.
   */
  public String sqlDialectFactory;

  /** JDBC connect string, for example "jdbc:mysql://localhost/foodmart".
   *
   * <p>Optional.
   */
  public String jdbcUrl;

  /** JDBC user name.
   *
   * <p>Optional.
   */
  public String jdbcUser;

  /** JDBC connect string, for example "jdbc:mysql://localhost/foodmart".
   *
   * <p>Optional.
   */
  public String jdbcPassword;

  /** Name of the initial catalog in the JDBC data source.
   *
   * <p>Optional.
   */
  public String jdbcCatalog;

  /** Name of the initial schema in the JDBC data source.
   *
   * <p>Optional.
   */
  public String jdbcSchema;

  @Override public void accept(ModelHandler handler) {
    handler.visit(this);
  }
}

// End JsonJdbcSchema.java
