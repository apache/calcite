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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

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
  public final String jdbcDriver;

  /** The FQN of the {@link org.apache.calcite.sql.SqlDialectFactory} implementation.
   *
   * <p>Optional. If not specified, uses whichever class the JDBC
   * {@link java.sql.DriverManager} chooses.
   */
  public final String sqlDialectFactory;

  /** JDBC connect string, for example "jdbc:mysql://localhost/foodmart".
   */
  public final String jdbcUrl;

  /** JDBC user name.
   *
   * <p>Optional.
   */
  public final String jdbcUser;

  /** JDBC connect string, for example "jdbc:mysql://localhost/foodmart".
   *
   * <p>Optional.
   */
  public final String jdbcPassword;

  /** Name of the initial catalog in the JDBC data source.
   *
   * <p>Optional.
   */
  public final String jdbcCatalog;

  /** Name of the initial schema in the JDBC data source.
   *
   * <p>Optional.
   */
  public final String jdbcSchema;

  @JsonCreator
  public JsonJdbcSchema(
      @JsonProperty(value = "name", required = true) String name,
      @JsonProperty("path") List<Object> path,
      @JsonProperty("cache") Boolean cache,
      @JsonProperty("autoLattice") Boolean autoLattice,
      @JsonProperty("jdbcDriver") String jdbcDriver,
      @JsonProperty("sqlDialectFactory") String sqlDialectFactory,
      @JsonProperty(value = "jdbcUrl", required = true)  String jdbcUrl,
      @JsonProperty("jdbcUser") String jdbcUser,
      @JsonProperty("jdbcPassword") String jdbcPassword,
      @JsonProperty("jdbcCatalog") String jdbcCatalog,
      @JsonProperty("jdbcSchema") String jdbcSchema) {
    super(name, path, cache, autoLattice);
    this.jdbcDriver = jdbcDriver;
    this.sqlDialectFactory = sqlDialectFactory;
    this.jdbcUrl = requireNonNull(jdbcUrl, "jdbcUrl");
    this.jdbcUser = jdbcUser;
    this.jdbcPassword = jdbcPassword;
    this.jdbcCatalog = jdbcCatalog;
    this.jdbcSchema = jdbcSchema;
  }

  @Override public void accept(ModelHandler handler) {
    handler.visit(this);
  }
}
