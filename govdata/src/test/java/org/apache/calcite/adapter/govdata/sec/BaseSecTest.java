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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;

import org.junit.jupiter.api.BeforeAll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Base class for all SEC adapter tests.
 * Ensures environment variables from .env.test are loaded and provides
 * common utilities for connecting to Calcite with model files.
 */
public abstract class BaseSecTest {

  @BeforeAll
  public static void setupEnvironment() {
    // Ensure .env.test is loaded before any tests run
    TestEnvironmentLoader.ensureLoaded();
  }

  /**
   * Create a connection to Calcite using the specified model resource.
   * 
   * @param modelResourcePath Path to model file in resources (e.g., "/sec-default-model.json")
   * @return Connection to Calcite
   * @throws SQLException if connection fails
   */
  protected Connection createConnection(String modelResourcePath) throws SQLException {
    String modelPath = getClass().getResource(modelResourcePath).getPath();
    return createConnectionFromPath(modelPath);
  }

  /**
   * Create a connection to Calcite using a file path.
   * 
   * @param modelPath Absolute path to model file
   * @return Connection to Calcite
   * @throws SQLException if connection fails
   */
  protected Connection createConnectionFromPath(String modelPath) throws SQLException {
    Properties props = getDefaultConnectionProperties();
    String jdbcUrl = "jdbc:calcite:model=" + modelPath;
    return DriverManager.getConnection(jdbcUrl, props);
  }

  /**
   * Get default connection properties for SEC tests.
   * 
   * @return Properties with standard settings
   */
  protected Properties getDefaultConnectionProperties() {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    return props;
  }
}