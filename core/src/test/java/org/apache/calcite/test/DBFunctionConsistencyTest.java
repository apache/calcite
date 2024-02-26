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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DBFunctionConsistencyTest {

  private enum Type {
    ORACLE {
      @Override String query(final String sqlFunction) {
        return "SELECT " + sqlFunction + " FROM DUAL";
      }
    }, POSTGRES_9_6, POSTGRES_12_2, MYSQL;

    String query(String sqlFunction) {
      return "SELECT " + sqlFunction;
    }
  }

  private final static Map<Type, JdbcDatabaseContainer<?>> DBS = initContainers();

  private static Map<Type, JdbcDatabaseContainer<?>> initContainers() {
    Map<Type, JdbcDatabaseContainer<?>> dbs = new HashMap<>();
    dbs.put(Type.POSTGRES_9_6, new PostgreSQLContainer<>("postgres:9.6"));
    dbs.put(Type.POSTGRES_12_2, new PostgreSQLContainer<>("postgres:12.2"));
    dbs.put(Type.MYSQL, new MySQLContainer<>());
    dbs.put(Type.ORACLE, new OracleContainer(
        "gvenzl/oracle-xe:21-slim-faststart"));
    return dbs;
  }

  @BeforeAll
  static void setup() throws SQLException {
    DBS.values().forEach(GenericContainer::start);
  }

  @AfterAll
  static void teardown() {
    DBS.values().forEach(GenericContainer::stop);
  }

  @ParameterizedTest
  @CsvSource({
      "SQRT(4),ORACLE",
      "SQRT(4),POSTGRES_9_6",
      "SQRT(4),POSTGRES_12_2",
      "SQRT(4),MYSQL",
      "SQRT(-1),ORACLE",
      "SQRT(-1),POSTGRES_9_6",
      "SQRT(-1),POSTGRES_12_2",
      "SQRT(-1),MYSQL"
  })
  void testFunction(String function, String db) {
    String calciteResult = execute(Type.valueOf(db), true, function);
    String rawResult = execute(Type.valueOf(db), false, function);
    Assertions.assertEquals(rawResult, calciteResult);
  }

  String execute(Type dbType, boolean useCalcite, String exp) {
    JdbcDatabaseContainer<?> db = DBS.get(dbType);
    final String url;
    final String query;
    if (useCalcite) {
      url = String.format(
          "jdbc:calcite:schemaType=JDBC; schema.jdbcUser=%s; schema.jdbcPassword=%s; schema"
              + ".jdbcUrl=%s", db.getUsername(), db.getPassword(), db.getJdbcUrl());
      query = "SELECT " + exp;
    } else {
      url = db.getJdbcUrl();
      query = dbType.query(exp);
    }
    try (Connection c = DriverManager.getConnection(url, db.getUsername(), db.getPassword())) {
      try (PreparedStatement stmt = c.prepareStatement(query)) {
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            return rs.getString(1);
          } else {
            return "";
          }
        }
      }
    } catch (Exception e) {
      return e.getMessage();
    }
  }
}
