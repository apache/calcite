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
package org.apache.calcite.chinook;

import org.hsqldb.cmdline.SqlFile;

import org.hsqldb.cmdline.SqlToolError;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Bootstrap of HSQLDB schema under calcite. It contains data for end 2 end tests.
 */
public class HsqlBootstrap {

  private static final String DB_FILENAME = "database.sql";
  private static final String DB_URL = "jdbc:hsqldb:mem:e2edb";
  private static final String DB_PASSWORD = "password";
  private static final String DB_USER = "admin";

  private static HsqlBootstrap instance;

  private HsqlBootstrap() {
  }

  public static HsqlBootstrap instance() {
    if (instance == null) {
      instance = new HsqlBootstrap();
      instance.init();
    }
    return instance;
  }

  private void init() {
    new org.hsqldb.jdbc.JDBCDriver(); // workaround for cheking unused dependiences in pom.xml
    try {
      initWithReportedException();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void initWithReportedException() throws IOException, SqlToolError, SQLException {
    try (Connection connection = connection(); InputStream inputStream = getDBScriptAsResource()) {
      SqlFile sqlFile = new SqlFile(
              new InputStreamReader(inputStream, StandardCharsets.UTF_8),
              DB_FILENAME,
              System.out,
              StandardCharsets.UTF_8.name(),
              false,
              new File("."));
      sqlFile.setConnection(connection);
      sqlFile.execute();
    }
  }

  private InputStream getDBScriptAsResource() {
    return HsqlBootstrap.class.getResourceAsStream("/chinook/database.sql");
  }

  public Connection connection() throws SQLException {
    return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
  }
}
// End HsqlBootstrap.java
