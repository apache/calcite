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

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Provider of calcite connections for end-to-end tests.
 */
public class CalciteConnectionProvider {

  public static final String DRIVER_URL = "jdbc:calcite:";

  public Connection connection() throws IOException, SQLException {
    return DriverManager.getConnection(DRIVER_URL, provideConnectionInfo());
  }

  public Properties provideConnectionInfo() throws IOException {
    Properties info = new Properties();
    info.setProperty("lex", "MYSQL");
    info.setProperty("model", "inline:" + provideSchema());
    return info;
  }

  private String provideSchema() throws IOException {
    final InputStream stream =
        getClass().getResourceAsStream("/chinook/chinook.json");
    return CharStreams.toString(new InputStreamReader(stream, Charsets.UTF_8));
  }

}

// End CalciteConnectionProvider.java
