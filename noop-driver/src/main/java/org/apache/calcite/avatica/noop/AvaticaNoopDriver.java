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
package org.apache.calcite.avatica.noop;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * A Noop JDBC Driver.
 */
public class AvaticaNoopDriver implements Driver {

  private static final AvaticaNoopDriver INSTANCE = new AvaticaNoopDriver();

  static {
    try {
      DriverManager.registerDriver(INSTANCE);
    } catch (Exception e) {
      System.err.println("Failed to register driver");
      e.printStackTrace(System.err);
    }
  }

  @Override public Connection connect(String url, Properties info) throws SQLException {
    return AvaticaNoopConnection.getInstance();
  }

  @Override public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith("jdbc:avatica:noop");
  }

  @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
      throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override public int getMajorVersion() {
    return 1;
  }

  @Override public int getMinorVersion() {
    return 7;
  }

  @Override public boolean jdbcCompliant() {
    return true;
  }

  @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return Logger.getLogger("");
  }
}

// End AvaticaNoopDriver.java
