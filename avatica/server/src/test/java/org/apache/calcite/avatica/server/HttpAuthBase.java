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
package org.apache.calcite.avatica.server;

import org.apache.calcite.avatica.ConnectionSpec;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Common test logic for HTTP basic and digest auth
 */
public class HttpAuthBase {

  static boolean userExists(Statement stmt, String user) throws SQLException {
    ResultSet results = stmt.executeQuery(
        "SELECT * FROM INFORMATION_SCHEMA.SYSTEM_USERS WHERE USER_NAME = '" + user + "'");
    return results.next();
  }

  static void createHsqldbUsers() throws SQLException {
    try (Connection conn = DriverManager.getConnection(ConnectionSpec.HSQLDB.url,
        ConnectionSpec.HSQLDB.username, ConnectionSpec.HSQLDB.password);
        Statement stmt = conn.createStatement()) {
      for (int i = 2; i <= 5; i++) {
        // Users 2-5 exist (not user1)
        final String username = "USER" + i;
        final String password = "password" + i;
        if (userExists(stmt, username)) {
          stmt.execute("DROP USER " + username);
        }
        stmt.execute("CREATE USER " + username + " PASSWORD '" + password + "'");
        // Grant permission to the user we create (defined in the scottdb hsqldb impl)
        stmt.execute("GRANT DBA TO " + username);
      }
    }
  }

  void readWriteData(String url, String tableName, Properties props) throws Exception {
    try (Connection conn = DriverManager.getConnection(url, props);
        Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      assertFalse(stmt.execute("CREATE TABLE " + tableName + " (pk integer, msg varchar(10))"));

      assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(1, 'abcd')"));
      assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(2, 'bcde')"));
      assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(3, 'cdef')"));

      ResultSet results = stmt.executeQuery("SELECT count(1) FROM " + tableName);
      assertNotNull(results);
      assertTrue(results.next());
      assertEquals(3, results.getInt(1));
    }
  }
}

// End HttpAuthBase.java
