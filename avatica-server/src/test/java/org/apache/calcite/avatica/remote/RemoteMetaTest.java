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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.RemoteDriverTest;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;

import com.google.common.cache.Cache;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests covering {@link RemoteMeta}. */
public class RemoteMetaTest {
  private static final RemoteDriverTest.ConnectionSpec CONNECTION_SPEC =
      RemoteDriverTest.ConnectionSpec.HSQLDB;

  private static HttpServer start;
  private static String url;

  /** Factory that provides a JMeta */
  public static class FullyRemoteJdbcMetaFactory implements Meta.Factory {

    private static JdbcMeta instance = null;

    private static JdbcMeta getInstance() {
      if (instance == null) {
        try {
          instance = new JdbcMeta(CONNECTION_SPEC.url, CONNECTION_SPEC.username,
              CONNECTION_SPEC.password);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
      return instance;
    }

    @Override public Meta create(List<String> args) {
      return getInstance();
    }
  }

  @BeforeClass public static void beforeClass() throws Exception {
    start = Main.start(new String[] { FullyRemoteJdbcMetaFactory.class.getName() });
    final int port = start.getPort();
    url = "jdbc:avatica:remote:url=http://localhost:" + port;
  }

  @AfterClass public static void afterClass() throws Exception {
    if (start != null) {
      start.stop();
    }
  }

  private static Meta getMeta(AvaticaConnection conn) throws Exception {
    Field f = AvaticaConnection.class.getDeclaredField("meta");
    f.setAccessible(true);
    return (Meta) f.get(conn);
  }

  private static Meta.ExecuteResult prepareAndExecuteInternal(AvaticaConnection conn,
    final AvaticaStatement statement, String sql, int maxRowCount) throws Exception {
    Method m = AvaticaConnection.class.getDeclaredMethod("prepareAndExecuteInternal",
      AvaticaStatement.class, String.class, int.class);
    m.setAccessible(true);
    return (Meta.ExecuteResult) m.invoke(conn, statement, sql, new Integer(maxRowCount));
  }

  private static Connection getConnection(JdbcMeta m, String id) throws Exception {
    Field f = JdbcMeta.class.getDeclaredField("connectionCache");
    f.setAccessible(true);
    //noinspection unchecked
    Cache<String, Connection> connectionCache = (Cache<String, Connection>) f.get(m);
    return connectionCache.getIfPresent(id);
  }

  @Test public void testRemoteExecuteMaxRowCount() throws Exception {
    try (AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url)) {
      final AvaticaStatement statement = conn.createStatement();
      prepareAndExecuteInternal(conn, statement,
        "select * from (values ('a', 1), ('b', 2))", 0);
      ResultSet rs = statement.getResultSet();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals("Check maxRowCount=0 and ResultSets is 0 row", count, 0);
      assertEquals("Check result set meta is still there",
        rs.getMetaData().getColumnCount(), 2);
      rs.close();
      statement.close();
      conn.close();
    }
  }

  @Test public void testRemoteConnectionProperties() throws Exception {
    try (AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url)) {
      String id = conn.id;
      final Map<String, ConnectionPropertiesImpl> m = ((RemoteMeta) getMeta(conn)).propsMap;
      assertFalse("remote connection map should start ignorant", m.containsKey(id));
      // force creating a connection object on the remote side.
      try (final Statement stmt = conn.createStatement()) {
        assertTrue("creating a statement starts a local object.", m.containsKey(id));
        assertTrue(stmt.execute("select count(1) from EMP"));
      }
      Connection remoteConn = getConnection(FullyRemoteJdbcMetaFactory.getInstance(), id);
      final boolean defaultRO = remoteConn.isReadOnly();
      final boolean defaultAutoCommit = remoteConn.getAutoCommit();
      final String defaultCatalog = remoteConn.getCatalog();
      final String defaultSchema = remoteConn.getSchema();
      conn.setReadOnly(!defaultRO);
      assertTrue("local changes dirty local state", m.get(id).isDirty());
      assertEquals("remote connection has not been touched", defaultRO, remoteConn.isReadOnly());
      conn.setAutoCommit(!defaultAutoCommit);
      assertEquals("remote connection has not been touched",
          defaultAutoCommit, remoteConn.getAutoCommit());

      // further interaction with the connection will force a sync
      try (final Statement stmt = conn.createStatement()) {
        assertEquals(!defaultAutoCommit, remoteConn.getAutoCommit());
        assertFalse("local values should be clean", m.get(id).isDirty());
      }
    }
  }
}

// End RemoteMetaTest.java
