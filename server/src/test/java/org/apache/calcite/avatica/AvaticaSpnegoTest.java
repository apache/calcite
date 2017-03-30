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
package org.apache.calcite.avatica;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.HttpServer;

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.apache.kerby.kerberos.kerb.client.KrbConfig;
import org.apache.kerby.kerberos.kerb.client.KrbConfigKey;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;

import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.security.auth.Subject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * End to end test case for SPNEGO with Avatica.
 */
@RunWith(Parameterized.class)
@Ignore("Disabled due to [CALCITE-1183] intermittent HTTP 404 failures")
public class AvaticaSpnegoTest {
  private static final Logger LOG = LoggerFactory.getLogger(AvaticaSpnegoTest.class);

  private static final ConnectionSpec CONNECTION_SPEC = ConnectionSpec.HSQLDB;
  private static final List<HttpServer> SERVERS_TO_STOP = new ArrayList<>();

  private static SimpleKdcServer kdc;
  private static KrbConfig clientConfig;
  private static File keytabDir;

  private static int kdcPort;
  private static File clientKeytab;
  private static File serverKeytab;

  private static boolean isKdcStarted = false;

  private static void setupKdc() throws Exception {
    kdc = new SimpleKdcServer();
    File target = new File(System.getProperty("user.dir"), "target");
    assertTrue(target.exists());

    File kdcDir = new File(target, AvaticaSpnegoTest.class.getSimpleName());
    if (kdcDir.exists()) {
      SpnegoTestUtil.deleteRecursively(kdcDir);
    }
    kdcDir.mkdirs();
    kdc.setWorkDir(kdcDir);

    kdc.setKdcHost(SpnegoTestUtil.KDC_HOST);
    kdcPort = SpnegoTestUtil.getFreePort();
    kdc.setAllowTcp(true);
    kdc.setAllowUdp(false);
    kdc.setKdcTcpPort(kdcPort);

    LOG.info("Starting KDC server at {}:{}", SpnegoTestUtil.KDC_HOST, kdcPort);

    kdc.init();
    kdc.start();
    isKdcStarted = true;

    keytabDir = new File(target, AvaticaSpnegoTest.class.getSimpleName()
        + "_keytabs");
    if (keytabDir.exists()) {
      SpnegoTestUtil.deleteRecursively(keytabDir);
    }
    keytabDir.mkdirs();
    setupServerUser(keytabDir);

    clientConfig = new KrbConfig();
    clientConfig.setString(KrbConfigKey.KDC_HOST, SpnegoTestUtil.KDC_HOST);
    clientConfig.setInt(KrbConfigKey.KDC_TCP_PORT, kdcPort);
    clientConfig.setString(KrbConfigKey.DEFAULT_REALM, SpnegoTestUtil.REALM);

    // Kerby sets "java.security.krb5.conf" for us!
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    //System.setProperty("sun.security.spnego.debug", "true");
    //System.setProperty("sun.security.krb5.debug", "true");
  }

  @AfterClass public static void stopKdc() throws Exception {
    for (HttpServer server : SERVERS_TO_STOP) {
      server.stop();
    }

    if (isKdcStarted) {
      LOG.info("Stopping KDC on {}", kdcPort);
      kdc.stop();
    }
  }

  private static void setupServerUser(File keytabDir) throws KrbException {
    // Create the client user
    String clientPrincipal = SpnegoTestUtil.CLIENT_PRINCIPAL.substring(0,
        SpnegoTestUtil.CLIENT_PRINCIPAL.indexOf('@'));
    clientKeytab = new File(keytabDir, clientPrincipal.replace('/', '_') + ".keytab");
    if (clientKeytab.exists()) {
      SpnegoTestUtil.deleteRecursively(clientKeytab);
    }
    LOG.info("Creating {} with keytab {}", clientPrincipal, clientKeytab);
    SpnegoTestUtil.setupUser(kdc, clientKeytab, clientPrincipal);

    // Create the server user
    String serverPrincipal = SpnegoTestUtil.SERVER_PRINCIPAL.substring(0,
        SpnegoTestUtil.SERVER_PRINCIPAL.indexOf('@'));
    serverKeytab = new File(keytabDir, serverPrincipal.replace('/', '_') + ".keytab");
    if (serverKeytab.exists()) {
      SpnegoTestUtil.deleteRecursively(serverKeytab);
    }
    LOG.info("Creating {} with keytab {}", SpnegoTestUtil.SERVER_PRINCIPAL, serverKeytab);
    SpnegoTestUtil.setupUser(kdc, serverKeytab, SpnegoTestUtil.SERVER_PRINCIPAL);
  }

  @Parameters public static List<Object[]> parameters() throws Exception {
    final ArrayList<Object[]> parameters = new ArrayList<>();

    // Start the KDC
    setupKdc();

    // Create a LocalService around HSQLDB
    final JdbcMeta jdbcMeta = new JdbcMeta(CONNECTION_SPEC.url,
        CONNECTION_SPEC.username, CONNECTION_SPEC.password);
    final LocalService localService = new LocalService(jdbcMeta);

    for (Driver.Serialization serialization : new Driver.Serialization[] {
      Driver.Serialization.JSON, Driver.Serialization.PROTOBUF}) {
      // Build and start the server
      HttpServer httpServer = new HttpServer.Builder()
          .withPort(0)
          .withAutomaticLogin(serverKeytab)
          .withSpnego(SpnegoTestUtil.SERVER_PRINCIPAL, SpnegoTestUtil.REALM)
          .withHandler(localService, serialization)
          .build();
      httpServer.start();
      SERVERS_TO_STOP.add(httpServer);

      final String url = "jdbc:avatica:remote:url=http://" + SpnegoTestUtil.KDC_HOST + ":"
          + httpServer.getPort() + ";authentication=SPNEGO;serialization=" + serialization;
      LOG.info("JDBC URL {}", url);

      parameters.add(new Object[] {url});
    }

    return parameters;
  }

  private final String jdbcUrl;

  public AvaticaSpnegoTest(String jdbcUrl) {
    this.jdbcUrl = Objects.requireNonNull(jdbcUrl);
  }

  @Test public void testAuthenticatedClient() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      final String tableName = "allowed_clients";
      // Create the subject for the client
      final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(SpnegoTestUtil.CLIENT_PRINCIPAL,
          clientKeytab);

      // The name of the principal

      // Run this code, logged in as the subject (the client)
      Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
        @Override public Void run() throws Exception {
          try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try (Statement stmt = conn.createStatement()) {
              assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
              assertFalse(stmt.execute("CREATE TABLE " + tableName + "(pk integer)"));
              assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(1)"));
              assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(2)"));
              assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(3)"));

              ResultSet results = stmt.executeQuery("SELECT count(1) FROM " + tableName);
              assertTrue(results.next());
              assertEquals(3, results.getInt(1));
            }
          }
          return null;
        }
      });
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testAutomaticLogin() throws Exception {
    final String tableName = "automaticAllowedClients";
    // Avatica should log in for us with this info
    String url = jdbcUrl + ";principal=" + SpnegoTestUtil.CLIENT_PRINCIPAL + ";keytab="
        + clientKeytab;
    LOG.info("Updated JDBC url: {}", url);
    try (Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      assertFalse(stmt.execute("CREATE TABLE " + tableName + "(pk integer)"));
      assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(1)"));
      assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(2)"));
      assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(3)"));

      ResultSet results = stmt.executeQuery("SELECT count(1) FROM " + tableName);
      assertTrue(results.next());
      assertEquals(3, results.getInt(1));
    }
  }
}

// End AvaticaSpnegoTest.java
