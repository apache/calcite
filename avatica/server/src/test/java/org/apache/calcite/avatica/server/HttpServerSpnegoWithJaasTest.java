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

import org.apache.calcite.avatica.SpnegoTestUtil;
import org.apache.calcite.avatica.remote.AvaticaCommonsHttpClientSpnegoImpl;

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.apache.kerby.kerberos.kerb.client.KrbConfig;
import org.apache.kerby.kerberos.kerb.client.KrbConfigKey;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class for SPNEGO with Kerberos. Purely testing SPNEGO, not the Avatica "protocol" on top
 * of that HTTP. This variant of the test requires that the user use JAAS configuration to
 * perform server-side login.
 */
public class HttpServerSpnegoWithJaasTest {
  private static final Logger LOG = LoggerFactory.getLogger(HttpServerSpnegoWithJaasTest.class);

  private static SimpleKdcServer kdc;
  private static HttpServer httpServer;

  private static KrbConfig clientConfig;

  private static int kdcPort;

  private static File clientKeytab;
  private static File serverKeytab;

  private static File serverSpnegoConfigFile;

  private static boolean isKdcStarted = false;
  private static boolean isHttpServerStarted = false;

  private static URL httpServerUrl;

  @BeforeClass public static void setupKdc() throws Exception {
    kdc = new SimpleKdcServer();
    File target = new File(System.getProperty("user.dir"), "target");
    assertTrue(target.exists());

    File kdcDir = new File(target, HttpServerSpnegoWithJaasTest.class.getSimpleName());
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

    File keytabDir = new File(target, HttpServerSpnegoWithJaasTest.class.getSimpleName()
        + "_keytabs");
    if (keytabDir.exists()) {
      SpnegoTestUtil.deleteRecursively(keytabDir);
    }
    keytabDir.mkdirs();
    setupUsers(keytabDir);

    clientConfig = new KrbConfig();
    clientConfig.setString(KrbConfigKey.KDC_HOST, SpnegoTestUtil.KDC_HOST);
    clientConfig.setInt(KrbConfigKey.KDC_TCP_PORT, kdcPort);
    clientConfig.setString(KrbConfigKey.DEFAULT_REALM, SpnegoTestUtil.REALM);

    serverSpnegoConfigFile = new File(kdcDir, "server-spnego.conf");
    SpnegoTestUtil.writeSpnegoConf(serverSpnegoConfigFile, serverKeytab);

    // Kerby sets "java.security.krb5.conf" for us!
    System.setProperty("java.security.auth.login.config", serverSpnegoConfigFile.toString());
    // http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/...
    //    tutorials/BasicClientServer.html#useSub
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    //System.setProperty("sun.security.spnego.debug", "true");
    //System.setProperty("sun.security.krb5.debug", "true");

    // Create and start an HTTP server configured only to allow SPNEGO requests
    // We're not using `withAutomaticLogin(File)` which means we're relying on JAAS to log the
    // server in.
    httpServer = new HttpServer.Builder()
        .withPort(0)
        .withSpnego(SpnegoTestUtil.SERVER_PRINCIPAL, SpnegoTestUtil.REALM)
        .withHandler(new SpnegoTestUtil.AuthenticationRequiredAvaticaHandler())
        .build();
    httpServer.start();
    isHttpServerStarted = true;

    httpServerUrl = new URL("http://" + SpnegoTestUtil.KDC_HOST + ":" + httpServer.getPort());
    LOG.info("HTTP server running at {}", httpServerUrl);

    SpnegoTestUtil.refreshJaasConfiguration();
  }

  @AfterClass public static void stopKdc() throws Exception {
    if (isHttpServerStarted) {
      LOG.info("Stopping HTTP server at {}", httpServerUrl);
      httpServer.stop();
    }

    if (isKdcStarted) {
      LOG.info("Stopping KDC on {}", kdcPort);
      kdc.stop();
    }
  }

  private static void setupUsers(File keytabDir) throws KrbException {
    String clientPrincipal = SpnegoTestUtil.CLIENT_PRINCIPAL.substring(0,
        SpnegoTestUtil.CLIENT_PRINCIPAL.indexOf('@'));
    clientKeytab = new File(keytabDir, clientPrincipal.replace('/', '_') + ".keytab");
    if (clientKeytab.exists()) {
      SpnegoTestUtil.deleteRecursively(clientKeytab);
    }
    LOG.info("Creating {} with keytab {}", clientPrincipal, clientKeytab);
    SpnegoTestUtil.setupUser(kdc, clientKeytab, clientPrincipal);

    String serverPrincipal = SpnegoTestUtil.SERVER_PRINCIPAL.substring(0,
        SpnegoTestUtil.SERVER_PRINCIPAL.indexOf('@'));
    serverKeytab = new File(keytabDir, serverPrincipal.replace('/', '_') + ".keytab");
    if (serverKeytab.exists()) {
      SpnegoTestUtil.deleteRecursively(serverKeytab);
    }
    LOG.info("Creating {} with keytab {}", SpnegoTestUtil.SERVER_PRINCIPAL, serverKeytab);
    SpnegoTestUtil.setupUser(kdc, serverKeytab, SpnegoTestUtil.SERVER_PRINCIPAL);
  }

  @Test public void testNormalClientsDisallowed() throws Exception {
    LOG.info("Connecting to {}", httpServerUrl.toString());
    HttpURLConnection conn = (HttpURLConnection) httpServerUrl.openConnection();
    conn.setRequestMethod("GET");
    // Authentication should fail because we didn't provide anything
    assertEquals(401, conn.getResponseCode());
  }

  @Test public void testAuthenticatedClientsAllowed() throws Exception {
    // Create the subject for the client
    final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(SpnegoTestUtil.CLIENT_PRINCIPAL,
        clientKeytab);
    final Set<Principal> clientPrincipals = clientSubject.getPrincipals();
    // Make sure the subject has a principal
    assertFalse(clientPrincipals.isEmpty());

    // Get a TGT for the subject (might have many, different encryption types). The first should
    // be the default encryption type.
    Set<KerberosTicket> privateCredentials =
            clientSubject.getPrivateCredentials(KerberosTicket.class);
    assertFalse(privateCredentials.isEmpty());
    KerberosTicket tgt = privateCredentials.iterator().next();
    assertNotNull(tgt);
    LOG.info("Using TGT with etype: {}", tgt.getSessionKey().getAlgorithm());

    // The name of the principal
    final String principalName = clientPrincipals.iterator().next().getName();

    // Run this code, logged in as the subject (the client)
    byte[] response = Subject.doAs(clientSubject, new PrivilegedExceptionAction<byte[]>() {
      @Override public byte[] run() throws Exception {
        // Logs in with Kerberos via GSS
        GSSManager gssManager = GSSManager.getInstance();
        Oid oid = new Oid(SpnegoTestUtil.JGSS_KERBEROS_TICKET_OID);
        GSSName gssClient = gssManager.createName(principalName, GSSName.NT_USER_NAME);
        GSSCredential credential = gssManager.createCredential(gssClient,
            GSSCredential.DEFAULT_LIFETIME, oid, GSSCredential.INITIATE_ONLY);

        // Passes the GSSCredential into the HTTP client implementation
        final AvaticaCommonsHttpClientSpnegoImpl httpClient =
            new AvaticaCommonsHttpClientSpnegoImpl(httpServerUrl, credential);

        return httpClient.send(new byte[0]);
      }
    });

    // We should get a response which is "OK" with our client's name
    assertNotNull(response);
    assertEquals("OK " + SpnegoTestUtil.CLIENT_PRINCIPAL,
        new String(response, StandardCharsets.UTF_8));
  }
}

// End HttpServerSpnegoWithJaasTest.java
