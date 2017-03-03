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
import org.apache.calcite.avatica.util.DateTimeUtils;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.IETFUtils;
import org.bouncycastle.asn1.x500.style.RFC4519Style;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.provider.X509CertificateObject;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test case for Avatica with TLS connectors.
 */
@RunWith(Parameterized.class)
public class SslDriverTest {
  private static final Logger LOG = LoggerFactory.getLogger(SslDriverTest.class);

  private static File keystore;
  private static final String KEYSTORE_PASSWORD = "avaticasecret";
  private static final ConnectionSpec CONNECTION_SPEC = ConnectionSpec.HSQLDB;
  private static final List<HttpServer> SERVERS_TO_STOP = new ArrayList<>();

  @Parameters public static List<Object[]> parameters() throws Exception {
    final ArrayList<Object[]> parameters = new ArrayList<>();

    // Create a self-signed cert
    File target = new File(System.getProperty("user.dir"), "target");
    keystore = new File(target, "avatica-test.jks");
    if (keystore.isFile()) {
      assertTrue("Failed to delete keystore: " + keystore, keystore.delete());
    }
    new CertTool().createSelfSignedCert(keystore, "avatica", KEYSTORE_PASSWORD);

    // Create a LocalService around HSQLDB
    final JdbcMeta jdbcMeta = new JdbcMeta(CONNECTION_SPEC.url,
        CONNECTION_SPEC.username, CONNECTION_SPEC.password);
    final LocalService localService = new LocalService(jdbcMeta);

    for (Driver.Serialization serialization : new Driver.Serialization[] {
      Driver.Serialization.JSON, Driver.Serialization.PROTOBUF}) {
      // Build and start the server, using TLS
      HttpServer httpServer = new HttpServer.Builder()
          .withPort(0)
          .withTLS(keystore, KEYSTORE_PASSWORD, keystore, KEYSTORE_PASSWORD)
          .withHandler(localService, serialization)
          .build();
      httpServer.start();
      SERVERS_TO_STOP.add(httpServer);

      final String url = "jdbc:avatica:remote:url=https://localhost:" + httpServer.getPort()
          + ";serialization=" + serialization + ";truststore=" + keystore.getAbsolutePath()
          + ";truststore_password=" + KEYSTORE_PASSWORD;
      LOG.info("JDBC URL {}", url);

      parameters.add(new Object[] {url});
    }

    return parameters;
  }

  @AfterClass public static void stopKdc() throws Exception {
    for (HttpServer server : SERVERS_TO_STOP) {
      server.stop();
    }
  }

  private final String jdbcUrl;

  public SslDriverTest(String jdbcUrl) {
    this.jdbcUrl = Objects.requireNonNull(jdbcUrl);
  }

  @Test
  public void testReadWrite() throws Exception {
    final String tableName = "testReadWrite";
    try (Connection conn = DriverManager.getConnection(jdbcUrl);
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

  /**
   * Utility class for creating certificates for testing.
   */
  private static class CertTool {
    private static final String SIGNING_ALGORITHM = "SHA256WITHRSA";
    private static final String ENC_ALGORITHM = "RSA";

    static {
      Security.addProvider(new BouncyCastleProvider());
    }

    private void createSelfSignedCert(File targetKeystore, String keyName,
        String keystorePassword) {
      if (targetKeystore.exists()) {
        throw new RuntimeException("Keystore already exists: " + targetKeystore);
      }

      try {
        KeyPair kp = generateKeyPair();

        X509CertificateObject cert = generateCert(keyName, kp, true, kp.getPublic(),
            kp.getPrivate());

        char[] password = keystorePassword.toCharArray();
        KeyStore keystore = KeyStore.getInstance("JKS");
        keystore.load(null, null);
        keystore.setCertificateEntry(keyName + "Cert", cert);
        keystore.setKeyEntry(keyName + "Key", kp.getPrivate(), password, new Certificate[] {cert});
        try (FileOutputStream fos = new FileOutputStream(targetKeystore)) {
          keystore.store(fos, password);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private KeyPair generateKeyPair() throws NoSuchAlgorithmException, NoSuchProviderException {
      KeyPairGenerator gen = KeyPairGenerator.getInstance(ENC_ALGORITHM);
      gen.initialize(2048);
      return gen.generateKeyPair();
    }

    private X509CertificateObject generateCert(String keyName, KeyPair kp, boolean isCertAuthority,
        PublicKey signerPublicKey, PrivateKey signerPrivateKey) throws IOException,
        CertIOException, OperatorCreationException, CertificateException,
        NoSuchAlgorithmException {
      Calendar startDate = DateTimeUtils.calendar();
      Calendar endDate = DateTimeUtils.calendar();
      endDate.add(Calendar.YEAR, 100);

      BigInteger serialNumber = BigInteger.valueOf(startDate.getTimeInMillis());
      X500Name issuer = new X500Name(
          IETFUtils.rDNsFromString("cn=localhost", RFC4519Style.INSTANCE));
      JcaX509v3CertificateBuilder certGen = new JcaX509v3CertificateBuilder(issuer,
          serialNumber, startDate.getTime(), endDate.getTime(), issuer, kp.getPublic());
      JcaX509ExtensionUtils extensionUtils = new JcaX509ExtensionUtils();
      certGen.addExtension(Extension.subjectKeyIdentifier, false,
          extensionUtils.createSubjectKeyIdentifier(kp.getPublic()));
      certGen.addExtension(Extension.basicConstraints, false,
          new BasicConstraints(isCertAuthority));
      certGen.addExtension(Extension.authorityKeyIdentifier, false,
          extensionUtils.createAuthorityKeyIdentifier(signerPublicKey));
      if (isCertAuthority) {
        certGen.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign));
      }
      X509CertificateHolder cert = certGen.build(
          new JcaContentSignerBuilder(SIGNING_ALGORITHM).build(signerPrivateKey));
      return new X509CertificateObject(cert.toASN1Structure());
    }
  }
}

// End SslDriverTest.java
