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
package org.apache.calcite.adapter.cassandra;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Utility class for creating SSL contexts for Cassandra connections.
 */
public final class CassandraSSLContext {

  private CassandraSSLContext() {
    // Utility class
  }

  public static SSLContext createSSLContext(String pathToCert, String pathToPrivateKey,
      String keyPassword, String pathToRootCert) throws Exception {
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(null, null);

    if (pathToCert != null && pathToPrivateKey != null) {
      // Load the certificate
      CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
      FileInputStream certFile = new FileInputStream(pathToCert);
      X509Certificate certificate = (X509Certificate) certFactory.generateCertificate(certFile);

      // Load the private key
      String key =
          new String(Files.readAllBytes(Paths.get(pathToPrivateKey)), StandardCharsets.UTF_8);
      key = key.replace("-----BEGIN PRIVATE KEY-----", "")
          .replace("-----END PRIVATE KEY-----", "").replace("\\s", "");
      byte[] keyBytes = Base64.getDecoder().decode(key);
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      PrivateKey privateKey = keyFactory.generatePrivate(keySpec);

      // Add the certificate and private key to the KeyStore
      keyStore.setKeyEntry("alias", privateKey,
          keyPassword != null ? keyPassword.toCharArray() : new char[0],
          new java.security.cert.Certificate[]{certificate});
    }

    // Initialize KeyManagerFactory with the KeyStore
    KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore,
        keyPassword != null ? keyPassword.toCharArray() : new char[0]);

    // Load the root CA certificate if provided
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null, null);
    if (pathToRootCert != null) {
      CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
      FileInputStream rootCertFile = new FileInputStream(pathToRootCert);
      X509Certificate rootCA = (X509Certificate) certFactory.generateCertificate(rootCertFile);
      trustStore.setCertificateEntry("rootCA", rootCA);
    }

    // Initialize TrustManagerFactory with the TrustStore
    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);

    // Initialize SSLContext with KeyManagers and TrustManagers
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagerFactory.getKeyManagers(),
        trustManagerFactory.getTrustManagers(), null);

    return sslContext;
  }
}
