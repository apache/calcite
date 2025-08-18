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
package org.apache.calcite.adapter.file.storage;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Token manager for SharePoint REST API using certificate authentication.
 * This implements the client assertion flow required for SharePoint REST API.
 */
public class SharePointCertificateTokenManager extends SharePointRestTokenManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(SharePointCertificateTokenManager.class);
  
  private final ObjectMapper mapper = new ObjectMapper();
  private final PrivateKey privateKey;
  private final X509Certificate certificate;
  private final String thumbprint;
  
  /**
   * Creates a token manager using certificate authentication.
   * 
   * @param tenantId The Azure AD tenant ID
   * @param clientId The application client ID
   * @param certificatePath Path to the PFX certificate file
   * @param certificatePassword Password for the PFX file
   * @param siteUrl The SharePoint site URL
   */
  public SharePointCertificateTokenManager(String tenantId, String clientId,
                                          String certificatePath, String certificatePassword,
                                          String siteUrl) throws Exception {
    // Pass a dummy client secret to trigger client credentials flow
    super(tenantId, clientId, "CERTIFICATE", siteUrl);
    
    // Load certificate from PFX file
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    try (InputStream certStream = certificatePath.startsWith("classpath:") 
        ? getClass().getResourceAsStream(certificatePath.substring(10))
        : new FileInputStream(certificatePath)) {
      keyStore.load(certStream, certificatePassword.toCharArray());
    }
    
    // Get the first alias (there should only be one)
    String alias = keyStore.aliases().nextElement();
    
    // Extract private key and certificate
    this.privateKey = (PrivateKey) keyStore.getKey(alias, certificatePassword.toCharArray());
    this.certificate = (X509Certificate) keyStore.getCertificate(alias);
    
    // Calculate thumbprint (SHA-1 hash of certificate)
    java.security.MessageDigest sha1 = java.security.MessageDigest.getInstance("SHA-1");
    byte[] certHash = sha1.digest(certificate.getEncoded());
    this.thumbprint = Base64.getUrlEncoder().withoutPadding().encodeToString(certHash);
    
    LOGGER.debug("Certificate loaded successfully");
    LOGGER.debug("  Subject: {}", certificate.getSubjectX500Principal().getName());
    LOGGER.debug("  Thumbprint: {}", thumbprint);
  }
  
  @Override
  protected void refreshWithClientCredentials() throws IOException {
    try {
      String tokenUrl = String.format(
          "https://login.microsoftonline.com/%s/oauth2/v2.0/token", 
          tenantId);
      
      // Extract SharePoint domain from site URL
      URI uri = URI.create(getSiteUrl());
      String sharePointDomain = uri.getHost();
      
      // Create scope for SharePoint REST API
      String scope = String.format("https://%s/.default", sharePointDomain);
      
      // Create client assertion (JWT)
      String clientAssertion = createClientAssertion(tokenUrl);
      
      LOGGER.debug("SharePoint Certificate Authentication:");
      LOGGER.debug("  Token URL: {}", tokenUrl);
      LOGGER.debug("  Scope: {}", scope);
      LOGGER.debug("  Client ID: {}", clientId);
      LOGGER.debug("  Using certificate thumbprint: {}", thumbprint);
      
      // Build request body
      String requestBody = String.format(
          "grant_type=client_credentials&client_id=%s&scope=%s&" +
          "client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer&" +
          "client_assertion=%s",
          URLEncoder.encode(clientId, StandardCharsets.UTF_8),
          URLEncoder.encode(scope, StandardCharsets.UTF_8),
          URLEncoder.encode(clientAssertion, StandardCharsets.UTF_8));
      
      // Make token request
      Map<String, Object> response = makeTokenRequest(tokenUrl, requestBody);
      
      // Update token from response
      if (response.containsKey("access_token")) {
        this.accessToken = (String) response.get("access_token");
        
        // Calculate expiry
        int expiresIn = ((Number) response.getOrDefault("expires_in", 3600)).intValue();
        this.tokenExpiry = Instant.now().plusSeconds(expiresIn - 60);
        
        LOGGER.debug("  Token acquired successfully (expires in {} seconds)", expiresIn);
      } else {
        throw new IOException("No access token in response");
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to refresh token with certificate", e);
    }
  }
  
  /**
   * Creates a client assertion JWT for certificate authentication.
   */
  private String createClientAssertion(String audience) throws Exception {
    // JWT Header
    String header = String.format(
        "{\"alg\":\"RS256\",\"typ\":\"JWT\",\"x5t\":\"%s\"}", 
        thumbprint);
    
    // JWT Payload
    long now = System.currentTimeMillis() / 1000;
    String payload = String.format(
        "{\"aud\":\"%s\",\"exp\":%d,\"iss\":\"%s\",\"jti\":\"%s\",\"nbf\":%d,\"sub\":\"%s\",\"iat\":%d}",
        audience,
        now + 600, // Expires in 10 minutes
        clientId,
        UUID.randomUUID().toString(),
        now,
        clientId,
        now);
    
    // Encode header and payload
    String encodedHeader = Base64.getUrlEncoder().withoutPadding()
        .encodeToString(header.getBytes(StandardCharsets.UTF_8));
    String encodedPayload = Base64.getUrlEncoder().withoutPadding()
        .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    
    // Create signature
    String signatureInput = encodedHeader + "." + encodedPayload;
    java.security.Signature signature = java.security.Signature.getInstance("SHA256withRSA");
    signature.initSign(privateKey);
    signature.update(signatureInput.getBytes(StandardCharsets.UTF_8));
    byte[] signatureBytes = signature.sign();
    String encodedSignature = Base64.getUrlEncoder().withoutPadding()
        .encodeToString(signatureBytes);
    
    // Combine to create JWT
    return signatureInput + "." + encodedSignature;
  }
  
  /**
   * Makes a token request to Azure AD.
   */
  private Map<String, Object> makeTokenRequest(String tokenUrl, String requestBody) 
      throws IOException {
    URL url = URI.create(tokenUrl).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    conn.setRequestProperty("Accept", "application/json");
    conn.setDoOutput(true);
    
    try (OutputStream os = conn.getOutputStream()) {
      os.write(requestBody.getBytes(StandardCharsets.UTF_8));
      os.flush();
    }
    
    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      String error = "";
      try {
        if (conn.getErrorStream() != null) {
          Map<String, Object> errorResponse = mapper.readValue(conn.getErrorStream(), Map.class);
          if (errorResponse.containsKey("error_description")) {
            error = (String) errorResponse.get("error_description");
          }
        }
      } catch (Exception e) {
        // Ignore JSON parsing errors
      }
      throw new IOException("Certificate token request failed: HTTP " + responseCode + 
                          (error.isEmpty() ? "" : " - " + error));
    }
    
    return mapper.readValue(conn.getInputStream(), Map.class);
  }
}