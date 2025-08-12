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

import org.apache.calcite.adapter.file.iceberg.IcebergStorageProvider;

import com.amazonaws.services.s3.AmazonS3;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating storage provider instances based on URL or storage type.
 */
public class StorageProviderFactory {

  private static final Map<String, StorageProvider> PROVIDER_CACHE = new ConcurrentHashMap<>();

  /**
   * Creates a storage provider based on the URL scheme.
   *
   * @param url The URL to access
   * @return Appropriate storage provider
   */
  public static StorageProvider createFromUrl(String url) {
    if (url == null || url.isEmpty()) {
      return new LocalFileStorageProvider();
    }

    // Extract scheme
    int schemeEnd = url.indexOf("://");
    if (schemeEnd < 0) {
      // No scheme, assume local file
      return getCachedProvider("local", LocalFileStorageProvider::new);
    }

    String scheme = url.substring(0, schemeEnd).toLowerCase(Locale.ROOT);

    switch (scheme) {
      case "file":
        return getCachedProvider("local", LocalFileStorageProvider::new);

      case "http":
      case "https":
        return getCachedProvider("http", HttpStorageProvider::new);

      case "s3":
        return getCachedProvider("s3", S3StorageProvider::new);

      case "ftp":
      case "ftps":
        return getCachedProvider("ftp", FtpStorageProvider::new);

      case "sftp":
        return getCachedProvider("sftp", SftpStorageProvider::new);

      case "iceberg":
        // Iceberg URLs would be like: iceberg://catalog/namespace/table
        // But typically Iceberg is accessed via storageType, not URL
        return new IcebergStorageProvider(new java.util.HashMap<>());

      default:
        throw new IllegalArgumentException("Unsupported URL scheme: " + scheme);
    }
  }

  /**
   * Creates a storage provider based on explicit storage type.
   *
   * @param storageType The storage type (local, s3, http, ftp, sharepoint)
   * @param config Configuration parameters for the provider
   * @return Appropriate storage provider
   */
  public static StorageProvider createFromType(String storageType, Map<String, Object> config) {
    if (storageType == null || storageType.isEmpty()) {
      storageType = "local";
    }

    switch (storageType.toLowerCase(Locale.ROOT)) {
      case "local":
      case "file":
        return getCachedProvider("local", LocalFileStorageProvider::new);

      case "http":
      case "https":
        if (config != null && (config.containsKey("method") || config.containsKey("body") 
            || config.containsKey("headers") || config.containsKey("mimeType"))) {
          // Create configured HTTP provider
          HttpConfig httpConfig = HttpConfig.fromMap(config);
          return httpConfig.createStorageProvider();
        }
        return getCachedProvider("http", HttpStorageProvider::new);

      case "s3":
        if (config != null && config.containsKey("s3Client")) {
          // Custom S3 client provided
          return new S3StorageProvider((AmazonS3) config.get("s3Client"));
        }
        return getCachedProvider("s3", S3StorageProvider::new);

      case "ftp":
      case "ftps":
        return getCachedProvider("ftp", FtpStorageProvider::new);

      case "sftp":
        if (config != null) {
          String username = (String) config.get("username");
          String password = (String) config.get("password");
          String privateKeyPath = (String) config.get("privateKeyPath");
          Boolean strictHostKey = (Boolean) config.get("strictHostKeyChecking");

          return new SftpStorageProvider(
              username,
              password,
              privateKeyPath,
              strictHostKey != null ? strictHostKey : false);
        }
        return getCachedProvider("sftp", SftpStorageProvider::new);

      case "sharepoint":
        if (config == null || !config.containsKey("siteUrl")) {
          throw new IllegalArgumentException("SharePoint storage requires 'siteUrl' in config");
        }

        String siteUrl = (String) config.get("siteUrl");

        // Check for different authentication methods
        SharePointTokenManager tokenManager = null;

        if (config.containsKey("accessToken")) {
          // Static token (backward compatibility)
          String accessToken = (String) config.get("accessToken");
          tokenManager = new SharePointTokenManager(accessToken, siteUrl);

        } else if (config.containsKey("clientId") && config.containsKey("clientSecret")) {
          // Client credentials flow (app-only)
          String tenantId = (String) config.get("tenantId");
          String clientId = (String) config.get("clientId");
          String clientSecret = (String) config.get("clientSecret");

          if (tenantId == null) {
            throw new IllegalArgumentException(
                "SharePoint client credentials auth requires 'tenantId'");
          }

          tokenManager = new SharePointTokenManager(tenantId, clientId, clientSecret, siteUrl);

        } else if (config.containsKey("refreshToken")) {
          // Refresh token flow
          String tenantId = (String) config.get("tenantId");
          String clientId = (String) config.get("clientId");
          String refreshToken = (String) config.get("refreshToken");

          if (tenantId == null || clientId == null) {
            throw new IllegalArgumentException(
                "SharePoint refresh token auth requires 'tenantId' and 'clientId'");
          }

          tokenManager = new SharePointTokenManager(tenantId, clientId, refreshToken, siteUrl, true);

        } else if (config.containsKey("certificatePath") && config.containsKey("certificatePassword")) {
          // Certificate authentication - still needs tenant and client IDs
          String tenantId = (String) config.get("tenantId");
          String clientId = (String) config.get("clientId");
          
          if (tenantId == null || clientId == null) {
            throw new IllegalArgumentException(
                "Certificate authentication requires 'tenantId' and 'clientId'");
          }
          // Create a dummy token manager - certificate handling is done below
          tokenManager = new SharePointTokenManager(tenantId, clientId, "CERTIFICATE", siteUrl);
        } else {
          throw new IllegalArgumentException(
              "SharePoint storage requires one of: 'accessToken', " +
              "'clientId+clientSecret', 'refreshToken', or 'certificatePath+certificatePassword' for authentication");
        }

        // Determine which API to use
        boolean useGraphApi = config.containsKey("useGraphApi") 
            && Boolean.TRUE.equals(config.get("useGraphApi"));
        boolean useLegacyAuth = config.containsKey("useLegacyAuth")
            && Boolean.TRUE.equals(config.get("useLegacyAuth"));
        
        if (useGraphApi) {
          // Use Microsoft Graph API
          MicrosoftGraphTokenManager graphTokenManager;
          if (tokenManager.clientSecret != null) {
            graphTokenManager =
                new MicrosoftGraphTokenManager(tokenManager.tenantId, tokenManager.clientId,
                tokenManager.clientSecret, tokenManager.getSiteUrl());
          } else if (tokenManager.refreshToken != null) {
            graphTokenManager =
                new MicrosoftGraphTokenManager(tokenManager.tenantId, tokenManager.clientId,
                tokenManager.refreshToken, tokenManager.getSiteUrl(), true);
          } else {
            // Static token
            graphTokenManager =
                new MicrosoftGraphTokenManager(tokenManager.accessToken, tokenManager.getSiteUrl());
          }
          return new MicrosoftGraphStorageProvider(graphTokenManager);
        } else if (useLegacyAuth && tokenManager.clientSecret != null) {
          // Use legacy SharePoint authentication (works with client secret)
          // Note: Legacy auth uses realm instead of tenantId
          String realm = config.containsKey("realm") ? (String) config.get("realm") : null;
          SharePointLegacyTokenManager legacyTokenManager = 
              realm != null 
              ? new SharePointLegacyTokenManager(tokenManager.clientId, 
                  tokenManager.clientSecret, tokenManager.getSiteUrl(), realm)
              : new SharePointLegacyTokenManager(tokenManager.clientId,
                  tokenManager.clientSecret, tokenManager.getSiteUrl());
          return new SharePointRestStorageProvider(legacyTokenManager);
        } else {
          // Use SharePoint REST API with modern auth
          // Check for certificate authentication
          if (config.containsKey("certificatePath") && config.containsKey("certificatePassword")) {
            try {
              String certificatePath = (String) config.get("certificatePath");
              String certificatePassword = (String) config.get("certificatePassword");
              SharePointCertificateTokenManager certTokenManager =
                  new SharePointCertificateTokenManager(tokenManager.tenantId, tokenManager.clientId,
                      certificatePath, certificatePassword, tokenManager.getSiteUrl());
              return new SharePointRestStorageProvider(certTokenManager);
            } catch (Exception e) {
              throw new RuntimeException("Failed to initialize certificate authentication", e);
            }
          }
          
          // Fall back to client secret (will fail with REST API but might work with Graph)
          SharePointRestTokenManager restTokenManager;
          if (tokenManager.clientSecret != null) {
            restTokenManager =
                new SharePointRestTokenManager(tokenManager.tenantId, tokenManager.clientId,
                tokenManager.clientSecret, tokenManager.getSiteUrl());
          } else if (tokenManager.refreshToken != null) {
            restTokenManager =
                new SharePointRestTokenManager(tokenManager.tenantId, tokenManager.clientId,
                tokenManager.refreshToken, tokenManager.getSiteUrl(), true);
          } else {
            // Static token
            restTokenManager =
                new SharePointRestTokenManager(tokenManager.accessToken, tokenManager.getSiteUrl());
          }
          return new SharePointRestStorageProvider(restTokenManager);
        }

      case "iceberg":
        // Create Iceberg storage provider with configuration
        return new IcebergStorageProvider(config != null ? config : new java.util.HashMap<>());

      default:
        throw new IllegalArgumentException("Unsupported storage type: " + storageType);
    }
  }

  /**
   * Clears the provider cache. Useful for testing.
   */
  public static void clearCache() {
    PROVIDER_CACHE.clear();
  }

  private static StorageProvider getCachedProvider(String key,
                                                   java.util.function.Supplier<StorageProvider> creator) {
    return PROVIDER_CACHE.computeIfAbsent(key, k -> creator.get());
  }

  private StorageProviderFactory() {
    // Utility class
  }
}
