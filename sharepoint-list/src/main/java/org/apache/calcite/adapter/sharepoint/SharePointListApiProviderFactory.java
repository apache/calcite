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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.adapter.sharepoint.auth.DirectAuthProvider;
import org.apache.calcite.adapter.sharepoint.auth.ProxyAuthProvider;
import org.apache.calcite.adapter.sharepoint.auth.SharePointAuthProvider;

import java.net.URI;
import java.util.Locale;
import java.util.Map;

/**
 * Factory for creating SharePoint List API providers.
 * Automatically selects the appropriate API (Graph or REST) based on configuration
 * and SharePoint deployment type.
 */
public class SharePointListApiProviderFactory {
  
  /**
   * Creates a SharePointListApiProvider based on configuration.
   *
   * @param config Configuration map containing auth and API selection settings
   * @return Appropriate API provider instance
   */
  public static SharePointListApiProvider create(Map<String, Object> config) {
    // First create the auth provider
    SharePointAuthProvider authProvider = createAuthProvider(config);
    
    // Then select the API provider
    return createApiProvider(authProvider, config);
  }
  
  /**
   * Creates the appropriate auth provider based on configuration.
   */
  private static SharePointAuthProvider createAuthProvider(Map<String, Object> config) {
    // Check for proxy auth first (Phase 3)
    if (config.containsKey("authProxy")) {
      return new ProxyAuthProvider(config);
    }
    
    // Check for custom auth provider class
    if (config.containsKey("authProvider")) {
      String className = (String) config.get("authProvider");
      try {
        Class<?> clazz = Class.forName(className);
        if (SharePointAuthProvider.class.isAssignableFrom(clazz)) {
          return (SharePointAuthProvider) clazz
              .getConstructor(Map.class)
              .newInstance(config);
        } else {
          throw new IllegalArgumentException(
              "Auth provider class must implement SharePointAuthProvider: " + className);
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to instantiate auth provider: " + className, e);
      }
    }
    
    // Default to direct auth provider (Phases 1 & 2)
    return new DirectAuthProvider(config);
  }
  
  /**
   * Creates the appropriate API provider based on auth capabilities and configuration.
   */
  private static SharePointListApiProvider createApiProvider(
      SharePointAuthProvider authProvider, Map<String, Object> config) {
    
    String siteUrl = authProvider.getSiteUrl();
    
    // Explicit API selection
    if (Boolean.TRUE.equals(config.get("useGraphApi"))) {
      if (!authProvider.supportsApiType("graph")) {
        throw new IllegalArgumentException(
            "Auth provider does not support Microsoft Graph API");
      }
      return new GraphListApiProvider(authProvider);
    }
    
    if (Boolean.TRUE.equals(config.get("useRestApi")) || 
        Boolean.TRUE.equals(config.get("useLegacyAuth"))) {
      if (!authProvider.supportsApiType("rest")) {
        throw new IllegalArgumentException(
            "Auth provider does not support SharePoint REST API");
      }
      return new SharePointRestListApiProvider(authProvider);
    }
    
    // Auto-detection based on site URL and auth type
    if (isOnPremisesSharePoint(siteUrl)) {
      // On-premises SharePoint requires REST API
      if (!authProvider.supportsApiType("rest")) {
        throw new IllegalArgumentException(
            "On-premises SharePoint requires REST API support, " +
            "but auth provider doesn't support it");
      }
      return new SharePointRestListApiProvider(authProvider);
    }
    
    // SharePoint Online - prefer Graph API if supported, fallback to REST
    if (authProvider.supportsApiType("graph")) {
      return new GraphListApiProvider(authProvider);
    } else if (authProvider.supportsApiType("rest")) {
      return new SharePointRestListApiProvider(authProvider);
    } else {
      throw new IllegalArgumentException(
          "Auth provider doesn't support any compatible API");
    }
  }
  
  /**
   * Determines if a SharePoint site is on-premises based on URL.
   */
  private static boolean isOnPremisesSharePoint(String siteUrl) {
    if (siteUrl == null) {
      return false;
    }
    
    URI uri = URI.create(siteUrl);
    String host = uri.getHost().toLowerCase(Locale.ROOT);
    
    // SharePoint Online domains
    if (host.endsWith(".sharepoint.com") || 
        host.endsWith(".sharepoint.cn") ||
        host.endsWith(".sharepoint.de") ||
        host.endsWith(".sharepoint-mil.us") ||
        host.endsWith(".sharepoint.us")) {
      return false;
    }
    
    // Check for common on-premises patterns
    if (host.startsWith("sharepoint.") ||
        host.contains(".local") ||
        host.contains(".corp") ||
        host.contains(".internal") ||
        isPrivateIP(host)) {
      return true;
    }
    
    // When in doubt, assume on-premises for non-Microsoft domains
    return true;
  }
  
  /**
   * Checks if a hostname is a private IP address.
   */
  private static boolean isPrivateIP(String host) {
    // Check for IP address patterns
    if (!host.matches("\\d+\\.\\d+\\.\\d+\\.\\d+")) {
      return false;
    }
    
    String[] parts = host.split("\\.");
    int first = Integer.parseInt(parts[0]);
    int second = Integer.parseInt(parts[1]);
    
    // Private IP ranges
    return (first == 10) ||
           (first == 172 && second >= 16 && second <= 31) ||
           (first == 192 && second == 168) ||
           (first == 127);
  }
  
  private SharePointListApiProviderFactory() {
    // Utility class
  }
}