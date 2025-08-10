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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for HTTP storage provider.
 * Supports GET and POST methods with optional body and headers.
 * 
 * <p>Supports three phases of authentication complexity:
 * <ul>
 *   <li>Phase 1: Simple static authentication (API keys, basic auth)</li>
 *   <li>Phase 2: External token management (commands, files, endpoints)</li>
 *   <li>Phase 3: Full proxy delegation for complex auth protocols</li>
 * </ul>
 */
public class HttpConfig {
  private final String method;
  private final @Nullable String body;
  private final Map<String, String> headers;
  private final @Nullable String mimeType;
  
  // Phase 1: Simple static auth
  private @Nullable String bearerToken;
  private @Nullable String apiKey;
  private @Nullable String username;
  private @Nullable String password;
  
  // Phase 2: External token sources
  private @Nullable String tokenCommand;
  private @Nullable String tokenEnv;
  private @Nullable String tokenFile;
  private @Nullable String tokenEndpoint;
  
  // Phase 2.5: Template-based headers
  private @Nullable Map<String, String> authHeaders;
  
  // Phase 3: Full delegation
  private @Nullable String proxyEndpoint;
  
  // Cache configuration
  private boolean cacheEnabled = true;
  private long cacheTtl = 300000; // 5 minutes default
  
  /**
   * Creates default HTTP config (GET request).
   */
  public HttpConfig() {
    this("GET", null, new HashMap<>(), null);
  }
  
  /**
   * Creates HTTP config with specified parameters.
   *
   * @param method HTTP method (GET, POST, etc.)
   * @param body Request body for POST/PUT
   * @param headers HTTP headers
   * @param mimeType Override for Content-Type detection
   */
  public HttpConfig(
      String method,
      @Nullable String body,
      Map<String, String> headers,
      @Nullable String mimeType) {
    this.method = method != null ? method : "GET";
    this.body = body;
    this.headers = headers != null ? headers : new HashMap<>();
    this.mimeType = mimeType;
  }
  
  /**
   * Creates HTTP config from a map (for JSON/YAML configuration).
   *
   * @param config Configuration map
   * @return HttpConfig instance
   */
  @SuppressWarnings("unchecked")
  public static HttpConfig fromMap(Map<String, Object> config) {
    String method = (String) config.getOrDefault("method", "GET");
    String body = (String) config.get("body");
    String mimeType = (String) config.get("mimeType");
    
    Map<String, String> headers = new HashMap<>();
    Object headersObj = config.get("headers");
    if (headersObj instanceof Map) {
      Map<?, ?> headerMap = (Map<?, ?>) headersObj;
      for (Map.Entry<?, ?> entry : headerMap.entrySet()) {
        headers.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
      }
    }
    
    HttpConfig httpConfig = new HttpConfig(method, body, headers, mimeType);
    
    // Parse auth configuration
    Object authConfig = config.get("authConfig");
    if (authConfig instanceof Map) {
      Map<String, Object> auth = (Map<String, Object>) authConfig;
      
      // Phase 1: Simple auth
      httpConfig.bearerToken = (String) auth.get("bearerToken");
      httpConfig.apiKey = (String) auth.get("apiKey");
      httpConfig.username = (String) auth.get("username");
      httpConfig.password = (String) auth.get("password");
      
      // Phase 2: External tokens
      httpConfig.tokenCommand = (String) auth.get("tokenCommand");
      httpConfig.tokenEnv = (String) auth.get("tokenEnv");
      httpConfig.tokenFile = (String) auth.get("tokenFile");
      httpConfig.tokenEndpoint = (String) auth.get("tokenEndpoint");
      
      // Phase 2.5: Auth headers
      Object authHeadersObj = auth.get("authHeaders");
      if (authHeadersObj instanceof Map) {
        httpConfig.authHeaders = new HashMap<>();
        Map<?, ?> authHeaderMap = (Map<?, ?>) authHeadersObj;
        for (Map.Entry<?, ?> entry : authHeaderMap.entrySet()) {
          httpConfig.authHeaders.put(
              String.valueOf(entry.getKey()), 
              String.valueOf(entry.getValue()));
        }
      }
      
      // Phase 3: Proxy
      httpConfig.proxyEndpoint = (String) auth.get("proxyEndpoint");
      
      // Cache settings
      Object cacheEnabled = auth.get("cacheEnabled");
      if (cacheEnabled instanceof Boolean) {
        httpConfig.cacheEnabled = (Boolean) cacheEnabled;
      }
      Object cacheTtl = auth.get("cacheTtl");
      if (cacheTtl instanceof Number) {
        httpConfig.cacheTtl = ((Number) cacheTtl).longValue();
      }
    }
    
    return httpConfig;
  }
  
  public String getMethod() {
    return method;
  }
  
  public @Nullable String getBody() {
    return body;
  }
  
  public Map<String, String> getHeaders() {
    return headers;
  }
  
  public @Nullable String getMimeType() {
    return mimeType;
  }
  
  // Phase 1 getters
  public @Nullable String getBearerToken() {
    return bearerToken;
  }
  
  public @Nullable String getApiKey() {
    return apiKey;
  }
  
  public @Nullable String getUsername() {
    return username;
  }
  
  public @Nullable String getPassword() {
    return password;
  }
  
  // Phase 2 getters
  public @Nullable String getTokenCommand() {
    return tokenCommand;
  }
  
  public @Nullable String getTokenEnv() {
    return tokenEnv;
  }
  
  public @Nullable String getTokenFile() {
    return tokenFile;
  }
  
  public @Nullable String getTokenEndpoint() {
    return tokenEndpoint;
  }
  
  public @Nullable Map<String, String> getAuthHeaders() {
    return authHeaders;
  }
  
  // Phase 3 getters
  public @Nullable String getProxyEndpoint() {
    return proxyEndpoint;
  }
  
  // Cache getters
  public boolean isCacheEnabled() {
    return cacheEnabled;
  }
  
  public long getCacheTtl() {
    return cacheTtl;
  }
  
  /**
   * Creates an HttpStorageProvider with this configuration.
   *
   * @return Configured HttpStorageProvider
   */
  public HttpStorageProvider createStorageProvider() {
    return new HttpStorageProvider(method, body, headers, mimeType, this);
  }
  
  /**
   * Builder for creating HttpConfig instances.
   */
  public static class Builder {
    private String bearerToken;
    private String apiKey;
    private String username;
    private String password;
    private String tokenCommand;
    private String tokenEnv;
    private String tokenFile;
    private String tokenEndpoint;
    private Map<String, String> authHeaders;
    private String proxyEndpoint;
    private boolean cacheEnabled = true;
    private long cacheTtl = 300000;
    
    public Builder bearerToken(String token) {
      this.bearerToken = token;
      return this;
    }
    
    public Builder apiKey(String key) {
      this.apiKey = key;
      return this;
    }
    
    public Builder basicAuth(String username, String password) {
      this.username = username;
      this.password = password;
      return this;
    }
    
    public Builder tokenCommand(String command) {
      this.tokenCommand = command;
      return this;
    }
    
    public Builder tokenEnv(String env) {
      this.tokenEnv = env;
      return this;
    }
    
    public Builder tokenFile(String file) {
      this.tokenFile = file;
      return this;
    }
    
    public Builder tokenEndpoint(String endpoint) {
      this.tokenEndpoint = endpoint;
      return this;
    }
    
    public Builder authHeaders(Map<String, String> headers) {
      this.authHeaders = headers;
      return this;
    }
    
    public Builder proxyEndpoint(String endpoint) {
      this.proxyEndpoint = endpoint;
      return this;
    }
    
    public Builder cacheEnabled(boolean enabled) {
      this.cacheEnabled = enabled;
      return this;
    }
    
    public Builder cacheTtl(long ttl) {
      this.cacheTtl = ttl;
      return this;
    }
    
    public HttpConfig build() {
      HttpConfig config = new HttpConfig();
      config.bearerToken = this.bearerToken;
      config.apiKey = this.apiKey;
      config.username = this.username;
      config.password = this.password;
      config.tokenCommand = this.tokenCommand;
      config.tokenEnv = this.tokenEnv;
      config.tokenFile = this.tokenFile;
      config.tokenEndpoint = this.tokenEndpoint;
      config.authHeaders = this.authHeaders;
      config.proxyEndpoint = this.proxyEndpoint;
      config.cacheEnabled = this.cacheEnabled;
      config.cacheTtl = this.cacheTtl;
      return config;
    }
  }
}