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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for HttpConfig authentication configuration.
 * Tests configuration parsing and builder patterns without network access.
 */
@Tag("integration")
public class HttpConfigAuthTest {
  
  @Test
  void testBuilderPhase1() {
    // Test bearer token
    HttpConfig config = new HttpConfig.Builder()
        .bearerToken("test-token")
        .build();
    assertEquals("test-token", config.getBearerToken());
    assertNull(config.getApiKey());
    
    // Test API key
    config = new HttpConfig.Builder()
        .apiKey("test-key")
        .build();
    assertEquals("test-key", config.getApiKey());
    assertNull(config.getBearerToken());
    
    // Test basic auth
    config = new HttpConfig.Builder()
        .basicAuth("user", "pass")
        .build();
    assertEquals("user", config.getUsername());
    assertEquals("pass", config.getPassword());
  }
  
  @Test
  void testBuilderPhase2() {
    // Test token sources
    HttpConfig config = new HttpConfig.Builder()
        .tokenCommand("echo token")
        .tokenEnv("TOKEN_ENV")
        .tokenFile("/path/to/token")
        .tokenEndpoint("http://token.service")
        .build();
    
    assertEquals("echo token", config.getTokenCommand());
    assertEquals("TOKEN_ENV", config.getTokenEnv());
    assertEquals("/path/to/token", config.getTokenFile());
    assertEquals("http://token.service", config.getTokenEndpoint());
  }
  
  @Test
  void testBuilderPhase3() {
    // Test proxy endpoint
    HttpConfig config = new HttpConfig.Builder()
        .proxyEndpoint("http://proxy.service/forward")
        .build();
    
    assertEquals("http://proxy.service/forward", config.getProxyEndpoint());
  }
  
  @Test
  void testBuilderAuthHeaders() {
    Map<String, String> authHeaders = new HashMap<>();
    authHeaders.put("X-Custom-Auth", "Bearer ${token}");
    authHeaders.put("X-API-Version", "v2");
    
    HttpConfig config = new HttpConfig.Builder()
        .authHeaders(authHeaders)
        .build();
    
    assertNotNull(config.getAuthHeaders());
    assertEquals("Bearer ${token}", config.getAuthHeaders().get("X-Custom-Auth"));
    assertEquals("v2", config.getAuthHeaders().get("X-API-Version"));
  }
  
  @Test
  void testBuilderCacheSettings() {
    HttpConfig config = new HttpConfig.Builder()
        .cacheEnabled(false)
        .cacheTtl(60000)
        .build();
    
    assertFalse(config.isCacheEnabled());
    assertEquals(60000, config.getCacheTtl());
    
    // Test defaults
    config = new HttpConfig.Builder().build();
    assertTrue(config.isCacheEnabled());
    assertEquals(300000, config.getCacheTtl());
  }
  
  @Test
  void testFromMapPhase1() {
    Map<String, Object> configMap = new HashMap<>();
    Map<String, Object> authConfig = new HashMap<>();
    
    // Test bearer token
    authConfig.put("bearerToken", "map-token");
    configMap.put("authConfig", authConfig);
    
    HttpConfig config = HttpConfig.fromMap(configMap);
    assertEquals("map-token", config.getBearerToken());
    
    // Test API key
    authConfig.clear();
    authConfig.put("apiKey", "map-key");
    configMap.put("authConfig", authConfig);
    
    config = HttpConfig.fromMap(configMap);
    assertEquals("map-key", config.getApiKey());
    
    // Test basic auth
    authConfig.clear();
    authConfig.put("username", "map-user");
    authConfig.put("password", "map-pass");
    configMap.put("authConfig", authConfig);
    
    config = HttpConfig.fromMap(configMap);
    assertEquals("map-user", config.getUsername());
    assertEquals("map-pass", config.getPassword());
  }
  
  @Test
  void testFromMapPhase2() {
    Map<String, Object> configMap = new HashMap<>();
    Map<String, Object> authConfig = new HashMap<>();
    
    authConfig.put("tokenCommand", "vault get token");
    authConfig.put("tokenEnv", "VAULT_TOKEN");
    authConfig.put("tokenFile", "/run/secrets/token");
    authConfig.put("tokenEndpoint", "http://vault:8200/token");
    
    configMap.put("authConfig", authConfig);
    
    HttpConfig config = HttpConfig.fromMap(configMap);
    assertEquals("vault get token", config.getTokenCommand());
    assertEquals("VAULT_TOKEN", config.getTokenEnv());
    assertEquals("/run/secrets/token", config.getTokenFile());
    assertEquals("http://vault:8200/token", config.getTokenEndpoint());
  }
  
  @Test
  void testFromMapPhase3() {
    Map<String, Object> configMap = new HashMap<>();
    Map<String, Object> authConfig = new HashMap<>();
    
    authConfig.put("proxyEndpoint", "http://auth-proxy:8080/forward");
    configMap.put("authConfig", authConfig);
    
    HttpConfig config = HttpConfig.fromMap(configMap);
    assertEquals("http://auth-proxy:8080/forward", config.getProxyEndpoint());
  }
  
  @Test
  void testFromMapAuthHeaders() {
    Map<String, Object> configMap = new HashMap<>();
    Map<String, Object> authConfig = new HashMap<>();
    Map<String, String> authHeaders = new HashMap<>();
    
    authHeaders.put("Authorization", "Custom ${token}");
    authHeaders.put("X-Request-ID", "static-id");
    authConfig.put("authHeaders", authHeaders);
    configMap.put("authConfig", authConfig);
    
    HttpConfig config = HttpConfig.fromMap(configMap);
    assertNotNull(config.getAuthHeaders());
    assertEquals("Custom ${token}", config.getAuthHeaders().get("Authorization"));
    assertEquals("static-id", config.getAuthHeaders().get("X-Request-ID"));
  }
  
  @Test
  void testFromMapCacheSettings() {
    Map<String, Object> configMap = new HashMap<>();
    Map<String, Object> authConfig = new HashMap<>();
    
    authConfig.put("cacheEnabled", false);
    authConfig.put("cacheTtl", 120000L);
    configMap.put("authConfig", authConfig);
    
    HttpConfig config = HttpConfig.fromMap(configMap);
    assertFalse(config.isCacheEnabled());
    assertEquals(120000L, config.getCacheTtl());
  }
  
  @Test
  void testFromMapWithoutAuthConfig() {
    // Should not throw when authConfig is missing
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("method", "POST");
    configMap.put("body", "test-body");
    
    HttpConfig config = HttpConfig.fromMap(configMap);
    assertEquals("POST", config.getMethod());
    assertEquals("test-body", config.getBody());
    assertNull(config.getBearerToken());
    assertNull(config.getApiKey());
  }
  
  @Test
  void testMethodAndBodyPreserved() {
    // Ensure existing functionality is preserved
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("method", "PUT");
    configMap.put("body", "request-body");
    configMap.put("mimeType", "application/xml");
    
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/xml");
    configMap.put("headers", headers);
    
    HttpConfig config = HttpConfig.fromMap(configMap);
    assertEquals("PUT", config.getMethod());
    assertEquals("request-body", config.getBody());
    assertEquals("application/xml", config.getMimeType());
    assertNotNull(config.getHeaders());
    assertEquals("application/xml", config.getHeaders().get("Content-Type"));
  }
  
  @Test
  void testCreateStorageProvider() {
    HttpConfig config = new HttpConfig.Builder()
        .bearerToken("provider-token")
        .build();
    
    HttpStorageProvider provider = config.createStorageProvider();
    assertNotNull(provider);
    // Provider should have the config
  }
}