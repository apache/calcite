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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for dual API support (Graph and REST) in SharePoint List adapter.
 */
public class DualApiSupportTest {
  
  @Test
  public void testDirectAuthProviderCreation() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://contoso.sharepoint.com");
    config.put("tenantId", "test-tenant");
    config.put("clientId", "test-client");
    config.put("clientSecret", "test-secret");
    
    SharePointAuthProvider provider = new DirectAuthProvider(config);
    
    assertNotNull(provider);
    assertEquals("https://contoso.sharepoint.com", provider.getSiteUrl());
    assertEquals("test-tenant", provider.getTenantId());
    assertTrue(provider.supportsApiType("graph"));
    assertTrue(provider.supportsApiType("rest"));
  }
  
  @Test
  public void testProxyAuthProviderCreation() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://contoso.sharepoint.com");
    
    Map<String, Object> proxyConfig = new HashMap<>();
    proxyConfig.put("endpoint", "https://auth-proxy.company.com");
    proxyConfig.put("apiKey", "test-api-key");
    config.put("authProxy", proxyConfig);
    
    SharePointAuthProvider provider = new ProxyAuthProvider(config);
    
    assertNotNull(provider);
    assertEquals("https://contoso.sharepoint.com", provider.getSiteUrl());
    assertTrue(provider.supportsApiType("graph"));
    assertTrue(provider.supportsApiType("rest"));
  }
  
  @Test
  public void testApiProviderFactoryGraphSelection() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://contoso.sharepoint.com");
    config.put("tenantId", "test-tenant");
    config.put("clientId", "test-client");
    config.put("clientSecret", "test-secret");
    config.put("useGraphApi", true);
    
    SharePointListApiProvider provider = SharePointListApiProviderFactory.create(config);
    
    assertNotNull(provider);
    assertEquals("graph", provider.getApiType());
  }
  
  @Test
  public void testApiProviderFactoryRestSelection() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://contoso.sharepoint.com");
    config.put("tenantId", "test-tenant");
    config.put("clientId", "test-client");
    config.put("clientSecret", "test-secret");
    config.put("useRestApi", true);
    
    SharePointListApiProvider provider = SharePointListApiProviderFactory.create(config);
    
    assertNotNull(provider);
    assertEquals("rest", provider.getApiType());
  }
  
  @Test
  public void testApiProviderFactoryOnPremisesDetection() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://sharepoint.company.local");
    config.put("tenantId", "test-tenant");
    config.put("clientId", "test-client");
    config.put("clientSecret", "test-secret");
    
    SharePointListApiProvider provider = SharePointListApiProviderFactory.create(config);
    
    assertNotNull(provider);
    // On-premises should use REST API
    assertEquals("rest", provider.getApiType());
  }
  
  @Test
  public void testApiProviderFactorySharePointOnlineDetection() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://contoso.sharepoint.com");
    config.put("tenantId", "test-tenant");
    config.put("clientId", "test-client");
    config.put("clientSecret", "test-secret");
    
    SharePointListApiProvider provider = SharePointListApiProviderFactory.create(config);
    
    assertNotNull(provider);
    // SharePoint Online should prefer Graph API
    assertEquals("graph", provider.getApiType());
  }
  
  @Test
  public void testLegacyAuthForcesRestApi() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://contoso.sharepoint.com");
    config.put("tenantId", "test-tenant");
    config.put("clientId", "test-client");
    config.put("clientSecret", "test-secret");
    config.put("useLegacyAuth", true);
    
    SharePointListApiProvider provider = SharePointListApiProviderFactory.create(config);
    
    assertNotNull(provider);
    // Legacy auth should force REST API
    assertEquals("rest", provider.getApiType());
  }
  
  @Test
  public void testPhase2TokenCommandAuth() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://contoso.sharepoint.com");
    config.put("tokenCommand", "echo test-token");
    
    SharePointAuthProvider provider = new DirectAuthProvider(config);
    
    assertNotNull(provider);
    assertEquals("https://contoso.sharepoint.com", provider.getSiteUrl());
  }
  
  @Test
  public void testPhase2TokenFileAuth() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://contoso.sharepoint.com");
    config.put("tokenFile", "/tmp/token.txt");
    
    SharePointAuthProvider provider = new DirectAuthProvider(config);
    
    assertNotNull(provider);
    assertEquals("https://contoso.sharepoint.com", provider.getSiteUrl());
  }
  
  @Test
  public void testPhase2TokenEndpointAuth() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://contoso.sharepoint.com");
    config.put("tokenEndpoint", "https://auth.company.com/token");
    
    SharePointAuthProvider provider = new DirectAuthProvider(config);
    
    assertNotNull(provider);
    assertEquals("https://contoso.sharepoint.com", provider.getSiteUrl());
  }
}