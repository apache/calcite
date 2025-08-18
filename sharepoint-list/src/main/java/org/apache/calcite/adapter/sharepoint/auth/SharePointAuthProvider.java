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
package org.apache.calcite.adapter.sharepoint.auth;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for SharePoint authentication providers.
 * Allows pluggable authentication mechanisms including direct OAuth2,
 * auth proxy services, and custom enterprise implementations.
 * 
 * <p>Follows the same phased approach as the file adapter's HTTP auth:
 * <ul>
 *   <li>Phase 1: Simple static authentication (client credentials, certificates)</li>
 *   <li>Phase 2: External token management (commands, files, endpoints)</li>
 *   <li>Phase 3: Full proxy delegation for complex auth protocols</li>
 * </ul>
 */
public interface SharePointAuthProvider {
  
  /**
   * Gets a valid access token for SharePoint API access.
   * Implementations should handle token refresh internally.
   *
   * @return Valid access token
   * @throws IOException if authentication fails
   */
  String getAccessToken() throws IOException;
  
  /**
   * Gets additional headers to include in API requests.
   * For example, custom correlation IDs or proxy headers.
   *
   * @return Map of header names to values, or empty map
   */
  Map<String, String> getAdditionalHeaders();
  
  /**
   * Invalidates the current token, forcing refresh on next request.
   */
  void invalidateToken();
  
  /**
   * Gets the SharePoint site URL this provider is configured for.
   *
   * @return SharePoint site URL
   */
  String getSiteUrl();
  
  /**
   * Determines if this provider supports the given API type.
   *
   * @param apiType Either "graph" or "rest"
   * @return true if the provider supports the API type
   */
  boolean supportsApiType(String apiType);
  
  /**
   * Gets the tenant ID if available.
   *
   * @return Tenant ID or null if not applicable
   */
  String getTenantId();
}