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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory and manager for Iceberg REST catalogs.
 */
public class IcebergRestCatalog {

  /**
   * Creates a REST catalog from configuration.
   *
   * @param config Configuration containing REST catalog parameters
   * @return Configured REST catalog
   */
  public static Catalog createRestCatalog(Map<String, Object> config) {
    String uri = (String) config.get("uri");
    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("REST catalog requires 'uri' parameter");
    }

    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put("uri", uri);
    
    // Optional authentication
    String credential = (String) config.get("credential");
    if (credential != null) {
      catalogProperties.put("credential", credential);
    }
    
    String token = (String) config.get("token");
    if (token != null) {
      catalogProperties.put("token", token);
    }
    
    // Optional warehouse configuration
    String warehouse = (String) config.get("warehouse");
    if (warehouse != null) {
      catalogProperties.put("warehouse", warehouse);
    }
    
    // Optional OAuth2 configuration
    String oauth2ServerUri = (String) config.get("oauth2-server-uri");
    if (oauth2ServerUri != null) {
      catalogProperties.put("oauth2-server-uri", oauth2ServerUri);
    }
    
    String clientId = (String) config.get("client-id");
    if (clientId != null) {
      catalogProperties.put("client-id", clientId);
    }
    
    String clientSecret = (String) config.get("client-secret");
    if (clientSecret != null) {
      catalogProperties.put("client-secret", clientSecret);
    }

    // Create and initialize the REST catalog
    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize("rest-catalog", catalogProperties);
    
    return catalog;
  }

  /**
   * Validates REST catalog configuration.
   *
   * @param config Configuration to validate
   * @throws IllegalArgumentException if configuration is invalid
   */
  public static void validateRestCatalogConfig(Map<String, Object> config) {
    if (config == null) {
      throw new IllegalArgumentException("REST catalog configuration cannot be null");
    }
    
    String uri = (String) config.get("uri");
    if (uri == null || uri.trim().isEmpty()) {
      throw new IllegalArgumentException("REST catalog requires 'uri' parameter");
    }
    
    // Validate URI format
    try {
      java.net.URI.create(uri);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid REST catalog URI: " + uri, e);
    }
  }
}