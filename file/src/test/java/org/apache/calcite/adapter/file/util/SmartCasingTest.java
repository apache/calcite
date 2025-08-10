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
package org.apache.calcite.adapter.file.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link SmartCasing}.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public class SmartCasingTest {

  @BeforeEach
  void setUp() {
    // Clear all user mappings before each test to ensure test isolation
    synchronized (SmartCasing.class) {
      SmartCasing.clearAllUserMappings();
    }
  }
  
  @AfterEach
  void tearDown() {
    // Clear all user mappings after each test as well for extra safety
    synchronized (SmartCasing.class) {
      SmartCasing.clearAllUserMappings();
    }
  }

  @Test void testBasicSnakeCaseConversion() {
    assertEquals("user_id", SmartCasing.toSnakeCase("userId"));
    assertEquals("user_name", SmartCasing.toSnakeCase("userName"));
    assertEquals("first_name", SmartCasing.toSnakeCase("first_name"));
    assertEquals("last_name", SmartCasing.toSnakeCase("last_name"));
  }

  @Test void testAcronymHandling() {
    // XML should be treated as a single unit
    assertEquals("xml_http_request", SmartCasing.toSnakeCase("XMLHttpRequest"));
    assertEquals("json_data", SmartCasing.toSnakeCase("JSONData"));
    assertEquals("http_response", SmartCasing.toSnakeCase("HTTPResponse"));
    assertEquals("sql_query", SmartCasing.toSnakeCase("SQLQuery"));
  }

  @Test void testProductNameHandling() {
    // Product names should be treated as single units
    assertEquals("postgresql_connection", SmartCasing.toSnakeCase("PostgreSQLConnection"));
    assertEquals("mongodb_client", SmartCasing.toSnakeCase("MongoDBClient"));
    assertEquals("oauth2_token", SmartCasing.toSnakeCase("OAuth2Token"));
  }

  @Test void testMixedCaseHandling() {
    assertEquals("api_key_id", SmartCasing.toSnakeCase("APIKeyId"));
    assertEquals("jwt_token_validator", SmartCasing.toSnakeCase("JWTTokenValidator"));
    assertEquals("rest_api_endpoint", SmartCasing.toSnakeCase("RESTAPIEndpoint"));
  }

  @Test void testNumberHandling() {
    assertEquals("oauth2_provider", SmartCasing.toSnakeCase("OAuth2Provider"));
    assertEquals("http2_client", SmartCasing.toSnakeCase("HTTP2Client"));
    assertEquals("tls1_2_config", SmartCasing.toSnakeCase("TLS12Config"));
  }

  @Test void testAlreadySnakeCase() {
    assertEquals("user_id", SmartCasing.toSnakeCase("user_id"));
    assertEquals("table_name", SmartCasing.toSnakeCase("table_name"));
  }

  @Test void testAllUpperCase() {
    assertEquals("user_id", SmartCasing.toSnakeCase("USER_ID"));
    assertEquals("table_name", SmartCasing.toSnakeCase("TABLE_NAME"));
  }

  @Test void testEmptyAndNull() {
    assertEquals(null, SmartCasing.toSnakeCase(null));
    assertEquals("", SmartCasing.toSnakeCase(""));
    assertEquals("a", SmartCasing.toSnakeCase("a"));
  }

  @Test void testAdditionalTerms() {
    // Add custom term mappings
    SmartCasing.addTermMappings(java.util.Map.of(
        "MyCustomAPI", "mycustomapi",
        "SpecialTerm", "specialterm"
    ));
    
    // Test that custom terms are handled as single units
    assertEquals("mycustomapi_client", SmartCasing.toSnakeCase("MyCustomAPIClient"));
    assertEquals("specialterm_handler", SmartCasing.toSnakeCase("SpecialTermHandler"));
  }

  @Test void testUnchangedTerms() {
    // Add terms that should be preserved exactly
    SmartCasing.overrideTermMappings(java.util.Map.of(
        "id", "id",
        "UUID", "UUID"
    ));
    
    // Test that unchanged terms are preserved
    assertEquals("userID", SmartCasing.toSnakeCase("userID"));
    assertEquals("UUID", SmartCasing.toSnakeCase("UUID"));
    assertEquals("table_UUID_mapping", SmartCasing.toSnakeCase("tableUUIDMapping"));
  }

  @Test void testApplyCasingMethod() {
    assertEquals("TEST", SmartCasing.applyCasing("TEST", "UPPER"));
    assertEquals("TEST", SmartCasing.applyCasing("TEST", "LOWER"));
    assertEquals("Test", SmartCasing.applyCasing("Test", "UNCHANGED"));
    assertEquals("user_id", SmartCasing.applyCasing("userId", "SMART_CASING"));
  }

  @Test void testComplexExamples() {
    // Real-world examples that should be handled intelligently
    assertEquals("aws_s3_bucket_policy", SmartCasing.toSnakeCase("AWSS3BucketPolicy"));
    assertEquals("microsoft_azure_active_directory", SmartCasing.toSnakeCase("MicrosoftAzureActiveDirectory"));
    assertEquals("google_cloud_platform_api", SmartCasing.toSnakeCase("GoogleCloudPlatformAPI"));
    assertEquals("json_web_token_validator", SmartCasing.toSnakeCase("JSONWebTokenValidator"));
  }

  @Test void testEdgeCases() {
    // Single character
    assertEquals("a", SmartCasing.toSnakeCase("a"));
    
    // Multiple consecutive uppercase
    assertEquals("html_css_js", SmartCasing.toSnakeCase("HTMLCSSJS"));
    
    // Mixed with underscores already present
    assertEquals("user_api_key", SmartCasing.toSnakeCase("user_APIKey"));
  }
}