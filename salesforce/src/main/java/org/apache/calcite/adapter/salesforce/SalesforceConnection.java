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
package org.apache.calcite.adapter.salesforce;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Connection to Salesforce REST API.
 */
public class SalesforceConnection implements Closeable {

  private static final String DESCRIBE_PATH = "/services/data/%s/sobjects/%s/describe";

  private final String loginUrl;
  private final AuthConfig authConfig;
  private final String apiVersion;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper mapper;

  private String accessToken;
  private String instanceUrl;

  public SalesforceConnection(String loginUrl, AuthConfig authConfig, String apiVersion)
      throws IOException {
    this.loginUrl = loginUrl;
    this.authConfig = authConfig;
    this.apiVersion = apiVersion;
    this.httpClient = HttpClients.createDefault();
    this.mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    authenticate();
  }

  private void authenticate() throws IOException {
    if (authConfig.type == AuthType.ACCESS_TOKEN) {
      this.accessToken = authConfig.accessToken;
      this.instanceUrl = authConfig.instanceUrl;
    } else {
      // Username/password OAuth flow
      HttpPost post = new HttpPost(loginUrl + "/services/oauth2/token");
      post.setHeader("Content-Type", "application/x-www-form-urlencoded");

      StringBuilder body = new StringBuilder();
      body.append("grant_type=password");
      body.append("&client_id=").append(encode(authConfig.clientId));
      body.append("&client_secret=").append(encode(authConfig.clientSecret));
      body.append("&username=").append(encode(authConfig.username));
      body.append("&password=").append(encode(authConfig.password));
      if (authConfig.securityToken != null) {
        body.append(encode(authConfig.securityToken));
      }

      post.setEntity(new StringEntity(body.toString()));

      try (CloseableHttpResponse response = httpClient.execute(post)) {
        String responseBody = EntityUtils.toString(response.getEntity());
        if (response.getStatusLine().getStatusCode() != 200) {
          throw new IOException("Authentication failed: " + responseBody);
        }

        JsonNode auth = mapper.readTree(responseBody);
        this.accessToken = auth.get("access_token").asText();
        this.instanceUrl = auth.get("instance_url").asText();
      }
    }
  }

  /**
   * Execute a SOQL query.
   */
  public QueryResult query(String soql) throws IOException {
    URIBuilder builder = new URIBuilder(URI.create(instanceUrl))
        .setPath("/services/data/" + apiVersion + "/query")
        .addParameter("q", soql);

    HttpGet get = new HttpGet(builder.toString());
    get.setHeader("Authorization", "Bearer " + accessToken);
    get.setHeader("Accept", "application/json");

    try (CloseableHttpResponse response = httpClient.execute(get)) {
      String responseBody = EntityUtils.toString(response.getEntity());
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new IOException("Query failed: " + responseBody);
      }

      return mapper.readValue(responseBody, QueryResult.class);
    } catch (Exception e) {
      throw new IOException("Query failed", e);
    }
  }

  /**
   * Continue a query using the nextRecordsUrl.
   */
  public QueryResult queryMore(String nextRecordsUrl) throws IOException {
    HttpGet get = new HttpGet(instanceUrl + nextRecordsUrl);
    get.setHeader("Authorization", "Bearer " + accessToken);
    get.setHeader("Accept", "application/json");

    try (CloseableHttpResponse response = httpClient.execute(get)) {
      String responseBody = EntityUtils.toString(response.getEntity());
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new IOException("QueryMore failed: " + responseBody);
      }

      return mapper.readValue(responseBody, QueryResult.class);
    } catch (Exception e) {
      throw new IOException("QueryMore failed", e);
    }
  }

  /**
   * Describe an sObject type.
   */
  public SObjectDescription describeSObject(String sObjectType) throws IOException {
    String path = String.format(Locale.ROOT, DESCRIBE_PATH, apiVersion, sObjectType);

    HttpGet get = new HttpGet(instanceUrl + path);
    get.setHeader("Authorization", "Bearer " + accessToken);
    get.setHeader("Accept", "application/json");

    try (CloseableHttpResponse response = httpClient.execute(get)) {
      String responseBody = EntityUtils.toString(response.getEntity());
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new IOException("Describe failed: " + responseBody);
      }

      return mapper.readValue(responseBody, SObjectDescription.class);
    } catch (Exception e) {
      throw new IOException("Describe failed", e);
    }
  }

  /**
   * Get list of all sObjects.
   */
  public List<SObjectBasicInfo> listSObjects() throws IOException {
    String path = String.format(Locale.ROOT, "/services/data/%s/sobjects", apiVersion);

    HttpGet get = new HttpGet(instanceUrl + path);
    get.setHeader("Authorization", "Bearer " + accessToken);
    get.setHeader("Accept", "application/json");

    try (CloseableHttpResponse response = httpClient.execute(get)) {
      String responseBody = EntityUtils.toString(response.getEntity());
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new IOException("List sObjects failed: " + responseBody);
      }

      JsonNode root = mapper.readTree(responseBody);
      JsonNode sobjects = root.get("sobjects");

      List<SObjectBasicInfo> result = new ArrayList<>();
      for (JsonNode node : sobjects) {
        SObjectBasicInfo info = mapper.treeToValue(node, SObjectBasicInfo.class);
        if (info.queryable) {
          result.add(info);
        }
      }
      return result;
    } catch (Exception e) {
      throw new IOException("List sObjects failed", e);
    }
  }

  private String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  @Override public void close() throws IOException {
    httpClient.close();
  }

  /**
   * Authentication configuration.
   */
  public static class AuthConfig {
    final AuthType type;
    final String username;
    final String password;
    final String securityToken;
    final String clientId;
    final String clientSecret;
    final String accessToken;
    final String instanceUrl;

    private AuthConfig(AuthType type, String username, String password,
        String securityToken, String clientId, String clientSecret,
        String accessToken, String instanceUrl) {
      this.type = type;
      this.username = username;
      this.password = password;
      this.securityToken = securityToken;
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.accessToken = accessToken;
      this.instanceUrl = instanceUrl;
    }

    public static AuthConfig usernamePassword(String username, String password,
        String securityToken, String clientId, String clientSecret) {
      return new AuthConfig(AuthType.USERNAME_PASSWORD, username, password,
          securityToken, clientId, clientSecret, null, null);
    }

    public static AuthConfig accessToken(String accessToken, String instanceUrl) {
      return new AuthConfig(AuthType.ACCESS_TOKEN, null, null, null, null, null,
          accessToken, instanceUrl);
    }
  }

  /**
   * Authentication type for Salesforce connection.
   */
  private enum AuthType {
    USERNAME_PASSWORD,
    ACCESS_TOKEN
  }

  /**
   * SOQL query result.
   */
  public static class QueryResult {
    public int totalSize;
    public boolean done;
    public String nextRecordsUrl;
    public List<Map<String, Object>> records;
  }

  /**
   * Basic sObject information.
   */
  public static class SObjectBasicInfo {
    public String name;
    public String label;
    public boolean queryable;
    public boolean custom;
  }

  /**
   * Full sObject description.
   */
  public static class SObjectDescription {
    public String name;
    public String label;
    public List<FieldDescription> fields;
    public boolean queryable;
    public boolean custom;
  }

  /**
   * Field description.
   */
  public static class FieldDescription {
    public String name;
    public String label;
    public String type;
    public int length;
    public boolean nillable;
    public boolean custom;
    public boolean sortable;
    public boolean filterable;
    public String relationshipName;
    public List<String> referenceTo;
  }
}
