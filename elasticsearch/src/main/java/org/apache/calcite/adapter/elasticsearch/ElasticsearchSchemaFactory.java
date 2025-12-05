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
package org.apache.calcite.adapter.elasticsearch;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.UnsafeX509ExtendedTrustManager;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Factory that creates an {@link ElasticsearchSchema}.
 *
 * <p>Allows a custom schema to be included in a model.json file.
 */
@SuppressWarnings("UnusedDeclaration")
public class ElasticsearchSchemaFactory implements SchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchSchemaFactory.class);

  private static final int REST_CLIENT_CACHE_SIZE = 100;

  // RestClient objects allocate system resources and are thread safe. Here, we cache
  // them using a key derived from the parameters that define a RestClient. The primary
  // reason to do this is to limit the resource leak that results from Calcite's
  // current inability to close clients that it creates.  Amongst the OS resources
  // leaked are file descriptors which are limited to 1024 per process by default on
  // Linux at the time of writing.
  private static final Cache<List, RestClient> REST_CLIENTS = CacheBuilder.newBuilder()
      .maximumSize(REST_CLIENT_CACHE_SIZE)
      .removalListener(new RemovalListener<List, RestClient>() {
        @Override public void onRemoval(RemovalNotification<List, RestClient> notice) {
          LOGGER.warn(
              "Will close an ES REST client to keep the number of open clients under {}. "
              + "Any schema objects that might still have been relying on this client are now "
              + "broken! Do not try to access more than {} distinct ES REST APIs through this "
              + "adapter.",
              REST_CLIENT_CACHE_SIZE,
              REST_CLIENT_CACHE_SIZE);

          try {
            // Free resources allocated by this RestClient
            notice.getValue().close();
          } catch (IOException ex) {
            LOGGER.warn("Could not close RestClient {}", notice.getValue(), ex);
          }
        }
      })
      .build();

  public ElasticsearchSchemaFactory() {
  }

  /**
   * Create an ElasticSearch {@link Schema}.
   * The operand property accepts the following key/value pairs:
   *
   * <ul>
   *   <li><b>username</b>: The username for the ES cluster</li>
   *   <li><b>password</b>: The password for the ES cluster</li>
   *   <li><b>hosts</b>: A {@link List} of hosts for the ES cluster. Either the hosts or
   *   coordinates must be populated.</li>
   *   <li><b>coordinates</b>: A {@link List} of coordinates for the ES cluster. Either the hosts
   *   list or
   *   the coordinates list must be populated.</li>
   *   <li><b>disableSSLVerification</b>: A boolean parameter to disable SSL verification. Defaults
   *   to false. This should always be set to false for production systems.</li>
   * </ul>
   *
   * @param parentSchema Parent schema
   * @param name Name of this schema
   * @param operand The "operand" JSON property
   * @return Returns a {@link Schema} for the ES cluster.
   */
  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {

    final Map map = (Map) operand;

    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

    try {

      List<HttpHost> hosts;

      if (map.containsKey("hosts")) {
        final List<String> configHosts =
            mapper.readValue((String) map.get("hosts"),
                new TypeReference<List<String>>() { });

        hosts =
            configHosts.stream()
                .map(host -> HttpHost.create(host))
                .collect(Collectors.toList());
      } else if (map.containsKey("coordinates")) {
        final Map<String, Integer> coordinates =
                mapper.readValue((String) map.get("coordinates"),
                    new TypeReference<Map<String, Integer>>() { });

        hosts =
            coordinates.entrySet()
                .stream()
                .map(entry -> new HttpHost(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        LOGGER.warn("Prefer using hosts, coordinates is deprecated.");
      } else {
        throw new IllegalArgumentException
        ("Both 'coordinates' and 'hosts' is missing in configuration. Provide one of them.");
      }
      List<HttpHost> sortedHost = getSortedHost(hosts);

      final String pathPrefix = (String) map.get("pathPrefix");

      // Enable or Disable SSL Verification
      boolean disableSSLVerification;
      if (map.containsKey("disableSSLVerification")) {
        String temp = (String) map.get("disableSSLVerification");
        disableSSLVerification = Boolean.getBoolean(temp.toLowerCase(Locale.ROOT));
      } else {
        disableSSLVerification = false;
      }

      // create client
      String username = (String) map.get("username");
      String password = (String) map.get("password");
      final RestClient client =
          connect(sortedHost, pathPrefix, username, password, disableSSLVerification);
      final String index = (String) map.get("index");

      return new ElasticsearchSchema(client, new ObjectMapper(), index);
    } catch (IOException e) {
      throw new RuntimeException("Cannot parse values from json", e);
    }
  }

  protected static List<HttpHost> getSortedHost(List<HttpHost> hosts) {
    List<HttpHost> sortedHosts =
        hosts
            .stream()
            .sorted(Comparator.comparing(HttpHost::toString, String::compareTo))
            .collect(Collectors.toList());
    return sortedHosts;
  }

  /**
   * Builds Elastic rest client from user configuration.
   *
   * @param hosts list of ES HTTP Hosts to connect to
   * @param username the username of ES
   * @param password the password of ES
   * @return new or cached low-level rest http client for ES
   */
  @SuppressWarnings({"java:S4830", "java:S5527"})
  private static RestClient connect(List<HttpHost> hosts, String pathPrefix,
                                    String username, String password,
                                    boolean disableSSLVerification) {

    requireNonNull(hosts, "hosts or coordinates");
    checkArgument(!hosts.isEmpty(), "no ES hosts specified");
    // Two lists are considered equal when all of their corresponding elements are equal
    // making a list of RestClient params a suitable cache key.
    ArrayList<Object> config = new ArrayList<>();
    config.add(hosts);
    if (pathPrefix != null) {
      config.add(pathPrefix);
    }
    if (username != null) {
      config.add(username);
    }
    if (password != null) {
      config.add(password);
    }
    List cacheKey = ImmutableList.copyOf(config);

    try {
      return REST_CLIENTS.get(cacheKey, new Callable<RestClient>() {
        @Override public RestClient call() throws NoSuchAlgorithmException, KeyManagementException {
          RestClientBuilder builder = RestClient.builder(hosts.toArray(new HttpHost[hosts.size()]));

          if (!Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password)) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(httpClientBuilder ->
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
          }

          if (disableSSLVerification) {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] {UnsafeX509ExtendedTrustManager.getInstance()},
                null);

            builder.setHttpClientConfigCallback(httpClientBuilder ->
                httpClientBuilder.setSSLContext(sslContext)
                    .setSSLHostnameVerifier((host, session) -> true));
          }

          if (pathPrefix != null && !pathPrefix.isEmpty()) {
            builder.setPathPrefix(pathPrefix);
          }
          return builder.build();
        }
      });
    } catch (ExecutionException ex) {
      throw new RuntimeException("Cannot return a cached RestClient", ex);
    }
  }
}
