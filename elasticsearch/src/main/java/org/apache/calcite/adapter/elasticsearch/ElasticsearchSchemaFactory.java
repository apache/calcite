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

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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
              REST_CLIENT_CACHE_SIZE
          );

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

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {

    final Map map = (Map) operand;

    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

    try {

      List<HttpHost> hosts;

      if (map.containsKey("hosts")) {
        final List<String> configHosts = mapper.readValue((String) map.get("hosts"),
                new TypeReference<List<String>>() { });

        hosts = configHosts
                .stream()
                .map(host -> HttpHost.create(host))
                .collect(Collectors.toList());
      } else if (map.containsKey("coordinates")) {
        final Map<String, Integer> coordinates = mapper.readValue((String) map.get("coordinates"),
                new TypeReference<Map<String, Integer>>() { });

        hosts =  coordinates
                .entrySet()
                .stream()
                .map(entry -> new HttpHost(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        LOGGER.warn("Prefer using hosts, coordinates is deprecated.");
      } else {
        throw new IllegalArgumentException
        ("Both 'coordinates' and 'hosts' is missing in configuration. Provide one of them.");
      }
      final String pathPrefix = (String) map.get("pathPrefix");
      // create client
      String username = (String) map.get("username");
      String password = (String) map.get("password");
      final RestClient client = connect(hosts, pathPrefix, username, password);
      final String index = (String) map.get("index");

      return new ElasticsearchSchema(client, new ObjectMapper(), index);
    } catch (IOException e) {
      throw new RuntimeException("Cannot parse values from json", e);
    }
  }

  /**
   * Builds Elastic rest client from user configuration.
   *
   * @param hosts list of ES HTTP Hosts to connect to
   * @param username the username of ES
   * @param password the password of ES
   * @return new or cached low-level rest http client for ES
   */
  private static RestClient connect(List<HttpHost> hosts, String pathPrefix,
                                    String username, String password) {

    Objects.requireNonNull(hosts, "hosts or coordinates");
    Preconditions.checkArgument(!hosts.isEmpty(), "no ES hosts specified");
    // Two lists are considered equal when all of their corresponding elements are equal
    // making a list of RestClient parms a suitable cache key.
    List cacheKey = ImmutableList.of(hosts, pathPrefix, username, password);

    try {
      return REST_CLIENTS.get(cacheKey, new Callable<RestClient>() {
        @Override public RestClient call() {
          RestClientBuilder builder = RestClient.builder(hosts.toArray(new HttpHost[hosts.size()]));

          if (!Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password)) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(httpClientBuilder ->
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
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
