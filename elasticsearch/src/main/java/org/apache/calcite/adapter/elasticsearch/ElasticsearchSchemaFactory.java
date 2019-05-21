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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Factory that creates an {@link ElasticsearchSchema}.
 *
 * <p>Allows a custom schema to be included in a model.json file.
 */
@SuppressWarnings("UnusedDeclaration")
public class ElasticsearchSchemaFactory implements SchemaFactory {

  public ElasticsearchSchemaFactory() {
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {

    final Map map = (Map) operand;

    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

    try {
      final String coordinatesString = (String) map.get("coordinates");
      Preconditions.checkState(coordinatesString != null,
          "'coordinates' is missing in configuration");

      final Map<String, Integer> coordinates = mapper.readValue(coordinatesString,
          new TypeReference<Map<String, Integer>>() { });

      // create client
      final RestClient client = connect(coordinates);

      final String index = (String) map.get("index");

      return new ElasticsearchSchema(client, new ObjectMapper(), index);
    } catch (IOException e) {
      throw new RuntimeException("Cannot parse values from json", e);
    }
  }

  /**
   * Builds elastic rest client from user configuration
   * @param coordinates list of {@code hostname/port} to connect to
   * @return newly initialized low-level rest http client for ES
   */
  private static RestClient connect(Map<String, Integer> coordinates) {
    Objects.requireNonNull(coordinates, "coordinates");
    Preconditions.checkArgument(!coordinates.isEmpty(), "no ES coordinates specified");
    final Set<HttpHost> set = new LinkedHashSet<>();
    for (Map.Entry<String, Integer> entry: coordinates.entrySet()) {
      set.add(new HttpHost(entry.getKey(), entry.getValue()));
    }

    return RestClient.builder(set.toArray(new HttpHost[0])).build();
  }

}

// End ElasticsearchSchemaFactory.java
