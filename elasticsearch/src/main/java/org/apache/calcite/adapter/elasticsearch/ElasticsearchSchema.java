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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Each table in the schema is an ELASTICSEARCH index.
 */
public class ElasticsearchSchema extends AbstractSchema {

  private final RestClient client;

  private final ObjectMapper mapper;

  private final Map<String, Table> tableMap;

  /**
   * Default batch size to be used during scrolling.
   */
  private final int fetchSize;

  /**
   * Allows schema to be instantiated from existing elastic search client.
   *
   * @param client existing client instance
   * @param mapper mapper for JSON (de)serialization
   * @param index name of ES index
   */
  public ElasticsearchSchema(RestClient client, ObjectMapper mapper, String index) {
    this(client, mapper, index, ElasticsearchTransport.DEFAULT_FETCH_SIZE);
  }

  @VisibleForTesting
  ElasticsearchSchema(RestClient client, ObjectMapper mapper,
                      String index, int fetchSize) {
    super();
    this.client = Objects.requireNonNull(client, "client");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    Preconditions.checkArgument(fetchSize > 0,
        "invalid fetch size. Expected %s > 0", fetchSize);
    this.fetchSize = fetchSize;

    if (index == null) {
      try {
        this.tableMap = createTables(indicesFromElastic());
      } catch (IOException e) {
        throw new UncheckedIOException("Couldn't get indices", e);
      }
    } else {
      this.tableMap = createTables(Collections.singleton(index));
    }
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  private Map<String, Table> createTables(Iterable<String> indices) {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (String index : indices) {
      final ElasticsearchTransport transport = new ElasticsearchTransport(client, mapper,
          index, fetchSize);
      builder.put(index, new ElasticsearchTable(transport));
    }
    return builder.build();
  }

  /**
   * Queries {@code _alias} definition to automatically detect all indices
   *
   * @return list of indices
   * @throws IOException for any IO related issues
   * @throws IllegalStateException if reply is not understood
   */
  private Set<String> indicesFromElastic() throws IOException {
    final String endpoint = "/_alias";
    final Response response = client.performRequest(new Request("GET", endpoint));
    try (InputStream is = response.getEntity().getContent()) {
      final JsonNode root = mapper.readTree(is);
      if (!(root.isObject() && root.size() > 0)) {
        final String message = String.format(Locale.ROOT, "Invalid response for %s/%s "
            + "Expected object of at least size 1 got %s (of size %d)", response.getHost(),
            response.getRequestLine(), root.getNodeType(), root.size());
        throw new IllegalStateException(message);
      }

      Set<String> indices = Sets.newHashSet(root.fieldNames());
      return indices;
    }
  }

}

// End ElasticsearchSchema.java
