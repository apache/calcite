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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.test.CalciteAssert;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Testing Elasticsearch join query.
 */
@ResourceLock(value = "elasticsearch-scrolls", mode = ResourceAccessMode.READ)
public class JoinTest {

  public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

  private static final String NAME_LEFT = "lt";

  private static final String NAME_RIGHT = "rt";


  @BeforeAll
  public static void setupInstance() throws Exception {
    final Map<String, String> ltMappings = ImmutableMap.<String, String>builder()
        .put("doc_id", "keyword")
        .put("val1", "long")
        .build();

    final Map<String, String> rtMappings = ImmutableMap.<String, String>builder()
        .put("doc_id", "keyword")
        .put("val2", "long")
        .build();

    NODE.createIndex(NAME_LEFT, ltMappings);
    NODE.createIndex(NAME_RIGHT, rtMappings);
    final ObjectMapper mapper = new ObjectMapper()
        .enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES) // user-friendly settings to
        .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES); // avoid too much quoting

    String ldoc1 = "{doc_id:'1', val1:1}";
    String ldoc2 = "{doc_id:'2', val1:2}";
    final List<ObjectNode> docs = new ArrayList<>();
    for (String text : Arrays.asList(ldoc1, ldoc2)) {
      docs.add((ObjectNode) mapper.readTree(text));
    }
    NODE.insertBulk(NAME_LEFT, docs);


    String rdoc1 = "{doc_id:'1', val2:1}";
    String rdoc2 = "{doc_id:'2', val2:2}";
    final List<ObjectNode> rdocs = new ArrayList<>();
    for (String text : Arrays.asList(rdoc1, rdoc2)) {
      rdocs.add((ObjectNode) mapper.readTree(text));
    }
    NODE.insertBulk(NAME_RIGHT, rdocs);
  }

  private static Connection createConnection() throws SQLException {
    final Connection connection = DriverManager.getConnection("jdbc:calcite:");
    final SchemaPlus root =
        connection.unwrap(CalciteConnection.class).getRootSchema();

    root.add("elastic0", new ElasticsearchSchema(NODE.restClient(), NODE.mapper(), NAME_LEFT));
    root.add("elastic1", new ElasticsearchSchema(NODE.restClient(), NODE.mapper(), NAME_RIGHT));

    return connection;
  }

  /**
   * Test two elasticserch index join.
   */
  @Test void join() {
    CalciteAssert.that()
        .with(JoinTest::createConnection)
        .query(
            String.format(Locale.ROOT,
                "select t._MAP['doc_id'] AS \"doc_id\", t._MAP['val1'] AS \"val1\" "
            + " from \"elastic0\".\"%s\" t "
            + " join \"elastic1\".\"%s\" s"
            + " on cast(t._MAP['doc_id'] as varchar) = cast(s._MAP['doc_id'] as varchar)",
                NAME_LEFT, NAME_RIGHT))
        .returnsUnordered("doc_id=1; val1=1",
            "doc_id=2; val1=2");

  }

}
