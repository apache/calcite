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
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Testing Search RexCall to ElasticSearch terms and range query.
 * There are three types of the Sarg included in SEARCH RexCall:
 * 1) Sarg is points (In ('a', 'b', 'c' ...)).
 *    In this case the search call can be translated to terms Query
 * 2) Sarg is complementedPoints (Not in ('a', 'b')).
 *    In this case the search call can be translated to MustNot terms Query
 * 3) Sarg is real Range( > 1 and <= 10).
 *    In this case the search call should be translated to rang Query
 */
@ResourceLock(value = "elasticsearch-scrolls", mode = ResourceAccessMode.READ)
public class SearchTest {
  public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

  private static final String NAME = "search";

  @BeforeAll
  public static void setupInstance() throws Exception {
    final Map<String, String> mappings = ImmutableMap.<String, String>builder()
        .put("cat1", "keyword")
        .put("cat2", "keyword")
        .put("cat3", "keyword")
        .put("cat4", "date")
        .put("cat5", "integer")
        .put("val1", "long")
        .put("val2", "long")
        .build();

    NODE.createIndex(NAME, mappings);

    String doc1 = "{cat1:'a', cat2:'g', val1:1, cat4:'2018-01-01', cat5:1}";
    String doc2 = "{cat2:'g', cat3:'y', val2:5, cat4:'2019-12-12'}";
    String doc3 = "{cat1:'b', cat2:'h', cat3:'z', cat5:2, val1:7, val2:42}";
    String doc4 = "{cat1:'c', cat2:'x', cat3:'z', cat4:'2020-12-12', cat5:20, val1:10, val2:50}";


    final ObjectMapper mapper = new ObjectMapper()
        .enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES) // user-friendly settings to
        .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES); // avoid too much quoting

    final List<ObjectNode> docs = new ArrayList<>();
    for (String text: Arrays.asList(doc1, doc2, doc3, doc4)) {
      docs.add((ObjectNode) mapper.readTree(text));
    }

    NODE.insertBulk(NAME, docs);
  }

  private CalciteAssert.ConnectionFactory newConnectionFactory() {
    return new CalciteAssert.ConnectionFactory() {
      @Override public Connection createConnection() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
        final SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();

        root.add("elastic", new ElasticsearchSchema(NODE.restClient(), NODE.mapper(), NAME));

        // add calcite view programmatically
        final String viewSql = String.format(Locale.ROOT,
            "select _MAP['cat1'] AS \"cat1\", "
                + " _MAP['cat2']  AS \"cat2\", "
                +  " _MAP['cat3'] AS \"cat3\", "
                +  " _MAP['cat4'] AS \"cat4\", "
                +  " _MAP['cat5'] AS \"cat5\", "
                +  " _MAP['val1'] AS \"val1\", "
                +  " _MAP['val2'] AS \"val2\" "
                +  " from \"elastic\".\"%s\"", NAME);

        ViewTableMacro macro = ViewTable.viewMacro(root, viewSql,
            Collections.singletonList("elastic"), Arrays.asList("elastic", "view"), false);
        root.add("view", macro);
        return connection;
      }
    };
  }

  @Test void searchToRange() {
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select count(*) from view where val1 >= 5 and val1 <=20")
        .returns("EXPR$0=2\n");

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select count(*) from view where val1 < 10 or val1 >20")
        .returns("EXPR$0=2\n");

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select count(*) from view where val1 <= 10 or (val1 > 15 and val1 <= 20)")
        .returns("EXPR$0=3\n");

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select count(*) from view where cat4 >= '2010-01-01T00:00:00' "
            + "and cat4 <= '2019-01-01'")
        .returns("EXPR$0=1\n");
  }

  @Test void searchToTerms() {
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select count(*) from view where cat1 in ('a', 'b')")
        .returns("EXPR$0=2\n");

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select count(*) from view where val1 in (10, 20)")
        .returns("EXPR$0=1\n");

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select count(*) from view where cat4 in ('2018-01-01', '2019-12-12')")
        .returns("EXPR$0=2\n");

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select count(*) from view where cat4 not in ('2018-01-01', '2019-12-12')")
        .returns("EXPR$0=2\n");
  }
}
