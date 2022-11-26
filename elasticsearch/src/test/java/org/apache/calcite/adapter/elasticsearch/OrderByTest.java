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
import org.junit.jupiter.api.Disabled;
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
 * Testing Elasticsearch ORDER BY transformations.
 */
@ResourceLock(value = "elasticsearch-scrolls", mode = ResourceAccessMode.READ)
public class OrderByTest {

  public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

  private static final String NAME = "orders";

  @BeforeAll
  public static void setupInstance() throws Exception {
    final Map<String, String> mappings = ImmutableMap.<String, String>builder()
        .put("val1", "keyword")
        .put("val2", "integer")
        .put("val3", "long")
        .put("val4", "date")
        .put("val5", "double")
        .build();

    NODE.createIndex(NAME, mappings);

    String doc1 = "{val1:'a', val2:21, val3:100, val4:'2018-01-01', val5:60.5}";
    String doc2 = "{val1:'b', val2:22, val3:99, val4:'2019-01-01', val5:61.5}";
    String doc3 = "{val1:'c', val2:23, val3:98, val4:'2020-01-01', val5:62.5}";
    String doc4 = "{val1:'d', val2:22, val3:96, val4:'2020-02-01'}";
    String doc5 = "{val2:23, val3:96, val5:62.5}";

    final ObjectMapper mapper = new ObjectMapper()
        .enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES)
        .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);

    final List<ObjectNode> docs = new ArrayList<>();
    for (String text: Arrays.asList(doc1, doc2, doc3, doc4, doc5)) {
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
            "select _MAP['val1'] AS \"val1\", "
                + " _MAP['val2']  AS \"val2\", "
                +  " _MAP['val3'] AS \"val3\", "
                +  " _MAP['val4'] AS \"val4\", "
                +  " _MAP['val5'] AS \"val5\" "
                +  " from \"elastic\".\"%s\"", NAME);

        ViewTableMacro macro = ViewTable.viewMacro(root, viewSql,
            Collections.singletonList("elastic"), Arrays.asList("elastic", "view"), false);
        root.add("view", macro);
        return connection;
      }
    };
  }

  @Test void testOrderAfterGroup1() {
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select val2, max(val3) as MAX_VAL3 from view group by val2 order by val2 desc")
        .returns("val2=23; MAX_VAL3=98.0\n"
              + "val2=22; MAX_VAL3=99.0\n"
              + "val2=21; MAX_VAL3=100.0\n");

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select val2, max(val3) as MAX_VAL3 "
            + "from view group by val2 order by MAX_VAL3 desc")
        .returns("val2=21; MAX_VAL3=100.0\n"
            + "val2=22; MAX_VAL3=99.0\n"
            + "val2=23; MAX_VAL3=98.0\n");
  }

  @Test void testOrderAfterGroup2() throws Exception {
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select val2, min(val3) as MIN_VAL3 from view group by val2 "
            + "order by MIN_VAL3 asc, val2 asc")
        .returns("val2=22; MIN_VAL3=96.0\n"
            + "val2=23; MIN_VAL3=96.0\n"
            + "val2=21; MIN_VAL3=100.0\n");

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select val2, min(val3) as MIN_VAL3, max(val5) as MAX_VAL5 from view "
            + "group by val2 order by MIN_VAL3 asc, val2 asc, MAX_VAL5 desc")
        .returns("val2=22; MIN_VAL3=96.0; MAX_VAL5=61.5\n"
            + "val2=23; MIN_VAL3=96.0; MAX_VAL5=62.5\n"
            + "val2=21; MIN_VAL3=100.0; MAX_VAL5=60.5\n");

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select val2, min(val3) as MIN_VAL3, max(val5) as MAX_VAL5 from view "
            + "group by val2 order by MIN_VAL3 asc, MAX_VAL5 desc, val2 asc")
        .returns("val2=23; MIN_VAL3=96.0; MAX_VAL5=62.5\n"
            + "val2=22; MIN_VAL3=96.0; MAX_VAL5=61.5\n"
            + "val2=21; MIN_VAL3=100.0; MAX_VAL5=60.5\n");

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select val2, val4, min(val3) as MIN_VAL3, max(val5) as MAX_VAL5 from view "
            + "group by val2, val4 order by val2 asc, MIN_VAL3 asc, val4 desc, MAX_VAL5 desc")
        .returns("val2=21; val4=1514764800000; MIN_VAL3=100.0; MAX_VAL5=60.5\n"
            + "val2=22; val4=1580515200000; MIN_VAL3=96.0; MAX_VAL5=null\n"
            + "val2=22; val4=1546300800000; MIN_VAL3=99.0; MAX_VAL5=61.5\n"
            + "val2=23; val4=null; MIN_VAL3=96.0; MAX_VAL5=62.5\n"
            + "val2=23; val4=1577836800000; MIN_VAL3=98.0; MAX_VAL5=62.5\n");
  }

  @Disabled
  @Test void testAbnormalOrderAfterGroup() {
    // all aggregation result must be ordered in the last "aggregation" script
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select val2, val4, min(val3) as MIN_VAL3, max(val5) as MAX_VAL5 from view "
            + "group by val2, val4 order by MIN_VAL3 asc, val2 asc, val4 desc, MAX_VAL5 desc")
        .throws_("The provided aggregation [MIN_VAL3] either does not exist, "
            + "or is a pipeline aggregation and cannot be used to sort the buckets");
  }
}
