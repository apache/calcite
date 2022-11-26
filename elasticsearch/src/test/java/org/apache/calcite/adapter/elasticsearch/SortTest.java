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
 * Testing Elasticsearch ORDER BY transformations.
 */
@ResourceLock(value = "elasticsearch-scrolls", mode = ResourceAccessMode.READ)
public class SortTest {

  public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

  private static final String NAME = "sort";

  @BeforeAll
  public static void setupInstance() throws Exception {
    final Map<String, String> mappings = ImmutableMap.<String, String>builder()
        .put("col1", "integer")
        .put("col2", "keyword")
        .put("col3", "double")
        .put("col4", "date")
        .put("col5", "long")
        .build();

    NODE.createIndex(NAME, mappings);

    String doc1 = "{col1:1, col2:'a', col3:1.1, col4:'2022-01-15', col5:100}";
    String doc2 = "{col1:2, col2:'b', col3:2.1, col4:'2021-01-15', col5:99}";
    String doc3 = "{col1:2, col2:'c', col5:98}";
    String doc4 = "{col3:0.1, col4:'2020-01-15'}";
    String doc5 = "{col1:1, col3:3.1, col4:'2019-01-15'}";
    String doc6 = "{col2:'z', col4:'2018-01-15', col5:98}";

    final ObjectMapper mapper = new ObjectMapper()
        .enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES) // user-friendly settings to
        .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES); // avoid too much quoting

    final List<ObjectNode> docs = new ArrayList<>();
    for (String text: Arrays.asList(doc1, doc2, doc3, doc4, doc5, doc6)) {
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
            "select _MAP['col1'] AS \"col1\", "
                + " _MAP['col2']  AS \"col2\", "
                +  " _MAP['col3'] AS \"col3\", "
                +  " _MAP['col4'] AS \"col4\", "
                +  " _MAP['col5'] AS \"col5\" "
                +  " from \"elastic\".\"%s\"", NAME);

        ViewTableMacro macro = ViewTable.viewMacro(root, viewSql,
            Collections.singletonList("elastic"), Arrays.asList("elastic", "view"), false);
        root.add("view", macro);
        return connection;
      }
    };
  }

  @Test void testBasicSort() {
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select * from view order by col1 asc, col2 desc")
        .returns("col1=1; col2=null; col3=3.1; col4=2019-01-15; col5=null\n"
                + "col1=1; col2=a; col3=1.1; col4=2022-01-15; col5=100\n"
                + "col1=2; col2=c; col3=null; col4=null; col5=98\n"
                + "col1=2; col2=b; col3=2.1; col4=2021-01-15; col5=99\n"
                + "col1=null; col2=null; col3=0.1; col4=2020-01-15; col5=null\n"
                + "col1=null; col2=z; col3=null; col4=2018-01-15; col5=98\n");
  }

  @Test void testNullsSort() {
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select col1, col2 from view order by col1 asc nulls first, col2 desc nulls last")
        .returns("col1=null; col2=z\n"
                + "col1=null; col2=null\n"
                + "col1=1; col2=a\n"
                + "col1=1; col2=null\n"
                + "col1=2; col2=c\n"
                + "col1=2; col2=b\n");

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select col3, col4 from view order by col3 desc nulls last, col4 asc nulls first")
        .returns("col3=3.1; col4=2019-01-15\n"
            + "col3=2.1; col4=2021-01-15\n"
            + "col3=1.1; col4=2022-01-15\n"
            + "col3=0.1; col4=2020-01-15\n"
            + "col3=null; col4=null\n"
            + "col3=null; col4=2018-01-15\n");
  }

  @Test void testNullsSortWithGroupBy() {
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select col1, col2 from view group by col1, col2\n"
            + " order by col2 desc nulls last, col1 asc nulls first")
        .returns("col1=null; col2=z\n"
            + "col1=2; col2=c\n"
            + "col1=2; col2=b\n"
            + "col1=1; col2=a\n"
            + "col1=null; col2=null\n"
            + "col1=1; col2=null\n");
  }
}
