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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

/**
 * Checks renaming of fields (also upper, lower cases) during projections
 */
public class Projection2Test {

  @ClassRule
  public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

  private static final String NAME = "nested";

  @BeforeClass
  public static void setupInstance() throws Exception {

    final Map<String, String> mappings = ImmutableMap.of("a", "long",
        "b.a", "long", "b.b", "long", "b.c.a", "keyword");

    NODE.createIndex(NAME, mappings);

    String doc = "{'a': 1, 'b':{'a': 2, 'b':'3', 'c':{'a': 'foo'}}}".replace('\'', '"');
    NODE.insertDocument(NAME, (ObjectNode) NODE.mapper().readTree(doc));
  }

  private CalciteAssert.ConnectionFactory newConnectionFactory() {
    return new CalciteAssert.ConnectionFactory() {
      @Override public Connection createConnection() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:calcite:");
        final SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();

        root.add("elastic", new ElasticsearchSchema(NODE.restClient(), NODE.mapper(), NAME));

        // add calcite view programmatically
        final String viewSql = String.format(Locale.ROOT,
            "select _MAP['a'] AS \"a\", "
                + " _MAP['b.a']  AS \"b.a\", "
                +  " _MAP['b.b'] AS \"b.b\", "
                +  " _MAP['b.c.a'] AS \"b.c.a\" "
                +  " from \"elastic\".\"%s\"", NAME);

        ViewTableMacro macro = ViewTable.viewMacro(root, viewSql,
            Collections.singletonList("elastic"), Arrays.asList("elastic", "view"), false);
        root.add("VIEW", macro);
        return connection;
      }
    };
  }

  @Test
  public void projection() {
    CalciteAssert.that()
            .with(newConnectionFactory())
            .query("select \"a\", \"b.a\", \"b.b\", \"b.c.a\" from view")
            .returns("a=1; b.a=2; b.b=3; b.c.a=foo\n");
  }

  @Test
  public void projection2() {
    String sql = String.format(Locale.ROOT, "select _MAP['a'], _MAP['b.a'], _MAP['b.b'], "
        + "_MAP['b.c.a'], _MAP['missing'], _MAP['b.missing'] from \"elastic\".\"%s\"", NAME);

    CalciteAssert.that()
            .with(newConnectionFactory())
            .query(sql)
            .returns("EXPR$0=1; EXPR$1=2; EXPR$2=3; EXPR$3=foo; EXPR$4=null; EXPR$5=null\n");
  }

}

// End Projection2Test.java
