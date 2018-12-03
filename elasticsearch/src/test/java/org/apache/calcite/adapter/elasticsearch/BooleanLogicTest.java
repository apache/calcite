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
 * Test of different boolean expressions (some more complex than others).
 */
public class BooleanLogicTest {

  @ClassRule
  public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

  private static final String NAME = "docs";

  /**
   * Used to create {@code zips} index and insert some data
   * @throws Exception when ES node setup failed
   */
  @BeforeClass
  public static void setupInstance() throws Exception {

    final Map<String, String> mapping = ImmutableMap.of("a", "keyword", "b", "keyword",
        "c", "keyword", "int", "long");

    NODE.createIndex(NAME, mapping);

    String doc = "{'a': 'a', 'b':'b', 'c':'c', 'int': 42}".replace('\'', '"');
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
            "select cast(_MAP['a'] AS varchar(2)) AS a, "
                + " cast(_MAP['b'] AS varchar(2)) AS b, "
                +  " cast(_MAP['c'] AS varchar(2)) AS c, "
                +  " cast(_MAP['int'] AS integer) AS num"
                +  " from \"elastic\".\"%s\"", NAME);

        ViewTableMacro macro = ViewTable.viewMacro(root, viewSql,
            Collections.singletonList("elastic"),
            Arrays.asList("elastic", "view"), false);
        root.add("VIEW", macro);

        return connection;
      }
    };
  }

  @Test
  public void expressions() {
    assertSingle("select * from view");
    assertSingle("select * from view where a = 'a'");
    assertEmpty("select * from view where a <> 'a'");
    assertSingle("select * from view where  'a' = a");
    assertEmpty("select * from view where a = 'b'");
    assertEmpty("select * from view where 'b' = a");
    assertSingle("select * from view where a in ('a', 'b')");
    assertSingle("select * from view where a in ('a', 'c') and b = 'b'");
    assertSingle("select * from view where (a = 'ZZ' or a = 'a')  and b = 'b'");
    assertSingle("select * from view where b = 'b' and a in ('a', 'c')");
    assertSingle("select * from view where num = 42 and a in ('a', 'c')");
    assertEmpty("select * from view where a in ('a', 'c') and b = 'c'");
    assertSingle("select * from view where a in ('a', 'c') and b = 'b' and num = 42");
    assertSingle("select * from view where a in ('a', 'c') and b = 'b' and num >= 42");
    assertEmpty("select * from view where a in ('a', 'c') and b = 'b' and num <> 42");
    assertEmpty("select * from view where a in ('a', 'c') and b = 'b' and num > 42");
    assertSingle("select * from view where num = 42");
    assertSingle("select * from view where 42 = num");
    assertEmpty("select * from view where num > 42");
    assertEmpty("select * from view where 42 > num");
    assertEmpty("select * from view where num > 42 and num > 42");
    assertEmpty("select * from view where num > 42 and num < 42");
    assertEmpty("select * from view where num > 42 and num < 42 and num <> 42");
    assertEmpty("select * from view where num > 42 and num < 42 and num = 42");
    assertEmpty("select * from view where num > 42 or num < 42 and num = 42");
    assertSingle("select * from view where num > 42 and num < 42 or num = 42");
    assertSingle("select * from view where num > 42 or num < 42 or num = 42");
    assertSingle("select * from view where num >= 42 and num <= 42 and num = 42");
    assertEmpty("select * from view where num >= 42 and num <= 42 and num <> 42");
    assertEmpty("select * from view where num < 42");
    assertEmpty("select * from view where num <> 42");
    assertSingle("select * from view where num >= 42");
    assertSingle("select * from view where num <= 42");
    assertSingle("select * from view where num < 43");
    assertSingle("select * from view where num < 50");
    assertSingle("select * from view where num > 41");
    assertSingle("select * from view where num > 0");
    assertSingle("select * from view where (a = 'a' and b = 'b') or (num = 42 and c = 'c')");
    assertSingle("select * from view where c = 'c' and (a in ('a', 'b') or num in (41, 42))");
    assertSingle("select * from view where (a = 'a' or b = 'b') or (num = 42 and c = 'c')");
    assertSingle("select * from view where a = 'a' and (b = '0' or (b = 'b' and "
        +  "(c = '0' or (c = 'c' and num = 42))))");
  }

  /**
   * Tests negations ({@code NOT} operator).
   */
  @Test
  public void notExpression() {
    assertEmpty("select * from view where not a = 'a'");
    assertSingle("select * from view where not not a = 'a'");
    assertEmpty("select * from view where not not not a = 'a'");
    assertSingle("select * from view where not a <> 'a'");
    assertSingle("select * from view where not not not a <> 'a'");
    assertEmpty("select * from view where not 'a' = a");
    assertSingle("select * from view where not 'a' <> a");
    assertSingle("select * from view where not a = 'b'");
    assertSingle("select * from view where not 'b' = a");
    assertEmpty("select * from view where not a in ('a')");
    assertEmpty("select * from view where a not in ('a')");
    assertSingle("select * from view where not a not in ('a')");
    assertEmpty("select * from view where not a not in ('b')");
    assertEmpty("select * from view where not not a not in ('a')");
    assertSingle("select * from view where not not a not in ('b')");
    assertEmpty("select * from view where not a in ('a', 'b')");
    assertEmpty("select * from view where a not in ('a', 'b')");
    assertEmpty("select * from view where not a not in ('z')");
    assertEmpty("select * from view where not a not in ('z')");
    assertSingle("select * from view where not a in ('z')");
    assertSingle("select * from view where not (not num = 42 or not a in ('a', 'c'))");
    assertEmpty("select * from view where not num > 0");
    assertEmpty("select * from view where num = 42 and a not in ('a', 'c')");
    assertSingle("select * from view where not (num > 42 or num < 42 and num = 42)");
  }

  private void assertSingle(String query) {
    CalciteAssert.that()
            .with(newConnectionFactory())
            .query(query)
            .returns("A=a; B=b; C=c; NUM=42\n");
  }

  private void assertEmpty(String query) {
    CalciteAssert.that()
            .with(newConnectionFactory())
            .query(query)
            .returns("");
  }

}

// End BooleanLogicTest.java
