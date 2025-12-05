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
import org.apache.calcite.test.ElasticsearchChecker;
import org.apache.calcite.util.TestUtil;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.PatternSyntaxException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Checks renaming of fields (also upper, lower cases) during projections.
 */
@ResourceLock(value = "elasticsearch-scrolls", mode = ResourceAccessMode.READ)
class Projection2Test {

  public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

  private static final String NAME = "nested";

  @BeforeAll
  public static void setupInstance() throws Exception {
    final Map<String, String> mappings =
        ImmutableMap.<String, String>builder()
            .put("a", "long")
            .put("b", "nested")
            .put("b.a", "long")
            .put("b.b", "long")
            .put("b.c", "nested")
            .put("b.c.a", "keyword")
            .build();

    NODE.createIndex(NAME, mappings);

    String doc = "{'a': 1, 'b':{'a': 2, 'b':'3', 'c':{'a': 'foo'}}}".replace('\'', '"');
    NODE.insertDocument(NAME, (ObjectNode) NODE.mapper().readTree(doc));
  }

  private static Connection createConnection() throws SQLException {
    final Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    final SchemaPlus root =
        connection.unwrap(CalciteConnection.class).getRootSchema();

    root.add("elastic", new ElasticsearchSchema(NODE.restClient(), NODE.mapper(), NAME));

    // add calcite view programmatically
    final String viewSql =
        String.format(Locale.ROOT, "select _MAP['a'] AS \"a\", "
            + " _MAP['b.a']  AS \"b.a\", "
            + " _MAP['b.b'] AS \"b.b\", "
            + " _MAP['b.c.a'] AS \"b.c.a\", "
            + " _MAP['_id'] AS \"id\" " // _id field is implicit
            + " from \"elastic\".\"%s\"", NAME);

    ViewTableMacro macro =
        ViewTable.viewMacro(root, viewSql, Collections.singletonList("elastic"),
            Arrays.asList("elastic", "view"), false);
    root.add("VIEW", macro);
    return connection;
  }

  @Test void projection() {
    CalciteAssert.that()
        .with(Projection2Test::createConnection)
        .query("select \"a\", \"b.a\", \"b.b\", \"b.c.a\" from view")
        .returns("a=1; b.a=2; b.b=3; b.c.a=foo\n");
  }

  @Test void projection2() {
    String sql = String.format(Locale.ROOT, "select _MAP['a'], _MAP['b.a'], _MAP['b.b'], "
        + "_MAP['b.c.a'], _MAP['missing'], _MAP['b.missing'] from \"elastic\".\"%s\"", NAME);

    CalciteAssert.that()
        .with(Projection2Test::createConnection)
        .query(sql)
        .returns("EXPR$0=1; EXPR$1=2; EXPR$2=3; EXPR$3=foo; EXPR$4=null; EXPR$5=null\n");
  }

  @Test void projection3() {
    CalciteAssert.that()
        .with(Projection2Test::createConnection)
        .query(
            String.format(Locale.ROOT, "select * from \"elastic\".\"%s\"", NAME))
        .returns("_MAP={a=1, b={a=2, b=3, c={a=foo}}}\n");

    CalciteAssert.that()
        .with(Projection2Test::createConnection)
        .query(
            String.format(Locale.ROOT, "select *, _MAP['a'] from \"elastic\".\"%s\"", NAME))
        .returns("_MAP={a=1, b={a=2, b=3, c={a=foo}}}; EXPR$1=1\n");
  }

  /**
   * Test that {@code _id} field is available when queried explicitly.
   *
   * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-id-field.html">ID Field</a>
   */
  @Test void projectionWithIdField() {
    final CalciteAssert.AssertThat fixture =
        CalciteAssert.that()
            .with(Projection2Test::createConnection);

    fixture.query("select \"id\" from view")
        .returns(regexMatch("id=\\p{Graph}+"));

    fixture.query("select \"id\", \"id\" from view")
        .returns(regexMatch("id=\\p{Graph}+; id=\\p{Graph}+"));

    fixture.query("select \"id\", \"a\" from view")
        .returns(regexMatch("id=\\p{Graph}+; a=1"));

    fixture.query("select \"a\", \"id\" from view")
        .returns(regexMatch("a=1; id=\\p{Graph}+"));

    // single _id column
    final String sql1 = String.format(Locale.ROOT, "select _MAP['_id'] "
        + " from \"elastic\".\"%s\"", NAME);
    fixture.query(sql1)
        .returns(regexMatch("EXPR$0=\\p{Graph}+"));

    // multiple columns: _id and a
    final String sql2 = String.format(Locale.ROOT, "select _MAP['_id'], _MAP['a'] "
        + " from \"elastic\".\"%s\"", NAME);
    fixture.query(sql2)
        .returns(regexMatch("EXPR$0=\\p{Graph}+; EXPR$1=1"));

    // multiple _id columns
    final String sql3 = String.format(Locale.ROOT, "select _MAP['_id'], _MAP['_id'] "
        + " from \"elastic\".\"%s\"", NAME);
    fixture.query(sql3)
        .returns(regexMatch("EXPR$0=\\p{Graph}+; EXPR$1=\\p{Graph}+"));

    // _id column with same alias
    final String sql4 = String.format(Locale.ROOT, "select _MAP['_id'] as \"_id\" "
        + " from \"elastic\".\"%s\"", NAME);
    fixture.query(sql4)
        .returns(regexMatch("_id=\\p{Graph}+"));

    // _id field not available implicitly
    String sql5 =
        String.format(Locale.ROOT, "select * from \"elastic\".\"%s\"", NAME);
    fixture.query(sql5)
        .returns(regexMatch("_MAP={a=1, b={a=2, b=3, c={a=foo}}}"));

    String sql6 =
        String.format(Locale.ROOT,
            "select *, _MAP['_id'] from \"elastic\".\"%s\"", NAME);
    fixture.query(sql6)
        .returns(regexMatch("_MAP={a=1, b={a=2, b=3, c={a=foo}}}; EXPR$1=\\p{Graph}+"));
  }

  /**
   * Avoid using scripting for simple projections.
   *
   * <p> When projecting simple fields (without expression) no
   * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting.html">scripting</a>
   * should be used just
   * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-source-filtering.html">_source</a>.
   */
  @Test void simpleProjectionNoScripting() {
    CalciteAssert.that()
        .with(Projection2Test::createConnection)
        .query(
            String.format(Locale.ROOT, "select _MAP['_id'], _MAP['a'], _MAP['b.a'] from "
                + " \"elastic\".\"%s\" where _MAP['b.a'] = 2", NAME))
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("'query.constant_score.filter.term.b.a':2",
                "_source:['a', 'b.a']", "size:5196"))
        .returns(regexMatch("EXPR$0=\\p{Graph}+; EXPR$1=1; EXPR$2=2"));

  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4450">[CALCITE-4450]
   * ElasticSearch query with varchar literal projection fails with JsonParseException</a>. */
  @Test void projectionStringLiteral() {
    CalciteAssert.that()
        .with(Projection2Test::createConnection)
        .query(
            String.format(Locale.ROOT, "select 'foo' as \"lit\"\n"
                + "from \"elastic\".\"%s\"", NAME))
        .returns("lit=foo\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4450">[CALCITE-4450]
   * ElasticSearch query with varchar literal projection fails with JsonParseException</a>. */
  @Test void projectionStringLiteralAndColumn() {
    CalciteAssert.that()
        .with(Projection2Test::createConnection)
        .query(
            String.format(Locale.ROOT, "select 'foo\\\"bar\\\"' as \"lit\", _MAP['a'] as \"a\"\n"
                + "from \"elastic\".\"%s\"", NAME))
        .returns("lit=foo\\\"bar\\\"; a=1\n");
  }

  /**
   * Allows values to contain regular expressions instead of exact values.
   * <pre>
   *   {@code
   *      key1=foo1; key2=\\w+; key4=\\d{3,4}
   *   }
   * </pre>
   *
   * @param lines lines with regexp
   * @return consumer to be used in {@link org.apache.calcite.test.CalciteAssert.AssertQuery}
   */
  private static Consumer<ResultSet> regexMatch(String...lines) {
    return rset -> {
      try {
        final int columnCount = rset.getMetaData().getColumnCount();
        final StringBuilder actual = new StringBuilder();
        int processedRows = 0;
        boolean fail = false;
        while (rset.next()) {
          if (processedRows >= lines.length) {
            fail = true;
          }

          for (int i = 1; i <= columnCount; i++) {
            final String name = rset.getMetaData().getColumnName(i);
            final String value = rset.getString(i);
            actual.append(name).append('=').append(value);
            if (i < columnCount) {
              actual.append("; ");
            }

            // don't re-check if already failed
            if (!fail) {
              // splitting string of type: key1=val1; key2=val2
              final String keyValue = lines[processedRows].split("; ")[i - 1];
              final String[] parts = keyValue.split("=", 2);
              final String expectedName = parts[0];
              final String expectedValue = parts[1];

              boolean valueMatches = expectedValue.equals(value);

              if (!valueMatches) {
                // try regex
                try {
                  valueMatches = value != null && value.matches(expectedValue);
                } catch (PatternSyntaxException ignore) {
                  // probably not a regular expression
                }
              }

              fail = !(name.equals(expectedName) && valueMatches);
            }

          }

          processedRows++;
        }

        // also check that processed same number of rows
        fail &= processedRows == lines.length;

        if (fail) {
          assertThat(actual, hasToString(String.join("\n", lines)));
          fail("Should have failed on previous line, but for some reason didn't");
        }
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }
}
