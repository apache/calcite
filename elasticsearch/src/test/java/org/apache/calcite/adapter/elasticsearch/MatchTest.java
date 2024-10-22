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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.util.NlsString;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import static java.util.Objects.requireNonNull;

/**
 * Testing Elasticsearch match query.
 */
@ResourceLock(value = "elasticsearch-scrolls", mode = ResourceAccessMode.READ)
class MatchTest {

  public static final EmbeddedElasticsearchPolicy NODE =
      EmbeddedElasticsearchPolicy.create();

  /** Default index/type name. */
  private static final String ZIPS = "match-zips";

  /**
   * Used to create {@code zips} index and insert zip data in bulk.
   *
   * @throws Exception when instance setup failed
   */
  @BeforeAll
  public static void setup() throws Exception {
    final Map<String, String> mapping =
        ImmutableMap.of("city", "text", "state", "keyword", "pop", "long");

    NODE.createIndex(ZIPS, mapping);

    // load records from file
    final List<ObjectNode> bulk = new ArrayList<>();
    final URL url =
        requireNonNull(
            ElasticSearchAdapterTest.class.getResource("/zips-mini.json"),
            "url");
    Resources.readLines(url,
        StandardCharsets.UTF_8, new LineProcessor<Void>() {
          @Override public boolean processLine(String line) throws IOException {
            line = line.replace("_id", "id"); // _id is a reserved attribute in ES
            bulk.add((ObjectNode) NODE.mapper().readTree(line));
            return true;
          }

          @Override public Void getResult() {
            return null;
          }
        });

    if (bulk.isEmpty()) {
      throw new IllegalStateException("No records to index. Empty file ?");
    }

    NODE.insertBulk(ZIPS, bulk);
  }

  private static CalciteConnection createConnection() throws SQLException {
    CalciteConnection connection =
        DriverManager.getConnection("jdbc:calcite:lex=JAVA")
            .unwrap(CalciteConnection.class);
    final SchemaPlus root = connection.getRootSchema();

    root.add("elastic",
        new ElasticsearchSchema(NODE.restClient(), NODE.mapper(), ZIPS));

    // add calcite view programmatically
    final String viewSql =
        String.format(Locale.ROOT, "select cast(_MAP['city'] AS varchar(20)) AS \"city\", "
            + " cast(_MAP['loc'][0] AS float) AS \"longitude\",\n"
            + " cast(_MAP['loc'][1] AS float) AS \"latitude\",\n"
            + " cast(_MAP['pop'] AS integer) AS \"pop\", "
            + " cast(_MAP['state'] AS varchar(2)) AS \"state\", "
            + " cast(_MAP['id'] AS varchar(5)) AS \"id\" "
            + "from \"elastic\".\"%s\"", ZIPS);

    root.add(ZIPS,
        ViewTable.viewMacro(root, viewSql,
            Collections.singletonList("elastic"),
            Arrays.asList("elastic", "view"), false));

    return connection;
  }

  /**
   * Tests the ElasticSearch match query. The match query is translated from
   * CONTAINS query which is build using RelBuilder, RexBuilder because the
   * normal SQL query assumes CONTAINS query is for date/period range.
   *
   * <p>Equivalent SQL query:
   *
   * <blockquote>
   * <code>select * from zips where city contains 'waltham'</code>
   * </blockquote>
   *
   * <p>ElasticSearch query for it:
   *
   * <blockquote><code>
   * {"query":{"constant_score":{"filter":{"match":{"city":"waltham"}}}}}
   * </code></blockquote>
   */
  @Test void testMatchQuery() throws Exception {
    CalciteConnection con = createConnection();
    SchemaPlus postSchema = con.getRootSchema().getSubSchema("elastic");

    FrameworkConfig postConfig = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(postSchema)
        .build();

    final RelBuilder builder = RelBuilder.create(postConfig);
    builder.scan(ZIPS);

    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);

    RexNode nameRexNode =
        rexBuilder.makeCall(SqlStdOperatorTable.ITEM,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.ANY), 0),
            rexBuilder.makeCharLiteral(
                new NlsString("city", typeFactory.getDefaultCharset().name(),
                    SqlCollation.COERCIBLE)));

    RelDataType mapType =
        typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), true));

    List<RexNode> namedList =
        ImmutableList.of(rexBuilder.makeInputRef(mapType, 0),
            nameRexNode);

    // Add fields in builder stack so it is accessible while filter preparation
    builder.projectNamed(namedList, Arrays.asList("_MAP", "city"), true);

    RexNode filterRexNode = builder
        .call(SqlStdOperatorTable.CONTAINS, builder.field("city"),
            builder.literal("waltham"));
    builder.filter(filterRexNode);

    String builderExpected = ""
        + "LogicalFilter(condition=[CONTAINS($1, 'waltham')])\n"
        + "  LogicalProject(_MAP=[$0], city=[ITEM($0, 'city')])\n"
        + "    ElasticsearchTableScan(table=[[elastic, " + ZIPS + "]])\n";

    RelNode root = builder.build();

    RelRunner ru = (RelRunner) con.unwrap(Class.forName("org.apache.calcite.tools.RelRunner"));
    try (PreparedStatement preparedStatement = ru.prepareStatement(root)) {
      String s = CalciteAssert.toString(preparedStatement.executeQuery());
      final String result = ""
          + "_MAP={id=02154, city=NORTH WALTHAM, loc=[-71.236497, 42.382492], "
          + "pop=57871, state=MA}; city=NORTH WALTHAM\n";

      // Validate query prepared
      assertThat(root, hasTree(builderExpected));

      // Validate result returned from ES
      assertThat(s, is(result));
    }
  }
}
