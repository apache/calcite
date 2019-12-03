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
import org.apache.calcite.schema.impl.ViewTableMacro;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
/**
 * Testing Elasticsearch match query.
 */
public class MatchTest {

  @ClassRule //init once for all tests
  public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

  /** Default index/type name */
  private static final String ZIPS = "zips";
  private static final int ZIPS_SIZE = 149;

  /**
   * Used to create {@code zips} index and insert zip data in bulk.
   * @throws Exception when instance setup failed
   */
  @BeforeClass
  public static void setup() throws Exception {
    final Map<String, String> mapping = ImmutableMap.of("city", "text", "state",
        "keyword", "pop", "long");

    NODE.createIndex(ZIPS, mapping);

    // load records from file
    final List<ObjectNode> bulk = new ArrayList<>();
    Resources.readLines(ElasticSearchAdapterTest.class.getResource("/zips-mini.json"),
        StandardCharsets.UTF_8, new LineProcessor<Void>() {
          @Override public boolean processLine(String line) throws IOException {
            line = line.replaceAll("_id", "id"); // _id is a reserved attribute in ES
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

  private CalciteAssert.ConnectionFactory newConnectionFactory() {
    return new CalciteAssert.ConnectionFactory() {
      @Override public Connection createConnection() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
        final SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();

        root.add("elastic", new ElasticsearchSchema(NODE.restClient(), NODE.mapper(), ZIPS));

        // add calcite view programmatically
        final String viewSql = "select cast(_MAP['city'] AS varchar(20)) AS \"city\", "
            + " cast(_MAP['loc'][0] AS float) AS \"longitude\",\n"
            + " cast(_MAP['loc'][1] AS float) AS \"latitude\",\n"
            + " cast(_MAP['pop'] AS integer) AS \"pop\", "
            +  " cast(_MAP['state'] AS varchar(2)) AS \"state\", "
            +  " cast(_MAP['id'] AS varchar(5)) AS \"id\" "
            +  "from \"elastic\".\"zips\"";

        ViewTableMacro macro = ViewTable.viewMacro(root, viewSql,
            Collections.singletonList("elastic"), Arrays.asList("elastic", "view"), false);
        root.add("zips", macro);

        return connection;
      }
    };
  }

      /**
   * Test the ElasticSearch match query. The match query is translated from CONTAINS query which
   * is build using RelBuilder, RexBuilder because the normal sql query assumes CONTAINS query
   * is for date/period range
   *
   * Equivalent SQL query: select * from zips where city contains 'waltham'
   *
   * ElasticSearch query for it:
   * {"query":{"constant_score":{"filter":{"match":{"city":"waltham"}}}}}
   *
   * @throws Exception
   */
  @Test public void testMatchQuery() throws Exception {

    CalciteConnection con = (CalciteConnection) newConnectionFactory()
        .createConnection();
    SchemaPlus postschema = con.getRootSchema().getSubSchema("elastic");

    FrameworkConfig postConfig = Frameworks.newConfigBuilder()
         .parserConfig(SqlParser.Config.DEFAULT)
         .defaultSchema(postschema)
         .build();

    final RelBuilder builder = RelBuilder.create(postConfig);
    builder.scan(ZIPS);

    final RelDataTypeFactory relDataTypeFactory = new SqlTypeFactoryImpl(
        RelDataTypeSystem.DEFAULT);
    final RexBuilder rexbuilder = new RexBuilder(relDataTypeFactory);

    RexNode nameRexNode = rexbuilder.makeCall(SqlStdOperatorTable.ITEM,
        rexbuilder.makeInputRef(relDataTypeFactory.createSqlType(SqlTypeName.ANY), 0),
        rexbuilder.makeCharLiteral(
          new NlsString("city", rexbuilder.getTypeFactory().getDefaultCharset().name(),
          SqlCollation.COERCIBLE)));

    RelDataType mapType = relDataTypeFactory.createMapType(
        relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
        relDataTypeFactory.createTypeWithNullability(
          relDataTypeFactory.createSqlType(SqlTypeName.ANY), true));

    ArrayList<RexNode> namedList = new ArrayList<RexNode>(2);
    namedList.add(rexbuilder.makeInputRef(mapType, 0));
    namedList.add(nameRexNode);

    //Add fields in builder stack so it is accessible while filter preparation
    builder.projectNamed(namedList, Arrays.asList("_MAP", "city"), true);

    RexNode filterRexNode = builder
         .call(SqlStdOperatorTable.CONTAINS, builder.field("city"),
         builder.literal("waltham"));
    builder.filter(filterRexNode);

    String builderExpected = ""
         + "LogicalFilter(condition=[CONTAINS($1, 'waltham')])\n"
         + "  LogicalProject(_MAP=[$0], city=[ITEM($0, 'city')])\n"
         + "    LogicalTableScan(table=[[elastic, zips]])\n";

    RelNode root = builder.build();

    RelRunner ru = (RelRunner) con.unwrap(Class.forName("org.apache.calcite.tools.RelRunner"));
    try (PreparedStatement preparedStatement = ru.prepare(root)) {

      String s = CalciteAssert.toString(preparedStatement.executeQuery());
      final String result = ""
          + "_MAP={id=02154, city=NORTH WALTHAM, loc=[-71.236497, 42.382492], pop=57871, state=MA}; city=NORTH WALTHAM\n";

      //Validate query prepared
      assertThat(root, hasTree(builderExpected));

      //Validate result returned from ES
      assertThat(s, is(result));
    }
  }
}
