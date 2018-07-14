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

import com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Set of tests for ES adapter. Uses real instance via {@link EmbeddedElasticsearchPolicy}. Document
 * source is local {@code zips-mini.json} file (located in test classpath).
 */
public class ElasticSearchAdapterTest {

  @ClassRule //init once for all tests
  public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

  /** Default index/type name */
  private static final String ZIPS = "zips";

  /**
   * Used to create {@code zips} index and insert zip data in bulk.
   * @throws Exception when instance setup failed
   */
  @BeforeClass
  public static void setupInstance() throws Exception {
    final Map<String, String> mapping = ImmutableMap.of("city", "keyword", "state",
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
        final Connection connection = DriverManager.getConnection("jdbc:calcite:");
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
        root.add("ZIPS", macro);

        return connection;
      }
    };
  }

  private CalciteAssert.AssertThat calciteAssert() {
    return CalciteAssert.that()
        .with(newConnectionFactory());
  }

  /**
   * Tests using calcite view
   */
  @Test
  public void view() {
    calciteAssert()
        .query("select * from zips where \"city\" = 'BROOKLYN'")
        .returns("city=BROOKLYN; longitude=-73.956985; latitude=40.646694; "
            + "pop=111396; state=NY; id=11226\n")
        .returnsCount(1);
  }

  @Test
  public void emptyResult() {
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select * from zips limit 0")
        .returnsCount(0);

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select * from \"elastic\".\"zips\" where _MAP['Foo'] = '_MISSING_'")
        .returnsCount(0);
  }

  @Test
  public void basic() throws Exception {
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select * from \"elastic\".\"zips\" where _MAP['city'] = 'BROOKLYN'")
        .returnsCount(1);

    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select * from \"elastic\".\"zips\" where"
            + " _MAP['city'] in ('BROOKLYN', 'WASHINGTON')")
        .returnsCount(2);

    // lower-case
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select * from \"elastic\".\"zips\" where "
            + "_MAP['city'] in ('brooklyn', 'Brooklyn', 'BROOK') ")
        .returnsCount(0);

    // missing field
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select * from \"elastic\".\"zips\" where _MAP['CITY'] = 'BROOKLYN'")
        .returnsCount(0);

    // limit works
    CalciteAssert.that()
        .with(newConnectionFactory())
        .query("select * from \"elastic\".\"zips\" limit 42")
        .returnsCount(42);

  }

  @Test public void testSort() {
    final String explain = "PLAN=ElasticsearchToEnumerableConverter\n"
        + "  ElasticsearchSort(sort0=[$4], dir0=[ASC])\n"
        + "    ElasticsearchProject(city=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], longitude=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], latitude=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], pop=[CAST(ITEM($0, 'pop')):INTEGER], state=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], id=[CAST(ITEM($0, 'id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
        + "      ElasticsearchTableScan(table=[[elastic, zips]])";

    calciteAssert()
        .query("select * from zips order by \"state\"")
        .returnsCount(10)
        .explainContains(explain);
  }

  @Test public void testSortLimit() {
    final String sql = "select \"state\", \"pop\" from zips\n"
        + "order by \"state\", \"pop\" offset 2 rows fetch next 3 rows only";
    calciteAssert()
        .query(sql)
        .returnsUnordered("state=AK; pop=32383",
            "state=AL; pop=42124",
            "state=AL; pop=43862")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker(
                "\"_source\" : [\"state\", \"pop\"]",
                "\"sort\": [ {\"state\": \"asc\"}, {\"pop\": \"asc\"}]",
                "\"from\": 2",
                "\"size\": 3"));
  }



  @Test public void testOffsetLimit() {
    final String sql = "select \"state\", \"id\" from zips\n"
        + "offset 2 fetch next 3 rows only";
    calciteAssert()
        .query(sql)
        .runs()
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker(
                "\"from\": 2",
                "\"size\": 3",
                "\"_source\" : [\"state\", \"id\"]"));
  }

  @Test public void testLimit() {
    final String sql = "select \"state\", \"id\" from zips\n"
        + "fetch next 3 rows only";

    calciteAssert()
        .query(sql)
        .runs()
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker(
                "\"size\": 3",
                "\"_source\" : [\"state\", \"id\"]"));
  }

  @Test public void testFilterSort() {
    final String sql = "select * from zips\n"
        + "where \"state\" = 'CA' and \"pop\" >= 94000\n"
        + "order by \"state\", \"pop\"";
    final String explain = "PLAN=ElasticsearchToEnumerableConverter\n"
        + "  ElasticsearchSort(sort0=[$4], sort1=[$3], dir0=[ASC], dir1=[ASC])\n"
        + "    ElasticsearchProject(city=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], longitude=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], latitude=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], pop=[CAST(ITEM($0, 'pop')):INTEGER], state=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], id=[CAST(ITEM($0, 'id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
        + "      ElasticsearchFilter(condition=[AND(=(CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'CA'), >=(CAST(ITEM($0, 'pop')):INTEGER, 94000))])\n"
        + "        ElasticsearchTableScan(table=[[elastic, zips]])\n\n";
    calciteAssert()
        .query(sql)
        .returnsOrdered("city=NORWALK; longitude=-118.081767; latitude=33.90564;"
                + " pop=94188; state=CA; id=90650",
            "city=LOS ANGELES; longitude=-118.258189; latitude=34.007856;"
                + " pop=96074; state=CA; id=90011",
            "city=BELL GARDENS; longitude=-118.17205; latitude=33.969177;"
                + " pop=99568; state=CA; id=90201")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("\"query\" : "
                    + "{\"constant_score\":{\"filter\":{\"bool\":"
                    + "{\"must\":[{\"term\":{\"state\":\"CA\"}},"
                    + "{\"range\":{\"pop\":{\"gte\":94000}}}]}}}}",
                "\"script_fields\": {\"longitude\":{\"script\":\"params._source.loc[0]\"}, "
                    + "\"latitude\":{\"script\":\"params._source.loc[1]\"}, "
                    + "\"city\":{\"script\": \"params._source.city\"}, "
                    + "\"pop\":{\"script\": \"params._source.pop\"}, "
                    + "\"state\":{\"script\": \"params._source.state\"}, "
                    + "\"id\":{\"script\": \"params._source.id\"}}",
                "\"sort\": [ {\"state\": \"asc\"}, {\"pop\": \"asc\"}]"))
        .explainContains(explain);
  }

  @Test public void testFilterSortDesc() {
    final String sql = "select * from zips\n"
        + "where \"pop\" BETWEEN 95000 AND 100000\n"
        + "order by \"state\" desc, \"pop\"";
    calciteAssert()
        .query(sql)
        .limit(4)
        .returnsOrdered(
            "city=LOS ANGELES; longitude=-118.258189; latitude=34.007856; pop=96074; state=CA; id=90011",
            "city=BELL GARDENS; longitude=-118.17205; latitude=33.969177; pop=99568; state=CA; id=90201");
  }

  @Ignore("Known issue when predicate analyzer doesn't simplify the expression (a = 1 and a > 0) ")
  @Test public void testFilterRedundant() {
    // known issue when PredicateAnalyzer doesn't simplify expressions
    // (a < 3 and and a > 0 and a = 1) equivalent to (a = 1)
    final String sql = "select * from zips\n"
        + "where \"state\" > 'CA' and \"state\" < 'AZ' and \"state\" = 'OK'";
    calciteAssert()
        .query(sql)
        .runs()
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker(""
                    + "\"query\" : {\"constant_score\":{\"filter\":{\"bool\":"
                    + "{\"must\":[{\"term\":{\"state\":\"OK\"}}]}}}}",
                "\"script_fields\": {\"longitude\":{\"script\":\"params._source.loc[0]\"}, "
                    +  "\"latitude\":{\"script\":\"params._source.loc[1]\"}, "
                    +   "\"city\":{\"script\": \"params._source.city\"}, "
                    +   "\"pop\":{\"script\": \"params._source.pop\"}, \"state\":{\"script\": \"params._source.state\"}, "
                    +            "\"id\":{\"script\": \"params._source.id\"}}"
            ));
  }

  @Test public void testInPlan() {
    final String[] searches = {
        "\"query\" : {\"constant_score\":{\"filter\":{\"bool\":{\"should\":"
            + "[{\"term\":{\"pop\":96074}},{\"term\":{\"pop\":99568}}]}}}}",
        "\"script_fields\": {\"longitude\":{\"script\":\"params._source.loc[0]\"}, "
            +  "\"latitude\":{\"script\":\"params._source.loc[1]\"}, "
            +  "\"city\":{\"script\": \"params._source.city\"}, "
            +  "\"pop\":{\"script\": \"params._source.pop\"}, "
            +  "\"state\":{\"script\": \"params._source.state\"}, "
            +  "\"id\":{\"script\": \"params._source.id\"}}"
    };

    calciteAssert()
        .query("select * from zips where \"pop\" in (96074, 99568)")
        .returnsUnordered(
            "city=BELL GARDENS; longitude=-118.17205; latitude=33.969177; pop=99568; state=CA; id=90201",
            "city=LOS ANGELES; longitude=-118.258189; latitude=34.007856; pop=96074; state=CA; id=90011"
        )
        .queryContains(ElasticsearchChecker.elasticsearchChecker(searches));
  }

  @Test public void testZips() {
    calciteAssert()
        .query("select \"state\", \"city\" from zips")
        .returnsCount(10);
  }

  @Test public void testProject() {
    final String sql = "select \"state\", \"city\", 0 as \"zero\"\n"
        + "from zips\n"
        + "order by \"state\", \"city\"";

    calciteAssert()
        .query(sql)
        .limit(2)
        .returnsUnordered("state=AK; city=ANCHORAGE; zero=0",
            "state=AK; city=FAIRBANKS; zero=0")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("\"script_fields\": "
                    + "{\"zero\":{\"script\": \"0\"}, "
                    + "\"state\":{\"script\": \"params._source.state\"}, "
                    + "\"city\":{\"script\": \"params._source.city\"}}",
                "\"sort\": [ {\"state\": \"asc\"}, {\"city\": \"asc\"}]"));
  }

  @Test public void testFilter() {
    final String explain = "PLAN=ElasticsearchToEnumerableConverter\n"
        + "  ElasticsearchProject(state=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], city=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
        + "    ElasticsearchFilter(condition=[=(CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'CA')])\n"
        + "      ElasticsearchTableScan(table=[[elastic, zips]])";
    calciteAssert()
        .query("select \"state\", \"city\" from zips where \"state\" = 'CA'")
        .limit(3)
        .returnsUnordered("state=CA; city=BELL GARDENS",
            "state=CA; city=LOS ANGELES",
            "state=CA; city=NORWALK")
        .explainContains(explain);
  }

  @Test public void testFilterReversed() {
    calciteAssert()
        .query("select \"state\", \"city\" from zips where 'WI' < \"state\" order by \"city\"")
        .limit(2)
        .returnsUnordered("state=WV; city=BECKLEY",
            "state=WY; city=CHEYENNE");
    calciteAssert()
        .query("select \"state\", \"city\" from zips where \"state\" > 'WI' order by \"city\"")
        .limit(2)
        .returnsUnordered("state=WV; city=BECKLEY",
            "state=WY; city=CHEYENNE");
  }

}

// End ElasticSearchAdapterTest.java
