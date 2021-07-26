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
package org.apache.calcite.test;

import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests for the {@code org.apache.calcite.adapter.druid} package.
 *
 * <p>Druid must be up and running with foodmart and wikipedia datasets loaded. Follow the
 * instructions on <a href="https://github.com/zabetak/calcite-druid-dataset">calcite-druid-dataset
 * </a> to setup Druid before launching these tests.
 *
 * <p>Features not yet implemented:
 * <ul>
 *   <li>push LIMIT into "select" query</li>
 *   <li>push SORT and/or LIMIT into "groupBy" query</li>
 *   <li>push HAVING into "groupBy" query</li>
 * </ul>
 *
 * <p>These tests use TIMESTAMP type for the Druid timestamp column, instead
 * of TIMESTAMP WITH LOCAL TIME ZONE type as {@link DruidAdapterIT}.
 */
public class DruidAdapter2IT {
  /** URL of the "druid-foodmart" model. */
  public static final URL FOODMART =
      DruidAdapter2IT.class.getResource("/druid-foodmart-model-timestamp.json");

  private static final String VARCHAR_TYPE =
      "VARCHAR";

  private static final String FOODMART_TABLE = "\"foodmart\"";

  /** Whether to run this test. */
  private static boolean enabled() {
    return CalciteSystemProperty.TEST_DRUID.value();
  }

  @BeforeAll
  public static void assumeDruidTestsEnabled() {
    assumeTrue(enabled(), "Druid tests disabled. Add -Dcalcite.test.druid to enable it");
  }

  /** Creates a query against FOODMART with approximate parameters. */
  private CalciteAssert.AssertQuery foodmartApprox(String sql) {
    return CalciteAssert.that()
        .enable(enabled())
        .withModel(FOODMART)
        .with(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT.camelName(), true)
        .with(CalciteConnectionProperty.APPROXIMATE_TOP_N.camelName(), true)
        .with(CalciteConnectionProperty.APPROXIMATE_DECIMAL.camelName(), true)
        .query(sql);
  }

  /** Creates a query against the {@link #FOODMART} data set. */
  private CalciteAssert.AssertQuery sql(String sql) {
    return CalciteAssert.that()
        .enable(enabled())
        .withModel(FOODMART)
        .query(sql);
  }

  @Test void testMetadataColumns() {
    sql("values 1")
        .withConnection(c -> {
          try {
            final DatabaseMetaData metaData = c.getMetaData();
            final ResultSet r =
                metaData.getColumns(null, null, "foodmart", null);
            Multimap<String, Boolean> map = ArrayListMultimap.create();
            while (r.next()) {
              map.put(r.getString("TYPE_NAME"), true);
            }
            if (CalciteSystemProperty.DEBUG.value()) {
              System.out.println(map);
            }
            // 1 timestamp, 2 float measure, 1 int measure, 88 dimensions
            assertThat(map.keySet().size(), is(4));
            assertThat(map.values().size(), is(92));
            assertThat(map.get("TIMESTAMP(0) NOT NULL").size(), is(1));
            assertThat(map.get("DOUBLE").size(), is(2));
            assertThat(map.get("BIGINT").size(), is(1));
            assertThat(map.get(VARCHAR_TYPE).size(), is(88));
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test void testSelectDistinct() {
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$30]], groups=[{0}], aggs=[[]])";
    final String sql = "select distinct \"state_province\" from \"foodmart\"";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
        + "'dimensions':[{'type':'default','dimension':'state_province','outputName':'state_province'"
        + ",'outputType':'STRING'}],'limitSpec':{'type':'default'},"
        + "'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql)
        .returnsUnordered("state_province=CA",
            "state_province=OR",
            "state_province=WA")
        .explainContains(explain)
        .queryContains(new DruidChecker(druidQuery));
  }

  @Test void testSelectGroupBySum() {
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "projects=[[$30, CAST($89):INTEGER]], groups=[{0}], aggs=[[SUM($1)]])";
    final String sql = "select \"state_province\", sum(cast(\"unit_sales\" as integer)) as u\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\"";
    sql(sql)
        .returnsUnordered("state_province=CA; U=74748",
            "state_province=OR; U=67659",
            "state_province=WA; U=124366")
        .explainContains(explain);
  }

  @Test void testGroupbyMetric() {
    final String sql = "select  \"store_sales\" ,\"product_id\" from \"foodmart\" "
        + "where \"product_id\" = 1020" + "group by \"store_sales\" ,\"product_id\" ";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[=(CAST($1):INTEGER, 1020)],"
        + " projects=[[$90, $1]], groups=[{0, 1}], aggs=[[]])";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
        + "'dimensions':[{'type':'default','dimension':'store_sales',\"outputName\":\"store_sales\","
        + "'outputType':'DOUBLE'},{'type':'default','dimension':'product_id','outputName':"
        + "'product_id','outputType':'STRING'}],'limitSpec':{'type':'default'},"
        + "'filter':{'type':'bound','dimension':'product_id','lower':'1020','lowerStrict':false,"
        + "'upper':'1020','upperStrict':false,'ordering':'numeric'},'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql)
        .explainContains(plan)
        .queryContains(new DruidChecker(druidQuery))
        .returnsUnordered("store_sales=0.51; product_id=1020",
            "store_sales=1.02; product_id=1020",
            "store_sales=1.53; product_id=1020",
            "store_sales=2.04; product_id=1020",
            "store_sales=2.55; product_id=1020");
  }

  @Test void testPushSimpleGroupBy() {
    final String sql = "select \"product_id\" from \"foodmart\" where "
        + "\"product_id\" = 1020 group by \"product_id\"";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default',"
        + "'dimension':'product_id','outputName':'product_id','outputType':'STRING'}],"
        + "'limitSpec':{'type':'default'},'filter':{'type':'bound','dimension':'product_id',"
        + "'lower':'1020','lowerStrict':false,'upper':'1020','upperStrict':false,"
        + "'ordering':'numeric'},'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql).returnsUnordered("product_id=1020").queryContains(new DruidChecker(druidQuery));
  }

  @Test void testComplexPushGroupBy() {
    final String innerQuery = "select \"product_id\" as \"id\" from \"foodmart\" where "
        + "\"product_id\" = 1020";
    final String sql = "select \"id\" from (" + innerQuery + ") group by \"id\"";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all',"
        + "'dimensions':[{'type':'default','dimension':'product_id','outputName':'product_id',"
        + "'outputType':'STRING'}],'limitSpec':{'type':'default'},"
        + "'filter':{'type':'bound','dimension':'product_id','lower':'1020','lowerStrict':false,"
        + "'upper':'1020','upperStrict':false,'ordering':'numeric'},'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql)
        .returnsUnordered("id=1020")
        .queryContains(new DruidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1281">[CALCITE-1281]
   * Druid adapter wrongly returns all numeric values as int or float</a>. */
  @Test void testSelectCount() {
    final String sql = "select count(*) as c from \"foodmart\"";
    sql(sql)
        .returns(input -> {
          try {
            assertThat(input.next(), is(true));
            assertThat(input.getInt(1), is(86829));
            assertThat(input.getLong(1), is(86829L));
            assertThat(input.getString(1), is("86829"));
            assertThat(input.wasNull(), is(false));
            assertThat(input.next(), is(false));
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test void testSort() {
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$39, $30]], "
        + "groups=[{0, 1}], aggs=[[]], sort0=[1], sort1=[0], dir0=[ASC], dir1=[DESC])";
    final String sql = "select distinct \"gender\", \"state_province\"\n"
        + "from \"foodmart\" order by 2, 1 desc";
    sql(sql)
        .returnsOrdered("gender=M; state_province=CA",
            "gender=F; state_province=CA",
            "gender=M; state_province=OR",
            "gender=F; state_province=OR",
            "gender=M; state_province=WA",
            "gender=F; state_province=WA")
        .queryContains(
            new DruidChecker("{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
                + "'dimensions':[{'type':'default','dimension':'gender','outputName':'gender',"
                + "'outputType':'STRING'},{'type':'default','dimension':'state_province',"
                + "'outputName':'state_province','outputType':'STRING'}],'limitSpec':"
                + "{'type':'default','columns':[{'dimension':'state_province','direction':'ascending'"
                + ",'dimensionOrder':'lexicographic'},{'dimension':'gender','direction':'descending',"
                + "'dimensionOrder':'lexicographic'}]},'aggregations':[],"
                + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}"))
        .explainContains(explain);
  }

  @Test void testSortLimit() {
    final String explain = "PLAN=EnumerableLimit(offset=[2], fetch=[3])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$39, $30]], "
        + "groups=[{0, 1}], aggs=[[]], sort0=[1], sort1=[0], dir0=[ASC], dir1=[DESC])";
    final String sql = "select distinct \"gender\", \"state_province\"\n"
        + "from \"foodmart\"\n"
        + "order by 2, 1 desc offset 2 rows fetch next 3 rows only";
    sql(sql)
        .returnsOrdered("gender=M; state_province=OR",
            "gender=F; state_province=OR",
            "gender=M; state_province=WA")
        .explainContains(explain);
  }

  @Test void testOffsetLimit() {
    // We do not yet push LIMIT into a Druid "select" query as a "threshold".
    // It is not possible to push OFFSET into Druid "select" query.
    final String sql = "select \"state_province\", \"product_name\"\n"
        + "from \"foodmart\"\n"
        + "offset 2 fetch next 3 rows only";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'columns':['state_province','product_name'],"
        + "'resultFormat':'compactedList'}";
    sql(sql)
        .runs()
        .queryContains(new DruidChecker(druidQuery));
  }

  @Test void testLimit() {
    final String sql = "select \"gender\", \"state_province\"\n"
        + "from \"foodmart\" fetch next 3 rows only";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'columns':['gender','state_province'],"
        + "'resultFormat':'compactedList','limit':3";
    sql(sql)
        .runs()
        .queryContains(new DruidChecker(druidQuery));
  }

  @Test void testDistinctLimit() {
    final String sql = "select distinct \"gender\", \"state_province\"\n"
        + "from \"foodmart\" fetch next 3 rows only";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default','dimension':'gender',"
        + "'outputName':'gender','outputType':'STRING'},"
        + "{'type':'default','dimension':'state_province','outputName':'state_province',"
        + "'outputType':'STRING'}],'limitSpec':{'type':'default',"
        + "'limit':3,'columns':[]},"
        + "'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$39, $30]], "
        + "groups=[{0, 1}], aggs=[[]], fetch=[3])";
    sql(sql)
        .runs()
        .explainContains(explain)
        .queryContains(new DruidChecker(druidQuery))
        .returnsUnordered("gender=F; state_province=CA", "gender=F; state_province=OR",
            "gender=F; state_province=WA");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1578">[CALCITE-1578]
   * Druid adapter: wrong semantics of topN query limit with granularity</a>. */
  @Test void testGroupBySortLimit() {
    final String sql = "select \"brand_name\", \"gender\", sum(\"unit_sales\") as s\n"
        + "from \"foodmart\"\n"
        + "group by \"brand_name\", \"gender\"\n"
        + "order by s desc limit 3";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default',"
        + "'dimension':'brand_name','outputName':'brand_name','outputType':'STRING'},"
        + "{'type':'default','dimension':'gender','outputName':'gender','outputType':'STRING'}],"
        + "'limitSpec':{'type':'default','limit':3,'columns':[{'dimension':'S',"
        + "'direction':'descending','dimensionOrder':'numeric'}]},"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$2, $39, $89]], groups=[{0, 1}], "
        + "aggs=[[SUM($2)]], sort0=[2], dir0=[DESC], fetch=[3])";
    sql(sql)
        .runs()
        .returnsOrdered("brand_name=Hermanos; gender=M; S=4286",
            "brand_name=Hermanos; gender=F; S=4183",
            "brand_name=Tell Tale; gender=F; S=4033")
        .explainContains(explain)
        .queryContains(new DruidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1587">[CALCITE-1587]
   * Druid adapter: topN returns approximate results</a>. */
  @Test void testGroupBySingleSortLimit() {
    checkGroupBySingleSortLimit(false);
  }

  /** As {@link #testGroupBySingleSortLimit}, but allowing approximate results
   * due to {@link CalciteConnectionConfig#approximateDistinctCount()}.
   * Therefore we send a "topN" query to Druid. */
  @Test void testGroupBySingleSortLimitApprox() {
    checkGroupBySingleSortLimit(true);
  }

  private void checkGroupBySingleSortLimit(boolean approx) {
    final String sql = "select \"brand_name\", sum(\"unit_sales\") as s\n"
        + "from \"foodmart\"\n"
        + "group by \"brand_name\"\n"
        + "order by s desc limit 3";
    final String approxDruid = "{'queryType':'topN','dataSource':'foodmart','granularity':'all',"
        + "'dimension':{'type':'default','dimension':'brand_name','outputName':'brand_name','outputType':'STRING'},'metric':'S',"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'threshold':3}";
    final String exactDruid = "{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
        + "'dimensions':[{'type':'default','dimension':'brand_name','outputName':'brand_name',"
        + "'outputType':'STRING'}],'limitSpec':{'type':'default','limit':3,'columns':"
        + "[{'dimension':'S','direction':'descending','dimensionOrder':'numeric'}]},'aggregations':"
        + "[{'type':'longSum','name':'S','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String druidQuery = approx ? approxDruid : exactDruid;
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$2, $89]], groups=[{0}], "
        + "aggs=[[SUM($1)]], sort0=[1], dir0=[DESC], fetch=[3])";
    CalciteAssert.that()
        .enable(enabled())
        .withModel(FOODMART)
        .with(CalciteConnectionProperty.APPROXIMATE_TOP_N.name(), approx)
        .query(sql)
        .runs()
        .returnsOrdered("brand_name=Hermanos; S=8469",
            "brand_name=Tell Tale; S=7877",
            "brand_name=Ebony; S=7438")
        .explainContains(explain)
        .queryContains(new DruidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1578">[CALCITE-1578]
   * Druid adapter: wrong semantics of groupBy query limit with granularity</a>.
   *
   * <p>Before CALCITE-1578 was fixed, this would use a "topN" query but return
   * the wrong results. */
  @Test void testGroupByDaySortDescLimit() {
    final String sql = "select \"brand_name\","
        + " floor(\"timestamp\" to DAY) as d,"
        + " sum(\"unit_sales\") as s\n"
        + "from \"foodmart\"\n"
        + "group by \"brand_name\", floor(\"timestamp\" to DAY)\n"
        + "order by s desc limit 30";
    final String explain =
        "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$2, FLOOR($0, FLAG(DAY)), $89]], "
            + "groups=[{0, 1}], aggs=[[SUM($2)]], sort0=[2], dir0=[DESC], fetch=[30])";
    sql(sql)
        .runs()
        .returnsStartingWith("brand_name=Ebony; D=1997-07-27 00:00:00; S=135",
            "brand_name=Tri-State; D=1997-05-09 00:00:00; S=120",
            "brand_name=Hermanos; D=1997-05-09 00:00:00; S=115")
        .explainContains(explain)
        .queryContains(
            new DruidChecker("'queryType':'groupBy'", "'granularity':'all'", "'limitSpec"
                + "':{'type':'default','limit':30,'columns':[{'dimension':'S',"
                + "'direction':'descending','dimensionOrder':'numeric'}]}"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1579">[CALCITE-1579]
   * Druid adapter: wrong semantics of groupBy query limit with
   * granularity</a>.
   *
   * <p>Before CALCITE-1579 was fixed, this would use a "groupBy" query but
   * wrongly try to use a {@code limitSpec} to sort and filter. (A "topN" query
   * was not possible because the sort was {@code ASC}.) */
  @Test void testGroupByDaySortLimit() {
    final String sql = "select \"brand_name\","
        + " floor(\"timestamp\" to DAY) as d,"
        + " sum(\"unit_sales\") as s\n"
        + "from \"foodmart\"\n"
        + "group by \"brand_name\", floor(\"timestamp\" to DAY)\n"
        + "order by s desc limit 30";
    final String druidQueryPart1 = "{'queryType':'groupBy','dataSource':'foodmart'";
    final String druidQueryPart2 = "'limitSpec':{'type':'default','limit':30,"
        + "'columns':[{'dimension':'S','direction':'descending',"
        + "'dimensionOrder':'numeric'}]},'aggregations':[{'type':'longSum',"
        + "'name':'S','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$2, FLOOR($0, FLAG(DAY)), $89]], groups=[{0, 1}], "
        + "aggs=[[SUM($2)]], sort0=[2], dir0=[DESC], fetch=[30])";
    sql(sql)
        .runs()
        .returnsStartingWith("brand_name=Ebony; D=1997-07-27 00:00:00; S=135",
            "brand_name=Tri-State; D=1997-05-09 00:00:00; S=120",
            "brand_name=Hermanos; D=1997-05-09 00:00:00; S=115")
        .explainContains(explain)
        .queryContains(new DruidChecker(druidQueryPart1, druidQueryPart2));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1580">[CALCITE-1580]
   * Druid adapter: Wrong semantics for ordering within groupBy queries</a>. */
  @Test void testGroupByDaySortDimension() {
    final String sql =
        "select \"brand_name\", floor(\"timestamp\" to DAY) as d,"
            + " sum(\"unit_sales\") as s\n"
            + "from \"foodmart\"\n"
            + "group by \"brand_name\", floor(\"timestamp\" to DAY)\n"
            + "order by \"brand_name\"";
    final String subDruidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default',"
        + "'dimension':'brand_name','outputName':'brand_name','outputType':'STRING'},"
        + "{'type':'extraction','dimension':'__time',"
        + "'outputName':'floor_day','extractionFn':{'type':'timeFormat'";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$2, FLOOR($0, FLAG(DAY)), $89]], groups=[{0, 1}],"
        + " aggs=[[SUM($2)]], sort0=[0], dir0=[ASC])";
    sql(sql)
        .runs()
        .returnsStartingWith("brand_name=ADJ; D=1997-01-11 00:00:00; S=2",
            "brand_name=ADJ; D=1997-01-12 00:00:00; S=3",
            "brand_name=ADJ; D=1997-01-17 00:00:00; S=3")
        .explainContains(explain)
        .queryContains(new DruidChecker(subDruidQuery));
  }

  /** Tests a query that contains no GROUP BY and is therefore executed as a
   * Druid "select" query. */
  @Test void testFilterSortDesc() {
    final String sql = "select \"product_name\" from \"foodmart\"\n"
        + "where \"product_id\" BETWEEN '1500' AND '1502'\n"
        + "order by \"state_province\" desc, \"product_id\"";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'filter':{'type':'and','fields':["
        + "{'type':'bound','dimension':'product_id','lower':'1500','lowerStrict':false,'ordering':'lexicographic'},"
        + "{'type':'bound','dimension':'product_id','upper':'1502','upperStrict':false,'ordering':'lexicographic'}]},"
        + "'columns':['product_name','state_province','product_id'],"
        + "'resultFormat':'compactedList'";
    sql(sql)
        .limit(4)
        .returns(resultSet -> {
          try {
            for (int i = 0; i < 4; i++) {
              assertTrue(resultSet.next());
              assertThat(resultSet.getString("product_name"),
                  is("Fort West Dried Apricots"));
            }
            assertFalse(resultSet.next());
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        })
        .queryContains(new DruidChecker(druidQuery));
  }

  /** As {@link #testFilterSortDesc()} but the bounds are numeric. */
  @Test void testFilterSortDescNumeric() {
    final String sql = "select \"product_name\" from \"foodmart\"\n"
        + "where \"product_id\" BETWEEN 1500 AND 1502\n"
        + "order by \"state_province\" desc, \"product_id\"";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'filter':{'type':'and','fields':["
        + "{'type':'bound','dimension':'product_id','lower':'1500','lowerStrict':false,'ordering':'numeric'},"
        + "{'type':'bound','dimension':'product_id','upper':'1502','upperStrict':false,'ordering':'numeric'}]},"
        + "'columns':['product_name','state_province','product_id'],"
        + "'resultFormat':'compactedList'";
    sql(sql)
        .limit(4)
        .returns(resultSet -> {
          try {
            for (int i = 0; i < 4; i++) {
              assertTrue(resultSet.next());
              assertThat(resultSet.getString("product_name"),
                  is("Fort West Dried Apricots"));
            }
            assertFalse(resultSet.next());
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        })
        .queryContains(new DruidChecker(druidQuery));
  }

  /** Tests a query whose filter removes all rows. */
  @Test void testFilterOutEverything() {
    final String sql = "select \"product_name\" from \"foodmart\"\n"
        + "where \"product_id\" = -1";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'filter':{'type':'bound','dimension':'product_id','lower':'-1','lowerStrict':false,"
        + "'upper':'-1','upperStrict':false,'ordering':'numeric'},"
        + "'columns':['product_name'],"
        + "'resultFormat':'compactedList'}";
    sql(sql)
        .limit(4)
        .returnsUnordered()
        .queryContains(new DruidChecker(druidQuery));
  }

  /** As {@link #testFilterSortDescNumeric()} but with a filter that cannot
   * be pushed down to Druid. */
  @Test void testNonPushableFilterSortDesc() {
    final String sql = "select \"product_name\" from \"foodmart\"\n"
        + "where cast(\"product_id\" as integer) - 1500 BETWEEN 0 AND 2\n"
        + "order by \"state_province\" desc, \"product_id\"";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],";
    final String druidFilter = "\"filter\":{\"type\":\"and\","
        + "\"fields\":[{\"type\":\"expression\",\"expression\":\"((CAST(\\\"product_id\\\"";
    final String druidQuery2 = "'columns':['product_name','state_province','product_id'],"
        + "'resultFormat':'compactedList'}";

    sql(sql)
        .limit(4)
        .returns(resultSet -> {
          try {
            for (int i = 0; i < 4; i++) {
              assertTrue(resultSet.next());
              assertThat(resultSet.getString("product_name"),
                  is("Fort West Dried Apricots"));
            }
            assertFalse(resultSet.next());
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        })
        .queryContains(new DruidChecker(druidQuery, druidFilter, druidQuery2));
  }

  @Test void testUnionPlan() {
    final String sql = "select distinct \"gender\" from \"foodmart\"\n"
        + "union all\n"
        + "select distinct \"marital_status\" from \"foodmart\"";
    final String explain = "PLAN="
        + "EnumerableUnion(all=[true])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$39]], groups=[{0}], aggs=[[]])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$37]], groups=[{0}], aggs=[[]])";
    sql(sql)
        .explainContains(explain)
        .returnsUnordered("gender=F",
            "gender=M",
            "gender=M",
            "gender=S");
  }

  @Test void testFilterUnionPlan() {
    final String sql = "select * from (\n"
        + "  select distinct \"gender\" from \"foodmart\"\n"
        + "  union all\n"
        + "  select distinct \"marital_status\" from \"foodmart\")\n"
        + "where \"gender\" = 'M'";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableFilter(condition=[=($0, 'M')])\n"
        + "    BindableUnion(all=[true])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$39]], groups=[{0}], aggs=[[]])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$37]], groups=[{0}], aggs=[[]])";
    sql(sql)
        .explainContains(explain)
        .returnsUnordered("gender=M",
            "gender=M");
  }

  @Test void testCountGroupByEmpty() {
    final String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart',"
        + "'descending':false,'granularity':'all',"
        + "'aggregations':[{'type':'count','name':'EXPR$0'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':false}}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{}], aggs=[[COUNT()]])";
    final String sql = "select count(*) from \"foodmart\"";
    sql(sql)
        .returns("EXPR$0=86829\n")
        .queryContains(new DruidChecker(druidQuery))
        .explainContains(explain);
  }

  @Test void testGroupByOneColumnNotProjected() {
    final String sql = "select count(*) as c from \"foodmart\"\n"
        + "group by \"state_province\" order by 1";
    sql(sql)
        .returnsOrdered("C=21610",
            "C=24441",
            "C=40778");
  }

  /** Unlike {@link #testGroupByTimeAndOneColumnNotProjected()}, we cannot use
   * "topN" because we have a global limit, and that requires
   * {@code granularity: all}. */
  @Test void testGroupByTimeAndOneColumnNotProjectedWithLimit() {
    final String sql = "select count(*) as \"c\","
        + " floor(\"timestamp\" to MONTH) as \"month\"\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH), \"state_province\"\n"
        + "order by \"c\" desc limit 3";
    sql(sql)
        .returnsOrdered("c=4070; month=1997-12-01 00:00:00",
            "c=4033; month=1997-11-01 00:00:00",
            "c=3511; month=1997-07-01 00:00:00")
        .queryContains(new DruidChecker("'queryType':'groupBy'"));
  }

  @Test void testGroupByTimeAndOneMetricNotProjected() {
    final String sql =
        "select count(*) as \"c\", floor(\"timestamp\" to MONTH) as \"month\", floor"
            + "(\"store_sales\") as sales\n"
            + "from \"foodmart\"\n"
            + "group by floor(\"timestamp\" to MONTH), \"state_province\", floor"
            + "(\"store_sales\")\n"
            + "order by \"c\" desc limit 3";
    sql(sql).returnsOrdered("c=494; month=1997-11-01 00:00:00; SALES=5.0",
        "c=475; month=1997-12-01 00:00:00; SALES=5.0",
        "c=468; month=1997-03-01 00:00:00; SALES=5.0").queryContains(new DruidChecker("'queryType':'groupBy'"));
  }

  @Test void testGroupByTimeAndOneColumnNotProjected() {
    final String sql = "select count(*) as \"c\",\n"
        + "  floor(\"timestamp\" to MONTH) as \"month\"\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH), \"state_province\"\n"
        + "having count(*) > 3500";
    sql(sql)
        .returnsUnordered("c=3511; month=1997-07-01 00:00:00",
            "c=4033; month=1997-11-01 00:00:00",
            "c=4070; month=1997-12-01 00:00:00")
        .queryContains(new DruidChecker("'queryType':'groupBy'"));
  }

  @Test void testOrderByOneColumnNotProjected() {
    // Result including state: CA=24441, OR=21610, WA=40778
    final String sql = "select count(*) as c from \"foodmart\"\n"
        + "group by \"state_province\" order by \"state_province\"";
    sql(sql)
        .returnsOrdered("C=24441",
            "C=21610",
            "C=40778");
  }

  @Test void testGroupByOneColumn() {
    final String sql = "select \"state_province\", count(*) as c\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\"\n"
        + "order by \"state_province\"";
    String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$30]], groups=[{0}], "
        + "aggs=[[COUNT()]], sort0=[0], dir0=[ASC])";
    sql(sql)
        .limit(2)
        .returnsOrdered("state_province=CA; C=24441",
            "state_province=OR; C=21610")
        .explainContains(explain);
  }

  @Test void testGroupByOneColumnReversed() {
    final String sql = "select count(*) as c, \"state_province\"\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\"\n"
        + "order by \"state_province\"";
    sql(sql)
        .limit(2)
        .returnsOrdered("C=24441; state_province=CA",
            "C=21610; state_province=OR");
  }

  @Test void testGroupByAvgSumCount() {
    final String sql = "select \"state_province\",\n"
        + " avg(\"unit_sales\") as a,\n"
        + " sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c,\n"
        + " count(*) as c0\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\"\n"
        + "order by 1";
    sql(sql)
        .limit(2)
        .returnsUnordered("state_province=CA; A=3; S=74748; C=16347; C0=24441",
            "state_province=OR; A=3; S=67659; C=21610; C0=21610")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableProject(state_province=[$0], A=[/(CASE(=($2, 0), null:BIGINT, $1), $2)], "
            + "S=[CASE(=($2, 0), null:BIGINT, $1)], C=[$3], C0=[$4])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$30, $89, $71]], groups=[{0}], "
            + "aggs=[[$SUM0($1), COUNT($1), COUNT($2), COUNT()]], sort0=[0], dir0=[ASC])")
        .queryContains(
            new DruidChecker("{'queryType':'groupBy','dataSource':'foodmart','granularity':'all'"
                + ",'dimensions':[{'type':'default','dimension':'state_province','outputName':'state_province'"
                + ",'outputType':'STRING'}],'limitSpec':"
                + "{'type':'default','columns':[{'dimension':'state_province',"
                + "'direction':'ascending','dimensionOrder':'lexicographic'}]},'aggregations':"
                + "[{'type':'longSum','name':'$f1','fieldName':'unit_sales'},{'type':'filtered',"
                + "'filter':{'type':'not','field':{'type':'selector','dimension':'unit_sales',"
                + "'value':null}},'aggregator':{'type':'count','name':'$f2','fieldName':'unit_sales'}}"
                + ",{'type':'filtered','filter':{'type':'not','field':{'type':'selector',"
                + "'dimension':'store_sqft','value':null}},'aggregator':{'type':'count','name':'C',"
                + "'fieldName':'store_sqft'}},{'type':'count','name':'C0'}],"
                + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}"));
  }

  @Test void testGroupByMonthGranularity() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH) order by s";
    String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart'";
    sql(sql)
        .limit(3)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableProject(S=[$1], C=[$2])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[FLOOR($0, FLAG(MONTH)), $89, $71]], "
            + "groups=[{0}], aggs=[[SUM($1), COUNT($2)]], sort0=[1], dir0=[ASC])")
        .returnsOrdered("S=19958; C=5606", "S=20179; C=5523", "S=20388; C=5591")
        .queryContains(new DruidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1577">[CALCITE-1577]
   * Druid adapter: Incorrect result - limit on timestamp disappears</a>. */
  @Test void testGroupByMonthGranularitySort() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH)\n"
        + "order by floor(\"timestamp\" to MONTH) ASC";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableProject(S=[$1], C=[$2], EXPR$2=[$0])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[FLOOR($0, "
        + "FLAG(MONTH)), $89, $71]], groups=[{0}], aggs=[[SUM($1), COUNT($2)]], sort0=[0], "
        + "dir0=[ASC])";
    sql(sql)
        .explainContains(explain)
        .returnsOrdered("S=21628; C=5957",
            "S=20957; C=5842",
            "S=23706; C=6528",
            "S=20179; C=5523",
            "S=21081; C=5793",
            "S=21350; C=5863",
            "S=23763; C=6762",
            "S=21697; C=5915",
            "S=20388; C=5591",
            "S=19958; C=5606",
            "S=25270; C=7026",
            "S=26796; C=7338");
  }

  @Test void testGroupByMonthGranularitySortLimit() {
    final String sql = "select floor(\"timestamp\" to MONTH) as m,\n"
        + " sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH)\n"
        + "order by floor(\"timestamp\" to MONTH) limit 3";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[FLOOR($0, FLAG(MONTH)), $89, $71]], groups=[{0}], "
        + "aggs=[[SUM($1), COUNT($2)]], sort0=[0], dir0=[ASC], fetch=[3])";
    sql(sql)
        .returnsOrdered("M=1997-01-01 00:00:00; S=21628; C=5957",
            "M=1997-02-01 00:00:00; S=20957; C=5842",
            "M=1997-03-01 00:00:00; S=23706; C=6528")
        .explainContains(explain);
  }

  @Test void testGroupByDayGranularity() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to DAY) order by c desc";
    String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart'";
    sql(sql)
        .limit(3)
        .queryContains(new DruidChecker(druidQuery))
        .returnsOrdered("S=3850; C=1230", "S=3342; C=1071", "S=3219; C=1024");
  }

  @Test void testGroupByMonthGranularityFiltered() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "where \"timestamp\" >= '1996-01-01 00:00:00' and "
        + " \"timestamp\" < '1998-01-01 00:00:00'\n"
        + "group by floor(\"timestamp\" to MONTH) order by s asc";
    String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart'";

    sql(sql)
        .limit(3)
        .returnsOrdered("S=19958; C=5606", "S=20179; C=5523", "S=20388; C=5591")
        .queryContains(new DruidChecker(druidQuery));
  }

  @Test void testTopNMonthGranularity() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + "max(\"unit_sales\") as m,\n"
        + "\"state_province\" as p\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\", floor(\"timestamp\" to MONTH)\n"
        + "order by s desc limit 3";
    // Cannot use a Druid "topN" query, granularity != "all";
    // have to use "groupBy" query followed by external Sort and fetch.
    final String explain = "PLAN="
        + "EnumerableCalc(expr#0..3=[{inputs}], S=[$t2], M=[$t3], P=[$t0])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$30, FLOOR"
        + "($0, FLAG(MONTH)), $89]], groups=[{0, 1}], aggs=[[SUM($2), MAX($2)]], sort0=[2], "
        + "dir0=[DESC], fetch=[3])";
    final String druidQueryPart1 = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default',"
        + "'dimension':'state_province',\"outputName\":\"state_province\",\"outputType\":\"STRING\"},"
        + "{'type':'extraction','dimension':'__time',"
        + "'outputName':'floor_month','extractionFn':{'type':'timeFormat','format'";
    final String druidQueryPart2 = "'limitSpec':{'type':'default','limit':3,"
        + "'columns':[{'dimension':'S','direction':'descending',"
        + "'dimensionOrder':'numeric'}]},'aggregations':[{'type':'longSum',"
        + "'name':'S','fieldName':'unit_sales'},{'type':'longMax','name':'M',"
        + "'fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql)
        .returnsUnordered("S=12399; M=6; P=WA",
            "S=12297; M=7; P=WA",
            "S=10640; M=6; P=WA")
        .explainContains(explain)
        .queryContains(new DruidChecker(druidQueryPart1, druidQueryPart2));
  }

  @Test void testTopNDayGranularityFiltered() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + "max(\"unit_sales\") as m,\n"
        + "\"state_province\" as p\n"
        + "from \"foodmart\"\n"
        + "where \"timestamp\" >= '1997-01-01 00:00:00' and "
        + " \"timestamp\" < '1997-09-01 00:00:00'\n"
        + "group by \"state_province\", floor(\"timestamp\" to DAY)\n"
        + "order by s desc limit 6";
    final String explain = "PLAN=EnumerableCalc(expr#0..3=[{inputs}], S=[$t2], M=[$t3], P=[$t0])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1997-01-01T00:00:00.000Z/1997-09-01T00:00:00.000Z]], projects=[[$30, FLOOR"
        + "($0, FLAG(DAY)), $89]], groups=[{0, 1}], aggs=[[SUM($2), MAX($2)]], sort0=[2], "
        + "dir0=[DESC], fetch=[6])";
    final String druidQueryType = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions'";
    final String limitSpec = "'limitSpec':{'type':'default','limit':6,"
        + "'columns':[{'dimension':'S','direction':'descending','dimensionOrder':'numeric'}]}";
    sql(sql)
        .returnsOrdered("S=2527; M=5; P=OR",
            "S=2525; M=6; P=OR",
            "S=2238; M=6; P=OR",
            "S=1715; M=5; P=OR",
            "S=1691; M=5; P=OR",
            "S=1629; M=5; P=WA")
        .explainContains(explain)
        .queryContains(new DruidChecker(druidQueryType, limitSpec));
  }

  @Test void testGroupByHaving() {
    final String sql = "select \"state_province\" as s, count(*) as c\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\" having count(*) > 23000 order by 1";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$30]], groups=[{0}], aggs=[[COUNT()]], "
        + "filter=[>($1, 23000)], sort0=[0], dir0=[ASC])";
    sql(sql)
        .returnsOrdered("S=CA; C=24441",
            "S=WA; C=40778")
        .explainContains(explain);
  }

  @Test void testGroupComposite() {
    // Note: We don't push down SORT-LIMIT yet
    final String sql = "select count(*) as c, \"state_province\", \"city\"\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\", \"city\"\n"
        + "order by c desc limit 2";
    final String explain = "PLAN=EnumerableCalc(expr#0..2=[{inputs}], C=[$t2], "
        + "state_province=[$t0], city=[$t1])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$30, $29]], groups=[{0, 1}], aggs=[[COUNT()]], sort0=[2], dir0=[DESC], fetch=[2])";
    sql(sql)
        .returnsOrdered("C=7394; state_province=WA; city=Spokane",
            "C=3958; state_province=WA; city=Olympia")
        .explainContains(explain);
  }

  /** Tests that distinct-count is pushed down to Druid and evaluated using
   * "cardinality". The result is approximate, but gives the correct result in
   * this example when rounded down using FLOOR. */
  @Test void testDistinctCount() {
    final String sql = "select \"state_province\",\n"
        + " floor(count(distinct \"city\")) as cdc\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\"\n"
        + "order by 2 desc limit 2";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$1], dir0=[DESC], fetch=[2])\n"
        + "    BindableProject(state_province=[$0], CDC=[FLOOR($1)])\n"
        + "      BindableAggregate(group=[{0}], agg#0=[COUNT($1)])\n"
        + "        DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$30, $29]], groups=[{0, 1}], aggs=[[]])";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':["
        + "{'type':'default','dimension':'state_province','outputName':'state_province','outputType':'STRING'},"
        + "{'type':'default','dimension':'city','outputName':'city','outputType':'STRING'}],"
        + "'limitSpec':{'type':'default'},'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql)
        .explainContains(explain)
        .queryContains(new DruidChecker(druidQuery))
        .returnsUnordered("state_province=CA; CDC=45",
            "state_province=WA; CDC=22");
  }

  /** Tests that projections of columns are pushed into the DruidQuery, and
   * projections of expressions that Druid cannot handle (in this case, a
   * literal 0) stay up. */
  @Test void testProject() {
    final String sql = "select \"product_name\", 0 as zero\n"
        + "from \"foodmart\"\n"
        + "order by \"product_name\"";
    final String explain = "PLAN="
        + "EnumerableSort(sort0=[$0], dir0=[ASC])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$3, 0]])";
    sql(sql)
        .limit(2)
        .returnsUnordered("product_name=ADJ Rosy Sunglasses; ZERO=0",
            "product_name=ADJ Rosy Sunglasses; ZERO=0")
        .explainContains(explain);
  }

  @Test void testFilterDistinct() {
    final String sql = "select distinct \"state_province\", \"city\",\n"
        + "  \"product_name\"\n"
        + "from \"foodmart\"\n"
        + "where \"product_name\" = 'High Top Dried Mushrooms'\n"
        + "and \"quarter\" in ('Q2', 'Q3')\n"
        + "and \"state_province\" = 'WA'";
    final String druidQuery1 = "{'queryType':'groupBy','dataSource':'foodmart','granularity':'all'";
    final String druidQuery2 = "'filter':{'type':'and','fields':[{'type':'selector','dimension':"
        + "'product_name','value':'High Top Dried Mushrooms'},{'type':'or','fields':[{'type':'selector',"
        + "'dimension':'quarter','value':'Q2'},{'type':'selector','dimension':'quarter',"
        + "'value':'Q3'}]},{'type':'selector','dimension':'state_province','value':'WA'}]},"
        + "'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[AND("
        + "=($3, 'High Top Dried Mushrooms'), "
        + "SEARCH($87, Sarg['Q2', 'Q3']:CHAR(2)), "
        + "=($30, 'WA'))], "
        + "projects=[[$30, $29, $3]], groups=[{0, 1, 2}], aggs=[[]])\n";
    sql(sql)
        .queryContains(new DruidChecker(druidQuery1, druidQuery2))
        .explainContains(explain)
        .returnsUnordered(
            "state_province=WA; city=Bremerton; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Everett; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Kirkland; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Lynnwood; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Olympia; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Port Orchard; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Puyallup; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Spokane; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Tacoma; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Yakima; product_name=High Top Dried Mushrooms");
  }

  @Test void testFilter() {
    final String sql = "select \"state_province\", \"city\",\n"
        + "  \"product_name\"\n"
        + "from \"foodmart\"\n"
        + "where \"product_name\" = 'High Top Dried Mushrooms'\n"
        + "and \"quarter\" in ('Q2', 'Q3')\n"
        + "and \"state_province\" = 'WA'";
    final String druidQuery = "{'queryType':'scan',"
        + "'dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'filter':{'type':'and','fields':["
        + "{'type':'selector','dimension':'product_name','value':'High Top Dried Mushrooms'},"
        + "{'type':'or','fields':["
        + "{'type':'selector','dimension':'quarter','value':'Q2'},"
        + "{'type':'selector','dimension':'quarter','value':'Q3'}]},"
        + "{'type':'selector','dimension':'state_province','value':'WA'}]},"
        + "'columns':['state_province','city','product_name'],"
        + "'resultFormat':'compactedList'}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[AND("
        + "=($3, 'High Top Dried Mushrooms'), "
        + "SEARCH($87, Sarg['Q2', 'Q3']:CHAR(2)), "
        + "=($30, 'WA'))], "
        + "projects=[[$30, $29, $3]])\n";
    sql(sql)
        .queryContains(new DruidChecker(druidQuery))
        .explainContains(explain)
        .returnsUnordered(
            "state_province=WA; city=Bremerton; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Everett; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Kirkland; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Lynnwood; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Olympia; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Port Orchard; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Puyallup; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Puyallup; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Spokane; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Spokane; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Spokane; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Tacoma; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Yakima; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Yakima; product_name=High Top Dried Mushrooms",
            "state_province=WA; city=Yakima; product_name=High Top Dried Mushrooms");
  }

  /** Tests that conditions applied to time units extracted via the EXTRACT
   * function become ranges on the timestamp column
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1334">[CALCITE-1334]
   * Convert predicates on EXTRACT function calls into date ranges</a>. */
  @Test void testFilterTimestamp() {
    String sql = "select count(*) as c\n"
        + "from \"foodmart\"\n"
        + "where extract(year from \"timestamp\") = 1997\n"
        + "and extract(month from \"timestamp\") in (4, 6)\n";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1997-04-01T00:00:00.000Z/"
        + "1997-05-01T00:00:00.000Z, 1997-06-01T00:00:00.000Z/1997-07-01T00:00:00.000Z]],"
        + " projects=[[0]], groups=[{}], aggs=[[COUNT()]])";
    CalciteAssert.AssertQuery q = sql(sql)
        .returnsUnordered("C=13500");
    Assumptions.assumeTrue(Bug.CALCITE_4213_FIXED, "CALCITE-4213");
    q.explainContains(explain);
  }

  @Test void testFilterSwapped() {
    String sql = "select \"state_province\"\n"
        + "from \"foodmart\"\n"
        + "where 'High Top Dried Mushrooms' = \"product_name\"";
    final String explain = "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[=('High Top Dried Mushrooms', $3)], projects=[[$30]])";
    final String druidQuery = "'filter':{'type':'selector','dimension':'product_name',"
        + "'value':'High Top Dried Mushrooms'}";
    sql(sql)
        .explainContains(explain)
        .queryContains(new DruidChecker(druidQuery));
  }

  @Test void testGroupByMetricAndExtractTime() {
    final String sql =
        "SELECT count(*), floor(\"timestamp\" to DAY), \"store_sales\" "
            + "FROM \"foodmart\"\n"
            + "GROUP BY \"store_sales\", floor(\"timestamp\" to DAY)\n ORDER BY \"store_sales\" DESC\n"
            + "LIMIT 10\n";
    sql(sql).queryContains(new DruidChecker("{\"queryType\":\"groupBy\""));
  }

  @Test void testFilterOnDouble() {
    String sql = "select \"product_id\" from \"foodmart\"\n"
        + "where cast(\"product_id\" as double) < 0.41024 and \"product_id\" < 12223";
    sql(sql).queryContains(
        new DruidChecker("'type':'bound','dimension':'product_id','upper':'0.41024'",
            "'upper':'12223'"));
  }

  @Test void testPushAggregateOnTime() {
    String sql = "select \"product_id\", \"timestamp\" as \"time\" "
        + "from \"foodmart\" "
        + "where \"product_id\" = 1016 "
        + "and \"timestamp\" < '1997-01-03 00:00:00' "
        + "and \"timestamp\" > '1990-01-01 00:00:00' "
        + "group by \"timestamp\", \"product_id\" ";
    String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'extraction',"
        + "'dimension':'__time','outputName':'extract',"
        + "'extractionFn':{'type':'timeFormat','format':'yyyy-MM-dd";
    sql(sql)
        .returnsUnordered("product_id=1016; time=1997-01-02 00:00:00")
        .queryContains(new DruidChecker(druidQuery));
  }

  @Test void testPushAggregateOnTimeWithExtractYear() {
    String sql = "select EXTRACT( year from \"timestamp\") as \"year\",\"product_id\" from "
        + "\"foodmart\" where \"product_id\" = 1016 and "
        + "\"timestamp\" < cast('1999-01-02' as timestamp) and \"timestamp\" > cast"
        + "('1997-01-01' as timestamp)" + " group by "
        + " EXTRACT( year from \"timestamp\"), \"product_id\" ";
    sql(sql)
        .queryContains(
            new DruidChecker(
                ",'granularity':'all'",
                "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_year',"
                    + "'extractionFn':{'type':'timeFormat','format':'yyyy',"
                    + "'timeZone':'UTC','locale':'en-US'}}"))
        .returnsUnordered("year=1997; product_id=1016");
  }

  @Test void testPushAggregateOnTimeWithExtractMonth() {
    String sql = "select EXTRACT( month from \"timestamp\") as \"month\",\"product_id\" from "
        + "\"foodmart\" where \"product_id\" = 1016 and "
        + "\"timestamp\" < cast('1997-06-02' as timestamp) and \"timestamp\" > cast"
        + "('1997-01-01' as timestamp)" + " group by "
        + " EXTRACT( month from \"timestamp\"), \"product_id\" ";
    sql(sql)
        .queryContains(
            new DruidChecker(
                ",'granularity':'all'",
                "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_month',"
                    + "'extractionFn':{'type':'timeFormat','format':'M',"
                    + "'timeZone':'UTC','locale':'en-US'}}"))
        .returnsUnordered("month=1; product_id=1016", "month=2; product_id=1016",
            "month=3; product_id=1016", "month=4; product_id=1016", "month=5; product_id=1016");
  }

  @Test void testPushAggregateOnTimeWithExtractDay() {
    String sql = "select EXTRACT( day from \"timestamp\") as \"day\","
        + "\"product_id\" from \"foodmart\""
        + " where \"product_id\" = 1016 and "
        + "\"timestamp\" < cast('1997-01-20' as timestamp) and \"timestamp\" > cast"
        + "('1997-01-01' as timestamp)" + " group by "
        + " EXTRACT( day from \"timestamp\"), \"product_id\" ";
    sql(sql)
        .queryContains(
            new DruidChecker(
                ",'granularity':'all'",
                "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_day',"
                    + "'extractionFn':{'type':'timeFormat','format':'d',"
                    + "'timeZone':'UTC','locale':'en-US'}}"))
        .returnsUnordered("day=2; product_id=1016", "day=10; product_id=1016",
            "day=13; product_id=1016", "day=16; product_id=1016");
  }

  @Test void testPushAggregateOnTimeWithExtractHourOfDay() {
    String sql =
        "select EXTRACT( hour from \"timestamp\") as \"hourOfDay\",\"product_id\"  from "
            + "\"foodmart\" where \"product_id\" = 1016 and "
            + "\"timestamp\" < cast('1997-06-02' as timestamp) and \"timestamp\" > cast"
            + "('1997-01-01' as timestamp)" + " group by "
            + " EXTRACT( hour from \"timestamp\"), \"product_id\" ";
    sql(sql)
        .queryContains(new DruidChecker("'queryType':'groupBy'"))
        .returnsUnordered("hourOfDay=0; product_id=1016");
  }

  @Test void testPushAggregateOnTimeWithExtractYearMonthDay() {
    String sql = "select EXTRACT( day from \"timestamp\") as \"day\", EXTRACT( month from "
        + "\"timestamp\") as \"month\",  EXTRACT( year from \"timestamp\") as \"year\",\""
        + "product_id\"  from \"foodmart\" where \"product_id\" = 1016 and "
        + "\"timestamp\" < cast('1997-01-20' as timestamp) and \"timestamp\" > cast"
        + "('1997-01-01' as timestamp)"
        + " group by "
        + " EXTRACT( day from \"timestamp\"), EXTRACT( month from \"timestamp\"),"
        + " EXTRACT( year from \"timestamp\"), \"product_id\" ";
    sql(sql)
        .queryContains(
            new DruidChecker(
                ",'granularity':'all'",
                "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_day',"
                    + "'extractionFn':{'type':'timeFormat','format':'d',"
                    + "'timeZone':'UTC','locale':'en-US'}}", "{'type':'extraction',"
                + "'dimension':'__time','outputName':'extract_month',"
                + "'extractionFn':{'type':'timeFormat','format':'M',"
                + "'timeZone':'UTC','locale':'en-US'}}", "{'type':'extraction',"
                + "'dimension':'__time','outputName':'extract_year',"
                + "'extractionFn':{'type':'timeFormat','format':'yyyy',"
                + "'timeZone':'UTC','locale':'en-US'}}"))
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1997-01-01T00:00:00.001Z/1997-01-20T00:00:00.000Z]], "
            + "filter=[=(CAST($1):INTEGER, 1016)], projects=[[EXTRACT(FLAG(DAY), $0), EXTRACT(FLAG(MONTH), $0), "
            + "EXTRACT(FLAG(YEAR), $0), $1]], groups=[{0, 1, 2, 3}], aggs=[[]])\n")
        .returnsUnordered("day=2; month=1; year=1997; product_id=1016",
            "day=10; month=1; year=1997; product_id=1016",
            "day=13; month=1; year=1997; product_id=1016",
            "day=16; month=1; year=1997; product_id=1016");
  }

  @Test void testPushAggregateOnTimeWithExtractYearMonthDayWithOutRenaming() {
    String sql = "select EXTRACT( day from \"timestamp\"), EXTRACT( month from "
        + "\"timestamp\"), EXTRACT( year from \"timestamp\"),\""
        + "product_id\"  from \"foodmart\" where \"product_id\" = 1016 and "
        + "\"timestamp\" < cast('1997-01-20' as timestamp) and \"timestamp\" > cast"
        + "('1997-01-01' as timestamp)"
        + " group by "
        + " EXTRACT( day from \"timestamp\"), EXTRACT( month from \"timestamp\"),"
        + " EXTRACT( year from \"timestamp\"), \"product_id\" ";
    sql(sql)
        .queryContains(
            new DruidChecker(
                ",'granularity':'all'",
                "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_day',"
                    + "'extractionFn':{'type':'timeFormat','format':'d',"
                    + "'timeZone':'UTC','locale':'en-US'}}",
                "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_month',"
                    + "'extractionFn':{'type':'timeFormat','format':'M',"
                    + "'timeZone':'UTC','locale':'en-US'}}",
                "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_year',"
                    + "'extractionFn':{'type':'timeFormat','format':'yyyy',"
                    + "'timeZone':'UTC','locale':'en-US'}}"))
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1997-01-01T00:00:00.001Z/1997-01-20T00:00:00.000Z]], "
            + "filter=[=(CAST($1):INTEGER, 1016)], projects=[[EXTRACT(FLAG(DAY), $0), EXTRACT(FLAG(MONTH), $0), "
            + "EXTRACT(FLAG(YEAR), $0), $1]], groups=[{0, 1, 2, 3}], aggs=[[]])\n")
        .returnsUnordered("EXPR$0=2; EXPR$1=1; EXPR$2=1997; product_id=1016",
            "EXPR$0=10; EXPR$1=1; EXPR$2=1997; product_id=1016",
            "EXPR$0=13; EXPR$1=1; EXPR$2=1997; product_id=1016",
            "EXPR$0=16; EXPR$1=1; EXPR$2=1997; product_id=1016");
  }

  @Test void testPushAggregateOnTimeWithExtractWithOutRenaming() {
    String sql = "select EXTRACT( day from \"timestamp\"), "
        + "\"product_id\" as \"dayOfMonth\" from \"foodmart\" "
        + "where \"product_id\" = 1016 and \"timestamp\" < cast('1997-01-20' as timestamp) "
        + "and \"timestamp\" > cast('1997-01-01' as timestamp)"
        + " group by "
        + " EXTRACT( day from \"timestamp\"), EXTRACT( day from \"timestamp\"),"
        + " \"product_id\" ";
    sql(sql)
        .queryContains(
            new DruidChecker(
                ",'granularity':'all'",
                "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_day',"
                    + "'extractionFn':{'type':'timeFormat','format':'d',"
                    + "'timeZone':'UTC','locale':'en-US'}}"))
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1997-01-01T00:00:00.001Z/1997-01-20T00:00:00.000Z]], "
            + "filter=[=(CAST($1):INTEGER, 1016)], projects=[[EXTRACT(FLAG(DAY), $0), $1]], "
            + "groups=[{0, 1}], aggs=[[]])\n")
        .returnsUnordered("EXPR$0=2; dayOfMonth=1016", "EXPR$0=10; dayOfMonth=1016",
            "EXPR$0=13; dayOfMonth=1016", "EXPR$0=16; dayOfMonth=1016");
  }

  @Test void testPushComplexFilter() {
    String sql = "select sum(\"store_sales\") from \"foodmart\" "
        + "where EXTRACT( year from \"timestamp\") = 1997 and "
        + "\"cases_per_pallet\" >= 8 and \"cases_per_pallet\" <= 10 and "
        + "\"units_per_case\" < 15 ";
    String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','filter':{'type':'and','fields':[{'type':'bound','dimension':"
        + "'cases_per_pallet','lower':'8','lowerStrict':false,'ordering':'numeric'},"
        + "{'type':'bound','dimension':'cases_per_pallet','upper':'10','upperStrict':false,"
        + "'ordering':'numeric'},{'type':'bound','dimension':'units_per_case','upper':'15',"
        + "'upperStrict':true,'ordering':'numeric'}]},'aggregations':[{'type':'doubleSum',"
        + "'name':'EXPR$0','fieldName':'store_sales'}],'intervals':['1997-01-01T00:00:00.000Z/"
        + "1998-01-01T00:00:00.000Z'],'context':{'skipEmptyBuckets':false}}";
    sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1997-01-01T00:00:00.000Z/1998-01-01T00:00:00.000Z]], filter=[AND(>=(CAST($11):INTEGER, 8), <=(CAST($11):INTEGER, 10), <(CAST($10):INTEGER, 15))], projects=[[$90]], groups=[{}], aggs=[[SUM($0)]])")
        .returnsUnordered("EXPR$0=75364.1")
        .queryContains(new DruidChecker(druidQuery));
  }

  @Test void testPushOfFilterExtractionOnDayAndMonth() {
    String sql = "SELECT \"product_id\" , EXTRACT(day from \"timestamp\"), EXTRACT(month from "
        + "\"timestamp\") from \"foodmart\" WHERE  EXTRACT(day from \"timestamp\") >= 30 AND "
        + "EXTRACT(month from \"timestamp\") = 11 "
        + "AND  \"product_id\" >= 1549 group by \"product_id\", EXTRACT(day from "
        + "\"timestamp\"), EXTRACT(month from \"timestamp\")";
    sql(sql)
        .returnsUnordered("product_id=1549; EXPR$1=30; EXPR$2=11",
            "product_id=1553; EXPR$1=30; EXPR$2=11");
  }

  @Test void testPushOfFilterExtractionOnDayAndMonthAndYear() {
    String sql = "SELECT \"product_id\" , EXTRACT(day from \"timestamp\"), EXTRACT(month from "
        + "\"timestamp\") , EXTRACT(year from \"timestamp\") from \"foodmart\" "
        + "WHERE  EXTRACT(day from \"timestamp\") >= 30 AND EXTRACT(month from \"timestamp\") = 11 "
        + "AND  \"product_id\" >= 1549 AND EXTRACT(year from \"timestamp\") = 1997"
        + "group by \"product_id\", EXTRACT(day from \"timestamp\"), "
        + "EXTRACT(month from \"timestamp\"), EXTRACT(year from \"timestamp\")";
    sql(sql)
        .returnsUnordered("product_id=1549; EXPR$1=30; EXPR$2=11; EXPR$3=1997",
            "product_id=1553; EXPR$1=30; EXPR$2=11; EXPR$3=1997")
        .queryContains(
            new DruidChecker("{'queryType':'groupBy','dataSource':'foodmart','granularity':'all'"));
  }

  @Test void testFilterExtractionOnMonthWithBetween() {
    String sqlQuery = "SELECT \"product_id\", EXTRACT(month from \"timestamp\") FROM \"foodmart\""
        + " WHERE EXTRACT(month from \"timestamp\") BETWEEN 10 AND 11 AND  \"product_id\" >= 1558"
        + " GROUP BY \"product_id\", EXTRACT(month from \"timestamp\")";
    String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart'";
    sql(sqlQuery)
        .returnsUnordered("product_id=1558; EXPR$1=10", "product_id=1558; EXPR$1=11",
            "product_id=1559; EXPR$1=11")
        .queryContains(new DruidChecker(druidQuery));
  }

  @Test void testFilterExtractionOnMonthWithIn() {
    String sqlQuery = "SELECT \"product_id\", EXTRACT(month from \"timestamp\") FROM \"foodmart\""
        + " WHERE EXTRACT(month from \"timestamp\") IN (10, 11) AND  \"product_id\" >= 1558"
        + " GROUP BY \"product_id\", EXTRACT(month from \"timestamp\")";
    sql(sqlQuery)
        .returnsUnordered("product_id=1558; EXPR$1=10", "product_id=1558; EXPR$1=11",
            "product_id=1559; EXPR$1=11")
        .queryContains(
            new DruidChecker("{'queryType':'groupBy',"
                + "'dataSource':'foodmart','granularity':'all',"
                + "'dimensions':[{'type':'default','dimension':'product_id','outputName':'product_id','outputType':'STRING'},"
                + "{'type':'extraction','dimension':'__time','outputName':'extract_month',"
                + "'extractionFn':{'type':'timeFormat','format':'M','timeZone':'UTC',"
                + "'locale':'en-US'}}],'limitSpec':{'type':'default'},"
                + "'filter':{'type':'and','fields':[{'type':'bound',"
                + "'dimension':'product_id','lower':'1558','lowerStrict':false,"
                + "'ordering':'numeric'},{'type':'or','fields':[{'type':'bound','dimension':'__time'"
                + ",'lower':'10','lowerStrict':false,'upper':'10','upperStrict':false,"
                + "'ordering':'numeric','extractionFn':{'type':'timeFormat',"
                + "'format':'M','timeZone':'UTC','locale':'en-US'}},{'type':'bound',"
                + "'dimension':'__time','lower':'11','lowerStrict':false,'upper':'11',"
                + "'upperStrict':false,'ordering':'numeric','extractionFn':{'type':'timeFormat',"
                + "'format':'M','timeZone':'UTC','locale':'en-US'}}]}]},"
                + "'aggregations':[],"
                + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}"));
  }

  @Test void testPushOfOrderByWithMonthExtract() {
    String sqlQuery = "SELECT  extract(month from \"timestamp\") as m , \"product_id\", SUM"
        + "(\"unit_sales\") as s FROM \"foodmart\""
        + " WHERE \"product_id\" >= 1558"
        + " GROUP BY extract(month from \"timestamp\"), \"product_id\" order by m, s, "
        + "\"product_id\"";
    sql(sqlQuery).queryContains(
        new DruidChecker("{'queryType':'groupBy','dataSource':'foodmart',"
            + "'granularity':'all','dimensions':[{'type':'extraction',"
            + "'dimension':'__time','outputName':'extract_month',"
            + "'extractionFn':{'type':'timeFormat','format':'M','timeZone':'UTC',"
            + "'locale':'en-US'}},{'type':'default','dimension':'product_id','outputName':"
            + "'product_id','outputType':'STRING'}],"
            + "'limitSpec':{'type':'default','columns':[{'dimension':'extract_month',"
            + "'direction':'ascending','dimensionOrder':'numeric'},{'dimension':'S',"
            + "'direction':'ascending','dimensionOrder':'numeric'},"
            + "{'dimension':'product_id','direction':'ascending',"
            + "'dimensionOrder':'lexicographic'}]},'filter':{'type':'bound',"
            + "'dimension':'product_id','lower':'1558','lowerStrict':false,"
            + "'ordering':'numeric'},'aggregations':[{'type':'longSum','name':'S',"
            + "'fieldName':'unit_sales'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}"))
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "filter=[>=(CAST($1):INTEGER, 1558)], projects=[[EXTRACT(FLAG(MONTH), $0), $1, $89]], "
            + "groups=[{0, 1}], aggs=[[SUM($2)]], sort0=[0], sort1=[2], sort2=[1], "
            + "dir0=[ASC], dir1=[ASC], dir2=[ASC])");
  }


  @Test void testGroupByFloorTimeWithoutLimit() {
    final String sql = "select floor(\"timestamp\" to MONTH) as \"month\"\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH)\n"
        + "order by \"month\" DESC";
    sql(sql)
        .queryContains(new DruidChecker("'queryType':'timeseries'", "'descending':true"))
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z"
            + "/2992-01-10T00:00:00.000Z]], projects=[[FLOOR($0, FLAG(MONTH))]], groups=[{0}], "
            + "aggs=[[]], sort0=[0], dir0=[DESC])");

  }

  @Test void testGroupByFloorTimeWithLimit() {
    final String sql =
        "select floor(\"timestamp\" to MONTH) as \"floorOfMonth\"\n"
            + "from \"foodmart\"\n"
            + "group by floor(\"timestamp\" to MONTH)\n"
            + "order by \"floorOfMonth\" DESC LIMIT 3";
    final String explain =
        "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[FLOOR($0, FLAG(MONTH))]], groups=[{0}], "
            + "aggs=[[]], sort0=[0], dir0=[DESC], fetch=[3])";
    sql(sql)
        .explainContains(explain)
        .returnsOrdered("floorOfMonth=1997-12-01 00:00:00", "floorOfMonth=1997-11-01 00:00:00",
            "floorOfMonth=1997-10-01 00:00:00")
        .queryContains(new DruidChecker("'queryType':'groupBy'", "'direction':'descending'"));
  }

  @Test void testPushofOrderByYearWithYearMonthExtract() {
    String sqlQuery = "SELECT year(\"timestamp\") as y, extract(month from \"timestamp\") as m , "
        + "\"product_id\", SUM"
        + "(\"unit_sales\") as s FROM \"foodmart\""
        + " WHERE \"product_id\" >= 1558"
        + " GROUP BY year(\"timestamp\"), extract(month from \"timestamp\"), \"product_id\" order"
        + " by y DESC, m ASC, s DESC, \"product_id\" LIMIT 3";
    final String expectedPlan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[>=(CAST($1):INTEGER, 1558)], projects=[[EXTRACT(FLAG(YEAR), $0), "
        + "EXTRACT(FLAG(MONTH), $0), $1, $89]], groups=[{0, 1, 2}], aggs=[[SUM($3)]], sort0=[0], "
        + "sort1=[1], sort2=[3], sort3=[2], dir0=[DESC], "
        + "dir1=[ASC], dir2=[DESC], dir3=[ASC], fetch=[3])";
    final String expectedDruidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'extraction',"
        + "'dimension':'__time','outputName':'extract_year',"
        + "'extractionFn':{'type':'timeFormat','format':'yyyy','timeZone':'UTC',"
        + "'locale':'en-US'}},{'type':'extraction','dimension':'__time',"
        + "'outputName':'extract_month','extractionFn':{'type':'timeFormat',"
        + "'format':'M','timeZone':'UTC','locale':'en-US'}},{'type':'default',"
        + "'dimension':'product_id','outputName':'product_id','outputType':'STRING'}],"
        + "'limitSpec':{'type':'default','limit':3,"
        + "'columns':[{'dimension':'extract_year','direction':'descending',"
        + "'dimensionOrder':'numeric'},{'dimension':'extract_month',"
        + "'direction':'ascending','dimensionOrder':'numeric'},{'dimension':'S',"
        + "'direction':'descending','dimensionOrder':'numeric'},"
        + "{'dimension':'product_id','direction':'ascending',"
        + "'dimensionOrder':'lexicographic'}]},'filter':{'type':'bound',"
        + "'dimension':'product_id','lower':'1558','lowerStrict':false,"
        + "'ordering':'numeric'},'aggregations':[{'type':'longSum','name':'S',"
        + "'fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sqlQuery).explainContains(expectedPlan).queryContains(new DruidChecker(expectedDruidQuery))
        .returnsOrdered("Y=1997; M=1; product_id=1558; S=6", "Y=1997; M=1; product_id=1559; S=6",
            "Y=1997; M=2; product_id=1558; S=24");
  }

  @Test void testPushofOrderByMetricWithYearMonthExtract() {
    String sqlQuery = "SELECT year(\"timestamp\") as y, extract(month from \"timestamp\") as m , "
        + "\"product_id\", SUM(\"unit_sales\") as s FROM \"foodmart\""
        + " WHERE \"product_id\" >= 1558"
        + " GROUP BY year(\"timestamp\"), extract(month from \"timestamp\"), \"product_id\" order"
        + " by s DESC, m DESC, \"product_id\" LIMIT 3";
    final String expectedPlan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[>=(CAST($1):INTEGER, 1558)], projects=[[EXTRACT(FLAG(YEAR), $0), "
        + "EXTRACT(FLAG(MONTH), $0), $1, $89]], groups=[{0, 1, 2}], aggs=[[SUM($3)]], "
        + "sort0=[3], sort1=[1], sort2=[2], dir0=[DESC], dir1=[DESC], dir2=[ASC], fetch=[3])";
    final String expectedDruidQueryType = "'queryType':'groupBy'";
    sql(sqlQuery)
        .returnsOrdered("Y=1997; M=12; product_id=1558; S=30", "Y=1997; M=3; product_id=1558; S=29",
            "Y=1997; M=5; product_id=1558; S=27")
        .explainContains(expectedPlan)
        .queryContains(new DruidChecker(expectedDruidQueryType));
  }

  @Test void testGroupByTimeSortOverMetrics() {
    final String sqlQuery = "SELECT count(*) as c , SUM(\"unit_sales\") as s,"
        + " floor(\"timestamp\" to month)"
        + " FROM \"foodmart\" group by floor(\"timestamp\" to month) order by s DESC";
    sql(sqlQuery)
        .returnsOrdered("C=8716; S=26796; EXPR$2=1997-12-01 00:00:00",
            "C=8231; S=25270; EXPR$2=1997-11-01 00:00:00",
            "C=7752; S=23763; EXPR$2=1997-07-01 00:00:00",
            "C=7710; S=23706; EXPR$2=1997-03-01 00:00:00",
            "C=7038; S=21697; EXPR$2=1997-08-01 00:00:00",
            "C=7033; S=21628; EXPR$2=1997-01-01 00:00:00",
            "C=6912; S=21350; EXPR$2=1997-06-01 00:00:00",
            "C=6865; S=21081; EXPR$2=1997-05-01 00:00:00",
            "C=6844; S=20957; EXPR$2=1997-02-01 00:00:00",
            "C=6662; S=20388; EXPR$2=1997-09-01 00:00:00",
            "C=6588; S=20179; EXPR$2=1997-04-01 00:00:00",
            "C=6478; S=19958; EXPR$2=1997-10-01 00:00:00")
        .queryContains(new DruidChecker("'queryType':'groupBy'"))
        .explainContains("DruidQuery(table=[[foodmart, foodmart]],"
            + " intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]],"
            + " projects=[[FLOOR($0, FLAG(MONTH)), $89]], groups=[{0}], "
            + "aggs=[[COUNT(), SUM($1)]], sort0=[2], dir0=[DESC])");
  }

  @Test void testNumericOrderingOfOrderByOperatorFullTime() {
    final String sqlQuery = "SELECT \"timestamp\" as \"timestamp\","
        + " count(*) as c, SUM(\"unit_sales\") as s FROM "
        + "\"foodmart\" group by \"timestamp\" order by \"timestamp\" DESC, c DESC, s LIMIT 5";
    final String druidSubQuery = "'limitSpec':{'type':'default','limit':5,"
        + "'columns':[{'dimension':'extract','direction':'descending',"
        + "'dimensionOrder':'lexicographic'},{'dimension':'C',"
        + "'direction':'descending','dimensionOrder':'numeric'},{'dimension':'S',"
        + "'direction':'ascending','dimensionOrder':'numeric'}]},"
        + "'aggregations':[{'type':'count','name':'C'},{'type':'longSum',"
        + "'name':'S','fieldName':'unit_sales'}]";
    sql(sqlQuery).returnsOrdered("timestamp=1997-12-30 00:00:00; C=22; S=36\ntimestamp=1997-12-29"
        + " 00:00:00; C=321; S=982\ntimestamp=1997-12-28 00:00:00; C=480; "
        + "S=1496\ntimestamp=1997-12-27 00:00:00; C=363; S=1156\ntimestamp=1997-12-26 00:00:00; "
        + "C=144; S=420").queryContains(new DruidChecker(druidSubQuery));

  }

  @Test void testNumericOrderingOfOrderByOperatorTimeExtract() {
    final String sqlQuery = "SELECT extract(day from \"timestamp\") as d, extract(month from "
        + "\"timestamp\") as m,  year(\"timestamp\") as y , count(*) as c, SUM(\"unit_sales\")  "
        + "as s FROM "
        + "\"foodmart\" group by  extract(day from \"timestamp\"), extract(month from \"timestamp\"), "
        + "year(\"timestamp\")  order by d DESC, m ASC, y DESC LIMIT 5";
    final String druidSubQuery = "'limitSpec':{'type':'default','limit':5,"
        + "'columns':[{'dimension':'extract_day','direction':'descending',"
        + "'dimensionOrder':'numeric'},{'dimension':'extract_month',"
        + "'direction':'ascending','dimensionOrder':'numeric'},"
        + "{'dimension':'extract_year','direction':'descending',"
        + "'dimensionOrder':'numeric'}]}";
    sql(sqlQuery).returnsOrdered("D=30; M=3; Y=1997; C=114; S=351\nD=30; M=5; Y=1997; "
        + "C=24; S=34\nD=30; M=6; Y=1997; C=73; S=183\nD=30; M=7; Y=1997; C=29; S=54\nD=30; M=8; "
        + "Y=1997; C=137; S=422").queryContains(new DruidChecker(druidSubQuery));

  }

  @Test void testNumericOrderingOfOrderByOperatorStringDims() {
    final String sqlQuery = "SELECT \"brand_name\", count(*) as c, SUM(\"unit_sales\")  "
        + "as s FROM "
        + "\"foodmart\" group by \"brand_name\" order by \"brand_name\"  DESC LIMIT 5";
    final String druidSubQuery = "'limitSpec':{'type':'default','limit':5,"
        + "'columns':[{'dimension':'brand_name','direction':'descending',"
        + "'dimensionOrder':'lexicographic'}]}";
    sql(sqlQuery).returnsOrdered("brand_name=Washington; C=576; S=1775\nbrand_name=Walrus; C=457;"
        + " S=1399\nbrand_name=Urban; C=299; S=924\nbrand_name=Tri-State; C=2339; "
        + "S=7270\nbrand_name=Toucan; C=123; S=380").queryContains(new DruidChecker(druidSubQuery));

  }

  @Test void testGroupByWeekExtract() {
    final String sql = "SELECT extract(week from \"timestamp\") from \"foodmart\" where "
        + "\"product_id\" = 1558 and extract(week from \"timestamp\") IN (10, 11) group by extract"
        + "(week from \"timestamp\")";

    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'extraction',"
        + "'dimension':'__time','outputName':'extract_week',"
        + "'extractionFn':{'type':'timeFormat','format':'w','timeZone':'UTC',"
        + "'locale':'en-US'}}],'limitSpec':{'type':'default'},"
        + "'filter':{'type':'and','fields':[{'type':'bound','dimension':'product_id',"
        + "'lower':'1558','lowerStrict':false,'upper':'1558','upperStrict':false,"
        + "'ordering':'numeric'},{'type':'or',"
        + "'fields':[{'type':'bound','dimension':'__time','lower':'10','lowerStrict':false,"
        + "'upper':'10','upperStrict':false,'ordering':'numeric',"
        + "'extractionFn':{'type':'timeFormat','format':'w','timeZone':'UTC',"
        + "'locale':'en-US'}},{'type':'bound','dimension':'__time','lower':'11','lowerStrict':false,"
        + "'upper':'11','upperStrict':false,'ordering':'numeric',"
        + "'extractionFn':{'type':'timeFormat','format':'w',"
        + "'timeZone':'UTC','locale':'en-US'}}]}]},"
        + "'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql).returnsOrdered("EXPR$0=10\nEXPR$0=11").queryContains(new DruidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1765">[CALCITE-1765]
   * Druid adapter: Gracefully handle granularity that cannot be pushed to
   * extraction function</a>. */
  @Test void testTimeExtractThatCannotBePushed() {
    final String sql = "SELECT extract(CENTURY from \"timestamp\") from \"foodmart\" where "
        + "\"product_id\" = 1558 group by extract(CENTURY from \"timestamp\")";
    final String plan = "PLAN="
        + "EnumerableAggregate(group=[{0}])\n"
        + "  EnumerableInterpreter\n"
        + "    BindableProject(EXPR$0=[EXTRACT(FLAG(CENTURY), $0)])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[=(CAST($1):INTEGER, 1558)], projects=[[$0]])";
    sql(sql).explainContains(plan).queryContains(new DruidChecker("'queryType':'scan'"))
        .returnsUnordered("EXPR$0=20");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1770">[CALCITE-1770]
   * Druid adapter: CAST(NULL AS ...) gives NPE</a>. */
  @Test void testPushCast() {
    final String sql = "SELECT \"product_id\"\n"
        + "from \"foodmart\"\n"
        + "where \"product_id\" = cast(NULL as varchar)\n"
        + "group by \"product_id\" order by \"product_id\" limit 5";
    final String plan = "EnumerableValues(tuples=[[]])";
    sql(sql).explainContains(plan);
  }

  @Test void testFalseFilter() {
    String sql = "Select count(*) as c from \"foodmart\" where false";
    final String plan = "EnumerableAggregate(group=[{}], C=[COUNT()])\n"
        + "  EnumerableValues(tuples=[[]])";
    sql(sql)
        .explainContains(plan)
        .returnsUnordered("C=0");
  }

  @Test void testTrueFilter() {
    String sql = "Select count(*) as c from \"foodmart\" where true";
    sql(sql).returnsUnordered("C=86829");
  }

  @Test void testFalseFilterCaseConjectionWithTrue() {
    String sql = "Select count(*) as c from \"foodmart\" where "
        + "\"product_id\" = 1558 and (true or false)";
    sql(sql).returnsUnordered("C=60")
        .queryContains(new DruidChecker("'queryType':'timeseries'"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1769">[CALCITE-1769]
   * Druid adapter: Push down filters involving numeric cast of literals</a>. */
  @Test void testPushCastNumeric() {
    String druidQuery = "'filter':{'type':'bound','dimension':'product_id',"
        + "'upper':'10','upperStrict':true,'ordering':'numeric'}";
    sql("?")
        .withRel(b -> {
          // select product_id
          // from foodmart.foodmart
          // where product_id < cast(10 as varchar)
          final RelDataType intType =
              b.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
          return b.scan("foodmart", "foodmart")
              .filter(
                  b.call(SqlStdOperatorTable.LESS_THAN,
                      b.getRexBuilder().makeCall(intType,
                          SqlStdOperatorTable.CAST,
                          ImmutableList.of(b.field("product_id"))),
                      b.getRexBuilder().makeCall(intType,
                          SqlStdOperatorTable.CAST,
                          ImmutableList.of(b.literal("10")))))
              .project(b.field("product_id"))
              .build();
        })
        .queryContains(new DruidChecker(druidQuery));
  }

  @Test void testPushFieldEqualsLiteral() {
    sql("?")
        .withRel(b -> {
          // select count(*) as c
          // from foodmart.foodmart
          // where product_id = 'id'
          return b.scan("foodmart", "foodmart")
              .filter(
                  b.call(SqlStdOperatorTable.EQUALS, b.field("product_id"),
                      b.literal("id")))
              .aggregate(b.groupKey(), b.countStar("c"))
              .build();
        })
        // Should return one row, "c=0"; logged
        // [CALCITE-1775] "GROUP BY ()" on empty relation should return 1 row
        .returnsUnordered("c=0")
        .queryContains(new DruidChecker("'queryType':'timeseries'"));
  }

  @Test void testPlusArithmeticOperation() {
    final String sqlQuery = "select sum(\"store_sales\") + sum(\"store_cost\") as a, "
        + "\"store_state\" from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "type':'expression','name':'A','expression':'(\\'$f1\\' + \\'$f2\\')'}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63, $90, $91]], groups=[{0}], "
        + "aggs=[[SUM($1), SUM($2)]], post_projects=[[+($1, $2), $0]], sort0=[0], dir0=[DESC])";
    CalciteAssert.AssertQuery q = sql(sqlQuery)
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
    q.returnsOrdered("A=369117.5279; store_state=WA",
        "A=222698.2651; store_state=CA",
        "A=199049.5706; store_state=OR");
  }

  @Test void testDivideArithmeticOperation() {
    final String sqlQuery = "select \"store_state\", sum(\"store_sales\") / sum(\"store_cost\") "
        + "as a from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "[{'type':'expression','name':'A','expression':'(\\'$f1\\' / \\'$f2\\')";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63, $90, $91]], groups=[{0}], "
        + "aggs=[[SUM($1), SUM($2)]], post_projects=[[$0, /($1, $2)]], sort0=[1], dir0=[DESC])";
    CalciteAssert.AssertQuery q = sql(sqlQuery)
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
    Assumptions.assumeTrue(Bug.CALCITE_4204_FIXED, "CALCITE-4204");
    q.returnsOrdered("store_state=OR; A=2.506091302943239",
        "store_state=CA; A=2.505379741272971",
        "store_state=WA; A=2.5045806163801996");
  }

  @Test void testMultiplyArithmeticOperation() {
    final String sqlQuery = "select \"store_state\", sum(\"store_sales\") * sum(\"store_cost\") "
        + "as a from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "{'type':'expression','name':'A','expression':'(\\'$f1\\' * \\'$f2\\')'";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63, $90, $91]], groups=[{0}], aggs=[[SUM($1),"
        + " SUM($2)]], post_projects=[[$0, *($1, $2)]], sort0=[1], dir0=[DESC])";
    CalciteAssert.AssertQuery q = sql(sqlQuery)
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
    Assumptions.assumeTrue(Bug.CALCITE_4204_FIXED, "CALCITE-4204");
    q.returnsOrdered("store_state=WA; A=2.7783838325212463E10",
        "store_state=CA; A=1.0112000537448784E10",
        "store_state=OR; A=8.077425041941243E9");
  }

  @Test void testMinusArithmeticOperation() {
    final String sqlQuery = "select \"store_state\", sum(\"store_sales\") - sum(\"store_cost\") "
        + "as a from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "'postAggregations':[{'type':'expression','name':'A',"
        + "'expression':'(\\'$f1\\' - \\'$f2\\')'}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63, $90, $91]], groups=[{0}], aggs=[[SUM($1), "
        + "SUM($2)]], post_projects=[[$0, -($1, $2)]], sort0=[1], dir0=[DESC])";
    CalciteAssert.AssertQuery q = sql(sqlQuery)
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
    q.returnsOrdered("store_state=WA; A=158468.9121",
        "store_state=CA; A=95637.4149",
        "store_state=OR; A=85504.5694");
  }

  @Test void testConstantPostAggregator() {
    final String sqlQuery = "select \"store_state\", sum(\"store_sales\") + 100 as a from "
        + "\"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "{'type':'expression','name':'A','expression':'(\\'$f1\\' + 100)'}";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63, $90]], groups=[{0}], aggs=[[SUM($1)]], "
        + "post_projects=[[$0, +($1, 100)]], sort0=[1], dir0=[DESC])";
    CalciteAssert.AssertQuery q = sql(sqlQuery)
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
    q.returnsOrdered("store_state=WA; A=263893.22",
        "store_state=CA; A=159267.84",
        "store_state=OR; A=142377.07");
  }

  @Test void testRecursiveArithmeticOperation() {
    final String sqlQuery = "select \"store_state\", -1 * (a + b) as c from (select "
        + "(sum(\"store_sales\")-sum(\"store_cost\")) / (count(*) * 3) "
        + "AS a,sum(\"unit_sales\") AS b, \"store_state\"  from \"foodmart\"  group "
        + "by \"store_state\") order by c desc";
    String postAggString = "'postAggregations':[{'type':'expression','name':'C','expression':"
        + "'(-1 * (((\\'$f1\\' - \\'$f2\\') / (\\'$f3\\' * 3)) + \\'B\\'))'}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$63, $90, $91, $89]], groups=[{0}], aggs=[[SUM($1), SUM($2), COUNT(), SUM($3)]], post_projects=[[$0, *(-1, +(/(-($1, $2), *($3, 3)), $4))]], sort0=[1], dir0=[DESC])";
    sql(sqlQuery)
        .returnsOrdered("store_state=OR; C=-67660.31890435601",
            "store_state=CA; C=-74749.30433035882",
            "store_state=WA; C=-124367.29537914316")
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
  }

  /** Turn on now {@code COUNT(DISTINCT ...)}. */
  @Test void testHyperUniquePostAggregator() {
    final String sqlQuery = "select \"store_state\", sum(\"store_cost\") / count(distinct "
        + "\"brand_name\") as a from \"foodmart\"  group by \"store_state\" order by a desc";
    final String postAggString = "[{'type':'expression','name':'A',"
        + "'expression':'(\\'$f1\\' / \\'$f2\\')'}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$63, $91, $2]], groups=[{0}], aggs=[[SUM($1), COUNT(DISTINCT $2)]], post_projects=[[$0, /($1, $2)]], sort0=[1], dir0=[DESC])";
    foodmartApprox(sqlQuery)
        .runs()
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
  }

  @Test void testExtractFilterWorkWithPostAggregations() {
    final String sql = "SELECT \"store_state\", \"brand_name\", sum(\"store_sales\") - "
        + "sum(\"store_cost\") as a  from \"foodmart\" where extract (week from \"timestamp\")"
        + " IN (10,11) and \"brand_name\"='Bird Call' group by \"store_state\", \"brand_name\"";
    final String druidQuery = "\"postAggregations\":[{\"type\":\"expression\",\"name\":\"A\","
        + "\"expression\":\"(\\\"$f2\\\" - \\\"$f3\\\")\"}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[AND(=(";
    sql(sql)
        .explainContains(plan)
        .returnsOrdered("store_state=CA; brand_name=Bird Call; A=34.3646",
            "store_state=OR; brand_name=Bird Call; A=39.1636",
            "store_state=WA; brand_name=Bird Call; A=53.7425")
        .queryContains(new DruidChecker(druidQuery));
  }

  @Test void testExtractFilterWorkWithPostAggregationsWithConstant() {
    final String sql = "SELECT \"store_state\", 'Bird Call' as \"brand_name\", "
        + "sum(\"store_sales\") - sum(\"store_cost\") as a  from \"foodmart\" "
        + "where extract (week from \"timestamp\")"
        + " IN (10,11) and \"brand_name\"='Bird Call' group by \"store_state\"";
    final String druidQuery = "type':'expression','name':'A','expression':'(\\'$f1\\' - \\'$f2\\')";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], filter=[AND(=($2, 'Bird Call'), "
        + "OR(=(EXTRACT(FLAG(WEEK), $0), 10), =(EXTRACT(FLAG(WEEK), $0), 11)))], "
        + "projects=[[$63, $90, $91]], "
        + "groups=[{0}], aggs=[[SUM($1), SUM($2)]], "
        + "post_projects=[[$0, 'Bird Call', -($1, $2)]])";
    sql(sql)
        .returnsOrdered("store_state=CA; brand_name=Bird Call; A=34.3646",
            "store_state=OR; brand_name=Bird Call; A=39.1636",
            "store_state=WA; brand_name=Bird Call; A=53.7425")
        .explainContains(plan)
        .queryContains(new DruidChecker(druidQuery));
  }

  @Test void testSingleAverageFunction() {
    final String sqlQuery = "select \"store_state\", sum(\"store_cost\") / count(*) as a from "
        + "\"foodmart\" group by \"store_state\" order by a desc";
    String postAggString = "\"postAggregations\":[{\"type\":\"expression\",\"name\":\"A\","
        + "\"expression\":\"(\\\"$f1\\\" / \\\"$f2\\\")";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63, $91]], groups=[{0}], "
        + "aggs=[[SUM($1), COUNT()]], post_projects=[[$0, /($1, $2)]], sort0=[1], dir0=[DESC])";
    CalciteAssert.AssertQuery q = sql(sqlQuery)
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
    Assumptions.assumeTrue(Bug.CALCITE_4204_FIXED, "CALCITE-4204");
    q.returnsOrdered("store_state=OR; A=2.6271402406293403",
        "store_state=CA; A=2.599338206292706",
        "store_state=WA; A=2.5828708592868717");
  }

  @Test void testPartiallyPostAggregation() {
    final String sqlQuery = "select \"store_state\", sum(\"store_sales\") / sum(\"store_cost\")"
        + " as a, case when sum(\"unit_sales\")=0 then 1.0 else sum(\"unit_sales\") "
        + "end as b from \"foodmart\"  group by \"store_state\" order by a desc";
    final String postAggString = "'postAggregations':[{'type':'expression','name':'A',"
        + "'expression':'(\\'$f1\\' / \\'$f2\\')'},{'type':'expression','name':'B',"
        + "'expression':'case_searched((\\'$f3\\' == 0),1.0,CAST(\\'$f3\\'";
    final String plan = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$63, $90, $91, $89]], groups=[{0}], aggs=[[SUM($1), SUM($2), SUM($3)]], post_projects=[[$0, /($1, $2), CASE(=($3, 0), 1.0:DECIMAL(19, 0), CAST($3):DECIMAL(19, 0))]], sort0=[1], dir0=[DESC])\n";
    CalciteAssert.AssertQuery q = sql(sqlQuery)
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
    Assumptions.assumeTrue(Bug.CALCITE_4204_FIXED, "CALCITE-4204");
    q.returnsOrdered("store_state=OR; A=2.506091302943239; B=67659.0",
        "store_state=CA; A=2.505379741272971; B=74748.0",
        "store_state=WA; A=2.5045806163801996; B=124366.0");
  }

  @Test void testDuplicateReferenceOnPostAggregation() {
    final String sqlQuery = "select \"store_state\", a, a - b as c from (select \"store_state\", "
        + "sum(\"store_sales\") + 100 as a, sum(\"store_cost\") as b from \"foodmart\"  group by "
        + "\"store_state\") order by a desc";
    String postAggString = "[{'type':'expression','name':'A','expression':'(\\'$f1\\' + 100)'},"
        + "{'type':'expression','name':'C','expression':'((\\'$f1\\' + 100) - \\'B\\')'}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63, $90, $91]], groups=[{0}], "
        + "aggs=[[SUM($1), SUM($2)]], post_projects=[[$0, +($1, 100), "
        + "-(+($1, 100), $2)]], sort0=[1], dir0=[DESC])";
    CalciteAssert.AssertQuery q = sql(sqlQuery)
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
    q.returnsOrdered("store_state=WA; A=263893.22; C=158568.9121",
        "store_state=CA; A=159267.84; C=95737.4149",
        "store_state=OR; A=142377.07; C=85604.5694");
  }

  @Test void testDivideByZeroDoubleTypeInfinity() {
    final String sqlQuery = "select \"store_state\", sum(\"store_cost\") / 0 as a from "
        + "\"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "'type':'expression','name':'A','expression':'(\\'$f1\\' / 0)'}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63, $91]], groups=[{0}], aggs=[[SUM($1)]], "
        + "post_projects=[[$0, /($1, 0)]], sort0=[1], dir0=[DESC])";
    sql(sqlQuery)
        .returnsOrdered("store_state=CA; A=Infinity",
            "store_state=OR; A=Infinity",
            "store_state=WA; A=Infinity")
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
  }

  @Test void testDivideByZeroDoubleTypeNegInfinity() {
    final String sqlQuery = "select \"store_state\", -1.0 * sum(\"store_cost\") / 0 as "
        + "a from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "\"postAggregations\":[{\"type\":\"expression\",\"name\":\"A\","
        + "\"expression\":\"((-1.0 * \\\"$f1\\\") / 0)\"}],";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63, $91]], groups=[{0}], "
        + "aggs=[[SUM($1)]], post_projects=[[$0, /(*(-1.0:DECIMAL(2, 1), $1), 0)]], "
        + "sort0=[1], dir0=[DESC])";
    sql(sqlQuery)
        .returnsOrdered("store_state=CA; A=-Infinity",
            "store_state=OR; A=-Infinity",
            "store_state=WA; A=-Infinity")
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
  }

  @Test void testDivideByZeroDoubleTypeNaN() {
    final String sqlQuery = "select \"store_state\", (sum(\"store_cost\") - sum(\"store_cost\")) "
        + "/ 0 as a from \"foodmart\"  group by \"store_state\" order by a desc";
    final String postAggString = "'postAggregations':[{'type':'expression','name':'A',"
        + "'expression':'((\\'$f1\\' - \\'$f1\\') / 0)'}";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63, $91]], groups=[{0}], aggs=[[SUM($1)]], "
        + "post_projects=[[$0, /(-($1, $1), 0)]], sort0=[1], dir0=[DESC])";
    sql(sqlQuery)
        .returnsOrdered("store_state=CA; A=NaN",
            "store_state=OR; A=NaN",
            "store_state=WA; A=NaN")
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
  }

  @Test void testDivideByZeroIntegerType() {
    final String sqlQuery = "select \"store_state\", (count(*) - "
        + "count(*)) / 0 as a from \"foodmart\"  group by \"store_state\" "
        + "order by a desc";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63]], groups=[{0}], aggs=[[COUNT()]], "
        + "post_projects=[[$0, /(-($1, $1), 0)]], sort0=[1], dir0=[DESC])";
    sql(sqlQuery)
        .explainContains(plan)
        .throws_("Server returned HTTP response code: 500");
    //@TODO It seems like calcite is not handling 500 error,
    // need to catch it and parse exception message from druid,
    // e.g., throws_("/ by zero");
  }

  @Test void testInterleaveBetweenAggregateAndGroupOrderByOnMetrics() {
    final String sqlQuery = "select \"store_state\", \"brand_name\", \"A\" from (\n"
        + "  select sum(\"store_sales\")-sum(\"store_cost\") as a, \"store_state\""
        + ", \"brand_name\"\n"
        + "  from \"foodmart\"\n"
        + "  group by \"store_state\", \"brand_name\" ) subq\n"
        + "order by \"A\" limit 5";
    String postAggString = "\"postAggregations\":[{\"type\":\"expression\",\"name\":\"A\","
        + "\"expression\":\"(\\\"$f2\\\" - \\\"$f3\\\")\"}";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$63, $2, $90, $91]], groups=[{0, 1}], aggs=[[SUM($2), SUM($3)]], post_projects=[[$0, $1, -($2, $3)]], sort0=[2], dir0=[ASC], fetch=[5])";
    CalciteAssert.AssertQuery q = sql(sqlQuery)
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
    q.returnsOrdered("store_state=CA; brand_name=King; A=21.4632",
        "store_state=OR; brand_name=Symphony; A=32.176",
        "store_state=CA; brand_name=Toretti; A=32.2465",
        "store_state=WA; brand_name=King; A=34.6104",
        "store_state=OR; brand_name=Toretti; A=36.3");
  }

  @Test void testInterleaveBetweenAggregateAndGroupOrderByOnDimension() {
    final String sqlQuery = "select \"store_state\", \"brand_name\", \"A\" from\n"
        + "(select \"store_state\", sum(\"store_sales\")+sum(\"store_cost\") "
        + "as a, \"brand_name\" from \"foodmart\" group by \"store_state\", \"brand_name\") "
        + "order by \"brand_name\", \"store_state\" limit 5";
    final String postAggString = "'postAggregations':[{'type':'expression','name':'A',"
        + "'expression':'(\\'$f2\\' + \\'$f3\\')'}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$63, $2, $90, $91]], groups=[{0, 1}], aggs=[[SUM($2), SUM($3)]], post_projects=[[$0, $1, +($2, $3)]], sort0=[1], sort1=[0], dir0=[ASC], dir1=[ASC], fetch=[5])";
    CalciteAssert.AssertQuery q = sql(sqlQuery)
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
    q.returnsOrdered("store_state=CA; brand_name=ADJ; A=222.1524",
        "store_state=OR; brand_name=ADJ; A=186.6036",
        "store_state=WA; brand_name=ADJ; A=216.9912",
        "store_state=CA; brand_name=Akron; A=250.349",
        "store_state=OR; brand_name=Akron; A=278.6972");
  }

  @Test void testOrderByOnMetricsInSelectDruidQuery() {
    final String sqlQuery = "select \"store_sales\" as a, \"store_cost\" as b, \"store_sales\" - "
        + "\"store_cost\" as c from \"foodmart\" where \"timestamp\" "
        + ">= '1997-01-01 00:00:00' and \"timestamp\" < '1997-09-01 00:00:00' order by c "
        + "limit 5";
    String queryType = "'queryType':'scan'";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$2], dir0=[ASC], fetch=[5])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1997-01-01T00:00:00.000Z/1997-09-01T00:00:00.000Z]], "
        + "projects=[[$90, $91, -($90, $91)]])";
    sql(sqlQuery)
        .returnsOrdered("A=0.51; B=0.2448; C=0.2652",
            "A=0.51; B=0.2397; C=0.2703",
            "A=0.57; B=0.285; C=0.285",
            "A=0.5; B=0.21; C=0.29",
            "A=0.57; B=0.2793; C=0.2907")
        .explainContains(plan)
        .queryContains(new DruidChecker(queryType));
  }

  /** Tests whether an aggregate with a filter clause has its filter factored
   * out when there is no outer filter. */
  @Test void testFilterClauseFactoredOut() {
    // Logically equivalent to
    // select sum("store_sales") from "foodmart" where "the_year" >= 1997
    String sql = "select sum(\"store_sales\") "
        + "filter (where \"the_year\" >= 1997) from \"foodmart\"";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','filter':{'type':'bound','dimension':'the_year','lower':'1997',"
        + "'lowerStrict':false,'ordering':'numeric'},'aggregations':[{'type':'doubleSum','name'"
        + ":'EXPR$0','fieldName':'store_sales'}],'intervals':['1900-01-09T00:00:00.000Z/2992-01"
        + "-10T00:00:00.000Z'],'context':{'skipEmptyBuckets':false}}";

    sql(sql).queryContains(new DruidChecker(expectedQuery));
  }

  /** Tests whether filter clauses with filters that are always true
   * disappear. */
  @Test void testFilterClauseAlwaysTrueGone() {
    // Logically equivalent to
    // select sum("store_sales") from "foodmart"
    String sql = "select sum(\"store_sales\") filter (where 1 = 1) from \"foodmart\"";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','aggregations':[{'type':'doubleSum','name':'EXPR$0','fieldName':"
        + "'store_sales'}],'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':false}}";

    sql(sql).queryContains(new DruidChecker(expectedQuery));
  }

  /** Tests whether filter clauses with filters that are always true disappear
   * in the presence of another aggregate without a filter clause. */
  @Test void testFilterClauseAlwaysTrueWithAggGone1() {
    // Logically equivalent to
    // select sum("store_sales"), sum("store_cost") from "foodmart"
    String sql = "select sum(\"store_sales\") filter (where 1 = 1), "
        + "sum(\"store_cost\") from \"foodmart\"";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','aggregations':[{'type':'doubleSum','name':'EXPR$0','fieldName':"
        + "'store_sales'},{'type':'doubleSum','name':'EXPR$1','fieldName':'store_cost'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':false}}";

    sql(sql).queryContains(new DruidChecker(expectedQuery));
  }

  /** Tests whether filter clauses with filters that are always true disappear
   * in the presence of another aggregate with a filter clause. */
  @Test void testFilterClauseAlwaysTrueWithAggGone2() {
    // Logically equivalent to
    // select sum("store_sales"),
    // sum("store_cost") filter (where "store_state" = 'CA') from "foodmart"
    String sql = "select sum(\"store_sales\") filter (where 1 = 1), "
        + "sum(\"store_cost\") filter (where \"store_state\" = 'CA') "
        + "from \"foodmart\"";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','aggregations':[{'type':'doubleSum','name':'EXPR$0','fieldName'"
        + ":'store_sales'},{'type':'filtered','filter':{'type':'selector','dimension':"
        + "'store_state','value':'CA'},'aggregator':{'type':'doubleSum','name':'EXPR$1',"
        + "'fieldName':'store_cost'}}],'intervals':"
        + "['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':false}}";

    sql(sql).queryContains(new DruidChecker(expectedQuery));
  }

  /** Tests whether an existing outer filter is untouched when an aggregate
   * has a filter clause that is always true. */
  @Test void testOuterFilterRemainsWithAlwaysTrueClause() {
    // Logically equivalent to
    // select sum("store_sales"), sum("store_cost") from "foodmart" where "store_city" = 'Seattle'
    String sql = "select sum(\"store_sales\") filter (where 1 = 1), sum(\"store_cost\") "
        + "from \"foodmart\" where \"store_city\" = 'Seattle'";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','filter':{'type':'selector','dimension':'store_city',"
        + "'value':'Seattle'},'aggregations':[{'type':'doubleSum','name':'EXPR$0',"
        + "'fieldName':'store_sales'},{'type':'doubleSum','name':'EXPR$1',"
        + "'fieldName':'store_cost'}],'intervals':"
        + "['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':false}}";

    sql(sql).queryContains(new DruidChecker(expectedQuery));
  }

  /** Tests that an aggregate with a filter clause that is always false does
   * not get pushed in. */
  @Test void testFilterClauseAlwaysFalseNotPushed() {
    String sql = "select sum(\"store_sales\") filter (where 1 > 1) from \"foodmart\"";
    // Calcite takes care of the unsatisfiable filter
    String expectedSubExplain =
        "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "filter=[false], projects=[[$90, false]], groups=[{}], aggs=[[SUM($0)]])";
    sql(sql)
        .queryContains(
            new DruidChecker("{\"queryType\":\"timeseries\","
                + "\"dataSource\":\"foodmart\",\"descending\":false,\"granularity\":\"all\","
                + "\"filter\":{\"type\":\"expression\",\"expression\":\"1 == 2\"},"
                + "\"aggregations\":[{\"type\":\"doubleSum\",\"name\":\"EXPR$0\","
                + "\"fieldName\":\"store_sales\"}],"
                + "\"intervals\":[\"1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z\"],"
                + "\"context\":{\"skipEmptyBuckets\":false}}"))
        .explainContains(expectedSubExplain);
  }

  /** Tests that an aggregate with a filter clause that is always false does
   * not get pushed when there is already an outer filter. */
  @Test void testFilterClauseAlwaysFalseNotPushedWithFilter() {
    String sql = "select sum(\"store_sales\") filter (where 1 > 1) "
        + "from \"foodmart\" where \"store_city\" = 'Seattle'";
    String expectedSubExplain =
        "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[AND"
            + "(false, =($62, 'Seattle'))], projects=[[$90, false]], groups=[{}], aggs=[[SUM"
            + "($0)]])";

    sql(sql)
        .explainContains(expectedSubExplain)
        .queryContains(
            new DruidChecker("\"filter\":{\"type"
                + "\":\"and\",\"fields\":[{\"type\":\"expression\",\"expression\":\"1 == 2\"},"
                + "{\"type\":\"selector\",\"dimension\":\"store_city\",\"value\":\"Seattle\"}]}"));
  }

  /** Tests that an aggregate with a filter clause that is the same as the
   * outer filter has no references to that filter, and that the original outer
   * filter remains. */
  @Test void testFilterClauseSameAsOuterFilterGone() {
    // Logically equivalent to
    // select sum("store_sales") from "foodmart" where "store_city" = 'Seattle'
    String sql = "select sum(\"store_sales\") filter (where \"store_city\" = 'Seattle') "
        + "from \"foodmart\" where \"store_city\" = 'Seattle'";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','filter':{'type':'selector','dimension':'store_city','value':"
        + "'Seattle'},'aggregations':[{'type':'doubleSum','name':'EXPR$0','fieldName':"
        + "'store_sales'}],'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':false}}";

    sql(sql)
        .queryContains(new DruidChecker(expectedQuery))
        .returnsUnordered("EXPR$0=52644.07");
  }

  /** Tests that an aggregate with a filter clause in the presence of another
   * aggregate without a filter clause does not have its filter factored out
   * into the outer filter. */
  @Test void testFilterClauseNotFactoredOut1() {
    String sql = "select sum(\"store_sales\") filter (where \"store_state\" = 'CA'), "
        + "sum(\"store_cost\") from \"foodmart\"";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','aggregations':[{'type':'filtered','filter':{'type':'selector',"
        + "'dimension':'store_state','value':'CA'},'aggregator':{'type':'doubleSum','name':"
        + "'EXPR$0','fieldName':'store_sales'}},{'type':'doubleSum','name':'EXPR$1','fieldName'"
        + ":'store_cost'}],'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':false}}";

    sql(sql).queryContains(new DruidChecker(expectedQuery));
  }

  /** Tests that an aggregate with a filter clause in the presence of another
   * aggregate without a filter clause, and an outer filter does not have its
   * filter factored out into the outer filter. */
  @Test void testFilterClauseNotFactoredOut2() {
    String sql = "select sum(\"store_sales\") filter (where \"store_state\" = 'CA'), "
        + "sum(\"store_cost\") from \"foodmart\" where \"the_year\" >= 1997";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','filter':{'type':'bound','dimension':'the_year','lower':'1997',"
        + "'lowerStrict':false,'ordering':'numeric'},'aggregations':[{'type':'filtered',"
        + "'filter':{'type':'selector','dimension':'store_state','value':'CA'},'aggregator':{"
        + "'type':'doubleSum','name':'EXPR$0','fieldName':'store_sales'}},{'type':'doubleSum',"
        + "'name':'EXPR$1','fieldName':'store_cost'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':false}}";

    sql(sql).queryContains(new DruidChecker(expectedQuery));
  }

  /** Tests that multiple aggregates with filter clauses have their filters
   * extracted to the outer filter field for data pruning. */
  @Test void testFilterClausesFactoredForPruning1() {
    String sql = "select "
        + "sum(\"store_sales\") filter (where \"store_state\" = 'CA'), "
        + "sum(\"store_sales\") filter (where \"store_state\" = 'WA') "
        + "from \"foodmart\"";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','filter':{'type':'or','fields':[{'type':'selector','dimension':"
        + "'store_state','value':'CA'},{'type':'selector','dimension':'store_state',"
        + "'value':'WA'}]},'aggregations':[{'type':'filtered','filter':{'type':'selector',"
        + "'dimension':'store_state','value':'CA'},'aggregator':{'type':'doubleSum','name':"
        + "'EXPR$0','fieldName':'store_sales'}},{'type':'filtered','filter':{'type':'selector',"
        + "'dimension':'store_state','value':'WA'},'aggregator':{'type':'doubleSum','name':"
        + "'EXPR$1','fieldName':'store_sales'}}],'intervals':"
        + "['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':false}}";

    sql(sql)
        .queryContains(new DruidChecker(expectedQuery))
        .returnsUnordered("EXPR$0=159167.84; EXPR$1=263793.22");
  }

  /** Tests that multiple aggregates with filter clauses have their filters
   * extracted to the outer filter field for data pruning in the presence of an
   * outer filter. */
  @Test void testFilterClausesFactoredForPruning2() {
    String sql = "select "
        + "sum(\"store_sales\") filter (where \"store_state\" = 'CA'), "
        + "sum(\"store_sales\") filter (where \"store_state\" = 'WA') "
        + "from \"foodmart\" where \"brand_name\" = 'Super'";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','filter':{'type':'and','fields':[{'type':'or','fields':[{'type':"
        + "'selector','dimension':'store_state','value':'CA'},{'type':'selector','dimension':"
        + "'store_state','value':'WA'}]},{'type':'selector','dimension':'brand_name','value':"
        + "'Super'}]},'aggregations':[{'type':'filtered','filter':{'type':'selector',"
        + "'dimension':'store_state','value':'CA'},'aggregator':{'type':'doubleSum','name':"
        + "'EXPR$0','fieldName':'store_sales'}},{'type':'filtered','filter':{'type':'selector',"
        + "'dimension':'store_state','value':'WA'},'aggregator':{'type':'doubleSum','name':"
        + "'EXPR$1','fieldName':'store_sales'}}],'intervals':"
        + "['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':false}}";

    sql(sql)
        .queryContains(new DruidChecker(expectedQuery))
        .returnsUnordered("EXPR$0=2600.01; EXPR$1=4486.44");
  }

  /** Tests that multiple aggregates with the same filter clause have them
   * factored out in the presence of an outer filter, and that they no longer
   * refer to those filters. */
  @Test void testMultipleFiltersFactoredOutWithOuterFilter() {
    // Logically Equivalent to
    // select sum("store_sales"), sum("store_cost")
    // from "foodmart" where "brand_name" = 'Super' and "store_state" = 'CA'
    String sql = "select "
        + "sum(\"store_sales\") filter (where \"store_state\" = 'CA'), "
        + "sum(\"store_cost\") filter (where \"store_state\" = 'CA') "
        + "from \"foodmart\" "
        + "where \"brand_name\" = 'Super'";
    // Aggregates should lose reference to any filter clause
    String expectedAggregateExplain = "aggs=[[SUM($0), SUM($2)]]";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','filter':{'type':'and','fields':[{'type':'selector','dimension':"
        + "'store_state','value':'CA'},{'type':'selector','dimension':'brand_name','value':"
        + "'Super'}]},'aggregations':[{'type':'doubleSum','name':'EXPR$0','fieldName':"
        + "'store_sales'},{'type':'doubleSum','name':'EXPR$1','fieldName':'store_cost'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':false}}";

    sql(sql)
        .queryContains(new DruidChecker(expectedQuery))
        .explainContains(expectedAggregateExplain)
        .returnsUnordered("EXPR$0=2600.01; EXPR$1=1013.162");
  }

  /**
   * Tests that when the resulting filter from factoring filter clauses out is always false,
   * that they are still pushed to Druid to handle.
   */
  @Test void testOuterFilterFalseAfterFactorSimplification() {
    // Normally we would factor out "the_year" > 1997 into the outer filter to prune the data
    // before aggregation and simplify the expression, but in this case that would produce:
    // "the_year" > 1997 AND "the_year" <= 1997 -> false (after simplification)
    // Since Druid cannot handle a "false" filter, we revert back to the
    // pre-simplified version. i.e the filter should be "the_year" > 1997 and "the_year" <= 1997
    // and let Druid handle an unsatisfiable expression
    String sql = "select sum(\"store_sales\") filter (where \"the_year\" > 1997) "
        + "from \"foodmart\" where \"the_year\" <= 1997";

    String expectedFilter = "filter':{'type':'and','fields':[{'type':'bound','dimension':'the_year'"
        + ",'lower':'1997','lowerStrict':true,'ordering':'numeric'},{'type':'bound',"
        + "'dimension':'the_year','upper':'1997','upperStrict':false,'ordering':'numeric'}]}";

    sql(sql)
        .queryContains(new DruidChecker(expectedFilter));
  }

  /**
   * Test to ensure that aggregates with filter clauses that Druid cannot handle are not pushed in
   * as filtered aggregates.
   */
  @Test void testFilterClauseNotPushable() {
    // Currently the adapter does not support the LIKE operator
    String sql = "select sum(\"store_sales\") "
        + "filter (where \"the_year\" like '199_') from \"foodmart\"";
    String expectedSubExplain =
        "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[LIKE"
            + "($83, '199_')], projects=[[$90, IS TRUE(LIKE($83, '199_'))]], groups=[{}], "
            + "aggs=[[SUM($0)]])";

    sql(sql)
        .explainContains(expectedSubExplain)
        .queryContains(
            new DruidChecker("\"filter\":{\"type"
                + "\":\"expression\",\"expression\":\"like(\\\"the_year\\\","));
  }

  @Test void testFilterClauseWithMetricRef() {
    String sql = "select sum(\"store_sales\") filter (where \"store_cost\" > 10) from \"foodmart\"";
    String expectedSubExplain =
        "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[>"
            + "($91, 10)], projects=[[$90, IS TRUE(>($91, 10))]], groups=[{}], aggs=[[SUM($0)"
            + "]])";

    sql(sql)
        .explainContains(expectedSubExplain)
        .queryContains(
            new DruidChecker("\"queryType\":\"timeseries\"", "\"filter\":{\"type\":\"bound\","
                + "\"dimension\":\"store_cost\",\"lower\":\"10\",\"lowerStrict\":true,"
                + "\"ordering\":\"numeric\"}"))
        .returnsUnordered("EXPR$0=25.06");
  }

  @Test void testFilterClauseWithMetricRefAndAggregates() {
    String sql = "select sum(\"store_sales\"), \"product_id\" "
        + "from \"foodmart\" where \"product_id\" > 1553 and \"store_cost\" > 5 group by \"product_id\"";
    String expectedSubExplain = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], EXPR$0=[$t1], product_id=[$t0])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[AND(>(CAST($1):INTEGER, 1553), >($91, 5))], projects=[[$1, $90]], groups=[{0}], aggs=[[SUM($1)]])";

    CalciteAssert.AssertQuery q = sql(sql)
        .explainContains(expectedSubExplain)
        .queryContains(
            new DruidChecker("\"queryType\":\"groupBy\"", "{\"type\":\"bound\","
                + "\"dimension\":\"store_cost\",\"lower\":\"5\",\"lowerStrict\":true,"
                + "\"ordering\":\"numeric\"}"));
    q.returnsUnordered("EXPR$0=10.16; product_id=1554\n"
        + "EXPR$0=45.05; product_id=1556\n"
        + "EXPR$0=88.5; product_id=1555");
  }

  @Test void testFilterClauseWithMetricAndTimeAndAggregates() {
    String sql = "select sum(\"store_sales\"), \"product_id\""
        + "from \"foodmart\" where \"product_id\" > 1555 and \"store_cost\" > 5 and extract(year "
        + "from \"timestamp\") = 1997 "
        + "group by floor(\"timestamp\" to DAY),\"product_id\"";
    sql(sql)
        .queryContains(
            new DruidChecker("\"queryType\":\"groupBy\"", "{\"type\":\"bound\","
                + "\"dimension\":\"store_cost\",\"lower\":\"5\",\"lowerStrict\":true,"
                + "\"ordering\":\"numeric\"}"))
        .returnsUnordered("EXPR$0=10.6; product_id=1556\n"
            + "EXPR$0=10.6; product_id=1556\n"
            + "EXPR$0=10.6; product_id=1556\n"
            + "EXPR$0=13.25; product_id=1556");
  }

  /** Tests that an aggregate with a nested filter clause has its filter
   * factored out. */
  @Test void testNestedFilterClauseFactored() {
    // Logically equivalent to
    // select sum("store_sales") from "foodmart" where "store_state" in ('CA', 'OR')
    String sql =
        "select sum(\"store_sales\") "
            + "filter (where \"store_state\" = 'CA' or \"store_state\" = 'OR') from \"foodmart\"";

    String expectedFilterJson =
        "filter':{'type':'or','fields':[{'type':'selector','dimension':"
            + "'store_state','value':'CA'},{'type':'selector',"
            + "'dimension':'store_state','value':'OR'}]}";

    String expectedAggregateJson =
        "'aggregations':[{'type':'doubleSum',"
            + "'name':'EXPR$0','fieldName':'store_sales'}]";

    sql(sql)
        .queryContains(new DruidChecker(expectedFilterJson))
        .queryContains(new DruidChecker(expectedAggregateJson))
        .returnsUnordered("EXPR$0=301444.91");
  }

  /** Tests that aggregates with nested filters have their filters factored out
   * into the outer filter for data pruning while still holding a reference to
   * the filter clause.  */
  @Test void testNestedFilterClauseInAggregates() {
    String sql =
        "select "
            + "sum(\"store_sales\") filter "
            + "(where \"store_state\" = 'CA' and \"the_month\" = 'October'), "
            + "sum(\"store_cost\") filter "
            + "(where \"store_state\" = 'CA' and \"the_day\" = 'Monday') "
            + "from \"foodmart\"";

    // (store_state = CA AND the_month = October) OR (store_state = CA AND the_day = Monday)
    String expectedFilterJson = "filter':{'type':'or','fields':[{'type':'and','fields':[{'type':"
        + "'selector','dimension':'store_state','value':'CA'},{'type':'selector','dimension':"
        + "'the_month','value':'October'}]},{'type':'and','fields':[{'type':'selector',"
        + "'dimension':'store_state','value':'CA'},{'type':'selector','dimension':'the_day',"
        + "'value':'Monday'}]}]}";

    String expectedAggregatesJson = "'aggregations':[{'type':'filtered','filter':{'type':'and',"
        + "'fields':[{'type':'selector','dimension':'store_state','value':'CA'},{'type':"
        + "'selector','dimension':'the_month','value':'October'}]},'aggregator':{'type':"
        + "'doubleSum','name':'EXPR$0','fieldName':'store_sales'}},{'type':'filtered',"
        + "'filter':{'type':'and','fields':[{'type':'selector','dimension':'store_state',"
        + "'value':'CA'},{'type':'selector','dimension':'the_day','value':'Monday'}]},"
        + "'aggregator':{'type':'doubleSum','name':'EXPR$1','fieldName':'store_cost'}}]";

    sql(sql)
        .queryContains(new DruidChecker(expectedFilterJson))
        .queryContains(new DruidChecker(expectedAggregatesJson))
        .returnsUnordered("EXPR$0=13077.79; EXPR$1=9830.7799");
  }

  @Test void testCountWithNonNull() {
    final String sql = "select count(\"timestamp\") from \"foodmart\"\n";
    final String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart'";
    sql(sql)
        .returnsUnordered("EXPR$0=86829")
        .queryContains(new DruidChecker(druidQuery));
  }

  /** Tests that the "not" filter has only 1 field, rather than an array of
   * fields. */
  @Test void testNotFilterForm() {
    String sql = "select count(distinct \"the_month\") from "
        + "\"foodmart\" where \"the_month\" <> 'October'";
    String druidFilter = "'filter':{'type':'not',"
        + "'field':{'type':'selector','dimension':'the_month','value':'October'}}";
    // Check that the filter actually worked, and that druid was responsible for the filter
    sql(sql)
        .queryContains(new DruidChecker(druidFilter))
        .returnsOrdered("EXPR$0=11");
  }

  /** Tests that {@code count(distinct ...)} gets pushed to Druid when
   * approximate results are acceptable. */
  @Test void testDistinctCountWhenApproxResultsAccepted() {
    String sql = "select count(distinct \"store_state\") from \"foodmart\"";
    String expectedSubExplain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$63]], groups=[{}], aggs=[[COUNT(DISTINCT $0)]])";
    String expectedAggregate = "{'type':'cardinality','name':"
        + "'EXPR$0','fieldNames':['store_state']}";

    testCountWithApproxDistinct(true, sql, expectedSubExplain, expectedAggregate);
  }

  /** Tests that {@code count(distinct ...)} doesn't get pushed to Druid
   * when approximate results are not acceptable. */
  @Test void testDistinctCountWhenApproxResultsNotAccepted() {
    String sql = "select count(distinct \"store_state\") from \"foodmart\"";
    String expectedSubExplain = ""
        + "EnumerableAggregate(group=[{}], EXPR$0=[COUNT($0)])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$63]], groups=[{0}], aggs=[[]])";

    testCountWithApproxDistinct(false, sql, expectedSubExplain);
  }

  @Test void testDistinctCountOnMetric() {
    final String sql = "select count(distinct \"store_sales\") from \"foodmart\" "
        + "where \"store_state\" = 'WA'";
    final String expectedSubExplainNoApprox = "PLAN="
        + "EnumerableAggregate(group=[{}], EXPR$0=[COUNT($0)])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[=($63, 'WA')], projects=[[$90]], groups=[{0}], aggs=[[]])";
    final String expectedSubPlanWithApprox = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[=($63, 'WA')], projects=[[$90]], groups=[{}], aggs=[[COUNT(DISTINCT $0)]])";

    testCountWithApproxDistinct(true, sql, expectedSubPlanWithApprox, "'queryType':'timeseries'");
    testCountWithApproxDistinct(false, sql, expectedSubExplainNoApprox, "'queryType':'groupBy'");
  }

  /** Tests that a count on a metric does not get pushed into Druid. */
  @Test void testCountOnMetric() {
    String sql = "select \"brand_name\", count(\"store_sales\") from \"foodmart\" "
        + "group by \"brand_name\"";
    String expectedSubExplain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$2, $90]], groups=[{0}], aggs=[[COUNT($1)]])";

    testCountWithApproxDistinct(true, sql, expectedSubExplain, "\"queryType\":\"groupBy\"");
    testCountWithApproxDistinct(false, sql, expectedSubExplain, "\"queryType\":\"groupBy\"");
  }

  /** Tests that {@code count(*)} is pushed into Druid. */
  @Test void testCountStar() {
    String sql = "select count(*) from \"foodmart\"";
    String expectedSubExplain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{}], aggs=[[COUNT()]])";

    sql(sql).explainContains(expectedSubExplain);
  }


  @Test void testCountOnMetricRenamed() {
    String sql = "select \"B\", count(\"A\") from "
        + "(select \"unit_sales\" as \"A\", \"store_state\" as \"B\" from \"foodmart\") "
        + "group by \"B\"";
    String expectedSubExplain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$63, $89]], groups=[{0}], aggs=[[COUNT($1)]])";

    testCountWithApproxDistinct(true, sql, expectedSubExplain);
    testCountWithApproxDistinct(false, sql, expectedSubExplain);
  }

  @Test void testDistinctCountOnMetricRenamed() {
    final String sql = "select \"B\", count(distinct \"A\") from "
        + "(select \"unit_sales\" as \"A\", \"store_state\" as \"B\" from \"foodmart\") "
        + "group by \"B\"";
    final String expectedSubExplainNoApprox = "PLAN="
        + "EnumerableAggregate(group=[{0}], EXPR$1=[COUNT($1)])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$63, $89]], groups=[{0, 1}], aggs=[[]])";
    final String expectedPlanWithApprox = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$63, $89]], groups=[{0}], aggs=[[COUNT(DISTINCT $1)]])";

    testCountWithApproxDistinct(true, sql, expectedPlanWithApprox, "'queryType':'groupBy'");
    testCountWithApproxDistinct(false, sql, expectedSubExplainNoApprox, "'queryType':'groupBy'");
  }

  private void testCountWithApproxDistinct(boolean approx, String sql, String expectedExplain) {
    testCountWithApproxDistinct(approx, sql, expectedExplain, "");
  }

  private void testCountWithApproxDistinct(boolean approx, String sql,
      String expectedExplain, String expectedDruidQuery) {
    CalciteAssert.that()
        .enable(enabled())
        .withModel(FOODMART)
        .with(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT.camelName(), approx)
        .query(sql)
        .runs()
        .explainContains(expectedExplain)
        .queryContains(new DruidChecker(expectedDruidQuery));
  }

  /**
   * Test to make sure that if a complex metric is also a dimension, then
   * {@link org.apache.calcite.adapter.druid.DruidTable} should allow it to be used like any other
   * column.
   * */
  @Test void testComplexMetricAlsoDimension() {
    foodmartApprox("select \"customer_id\" from \"foodmart\"")
        .runs();

    foodmartApprox("select count(distinct \"the_month\"), \"customer_id\" "
        + "from \"foodmart\" group by \"customer_id\"")
        .queryContains(
            new DruidChecker("{'queryType':'groupBy','dataSource':'foodmart',"
                + "'granularity':'all','dimensions':[{'type':'default','dimension':"
                + "'customer_id','outputName':'customer_id','outputType':'STRING'}],"
                + "'limitSpec':{'type':'default'},'aggregations':[{"
                + "'type':'cardinality','name':'EXPR$0','fieldNames':['the_month']}],"
                + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}"));
  }

  /**
   * Test to make sure that the mapping from a Table name to a Table returned from
   * {@link org.apache.calcite.adapter.druid.DruidSchema} is always the same Java object.
   * */
  @Test void testTableMapReused() {
    AbstractSchema schema = new DruidSchema(
        "http://localhost:8082", "http://localhost:8081", true);
    assertSame(schema.getTable("wikiticker"), schema.getTable("wikiticker"));
  }

  @Test void testPushEqualsCastDimension() {
    final String sqlQuery = "select sum(\"store_cost\") as a "
        + "from \"foodmart\" "
        + "where cast(\"product_id\" as double) = 1016.0";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[=(CAST($1):DOUBLE, 1016.0)], projects=[[$91]], groups=[{}], aggs=[[SUM($0)]])";
    final String druidQuery =
        "{'queryType':'timeseries','dataSource':'foodmart','descending':false,'granularity':'all',"
            + "'filter':{'type':'bound','dimension':'product_id','lower':'1016.0',"
            + "'lowerStrict':false,'upper':'1016.0','upperStrict':false,'ordering':'numeric'},"
            + "'aggregations':[{'type':'doubleSum','name':'A','fieldName':'store_cost'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':false}}";
    sql(sqlQuery)
        .explainContains(plan)
        .queryContains(new DruidChecker(druidQuery))
        .returnsUnordered("A=85.3164");

    final String sqlQuery2 = "select sum(\"store_cost\") as a "
        + "from \"foodmart\" "
        + "where cast(\"product_id\" as double) <= 1016.0 "
        + "and cast(\"product_id\" as double) >= 1016.0";
    sql(sqlQuery2)
        .returnsUnordered("A=85.3164");
  }

  @Test void testPushNotEqualsCastDimension() {
    final String sqlQuery = "select sum(\"store_cost\") as a "
        + "from \"foodmart\" "
        + "where cast(\"product_id\" as double) <> 1016.0";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[<>(CAST($1):DOUBLE, 1016.0)], projects=[[$91]], groups=[{}], aggs=[[SUM($0)]])";
    final String druidQuery =
        "{'queryType':'timeseries','dataSource':'foodmart','descending':false,'granularity':'all',"
            + "'filter':{'type':'not','field':{'type':'bound','dimension':'product_id','"
            + "lower':'1016.0','lowerStrict':false,'upper':'1016.0','upperStrict':false,'ordering':'numeric'}},"
            + "'aggregations':[{'type':'doubleSum','name':'A','fieldName':'store_cost'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],'context':{'skipEmptyBuckets':false}}";
    sql(sqlQuery)
        .explainContains(plan)
        .returnsUnordered("A=225541.9172")
        .queryContains(new DruidChecker(druidQuery));

    final String sqlQuery2 = "select sum(\"store_cost\") as a "
        + "from \"foodmart\" "
        + "where cast(\"product_id\" as double) < 1016.0 "
        + "or cast(\"product_id\" as double) > 1016.0";
    sql(sqlQuery2)
        .returnsUnordered("A=225541.9172");
  }

  @Test void testIsNull() {
    final String sql = "select count(*) as c "
        + "from \"foodmart\" "
        + "where \"product_id\" is null";
    final String druidQuery =
        "{'queryType':'timeseries','dataSource':'foodmart','descending':false,'granularity':'all',"
            + "'filter':{'type':'selector','dimension':'product_id','value':null},"
            + "'aggregations':[{'type':'count','name':'C'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':false}}";
    sql(sql)
        .queryContains(new DruidChecker(druidQuery))
        .returnsUnordered("C=0")
        .returnsCount(1);
  }

  @Test void testIsNotNull() {
    final String sql = "select count(*) as c "
        + "from \"foodmart\" "
        + "where \"product_id\" is not null";
    final String druidQuery =
        "{'queryType':'timeseries','dataSource':'foodmart','descending':false,'granularity':'all',"
            + "'filter':{'type':'not','field':{'type':'selector','dimension':'product_id','value':null}},"
            + "'aggregations':[{'type':'count','name':'C'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':false}}";
    sql(sql)
        .queryContains(new DruidChecker(druidQuery))
        .returnsUnordered("C=86829");
  }

  @Test void testFilterWithFloorOnTime() {
    // Test filter on floor on time column is pushed to druid
    final String sql = "select"
        + " floor(\"timestamp\" to MONTH) as t from \"foodmart\" where "
        + "floor(\"timestamp\" to MONTH) between '1997-01-01 00:00:00'"
        + "and '1997-03-01 00:00:00' order by t limit 2";

    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart','intervals':"
        + "['1997-01-01T00:00:00.000Z/1997-04-01T00:00:00.000Z'],'virtualColumns':"
        + "[{'type':'expression','name':'vc','expression':'timestamp_floor(\\'__time\\'";
    sql(sql)
        .returnsOrdered("T=1997-01-01 00:00:00", "T=1997-01-01 00:00:00")
        .queryContains(
            new DruidChecker(druidQuery));
  }

  @Test void testSelectFloorOnTimeWithFilterOnFloorOnTime() {
    final String sql = "Select floor(\"timestamp\" to MONTH) as t from "
        + "\"foodmart\" where floor(\"timestamp\" to MONTH) >= '1997-05-01 00:00:00' order by t"
        + " limit 1";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$0], dir0=[ASC], fetch=[1])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], filter=[>=(FLOOR($0, FLAG(MONTH)), 1997-05-01 00:00:00)], "
        + "projects=[[FLOOR($0, FLAG(MONTH))]])";

    sql(sql).returnsOrdered("T=1997-05-01 00:00:00").explainContains(plan);
  }

  @Test void testTimeWithFilterOnFloorOnTimeAndCastToTimestamp() {
    final String sql = "Select floor(\"timestamp\" to MONTH) as t from "
        + "\"foodmart\" where floor(\"timestamp\" to MONTH) >= cast('1997-05-01 00:00:00' as TIMESTAMP) order by t"
        + " limit 1";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart','intervals':"
        + "['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],'filter':{'type':'bound',"
        + "'dimension':'__time','lower':'1997-05-01T00:00:00.000Z',"
        + "'lowerStrict':false,'ordering':'lexicographic','";
    sql(sql)
        .returnsOrdered("T=1997-05-01 00:00:00")
        .queryContains(new DruidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2122">[CALCITE-2122]
   * DateRangeRules issues</a>. */
  @Test void testCombinationOfValidAndNotValidAndInterval() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\" "
        + "WHERE  \"timestamp\" < CAST('1998-01-02' as TIMESTAMP) AND "
        + "EXTRACT(MONTH FROM \"timestamp\") = 01 AND EXTRACT(YEAR FROM \"timestamp\") = 1996 ";
    sql(sql)
        .runs()
        .queryContains(new DruidChecker("{\"queryType\":\"timeseries\""));
  }

  @Test void testFloorToDateRangeWithTimeZone() {
    final String sql = "Select floor(\"timestamp\" to MONTH) as t from "
        + "\"foodmart\" where floor(\"timestamp\" to MONTH) >= '1997-05-01 00:00:00' "
        + "and floor(\"timestamp\" to MONTH) < '1997-05-02 00:00:00' order by t"
        + " limit 1";
    final String druidQuery = "{\"queryType\":\"scan\",\"dataSource\":\"foodmart\",\"intervals\":"
        + "[\"1997-05-01T00:00:00.000Z/1997-06-01T00:00:00.000Z\"],\"virtualColumns\":[{\"type\":"
        + "\"expression\",\"name\":\"vc\",\"expression\":\"timestamp_floor(\\\"__time\\\"";
    CalciteAssert.that()
        .enable(enabled())
        .withModel(FOODMART)
        .query(sql)
        .runs()
        .queryContains(new DruidChecker(druidQuery))
        .returnsOrdered("T=1997-05-01 00:00:00");
  }

  @Test void testExpressionsFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where ABS(-EXP(LN(SQRT"
        + "(\"store_sales\")))) = 1";
    sql(sql)
        .queryContains(new DruidChecker("pow(\\\"store_sales\\\""))
        .returnsUnordered("EXPR$0=32");
  }

  @Test void testExpressionsFilter2() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where CAST(SQRT(ABS(-\"store_sales\"))"
        + " /2 as INTEGER) = 1";
    sql(sql)
        .queryContains(new DruidChecker("(CAST((pow(abs((- \\\"store_sales\\\")),0.5) / 2),"))
        .returnsUnordered("EXPR$0=62449");
  }

  @Test void testExpressionsLikeFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where \"product_id\" LIKE '1%'";
    sql(sql)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"like"))
        .returnsUnordered("EXPR$0=36839");
  }

  @Test void testExpressionsSTRLENFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where CHAR_LENGTH(\"product_id\") = 2";
    sql(sql)
        .queryContains(
            new DruidChecker("\"expression\":\"(strlen(\\\"product_id\\\") == 2"))
        .returnsUnordered("EXPR$0=4876");
  }

  @Test void testExpressionsUpperLowerFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where upper(lower(\"city\")) = "
        + "'SPOKANE'";
    sql(sql)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"(upper"
                + "(lower(\\\"city\\\")) ==", "SPOKANE"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test void testExpressionsLowerUpperFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where lower(upper(\"city\")) = "
        + "'spokane'";
    sql(sql)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"(lower"
                + "(upper(\\\"city\\\")) ==", "spokane"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test void testExpressionsLowerFilterNotMatching() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where lower(\"city\") = 'Spokane'";
    sql(sql)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"(lower"
                + "(\\\"city\\\") ==", "Spokane"))
        .returnsUnordered("EXPR$0=0");
  }

  @Test void testExpressionsLowerFilterMatching() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where lower(\"city\") = 'spokane'";
    sql(sql)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"(lower"
                + "(\\\"city\\\") ==", "spokane"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test void testExpressionsUpperFilterNotMatching() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where upper(\"city\") = 'Spokane'";
    sql(sql)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"(upper"
                + "(\\\"city\\\") ==", "Spokane"))
        .returnsUnordered("EXPR$0=0");
  }

  @Test void testExpressionsUpperFilterMatching() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where upper(\"city\") = 'SPOKANE'";
    sql(sql)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"(upper"
                + "(\\\"city\\\") ==", "SPOKANE"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test void testExpressionsConcatFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where (\"city\" || '_extra') = "
        + "'Spokane_extra'";
    sql(sql)
        .queryContains(
            new DruidChecker("{\"type\":\"expression\",\"expression\":\"(concat"
                + "(\\\"city\\\",", "Spokane_extra"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test void testExpressionsNotNull() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where (\"city\" || 'extra') IS NOT NULL";
    sql(sql)
        .queryContains(
            new DruidChecker("{\"type\":\"expression\",\"expression\":\"(concat"
                + "(\\\"city\\\",", "!= null"))
        .returnsUnordered("EXPR$0=86829");
  }

  @Test void testComplexExpressionsIsNull() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where ( cast(null as INTEGER) + cast"
        + "(\"city\" as INTEGER)) IS NULL";
    sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{}], aggs=[[COUNT()]])")
        .queryContains(
            new DruidChecker(
                "{\"queryType\":\"timeseries\",\"dataSource\":\"foodmart\",\"descending\":false,"
                    + "\"granularity\":\"all\",\"aggregations\":[{\"type\":\"count\","
                    + "\"name\":\"EXPR$0\"}],\"intervals\":[\"1900-01-09T00:00:00.000Z/"
                    + "2992-01-10T00:00:00.000Z\"],\"context\":{\"skipEmptyBuckets\":false}}"))
        .returnsUnordered("EXPR$0=86829");
  }

  @Test void testExpressionsConcatFilterMultipleColumns() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where (\"city\" || \"state_province\")"
        + " = 'SpokaneWA'";
    sql(sql)
        .queryContains(
            new DruidChecker("(concat(\\\"city\\\",\\\"state_province\\\") ==", "SpokaneWA"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test void testAndCombinationOfExpAndSimpleFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where (\"city\" || \"state_province\")"
        + " = 'SpokaneWA' "
        + "AND \"state_province\" = 'WA'";
    sql(sql)
        .queryContains(
            new DruidChecker("(concat(\\\"city\\\",\\\"state_province\\\") ==",
                "SpokaneWA",
                "{\"type\":\"selector\",\"dimension\":\"state_province\",\"value\":\"WA\"}]}"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test void testOrCombinationOfExpAndSimpleFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where (\"city\" || \"state_province\")"
        + " = 'SpokaneWA' "
        + "OR (\"state_province\" = 'CA' AND \"city\" IS NOT NULL)";
    sql(sql)
        .queryContains(
            new DruidChecker("(concat(\\\"city\\\",\\\"state_province\\\") ==",
                "SpokaneWA",
                "{\"type\":\"and\",\"fields\":[{\"type\":\"selector\","
                    + "\"dimension\":\"state_province\",\"value\":\"CA\"},{\"type\":\"not\","
                    + "\"field\":{\"type\":\"selector\",\"dimension\":\"city\",\"value\":null}}]}"))
        .returnsUnordered("EXPR$0=31835");
  }

  @Test void testColumnAEqColumnB() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where \"city\" = \"state_province\"";
    sql(sql)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\""
                + "(\\\"city\\\" == \\\"state_province\\\")\"}"))
        .returnsUnordered("EXPR$0=0");
  }

  @Test void testColumnANotEqColumnB() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where \"city\" <> \"state_province\"";
    sql(sql)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\""
                + "(\\\"city\\\" != \\\"state_province\\\")\"}"))
        .returnsUnordered("EXPR$0=86829");
  }

  @Test void testAndCombinationOfComplexExpAndSimpleFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where ((\"city\" || "
        + "\"state_province\") = 'SpokaneWA' OR (\"city\" || '_extra') = 'Spokane_extra') "
        + "AND \"state_province\" = 'WA'";
    sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[AND(OR(="
            + "(||($29, $30), 'SpokaneWA'), =(||($29, '_extra'), 'Spokane_extra')), =($30, 'WA'))"
            + "], groups=[{}], aggs=[[COUNT()]])")
        .queryContains(
            new DruidChecker("(concat(\\\"city\\\",\\\"state_province\\\") ==",
                "SpokaneWA",
                "{\"type\":\"selector\",\"dimension\":\"state_province\","
                    + "\"value\":\"WA\"}]}"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test void testExpressionsFilterWithCast() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\" where CAST(( SQRT(\"store_sales\") - 1 "
        + ") / 3 + 1 AS INTEGER) > 1";
    sql(sql)
        .queryContains(
            new DruidChecker("(CAST((((pow(\\\"store_sales\\\",0.5) - 1) / 3) + 1)", "LONG"))
        .returnsUnordered("EXPR$0=476");
  }

  @Test void testExpressionsFilterWithCastTimeToDateToChar() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\" where CAST(CAST(\"timestamp\" as "
        + "DATE) as VARCHAR) = '1997-01-01'";
    sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], "
            + "filter=[=(CAST(CAST($0):DATE NOT NULL):VARCHAR NOT NULL, '1997-01-01')], "
            + "groups=[{}], aggs=[[COUNT()]])")
        .queryContains(
            new DruidChecker("{\"type\":\"expression\","
                + "\"expression\":\"(timestamp_format(timestamp_floor(\\\"__time\\\""))
        .returnsUnordered("EXPR$0=117");
  }

  @Test void testExpressionsFilterWithExtract() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where CAST((EXTRACT(MONTH FROM "
        + "\"timestamp\") - 1 ) / 3 + 1 AS INTEGER) = 1";
    sql(sql)
        .queryContains(
            new DruidChecker(",\"filter\":{\"type\":\"expression\",\"expression\":\"((("
                + "(timestamp_extract(\\\"__time\\\"", "MONTH", ") - 1) / 3) + 1) == 1"))
        .returnsUnordered("EXPR$0=21587");
  }

  @Test void testExtractYearFilterExpression() {
    final String sql = "SELECT count(*) from \"foodmart\" WHERE"
        + " EXTRACT(YEAR from \"timestamp\") + 1 > 1997";
    final String filterPart1 = "'filter':{'type':'expression','expression':"
        + "'((timestamp_extract(\\'__time\\'";
    CalciteAssert.that()
        .enable(enabled())
        .withModel(FOODMART)
        .query(sql)
        .runs()
        .returnsOrdered("EXPR$0=86829")
        .queryContains(new DruidChecker(filterPart1));
  }

  @Test void testExtractMonthFilterExpression() {
    final String sql = "SELECT count(*) from \"foodmart\" WHERE"
        + " EXTRACT(MONTH from \"timestamp\") + 1 = 02";
    final String filterPart1 = "'filter':{'type':'expression','expression':"
        + "'((timestamp_extract(\\'__time\\'";
    CalciteAssert.that()
        .enable(enabled())
        .withModel(FOODMART)
        .query(sql)
        .runs()
        .returnsOrdered("EXPR$0=7033")
        .queryContains(new DruidChecker(filterPart1, "MONTH", "== 2"));
  }

  @Test void testTimeFloorExpressions() {

    final String sql =
        "SELECT FLOOR(\"timestamp\" to DAY) as d from \"foodmart\" WHERE "
            + "CAST(FLOOR(CAST(\"timestamp\" AS DATE) to MONTH) AS DATE) = "
            + " CAST('1997-01-01' as DATE) GROUP BY  floor(\"timestamp\" to DAY) order by d limit 3";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], filter=[=(FLOOR(CAST($0):DATE NOT NULL, FLAG(MONTH)), "
        + "1997-01-01)], projects=[[FLOOR($0, FLAG(DAY))]], groups=[{0}], aggs=[[]], sort0=[0], "
        + "dir0=[ASC], fetch=[3])";
    sql(sql)
        .explainContains(plan)
        .returnsOrdered("D=1997-01-01 00:00:00", "D=1997-01-02 00:00:00", "D=1997-01-03 00:00:00");
  }

  @Test void testDruidTimeFloorAndTimeParseExpressions() {
    final String sql = "SELECT \"timestamp\", count(*) "
        + "from \"foodmart\" WHERE "
        + "CAST(('1997' || '-01' || '-01') AS DATE) = CAST(\"timestamp\" AS DATE) "
        + "GROUP BY \"timestamp\"";
    sql(sql)
        .returnsOrdered("timestamp=1997-01-01 00:00:00; EXPR$1=117")
        .queryContains(
            new DruidChecker(
                "\"filter\":{\"type\":\"expression\",\"expression\":\"(852076800000 == "
                    + "timestamp_floor"));
  }

  @Test void testDruidTimeFloorAndTimeParseExpressions2() {
    Assumptions.assumeTrue(Bug.CALCITE_4205_FIXED, "CALCITE-4205");
    final String sql = "SELECT \"timestamp\", count(*) "
        + "from \"foodmart\" WHERE "
        + "CAST(('1997' || '-01' || '-01') AS TIMESTAMP) = CAST(\"timestamp\" AS TIMESTAMP) "
        + "GROUP BY \"timestamp\"";
    sql(sql)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\""
                + "(timestamp_parse(concat(concat("))
        .returnsOrdered("timestamp=1997-01-01 00:00:00; EXPR$1=117");
  }

  @Test void testFilterFloorOnMetricColumn() {
    final String sql = "SELECT count(*) from \"foodmart\" WHERE floor(\"store_sales\") = 23";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]],"
        + " filter=[=(FLOOR($90), 23)], groups=[{}], aggs=[[COUNT()]]";
    sql(sql)
        .returnsOrdered("EXPR$0=2")
        .explainContains(plan)
        .queryContains(new DruidChecker("\"queryType\":\"timeseries\""));
  }


  @Test void testExpressionFilterSimpleColumnAEqColumnB() {
    final String sql = "SELECT count(*) from \"foodmart\" where \"product_id\" = \"city\"";
    sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "filter=[=($1, $29)], groups=[{}], aggs=[[COUNT()]])")
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\","
                + "\"expression\":\"(\\\"product_id\\\" == \\\"city\\\")\"}"))
        .returnsOrdered("EXPR$0=0");
  }

  @Test void testCastPlusMathOps() {
    final String sql = "SELECT COUNT(*) FROM " + FOODMART_TABLE
        + "WHERE (CAST(\"product_id\" AS INTEGER) + 1 * \"store_sales\")/(\"store_cost\" - 5) "
        + "<= floor(\"store_sales\") * 25 + 2";
    sql(sql)
        .queryContains(
            new DruidChecker(
                "\"filter\":{\"type\":\"expression\",\"expression\":\"(((CAST(\\\"product_id\\\", ",
                "LONG",
                ") + \\\"store_sales\\\") / (\\\"store_cost\\\" - 5))",
                " <= ((floor(\\\"store_sales\\\") * 25) + 2))\"}"))
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "filter=[<=(/(+(CAST($1):INTEGER, $90), -($91, 5)), +(*(FLOOR($90), 25), 2))], "
            + "groups=[{}], aggs=[[COUNT()]])")
        .returnsOrdered("EXPR$0=82129");
  }

  @Test void testBooleanFilterExpressions() {
    final String sql = "SELECT count(*) from " + FOODMART_TABLE
        + " WHERE (CAST((\"product_id\" <> '1') AS BOOLEAN)) IS TRUE";
    sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], filter=[<>($1, '1')], groups=[{}], aggs=[[COUNT()]])")
        .queryContains(new DruidChecker("\"queryType\":\"timeseries\""))
        .returnsOrdered("EXPR$0=86803");
  }


  @Test void testCombinationOfValidAndNotValidFilters() {
    final String sql = "SELECT COUNT(*) FROM " + FOODMART_TABLE
        + "WHERE ((CAST(\"product_id\" AS INTEGER) + 1 * \"store_sales\")/(\"store_cost\" - 5) "
        + "<= floor(\"store_sales\") * 25 + 2) AND \"timestamp\" < CAST('1997-01-02' as TIMESTAMP)"
        + "AND CAST(\"store_sales\" > 0 AS BOOLEAN) IS TRUE "
        + "AND \"product_id\" like '1%' AND \"store_cost\" > 1 "
        + "AND EXTRACT(MONTH FROM \"timestamp\") = 01 AND EXTRACT(DAY FROM \"timestamp\") = 01 "
        + "AND EXTRACT(MONTH FROM \"timestamp\") / 4 + 1 = 1";
    final String queryType = "{'queryType':'timeseries','dataSource':'foodmart'";
    final String filterExp1 = "{'type':'expression','expression':'(((CAST(\\'product_id\\'";
    final String filterExpPart2 =  " \\'store_sales\\') / (\\'store_cost\\' - 5)) "
        + "<= ((floor(\\'store_sales\\') * 25) + 2))'}";
    final String likeExpressionFilter = "{'type':'expression','expression':'like(\\'product_id\\'";
    final String likeExpressionFilter2 = "1%";
    final String simpleBound = "{'type':'bound','dimension':'store_cost','lower':'1',"
        + "'lowerStrict':true,'ordering':'numeric'}";
    final String timeSimpleFilter =
        "{'type':'bound','dimension':'__time','upper':'1997-01-02T00:00:00.000Z',"
            + "'upperStrict':true,'ordering':'lexicographic','extractionFn':{'type':'timeFormat','format':'yyyy-MM-dd";
    final String simpleExtractFilterMonth = "{'type':'bound','dimension':'__time','lower':'1',"
        + "'lowerStrict':false,'upper':'1','upperStrict':false,'ordering':'numeric',"
        + "'extractionFn':{'type':'timeFormat','format':'M','timeZone':'UTC','locale':'en-US'}}";
    final String simpleExtractFilterDay = "{'type':'bound','dimension':'__time','lower':'1',"
        + "'lowerStrict':false,'upper':'1','upperStrict':false,'ordering':'numeric',"
        + "'extractionFn':{'type':'timeFormat','format':'d','timeZone':'UTC','locale':'en-US'}}";
    final String quarterAsExpressionFilter = "{'type':'expression','expression':"
        + "'(((timestamp_extract(\\'__time\\'";
    final String quarterAsExpressionFilter2 = "MONTH";
    final String quarterAsExpressionFilterTimeZone = "UTC";
    final String quarterAsExpressionFilter3 = "/ 4) + 1) == 1)'}]}";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], filter=[AND(<=(/(+(CAST($1):INTEGER, $90), "
        + "-($91, 5)), +(*(FLOOR($90), 25), 2)), >($90, 0), LIKE($1, '1%'), >($91, 1), "
        + "<($0, 1997-01-02 00:00:00), =(EXTRACT(FLAG(MONTH), $0), 1), "
        + "=(EXTRACT(FLAG(DAY), $0), 1), =(+(/(EXTRACT(FLAG(MONTH), $0), 4), 1), 1))], "
        + "groups=[{}], aggs=[[COUNT()]])";
    sql(sql)
        .returnsOrdered("EXPR$0=36")
        .explainContains(plan)
        .queryContains(
            new DruidChecker(
                queryType, filterExp1, filterExpPart2, likeExpressionFilter, likeExpressionFilter2,
                simpleBound, timeSimpleFilter, simpleExtractFilterMonth, simpleExtractFilterDay,
                quarterAsExpressionFilter, quarterAsExpressionFilterTimeZone,
                quarterAsExpressionFilter2, quarterAsExpressionFilter3));
  }


  @Test void testCeilFilterExpression() {
    final String sql = "SELECT COUNT(*) FROM " + FOODMART_TABLE + " WHERE ceil(\"store_sales\") > 1"
        + " AND ceil(\"timestamp\" TO DAY) < CAST('1997-01-05' AS TIMESTAMP)"
        + " AND ceil(\"timestamp\" TO MONTH) < CAST('1997-03-01' AS TIMESTAMP)"
        + " AND ceil(\"timestamp\" TO HOUR) > CAST('1997-01-01' AS TIMESTAMP) "
        + " AND ceil(\"timestamp\" TO MINUTE) > CAST('1997-01-01' AS TIMESTAMP) "
        + " AND ceil(\"timestamp\" TO SECOND) > CAST('1997-01-01' AS TIMESTAMP) ";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1997-01-01T00:00:00.001Z/"
        + "1997-01-04T00:00:00.001Z]], filter=[>(CEIL($90), 1)], groups=[{}], aggs=[[COUNT()]])";
    sql(sql)
        .explainContains(plan)
        .returnsOrdered("EXPR$0=408");
  }

  @Test void testSubStringExpressionFilter() {
    final String sql =
        "SELECT COUNT(*) AS C, SUBSTRING(\"product_id\" from 1 for 4) FROM " + FOODMART_TABLE
            + " WHERE SUBSTRING(\"product_id\" from 1 for 4) like '12%' "
            + " AND CHARACTER_LENGTH(\"product_id\") = 4"
            + " AND SUBSTRING(\"product_id\" from 3 for 1) = '2'"
            + " AND CAST(SUBSTRING(\"product_id\" from 2 for 1) AS INTEGER) = 2"
            + " AND CAST(SUBSTRING(\"product_id\" from 4 for 1) AS INTEGER) = 7"
            + " AND CAST(SUBSTRING(\"product_id\" from 4) AS INTEGER) = 7"
            + " Group by SUBSTRING(\"product_id\" from 1 for 4)";
    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], C=[$t1], EXPR$1=[$t0])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[AND(LIKE(SUBSTRING($1, 1, 4), '12%'), =(CHAR_LENGTH($1), 4), =(SUBSTRING($1, 3, 1), '2'), =(CAST(SUBSTRING($1, 2, 1)):INTEGER, 2), =(CAST(SUBSTRING($1, 4, 1)):INTEGER, 7), =(CAST(SUBSTRING($1, 4)):INTEGER, 7))], projects=[[SUBSTRING($1, 1, 4)]], groups=[{0}], aggs=[[COUNT()]])";
    sql(sql)
        .returnsOrdered("C=60; EXPR$1=1227")
        .explainContains(plan)
        .queryContains(
            new DruidChecker("\"queryType\":\"groupBy\"", "substring(\\\"product_id\\\"",
                "\"(strlen(\\\"product_id\\\")",
                ",\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"vc\","
                    + "\"expression\":\"substring(\\\"product_id\\\", 0, 4)\","
                    + "\"outputType\":\"STRING\"}]"));
  }

  @Test void testSubStringWithNonConstantIndexes() {
    final String sql = "SELECT COUNT(*) FROM "
        + FOODMART_TABLE
        + " WHERE SUBSTRING(\"product_id\" from CAST(\"store_cost\" as INT)/1000 + 2  "
        + "for CAST(\"product_id\" as INT)) like '1%'";

    sql(sql).returnsOrdered("EXPR$0=10893")
        .queryContains(
            new DruidChecker("\"queryType\":\"timeseries\"", "like(substring(\\\"product_id\\\""))
        .explainContains(
            "PLAN=EnumerableInterpreter\n  DruidQuery(table=[[foodmart, foodmart]], "
                + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
                + "filter=[LIKE(SUBSTRING($1, +(/(CAST($91):INTEGER, 1000), 2), CAST($1):INTEGER), '1%')], "
                + "groups=[{}], aggs=[[COUNT()]])\n\n");
  }

  @Test void testSubStringWithNonConstantIndex() {
    final String sql = "SELECT COUNT(*) FROM "
        + FOODMART_TABLE
        + " WHERE SUBSTRING(\"product_id\" from CAST(\"store_cost\" as INT)/1000 + 1) like '1%'";

    sql(sql).returnsOrdered("EXPR$0=36839")
        .queryContains(new DruidChecker("like(substring(\\\"product_id\\\""))
        .explainContains(
            "PLAN=EnumerableInterpreter\n  DruidQuery(table=[[foodmart, foodmart]], "
                + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
                + "filter=[LIKE(SUBSTRING($1, +(/(CAST($91):INTEGER, 1000), 1)), '1%')],"
                + " groups=[{}], aggs=[[COUNT()]])\n\n");
  }


  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2098">[CALCITE-2098]
   * Push filters to Druid Query Scan when we have OR of AND clauses</a>.
   *
   * <p>Need to make sure that when there we have a valid filter with no
   * conjunction we still push all the valid filters.
   */
  @Test void testFilterClauseWithNoConjunction() {
    String sql = "select sum(\"store_sales\")"
        + "from \"foodmart\" where \"product_id\" > 1555 or \"store_cost\" > 5 or extract(year "
        + "from \"timestamp\") = 1997 "
        + "group by floor(\"timestamp\" to DAY),\"product_id\"";
    sql(sql)
        .queryContains(
            new DruidChecker("\"queryType\":\"groupBy\"", "{\"type\":\"bound\","
                + "\"dimension\":\"store_cost\",\"lower\":\"5\",\"lowerStrict\":true,"
                + "\"ordering\":\"numeric\"}"))
        .runs();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2123">[CALCITE-2123]
   * Bug in the Druid Filter Translation when Comparing String Ref to a Constant
   * Number</a>.
   */
  @Test void testBetweenFilterWithCastOverNumeric() {
    final String sql = "SELECT COUNT(*) FROM " + FOODMART_TABLE + " WHERE \"product_id\" = 16.0";
    // After CALCITE-2302 the Druid query changed a bit and the type of the
    // filter became an expression (instead of a bound filter) but it still
    // seems correct.
    sql(sql).runs().queryContains(
        new DruidChecker(
            false,
            "\"filter\":{\"type\":\"bound\",\"dimension\":\"product_id\",\"lower\":\"16.0\","
                + "\"lowerStrict\":false,\"upper\":\"16.0\","
                + "\"upperStrict\":false,\"ordering\":\"numeric\"}"));
  }

  @Test void testTrigonometryMathFunctions() {
    final String sql = "SELECT COUNT(*) FROM " + FOODMART_TABLE + "WHERE "
        + "SIN(\"store_cost\") > SIN(20) AND COS(\"store_sales\") > COS(20) "
        + "AND FLOOR(TAN(\"store_cost\")) = 2 "
        + "AND ABS(TAN(\"store_cost\") - SIN(\"store_cost\") / COS(\"store_cost\")) < 10e-7";
    sql(sql)
        .returnsOrdered("EXPR$0=2")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00"
            + ".000Z/2992-01-10T00:00:00.000Z]], filter=[AND(>(SIN($91), 9.129452507276277E-1), >"
            + "(COS($90), 4.08082061813392E-1), =(FLOOR(TAN($91)), 2), <(ABS(-(TAN($91), /(SIN"
            + "($91), COS($91)))), 1.0E-6))], groups=[{}], aggs=[[COUNT()]])");
  }

  @Test void testCastLiteralToTimestamp() {
    final String sql = "SELECT COUNT(*) FROM "
        + FOODMART_TABLE + " WHERE \"timestamp\" < CAST('1997-01-02' as TIMESTAMP)"
        + " AND EXTRACT(MONTH FROM \"timestamp\") / 4 + 1 = 1 ";
    sql(sql)
        .returnsOrdered("EXPR$0=117")
        .queryContains(
            new DruidChecker("{'queryType':'timeseries','dataSource':'foodmart',"
                + "'descending':false,'granularity':'all','filter':{'type':'and','fields':"
                + "[{'type':'bound','dimension':'__time','upper':'1997-01-02T00:00:00.000Z',"
                + "'upperStrict':true,'ordering':'lexicographic',"
                + "'extractionFn':{'type':'timeFormat','format':'yyyy-MM-dd",
                "{'type':'expression','expression':'(((timestamp_extract(\\'__time\\',",
                "/ 4) + 1) == 1)'}]},",
                "'aggregations':[{'type':'count','name':'EXPR$0'}],"
                    + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],'context':{'skipEmptyBuckets':false}}"));
  }

  @Test void testNotTrueSimpleFilter() {
    final String sql = "SELECT COUNT(*) FROM " + FOODMART_TABLE + "WHERE "
        + "(\"product_id\" = 1020 ) IS NOT TRUE AND (\"product_id\" = 1020 ) IS FALSE";
    final String result = "EXPR$0=86773";
    sql(sql)
        .returnsOrdered(result)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[<>(CAST($1):INTEGER, 1020)], groups=[{}], aggs=[[COUNT()]])");
    final String sql2 = "SELECT COUNT(*) FROM " + FOODMART_TABLE + "WHERE "
        + "\"product_id\" <> 1020";
    sql(sql2).returnsOrdered(result);
  }

  // ADDING COMPLEX PROJECT PUSHDOWN

  @Test void testPushOfSimpleMathOps() {
    final String sql =
        "SELECT COS(\"store_sales\") + 1, SIN(\"store_cost\"), EXTRACT(DAY from \"timestamp\") + 1  as D FROM "
            + FOODMART_TABLE + "WHERE \"store_sales\" < 20 order by D limit 3";
    sql(sql)
        .runs()
        .returnsOrdered("EXPR$0=1.060758881219386; EXPR$1=0.5172204046388567; D=2\n"
            + "EXPR$0=0.8316025520509229; EXPR$1=0.6544084288365644; D=2\n"
            + "EXPR$0=0.24267723077545622; EXPR$1=0.9286289016881148; D=2")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableSort(sort0=[$2], dir0=[ASC], fetch=[3])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], filter=[<($90, 20)], projects=[[+(COS($90), 1), SIN($91),"
            + " +(EXTRACT(FLAG(DAY), $0), 1)]])");
  }

  @Test void testPushOfSimpleColumnAPlusColumnB() {
    final String sql =
        "SELECT COS(\"store_sales\" + \"store_cost\") + 1, EXTRACT(DAY from \"timestamp\") + 1  as D FROM "
            + FOODMART_TABLE + "WHERE \"store_sales\" < 20 order by D limit 3";
    sql(sql)
        .runs()
        .returnsOrdered("EXPR$0=0.5357357987441458; D=2\n"
            + "EXPR$0=0.22760480207557643; D=2\n"
            + "EXPR$0=0.11259322182897047; D=2")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableSort(sort0=[$1], dir0=[ASC], fetch=[3])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], filter=[<($90, 20)], projects=[[+(COS(+($90, $91)), 1), "
            + "+(EXTRACT(FLAG(DAY), $0), 1)]])");
  }

  @Test void testSelectExtractMonth() {
    final String sql = "SELECT  EXTRACT(YEAR FROM \"timestamp\") FROM " + FOODMART_TABLE;
    sql(sql)
        .limit(1)
        .returnsOrdered("EXPR$0=1997")
        .explainContains("DruidQuery(table=[[foodmart, foodmart]], intervals="
            + "[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "projects=[[EXTRACT(FLAG(YEAR), $0)]])")
        .queryContains(
            new DruidChecker("\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"vc\","
                + "\"expression\":\"timestamp_extract(\\\"__time\\\""));
  }

  @Test void testAggOnArithmeticProject() {
    final String sql = "SELECT SUM(\"store_sales\" + 1) FROM " + FOODMART_TABLE;
    sql(sql)
        .returnsOrdered("EXPR$0=652067.13")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "projects=[[+($90, 1)]], groups=[{}], aggs=[[SUM($0)]])")
        .queryContains(
            new DruidChecker("\"queryType\":\"timeseries\"",
                "\"doubleSum\",\"name\":\"EXPR$0\",\"expression\":\"(\\\"store_sales\\\" + 1)\""));
  }

  @Test void testAggOnArithmeticProject2() {
    final String sql = "SELECT SUM(-\"store_sales\" * 2) as S FROM " + FOODMART_TABLE
        + "Group by \"timestamp\" order by s LIMIT 2";
    sql(sql)
        .returnsOrdered("S=-15918.02",
            "S=-14115.96")
        .explainContains("PLAN=EnumerableCalc(expr#0..1=[{inputs}], S=[$t1])\n"
            + "  EnumerableInterpreter\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$0, *(-($90), 2)]], groups=[{0}], "
            + "aggs=[[SUM($1)]], sort0=[1], dir0=[ASC], fetch=[2])")
        .queryContains(
            new DruidChecker("'queryType':'groupBy'", "'granularity':'all'",
                "{'dimension':'S','direction':'ascending','dimensionOrder':'numeric'}",
                "{'type':'doubleSum','name':'S','expression':'((- \\'store_sales\\') * 2)'}]"));
  }

  @Test void testAggOnArithmeticProject3() {
    final String sql = "SELECT SUM(-\"store_sales\" * 2)-Max(\"store_cost\" * \"store_cost\") AS S,"
        + "Min(\"store_sales\" + \"store_cost\") as S2 FROM " + FOODMART_TABLE
        + "Group by \"timestamp\" order by s LIMIT 2";
    sql(sql)
        .returnsOrdered("S=-16003.314460250002; S2=1.4768",
            "S=-14181.57; S2=0.8094")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$0, *(-($90), 2), *($91, $91), +($90, $91)]],"
            + " groups=[{0}], aggs=[[SUM($1), MAX($2), MIN($3)]], post_projects=[[-($1, $2), $3]],"
            + " sort0=[0], dir0=[ASC], fetch=[2])")
        .queryContains(
            new DruidChecker(",\"aggregations\":[{\"type\":\"doubleSum\",\"name\":\"$f1\","
                + "\"expression\":\"((- \\\"store_sales\\\") * 2)\"},{\"type\":\"doubleMax\",\"name\""
                + ":\"$f2\",\"expression\":\"(\\\"store_cost\\\" * \\\"store_cost\\\")\"},"
                + "{\"type\":\"doubleMin\",\"name\":\"S2\",\"expression\":\"(\\\"store_sales\\\" "
                + "+ \\\"store_cost\\\")\"}],\"postAggregations\":[{\"type\":\"expression\","
                + "\"name\":\"S\",\"expression\":\"(\\\"$f1\\\" - \\\"$f2\\\")\"}]"));
  }

  @Test void testGroupByVirtualColumn() {
    final String sql =
        "SELECT \"product_id\" || '_' ||\"city\", SUM(\"store_sales\" + "
            + "CAST(\"cost\" AS DOUBLE)) as S FROM " + FOODMART_TABLE
            + "GROUP BY \"product_id\" || '_' || \"city\" LIMIT 2";
    sql(sql)
        .returnsOrdered("EXPR$0=1000_Albany; S=12385.21", "EXPR$0=1000_Altadena; S=8.07")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[||(||($1, '_'), $29), "
            + "+($90, CAST($53):DOUBLE)]], groups=[{0}], aggs=[[SUM($1)]], fetch=[2])")
        .queryContains(
            new DruidChecker("'queryType':'groupBy'",
                "{'type':'doubleSum','name':'S','expression':'(\\'store_sales\\' + CAST(\\'cost\\'",
                "'expression':'concat(concat(\\'product_id\\'",
                "{'type':'default','dimension':'vc','outputName':'vc','outputType':'STRING'}],"
                    + "'virtualColumns':[{'type':'expression','name':'vc"));
  }

  @Test void testCountOverVirtualColumn() {
    final String sql = "SELECT COUNT(\"product_id\" || '_' || \"city\") FROM "
        + FOODMART_TABLE + "WHERE \"state_province\" = 'CA'";
    sql(sql)
        .returnsOrdered("EXPR$0=24441")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], filter=[=($30, 'CA')], projects=[[||(||($1, '_'), $29)]],"
            + " groups=[{}], aggs=[[COUNT($0)]])")
        .queryContains(
            new DruidChecker("\"queryType\":\"timeseries\"",
                "\"aggregator\":{\"type\":\"count\",\"name\":\"EXPR$0\",\"expression\":"
                    + "\"concat(concat(\\\"product_id\\\"",
                "\"aggregations\":[{\"type\":\"filtered\",\"filter\":{\"type\":\"not\",\"field\":"
                    + "{\"type\":\"expression\",\"expression\":\"concat(concat(\\\"product_id\\\""));
  }

  @Test void testAggOverStringToLong() {
    final String sql = "SELECT SUM(cast(\"product_id\" AS INTEGER)) FROM " + FOODMART_TABLE;
    sql(sql)
        .queryContains(
            new DruidChecker("{'queryType':'timeseries','dataSource':'foodmart',"
                + "'descending':false,'granularity':'all','aggregations':[{'type':'longSum',"
                + "'name':'EXPR$0','expression':'CAST(\\'product_id\\'", "LONG"))
        .returnsOrdered("EXPR$0=68222919");
  }

  @Test void testAggOnTimeExtractColumn2() {
    final String sql = "SELECT MAX(EXTRACT(MONTH FROM \"timestamp\")) FROM \"foodmart\"";
    sql(sql)
        .returnsOrdered("EXPR$0=12")
        .queryContains(
            new DruidChecker("{'queryType':'timeseries','dataSource':'foodmart',"
                + "'descending':false,'granularity':'all','aggregations':[{"
                + "'type':'longMax','name':'EXPR$0','expression':'timestamp_extract(\\'__time\\'"));
  }

  @Test void testStackedAggregateFilters() {
    final String sql = "SELECT COUNT(\"product_id\") filter (WHERE \"state_province\" = 'CA' "
        + "OR \"store_sales\" > 100 AND \"product_id\" <> '100'), count(*) FROM " + FOODMART_TABLE;
    final String query = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','aggregations':[{'type':'filtered','filter':{'type':'or','fields':"
        + "[{'type':'selector','dimension':'state_province','value':'CA'},{'type':'and','fields':"
        + "[{'type':'bound','dimension':'store_sales','lower':'100','lowerStrict':true,"
        + "'ordering':'numeric'},{'type':'not','field':{'type':'selector','dimension':'product_id',"
        + "'value':'100'}}]}]},'aggregator':{'type':'filtered','filter':{'type':'not',"
        + "'field':{'type':'selector','dimension':'product_id','value':null}},'aggregator':"
        + "{'type':'count','name':'EXPR$0','fieldName':'product_id'}}},"
        + "{'type':'count','name':'EXPR$1'}],'intervals':['1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z'],'context':{'skipEmptyBuckets':false}}";

    sql(sql)
        .returnsOrdered("EXPR$0=24441; EXPR$1=86829")
        .queryContains(new DruidChecker(query));
  }

  @Test void testCastOverPostAggregates() {
    final String sql =
        "SELECT CAST(COUNT(*) + SUM(\"store_sales\") as INTEGER) FROM " + FOODMART_TABLE;
    sql(sql)
        .returnsOrdered("EXPR$0=652067")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$90]], groups=[{}], "
            + "aggs=[[COUNT(), SUM($0)]], post_projects=[[CAST(+($0, $1)):INTEGER]])");
  }

  @Test void testSubStringOverPostAggregates() {
    final String sql =
        "SELECT \"product_id\", SUBSTRING(\"product_id\" from 1 for 2) FROM " + FOODMART_TABLE
            + " GROUP BY \"product_id\"";
    sql(sql).limit(3).returnsOrdered(
        "product_id=1; EXPR$1=1\nproduct_id=10; EXPR$1=10\nproduct_id=100; EXPR$1=10")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$1]], groups=[{0}], aggs=[[]], "
            + "post_projects=[[$0, SUBSTRING($0, 1, 2)]])");
  }

  @Test void testTableQueryExtractYearQuarter() {
    final String sql = "SELECT * FROM (SELECT CAST((MONTH(\"timestamp\") - 1) / 3 + 1 AS BIGINT)"
        + "AS qr_timestamp_ok,  SUM(\"store_sales\") AS sum_store_sales, YEAR(\"timestamp\") AS yr_timestamp_ok"
        + " FROM \"foodmart\" GROUP BY CAST((MONTH(\"timestamp\") - 1) / 3 + 1 AS BIGINT),"
        + " YEAR(\"timestamp\")) LIMIT_ZERO LIMIT 1";

    final String extract_year = "{\"type\":\"extraction\",\"dimension\":\"__time\",\"outputName\":"
        + "\"extract_year\",\"extractionFn\":{\"type\":\"timeFormat\",\"format\":\"yyyy\","
        + "\"timeZone\":\"UTC\",\"locale\":\"en-US\"}}";

    final String extract_expression = "\"expression\":\"(((timestamp_extract(\\\"__time\\\",";
    CalciteAssert.AssertQuery q = sql(sql)
        .queryContains(
            new DruidChecker("\"queryType\":\"groupBy\"", extract_year, extract_expression))
        .explainContains("PLAN=EnumerableCalc(expr#0..2=[{inputs}], QR_TIMESTAMP_OK=[$t0], "
            + "SUM_STORE_SALES=[$t2], YR_TIMESTAMP_OK=[$t1])\n"
            + "  EnumerableInterpreter\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[+(/(-(EXTRACT(FLAG(MONTH), $0), 1), 3), 1), "
            + "EXTRACT(FLAG(YEAR), $0), $90]], groups=[{0, 1}], aggs=[[SUM($2)]], fetch=[1])");
    q.returnsOrdered("QR_TIMESTAMP_OK=1; SUM_STORE_SALES=139628.35; YR_TIMESTAMP_OK=1997");
  }

  @Test void testTableauQueryExtractMonthDayYear() {
    final String sql = "SELECT * FROM (SELECT (((YEAR(\"foodmart\".\"timestamp\") * 10000) + "
        + "(MONTH(\"foodmart\".\"timestamp\") * 100)) + "
        + "EXTRACT(DAY FROM \"foodmart\".\"timestamp\")) AS md_t_timestamp_ok,\n"
        + "  SUM(\"foodmart\".\"store_sales\") AS sum_t_other_ok\n"
        + "FROM \"foodmart\"\n"
        + "GROUP BY (((YEAR(\"foodmart\".\"timestamp\") * 10000) + (MONTH(\"foodmart\".\"timestamp\")"
        + " * 100)) + EXTRACT(DAY FROM\"foodmart\".\"timestamp\"))) LIMIT 1";
    sql(sql)
        .returnsOrdered("MD_T_TIMESTAMP_OK=19970101; SUM_T_OTHER_OK=706.34")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[+(+(*(EXTRACT(FLAG(YEAR), $0), 10000), "
            + "*(EXTRACT(FLAG(MONTH), $0), 100)), EXTRACT(FLAG(DAY), $0)), $90]], groups=[{0}], "
            + "aggs=[[SUM($1)]], fetch=[1])")
        .queryContains(new DruidChecker("\"queryType\":\"groupBy\""));
  }

  @Test void testTableauQuerySubStringHourMinutes() {
    final String sql = "SELECT * FROM (SELECT CAST(SUBSTRING(CAST(\"foodmart\".\"timestamp\" "
        + " AS VARCHAR) from 12 for 2) AS INT) AS hr_t_timestamp_ok,\n"
        + "  MINUTE(\"foodmart\".\"timestamp\") AS mi_t_timestamp_ok,\n"
        + "  SUM(\"foodmart\".\"store_sales\") AS sum_t_other_ok, EXTRACT(HOUR FROM \"timestamp\") "
        + " AS hr_t_timestamp_ok2 FROM  \"foodmart\" GROUP BY "
        + " CAST(SUBSTRING(CAST(\"foodmart\".\"timestamp\" AS VARCHAR) from 12 for 2 ) AS INT),"
        + "  MINUTE(\"foodmart\".\"timestamp\"), EXTRACT(HOUR FROM \"timestamp\")) LIMIT 1";
    CalciteAssert.AssertQuery q = sql(sql)
        .explainContains("PLAN=EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}], "
            + "SUM_T_OTHER_OK=[$t3], HR_T_TIMESTAMP_OK2=[$t2])\n"
            + "  EnumerableInterpreter\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[CAST(SUBSTRING(CAST($0):VARCHAR"
            + " "
            + "NOT NULL, 12, 2)):INTEGER NOT NULL, EXTRACT(FLAG(MINUTE), $0), "
            + "EXTRACT(FLAG(HOUR), $0), $90]], groups=[{0, 1, 2}], aggs=[[SUM($3)]], fetch=[1])")
        .queryContains(new DruidChecker("\"queryType\":\"groupBy\""));
    q.returnsOrdered("HR_T_TIMESTAMP_OK=0; MI_T_TIMESTAMP_OK=0; "
        + "SUM_T_OTHER_OK=565238.13; HR_T_TIMESTAMP_OK2=0");
  }

  @Test void testTableauQueryMinutesSecondsExtract() {
    final String sql =  "SELECT * FROM (SELECT SECOND(\"timestamp\") AS sc_t_timestamp_ok,"
        + "MINUTE(\"timestamp\") AS mi_t_timestamp_ok,  SUM(\"store_sales\") AS sum_store_sales "
        + " FROM \"foodmart\" GROUP BY SECOND(\"timestamp\"), MINUTE(\"timestamp\"))"
        + " LIMIT_ZERO LIMIT 1";
    CalciteAssert.AssertQuery q = sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[EXTRACT(FLAG(SECOND), $0), "
            + "EXTRACT(FLAG(MINUTE), $0), $90]], groups=[{0, 1}], aggs=[[SUM($2)]], fetch=[1])")
        .queryContains(new DruidChecker("\"queryType\":\"groupBy\""));
    q.returnsOrdered("SC_T_TIMESTAMP_OK=0; MI_T_TIMESTAMP_OK=0; SUM_STORE_SALES=565238.13");
  }

  @Test void testCastConcatOverPostAggregates() {
    final String sql =
        "SELECT CAST(COUNT(*) + SUM(\"store_sales\") as VARCHAR) || '_' || CAST(SUM(\"store_cost\") "
            + "AS VARCHAR) FROM " + FOODMART_TABLE;
    sql(sql)
        .returnsOrdered("EXPR$0=652067.1299999986_225627.2336000002")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$90, $91]], groups=[{}], aggs=[[COUNT(), "
            + "SUM($0), SUM($1)]], post_projects=[[||(||(CAST(+($0, $1)):VARCHAR, '_'), "
            + "CAST($2):VARCHAR)]])");
  }

  @Test void testHavingSpecs() {
    final String sql = "SELECT \"product_id\" AS P, SUM(\"store_sales\") AS S FROM \"foodmart\" "
        + " GROUP BY  \"product_id\" HAVING  SUM(\"store_sales\") > 220  ORDER BY P LIMIT 2";
    CalciteAssert.AssertQuery q = sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$1, $90]], groups=[{0}], aggs=[[SUM($1)]], "
            + "filter=[>($1, 220)], sort0=[0], dir0=[ASC], fetch=[2])")
        .queryContains(
            new DruidChecker("'having':{'type':'filter','filter':{'type':'bound',"
                + "'dimension':'S','lower':'220','lowerStrict':true,'ordering':'numeric'}}"));
    q.returnsOrdered("P=1; S=236.55", "P=10; S=230.04");
  }

  @Test void testTransposableHavingFilter() {
    final String sql = "SELECT \"product_id\" AS P, SUM(\"store_sales\") AS S FROM \"foodmart\" "
        + " GROUP BY  \"product_id\" HAVING  SUM(\"store_sales\") > 220 AND \"product_id\" > '10'"
        + "  ORDER BY P LIMIT 2";
    CalciteAssert.AssertQuery q = sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], filter=[>($1, '10')], projects=[[$1, $90]], groups=[{0}],"
            + " aggs=[[SUM($1)]], filter=[>($1, 220)], sort0=[0], dir0=[ASC], fetch=[2])\n")
        .queryContains(
            new DruidChecker("{'queryType':'groupBy','dataSource':'foodmart','granularity':'all'"));
    q.returnsOrdered("P=100; S=343.2", "P=1000; S=532.62");
  }

  @Test void testProjectSameColumnMultipleTimes() {
    final String sql =
        "SELECT \"product_id\" as prod_id1, \"product_id\" as prod_id2, "
            + "\"store_sales\" as S1, \"store_sales\" as S2 FROM " + FOODMART_TABLE
            + " order by prod_id1 LIMIT 1";
    sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableSort(sort0=[$0], dir0=[ASC], fetch=[1])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$1, $1, $90, $90]])")
        .queryContains(
            new DruidChecker("{'queryType':'scan','dataSource':'foodmart','intervals':"
                + "['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],'virtualColumns':["
                + "{'type':'expression','name':'vc','expression':'\\'product_id\\'','outputType':"
                + "'STRING'},{'type':'expression','name':'vc0','expression':'\\'store_sales\\'',"
                + "'outputType':'DOUBLE'}],'columns':['product_id','vc','store_sales','vc0'],"
                + "'resultFormat':'compactedList'}"))
        .returnsOrdered("PROD_ID1=1; PROD_ID2=1; S1=11.4; S2=11.4");
  }

  @Test void testProjectSameMetricsColumnMultipleTimes() {
    final String sql =
        "SELECT \"product_id\" as prod_id1, \"product_id\" as prod_id2, "
            + "\"store_sales\" as S1, \"store_sales\" as S2 FROM " + FOODMART_TABLE
            + " order by prod_id1 LIMIT 1";
    sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableSort(sort0=[$0], dir0=[ASC], fetch=[1])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$1, $1, $90, $90]])")
        .queryContains(
            new DruidChecker("{\"queryType\":\"scan\",\"dataSource\":\"foodmart\",\"intervals\":"
                + "[\"1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z\"],\"virtualColumns\":"
                + "[{\"type\":\"expression\",\"name\":\"vc\",\"expression\":\"\\\"product_id\\\"\","
                + "\"outputType\":\"STRING\"},{\"type\":\"expression\",\"name\":\"vc0\","
                + "\"expression\":\"\\\"store_sales\\\"\",\"outputType\":\"DOUBLE\"}],\"columns\":"
                + "[\"product_id\",\"vc\",\"store_sales\",\"vc0\"],\"resultFormat\":\"compactedList\"}"))
        .returnsOrdered("PROD_ID1=1; PROD_ID2=1; S1=11.4; S2=11.4");
  }

  @Test void testAggSameColumnMultipleTimes() {
    final String sql =
        "SELECT \"product_id\" as prod_id1, \"product_id\" as prod_id2, "
            + "SUM(\"store_sales\") as S1, SUM(\"store_sales\") as S2 FROM " + FOODMART_TABLE
            + " GROUP BY \"product_id\" ORDER BY prod_id2 LIMIT 1";
    CalciteAssert.AssertQuery q = sql(sql)
        .explainContains("PLAN=EnumerableCalc(expr#0..1=[{inputs}], PROD_ID1=[$t0], "
            + "PROD_ID2=[$t0], S1=[$t1], S2=[$t1])\n"
            + "  EnumerableInterpreter\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$1, $90]], groups=[{0}], aggs=[[SUM($1)]], "
            + "sort0=[0], dir0=[ASC], fetch=[1])")
        .queryContains(
            new DruidChecker("\"queryType\":\"groupBy\""));
    q.returnsOrdered("PROD_ID1=1; PROD_ID2=1; S1=236.55; S2=236.55");
  }

  @Test void testGroupBy1() {
    final String sql = "SELECT SUM(\"store_sales\") FROM \"foodmart\" "
        + "GROUP BY 1 HAVING (COUNT(1) > 0)";
    CalciteAssert.AssertQuery q = sql(sql)
        .queryContains(
            new DruidChecker("{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
                + "'dimensions':[{'type':'default','dimension':'vc','outputName':'vc','outputType':'LONG'}],"
                + "'virtualColumns':[{'type':'expression','name':'vc','expression':'1','outputType':'LONG'}],"
                + "'limitSpec':{'type':'default'},'aggregations':[{'type':'doubleSum','name':'EXPR$0',"
                + "'fieldName':'store_sales'},{'type':'count','name':'$f2'}],'intervals':"
                + "['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],'having':"
                + "{'type':'filter','filter':{'type':'bound','dimension':'$f2','lower':'0',"
                + "'lowerStrict':true,'ordering':'numeric'}}}"));
    q.returnsOrdered("EXPR$0=565238.13");
  }

  @Test void testFloorQuarter() {
    String sql = "SELECT floor(\"timestamp\" TO quarter), SUM(\"store_sales\") FROM "
        + FOODMART_TABLE
        + " GROUP BY floor(\"timestamp\" TO quarter)";

    sql(sql).queryContains(
        new DruidChecker(
            "{\"queryType\":\"timeseries\",\"dataSource\":\"foodmart\",\"descending\":false,"
                + "\"granularity\":{\"type\":\"period\",\"period\":\"P3M\",\"timeZone\":\"UTC\"},"
                + "\"aggregations\":[{\"type\":\"doubleSum\",\"name\":\"EXPR$1\",\"fieldName\":\"store_sales\"}],"
                + "\"intervals\":[\"1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z\"],\"context\":{\"skipEmptyBuckets\":true}}"));
  }

  @Test void testFloorQuarterPlusDim() {
    String sql =
        "SELECT floor(\"timestamp\" TO quarter),\"product_id\",  SUM(\"store_sales\") FROM "
            + FOODMART_TABLE
            + " GROUP BY floor(\"timestamp\" TO quarter), \"product_id\"";

    sql(sql).queryContains(
        new DruidChecker(
            "{\"queryType\":\"groupBy\",\"dataSource\":\"foodmart\",\"granularity\":\"all\",\"dimensions\":"
                + "[{\"type\":\"extraction\",\"dimension\":\"__time\",\"outputName\":\"floor_quarter\",\"extractionFn\":{\"type\":\"timeFormat\"",
            "\"granularity\":{\"type\":\"period\",\"period\":\"P3M\",\"timeZone\":\"UTC\"},\"timeZone\":\"UTC\",\"locale\":\"und\"}},"
                + "{\"type\":\"default\",\"dimension\":\"product_id\",\"outputName\":\"product_id\",\"outputType\":\"STRING\"}],"
                + "\"limitSpec\":{\"type\":\"default\"},\"aggregations\":[{\"type\":\"doubleSum\",\"name\":\"EXPR$2\",\"fieldName\":\"store_sales\"}],"
                + "\"intervals\":[\"1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z\"]}"));
  }


  @Test void testExtractQuarterPlusDim() {
    String sql =
        "SELECT EXTRACT(quarter from \"timestamp\"),\"product_id\",  SUM(\"store_sales\") FROM "
            + FOODMART_TABLE
            + " WHERE \"product_id\" = 1"
            + " GROUP BY EXTRACT(quarter from \"timestamp\"), \"product_id\"";

    CalciteAssert.AssertQuery q = sql(sql)
        .queryContains(
            new DruidChecker(
                "{\"queryType\":\"groupBy\",\"dataSource\":\"foodmart\",\"granularity\":\"all\",\"dimensions\":"
                    + "[{\"type\":\"default\",\"dimension\":\"vc\",\"outputName\":\"vc\",\"outputType\":\"LONG\"},"
                    + "{\"type\":\"default\",\"dimension\":\"product_id\",\"outputName\":\"product_id\",\"outputType\":\"STRING\"}],"
                    + "\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"vc\",\"expression\":\"timestamp_extract(\\\"__time\\\",",
                "QUARTER"));
    q.returnsOrdered("EXPR$0=1; product_id=1; EXPR$2=37.05\n"
        + "EXPR$0=2; product_id=1; EXPR$2=62.7\n"
        + "EXPR$0=3; product_id=1; EXPR$2=88.35\n"
        + "EXPR$0=4; product_id=1; EXPR$2=48.45");
  }

  @Test void testExtractQuarter() {
    String sql = "SELECT EXTRACT(quarter from \"timestamp\"),  SUM(\"store_sales\") FROM "
        + FOODMART_TABLE
        + " GROUP BY EXTRACT(quarter from \"timestamp\")";

    CalciteAssert.AssertQuery q = sql(sql)
        .queryContains(
            new DruidChecker(
                "{\"queryType\":\"groupBy\",\"dataSource\":\"foodmart\",\"granularity\":\"all\","
                    + "\"dimensions\":[{\"type\":\"default\",\"dimension\":\"vc\",\"outputName\":\"vc\",\"outputType\":\"LONG\"}],"
                    + "\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"vc\",\"expression\":\"timestamp_extract(\\\"__time\\\",",
                "QUARTER"));
    q.returnsOrdered("EXPR$0=1; EXPR$1=139628.35\n"
        + "EXPR$0=2; EXPR$1=132666.27\n"
        + "EXPR$0=3; EXPR$1=140271.89\n"
        + "EXPR$0=4; EXPR$1=152671.62");
  }

  @Test void testCastTimestamp1() {
    final String sql = "Select cast(\"timestamp\" as varchar) as t"
        + " from \"foodmart\" order by t limit 1";

    sql(sql)
        .returnsOrdered("T=1997-01-01 00:00:00")
        .queryContains(
            new DruidChecker("UTC"));
  }

  @Test void testCastTimestamp2() {
    final String sql = "Select cast(cast(\"timestamp\" as timestamp) as varchar) as t"
        + " from \"foodmart\" order by t limit 1";

    sql(sql)
        .returnsOrdered("T=1997-01-01 00:00:00")
        .queryContains(
            new DruidChecker("UTC"));
  }
}
