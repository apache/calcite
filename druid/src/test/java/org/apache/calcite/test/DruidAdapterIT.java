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

import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import org.junit.Ignore;
import org.junit.Test;

import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@code org.apache.calcite.adapter.druid} package.
 *
 * <p>Before calling this test, you need to populate Druid, as follows:
 *
 * <blockquote><code>
 * git clone https://github.com/vlsi/calcite-test-dataset<br>
 * cd calcite-test-dataset<br>
 * mvn install
 * </code></blockquote>
 *
 * <p>This will create a virtual machine with Druid and test data set.
 *
 * <p>Features not yet implemented:
 * <ul>
 *   <li>push LIMIT into "select" query</li>
 *   <li>push SORT and/or LIMIT into "groupBy" query</li>
 *   <li>push HAVING into "groupBy" query</li>
 * </ul>
 */
public class DruidAdapterIT {
  /** URL of the "druid-foodmart" model. */
  public static final URL FOODMART =
      DruidAdapterIT.class.getResource("/druid-foodmart-model.json");

  /** URL of the "druid-wiki" model
   * and the "wikiticker" data set. */
  public static final URL WIKI =
      DruidAdapterIT.class.getResource("/druid-wiki-model.json");

  /** URL of the "druid-wiki-no-columns" model
   * and the "wikiticker" data set. */
  public static final URL WIKI_AUTO =
      DruidAdapterIT.class.getResource("/druid-wiki-no-columns-model.json");

  /** URL of the "druid-wiki-no-tables" model
   * and the "wikiticker" data set. */
  public static final URL WIKI_AUTO2 =
      DruidAdapterIT.class.getResource("/druid-wiki-no-tables-model.json");

  /** Whether to run Druid tests. Enabled by default, however test is only
   * included if "it" profile is activated ({@code -Pit}). To disable,
   * specify {@code -Dcalcite.test.druid=false} on the Java command line. */
  public static final boolean ENABLED =
      Util.getBooleanProperty("calcite.test.druid", true);

  private static final String VARCHAR_TYPE =
      "VARCHAR CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"";

  /** Whether to run this test. */
  protected boolean enabled() {
    return ENABLED;
  }

  /** Returns a function that checks that a particular Druid query is
   * generated to implement a query. */
  private static Function<List, Void> druidChecker(final String... lines) {
    return new Function<List, Void>() {
      public Void apply(List list) {
        assertThat(list.size(), is(1));
        DruidQuery.QuerySpec querySpec = (DruidQuery.QuerySpec) list.get(0);
        for (String line : lines) {
          final String s = line.replace('\'', '"');
          assertThat(querySpec.getQueryString(null, -1), containsString(s));
        }
        return null;
      }
    };
  }

  /**
   * Creates a query against FOODMART with approximate parameters
   * */
  private CalciteAssert.AssertQuery foodmartApprox(String sql) {
    return approxQuery(FOODMART, sql);
  }

  /**
   * Creates a query against WIKI with approximate parameters
   * */
  private CalciteAssert.AssertQuery wikiApprox(String sql) {
    return approxQuery(WIKI, sql);
  }

  private CalciteAssert.AssertQuery approxQuery(URL url, String sql) {
    return CalciteAssert.that()
            .enable(enabled())
            .with(ImmutableMap.of("model", url.getPath()))
            .with(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT.camelName(), true)
            .with(CalciteConnectionProperty.APPROXIMATE_TOP_N.camelName(), true)
            .with(CalciteConnectionProperty.APPROXIMATE_DECIMAL.camelName(), true)
            .query(sql);
  }

  /** Creates a query against a data set given by a map. */
  private CalciteAssert.AssertQuery sql(String sql, URL url) {
    return CalciteAssert.that()
        .enable(enabled())
        .with(ImmutableMap.of("model", url.getPath()))
        .query(sql);
  }

  /** Creates a query against the {@link #FOODMART} data set. */
  private CalciteAssert.AssertQuery sql(String sql) {
    return sql(sql, FOODMART);
  }

  /** Tests a query against the {@link #WIKI} data set.
   *
   * <p>Most of the others in this suite are against {@link #FOODMART},
   * but our examples in "druid-adapter.md" use wikiticker. */
  @Test public void testSelectDistinctWiki() {
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[wiki, wiki]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[=($13, 'Jeremy Corbyn')], groups=[{5}], aggs=[[]])\n";
    checkSelectDistinctWiki(WIKI, "wiki")
        .explainContains(explain);
  }

  @Test public void testSelectDistinctWikiNoColumns() {
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[wiki, wiki]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[=($17, 'Jeremy Corbyn')], groups=[{7}], aggs=[[]])\n";
    checkSelectDistinctWiki(WIKI_AUTO, "wiki")
        .explainContains(explain);
  }

  @Test public void testSelectDistinctWikiNoTables() {
    // Compared to testSelectDistinctWiki, table name is different (because it
    // is the raw dataSource name from Druid) and the field offsets are
    // different. This is expected.
    // Interval is different, as default is taken.
    final String sql = "select distinct \"countryName\"\n"
        + "from \"wikiticker\"\n"
        + "where \"page\" = 'Jeremy Corbyn'";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[wiki, wikiticker]], "
        + "intervals=[[1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z]], "
        + "filter=[=($17, 'Jeremy Corbyn')], groups=[{7}], aggs=[[]])\n";
    final String druidQuery = "{'queryType':'groupBy',"
        + "'dataSource':'wikiticker','granularity':'all',"
        + "'dimensions':[{'type':'default','dimension':'countryName'}],'limitSpec':{'type':'default'},"
        + "'filter':{'type':'selector','dimension':'page','value':'Jeremy Corbyn'},"
        + "'aggregations':[],"
        + "'intervals':['1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z']}";
    sql(sql, WIKI_AUTO2)
        .returnsUnordered("countryName=United Kingdom",
            "countryName=null")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
    // Because no tables are declared, foodmart is automatically present.
    sql("select count(*) as c from \"foodmart\"", WIKI_AUTO2)
        .returnsUnordered("C=86829");
  }

  @Test public void testSelectTimestampColumnNoTables1() {
    // Since columns are not explicitly declared, we use the default time
    // column in the query.
    final String sql = "select sum(\"added\")\n"
        + "from \"wikiticker\"\n"
        + "group by floor(\"__time\" to DAY)";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableProject(EXPR$0=[$1])\n"
        + "    DruidQuery(table=[[wiki, wikiticker]], intervals=[[1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z]], projects=[[FLOOR($0, FLAG(DAY)), $1]], groups=[{0}], aggs=[[SUM($1)]])\n";
    final String druidQuery = "{'queryType':'timeseries',"
        + "'dataSource':'wikiticker','descending':false,'granularity':'day',"
        + "'aggregations':[{'type':'longSum','name':'EXPR$0','fieldName':'added'}],"
        + "'intervals':['1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':true}}";
    sql(sql, WIKI_AUTO2)
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testSelectTimestampColumnNoTables2() {
    // Since columns are not explicitly declared, we use the default time
    // column in the query.
    final String sql = "select cast(\"__time\" as timestamp) as \"__time\"\n"
        + "from \"wikiticker\"\n"
        + "limit 1\n";
    final String explain =
        "DruidQuery(table=[[wiki, wikiticker]], intervals=[[1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z]], projects=[[$0]], fetch=[1])\n";
    final String druidQuery = "{'queryType':'scan',"
        + "'dataSource':'wikiticker',"
        + "'intervals':['1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z'],"
        + "'columns':['__time'],'granularity':'all',"
        + "'resultFormat':'compactedList','limit':1}";

    sql(sql, WIKI_AUTO2)
        .returnsUnordered("__time=2015-09-12 00:46:58")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testSelectTimestampColumnNoTables3() {
    // Since columns are not explicitly declared, we use the default time
    // column in the query.
    final String sql =
        "select cast(floor(\"__time\" to DAY) as timestamp) as \"day\", sum(\"added\")\n"
        + "from \"wikiticker\"\n"
        + "group by floor(\"__time\" to DAY)";
    final String explain =
        "DruidQuery(table=[[wiki, wikiticker]], intervals=[[1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z]], projects=[[FLOOR($0, FLAG(DAY)), $1]], groups=[{0}], aggs=[[SUM($1)]])\n";
    final String druidQuery = "{'queryType':'timeseries',"
        + "'dataSource':'wikiticker','descending':false,'granularity':'day',"
        + "'aggregations':[{'type':'longSum','name':'EXPR$1','fieldName':'added'}],"
        + "'intervals':['1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':true}}";
    sql(sql, WIKI_AUTO2)
        .returnsUnordered("day=2015-09-12 00:00:00; EXPR$1=9385573")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testSelectTimestampColumnNoTables4() {
    // Since columns are not explicitly declared, we use the default time
    // column in the query.
    final String sql = "select sum(\"added\") as \"s\", \"page\", "
        + "cast(floor(\"__time\" to DAY) as timestamp) as \"day\"\n"
        + "from \"wikiticker\"\n"
        + "group by \"page\", floor(\"__time\" to DAY)\n"
        + "order by \"s\" desc";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableProject(s=[$2], page=[$0], day=[CAST($1):TIMESTAMP(0) NOT NULL])\n"
        + "    DruidQuery(table=[[wiki, wikiticker]], "
        + "intervals=[[1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z]], projects=[[$17, FLOOR"
        + "($0, FLAG(DAY)), $1]], groups=[{0, 1}], aggs=[[SUM($2)]], sort0=[2], dir0=[DESC])";
    sql(sql, WIKI_AUTO2)
        .limit(1)
        .returnsUnordered("s=199818; page=User:QuackGuru/Electronic cigarettes 1; "
            + "day=2015-09-12 00:00:00")
        .explainContains(explain)
        .queryContains(
            druidChecker("'queryType':'groupBy'", "'limitSpec':{'type':'default',"
            + "'columns':[{'dimension':'s','direction':'descending','dimensionOrder':'numeric'}]}"));
  }

  @Test public void testSkipEmptyBuckets() {
    final String sql =
        "select cast(floor(\"__time\" to SECOND) as timestamp) as \"second\", sum(\"added\")\n"
        + "from \"wikiticker\"\n"
        + "where \"page\" = 'Jeremy Corbyn'\n"
        + "group by floor(\"__time\" to SECOND)";
    final String druidQuery = "{'queryType':'timeseries',"
        + "'dataSource':'wikiticker','descending':false,'granularity':'second',"
        + "'filter':{'type':'selector','dimension':'page','value':'Jeremy Corbyn'},"
        + "'aggregations':[{'type':'longSum','name':'EXPR$1','fieldName':'added'}],"
        + "'intervals':['1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':true}}";
    sql(sql, WIKI_AUTO2)
        .limit(1)
        // Result without 'skipEmptyBuckets':true => "second=2015-09-12 00:46:58; EXPR$1=0"
        .returnsUnordered("second=2015-09-12 01:20:19; EXPR$1=1075")
        .queryContains(druidChecker(druidQuery));
  }

  private CalciteAssert.AssertQuery checkSelectDistinctWiki(URL url, String tableName) {
    final String sql = "select distinct \"countryName\"\n"
        + "from \"" + tableName + "\"\n"
        + "where \"page\" = 'Jeremy Corbyn'";
    final String druidQuery = "{'queryType':'groupBy',"
        + "'dataSource':'wikiticker','granularity':'all',"
        + "'dimensions':[{'type':'default','dimension':'countryName'}],'limitSpec':{'type':'default'},"
        + "'filter':{'type':'selector','dimension':'page','value':'Jeremy Corbyn'},"
        + "'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    return sql(sql, url)
        .returnsUnordered("countryName=United Kingdom",
            "countryName=null")
        .queryContains(druidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1617">[CALCITE-1617]
   * Druid adapter: Send timestamp literals to Druid as local time, not
   * UTC</a>. */
  @Test public void testFilterTime() {
    final String sql = "select cast(\"__time\" as timestamp) as \"__time\"\n"
        + "from \"wikiticker\"\n"
        + "where \"__time\" < '2015-10-12 00:00:00 UTC'";
    final String explain = "\n    DruidQuery(table=[[wiki, wikiticker]], "
        + "intervals=[[1900-01-01T00:00:00.000Z/2015-10-12T00:00:00.000Z]], "
        + "projects=[[$0]])\n";
    final String druidQuery = "{'queryType':'scan',"
        + "'dataSource':'wikiticker',"
        + "'intervals':['1900-01-01T00:00:00.000Z/2015-10-12T00:00:00.000Z'],"
        + "'columns':['__time'],'granularity':'all',"
        + "'resultFormat':'compactedList'";
    sql(sql, WIKI_AUTO2)
        .limit(2)
        .returnsUnordered("__time=2015-09-12 00:46:58",
            "__time=2015-09-12 00:47:00")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testFilterTimeDistinct() {
    final String sql = "select CAST(\"c1\" AS timestamp) as \"__time\" from\n"
        + "(select distinct \"__time\" as \"c1\"\n"
        + "from \"wikiticker\"\n"
        + "where \"__time\" < '2015-10-12 00:00:00 UTC')";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableProject(__time=[CAST($0):TIMESTAMP(0) NOT NULL])\n"
        + "    DruidQuery(table=[[wiki, wikiticker]], "
        + "intervals=[[1900-01-01T00:00:00.000Z/2015-10-12T00:00:00.000Z]], "
        + "groups=[{0}], aggs=[[]])\n";
    final String subDruidQuery = "{'queryType':'groupBy','dataSource':'wikiticker',"
        + "'granularity':'all','dimensions':[{'type':'extraction',"
        + "'dimension':'__time','outputName':'extract',"
        + "'extractionFn':{'type':'timeFormat'";
    sql(sql, WIKI_AUTO2)
        .limit(2)
        .explainContains(explain)
        .queryContains(druidChecker(subDruidQuery))
        .returnsUnordered("__time=2015-09-12 00:46:58",
            "__time=2015-09-12 00:47:00");
  }

  @Test public void testMetadataColumns() throws Exception {
    sql("values 1")
        .withConnection(
            new Function<Connection, Void>() {
              public Void apply(Connection c) {
                try {
                  final DatabaseMetaData metaData = c.getMetaData();
                  final ResultSet r =
                      metaData.getColumns(null, null, "foodmart", null);
                  Multimap<String, Boolean> map = ArrayListMultimap.create();
                  while (r.next()) {
                    map.put(r.getString("TYPE_NAME"), true);
                  }
                  System.out.println(map);
                  // 1 timestamp, 2 float measure, 1 int measure, 88 dimensions
                  assertThat(map.keySet().size(), is(4));
                  assertThat(map.values().size(), is(92));
                  assertThat(map.get("TIMESTAMP_WITH_LOCAL_TIME_ZONE(0) NOT NULL").size(), is(1));
                  assertThat(map.get("DOUBLE").size(), is(2));
                  assertThat(map.get("BIGINT").size(), is(1));
                  assertThat(map.get(VARCHAR_TYPE).size(), is(88));
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
                return null;
              }
            });
  }

  @Test public void testSelectDistinct() {
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{30}], aggs=[[]])";
    final String sql = "select distinct \"state_province\" from \"foodmart\"";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
        + "'dimensions':[{'type':'default','dimension':'state_province'}],'limitSpec':{'type':'default'},"
        + "'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql)
        .returnsUnordered("state_province=CA",
            "state_province=OR",
            "state_province=WA")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testSelectGroupBySum() {
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableAggregate(group=[{0}], U=[SUM($1)])\n"
        + "    BindableProject(state_province=[$0], $f1=[CAST($1):INTEGER])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]],"
        + " projects=[[$30, $89]])";
    final String sql = "select \"state_province\", sum(cast(\"unit_sales\" as integer)) as u\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\"";
    sql(sql)
        .returnsUnordered("state_province=CA; U=74748",
            "state_province=OR; U=67659",
            "state_province=WA; U=124366")
        .explainContains(explain);
  }

  @Test public void testGroupbyMetric() {
    final String sql = "select  \"store_sales\" ,\"product_id\" from \"foodmart\" "
            + "where \"product_id\" = 1020" + "group by \"store_sales\" ,\"product_id\" ";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[=($1, 1020)],"
        + " projects=[[$90, $1]], groups=[{0, 1}], aggs=[[]])";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
            + "'dimensions':[{'type':'default','dimension':'store_sales'},"
            + "{'type':'default','dimension':'product_id'}],'limitSpec':{'type':'default'},'"
            + "filter':{'type':'selector','dimension':'product_id','value':'1020'},"
            + "'aggregations':[],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql)
        .explainContains(plan)
        .queryContains(druidChecker(druidQuery))
        .returnsUnordered("store_sales=0.51; product_id=1020",
            "store_sales=1.02; product_id=1020",
            "store_sales=1.53; product_id=1020",
            "store_sales=2.04; product_id=1020",
            "store_sales=2.55; product_id=1020");
  }

  @Test public void testPushSimpleGroupBy() {
    final String sql = "select \"product_id\" from \"foodmart\" where "
            + "\"product_id\" = 1020 group by \"product_id\"";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
            + "'granularity':'all','dimensions':[{'type':'default',"
            + "'dimension':'product_id'}],"
            + "'limitSpec':{'type':'default'},'filter':{'type':'selector',"
            + "'dimension':'product_id','value':'1020'},"
            + "'aggregations':[],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql).queryContains(druidChecker(druidQuery)).returnsUnordered("product_id=1020");
  }

  @Test public void testComplexPushGroupBy() {
    final String innerQuery = "select \"product_id\" as \"id\" from \"foodmart\" where "
            + "\"product_id\" = 1020";
    final String sql = "select \"id\" from (" + innerQuery + ") group by \"id\"";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
            + "'granularity':'all',"
            + "'dimensions':[{'type':'default','dimension':'product_id'}],"
            + "'limitSpec':{'type':'default'},"
            + "'filter':{'type':'selector','dimension':'product_id','value':'1020'},"
            + "'aggregations':[],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql)
        .returnsUnordered("id=1020")
        .queryContains(druidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1281">[CALCITE-1281]
   * Druid adapter wrongly returns all numeric values as int or float</a>. */
  @Test public void testSelectCount() {
    final String sql = "select count(*) as c from \"foodmart\"";
    sql(sql)
        .returns(new Function<ResultSet, Void>() {
          public Void apply(ResultSet input) {
            try {
              assertThat(input.next(), is(true));
              assertThat(input.getInt(1), is(86829));
              assertThat(input.getLong(1), is(86829L));
              assertThat(input.getString(1), is("86829"));
              assertThat(input.wasNull(), is(false));
              assertThat(input.next(), is(false));
              return null;
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
        });
  }

  @Test public void testSort() {
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
            druidChecker("{'queryType':'groupBy','dataSource':'foodmart',"
                + "'granularity':'all','dimensions':[{'type':'default',"
                + "'dimension':'gender'},{'type':'default',"
                + "'dimension':'state_province'}],'limitSpec':{'type':'default',"
                + "'columns':[{'dimension':'state_province','direction':'ascending',"
                + "'dimensionOrder':'alphanumeric'},{'dimension':'gender',"
                + "'direction':'descending','dimensionOrder':'alphanumeric'}]},"
                + "'aggregations':[],"
                + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}"))
        .explainContains(explain);
  }

  @Test public void testSortLimit() {
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC], offset=[2], fetch=[3])\n"
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

  @Test public void testOffsetLimit() {
    // We do not yet push LIMIT into a Druid "select" query as a "threshold".
    // It is not possible to push OFFSET into Druid "select" query.
    final String sql = "select \"state_province\", \"product_name\"\n"
        + "from \"foodmart\"\n"
        + "offset 2 fetch next 3 rows only";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'columns':['state_province','product_name'],'granularity':'all',"
        + "'resultFormat':'compactedList'}";
    sql(sql)
        .runs()
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testLimit() {
    final String sql = "select \"gender\", \"state_province\"\n"
        + "from \"foodmart\" fetch next 3 rows only";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'columns':['gender','state_province'],'granularity':'all',"
        + "'resultFormat':'compactedList','limit':3";
    sql(sql)
        .runs()
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testDistinctLimit() {
    final String sql = "select distinct \"gender\", \"state_province\"\n"
        + "from \"foodmart\" fetch next 3 rows only";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default','dimension':'gender'},"
        + "{'type':'default','dimension':'state_province'}],'limitSpec':{'type':'default',"
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
        .queryContains(druidChecker(druidQuery))
        .returnsUnordered("gender=F; state_province=CA", "gender=F; state_province=OR",
            "gender=F; state_province=WA");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1578">[CALCITE-1578]
   * Druid adapter: wrong semantics of topN query limit with granularity</a>. */
  @Test public void testGroupBySortLimit() {
    final String sql = "select \"brand_name\", \"gender\", sum(\"unit_sales\") as s\n"
        + "from \"foodmart\"\n"
        + "group by \"brand_name\", \"gender\"\n"
        + "order by s desc limit 3";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default',"
        + "'dimension':'brand_name'},{'type':'default','dimension':'gender'}],"
        + "'limitSpec':{'type':'default','limit':3,'columns':[{'dimension':'S',"
        + "'direction':'descending','dimensionOrder':'numeric'}]},"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "groups=[{2, 39}], aggs=[[SUM($89)]], sort0=[2], dir0=[DESC], fetch=[3])\n";
    sql(sql)
        .runs()
        .returnsOrdered("brand_name=Hermanos; gender=M; S=4286",
            "brand_name=Hermanos; gender=F; S=4183",
            "brand_name=Tell Tale; gender=F; S=4033")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1587">[CALCITE-1587]
   * Druid adapter: topN returns approximate results</a>. */
  @Test public void testGroupBySingleSortLimit() {
    checkGroupBySingleSortLimit(false);
  }

  /** As {@link #testGroupBySingleSortLimit}, but allowing approximate results
   * due to {@link CalciteConnectionConfig#approximateDistinctCount()}.
   * Therefore we send a "topN" query to Druid. */
  @Test public void testGroupBySingleSortLimitApprox() {
    checkGroupBySingleSortLimit(true);
  }

  private void checkGroupBySingleSortLimit(boolean approx) {
    final String sql = "select \"brand_name\", sum(\"unit_sales\") as s\n"
        + "from \"foodmart\"\n"
        + "group by \"brand_name\"\n"
        + "order by s desc limit 3";
    final String approxDruid = "{'queryType':'topN','dataSource':'foodmart','granularity':'all',"
        + "'dimension':{'type':'default','dimension':'brand_name'},'metric':'S',"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'threshold':3}";
    final String exactDruid = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default',"
        + "'dimension':'brand_name'}],'limitSpec':{'type':'default','limit':3,"
        + "'columns':[{'dimension':'S','direction':'descending',"
        + "'dimensionOrder':'numeric'}]},'aggregations':[{'type':'longSum',"
        + "'name':'S','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String druidQuery = approx ? approxDruid : exactDruid;
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "groups=[{2}], aggs=[[SUM($89)]], sort0=[1], dir0=[DESC], fetch=[3])\n";
    CalciteAssert.that()
        .enable(enabled())
        .with(ImmutableMap.of("model", FOODMART.getPath()))
        .with(CalciteConnectionProperty.APPROXIMATE_TOP_N.name(), approx)
        .query(sql)
        .runs()
        .returnsOrdered("brand_name=Hermanos; S=8469",
            "brand_name=Tell Tale; S=7877",
            "brand_name=Ebony; S=7438")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1578">[CALCITE-1578]
   * Druid adapter: wrong semantics of groupBy query limit with granularity</a>.
   *
   * <p>Before CALCITE-1578 was fixed, this would use a "topN" query but return
   * the wrong results. */
  @Test public void testGroupByDaySortDescLimit() {
    final String sql = "select \"brand_name\","
        + " cast(floor(\"timestamp\" to DAY) as timestamp) as d,"
        + " sum(\"unit_sales\") as s\n"
        + "from \"foodmart\"\n"
        + "group by \"brand_name\", floor(\"timestamp\" to DAY)\n"
        + "order by s desc limit 30";
    final String explain =
        "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$2, FLOOR"
        + "($0, FLAG(DAY)), $89]], groups=[{0, 1}], aggs=[[SUM($2)]], sort0=[2], dir0=[DESC], "
        + "fetch=[30])";
    sql(sql)
        .runs()
        .returnsStartingWith("brand_name=Ebony; D=1997-07-27 00:00:00; S=135",
            "brand_name=Tri-State; D=1997-05-09 00:00:00; S=120",
            "brand_name=Hermanos; D=1997-05-09 00:00:00; S=115")
        .explainContains(explain)
        .queryContains(
            druidChecker("'queryType':'groupBy'", "'granularity':'all'", "'limitSpec"
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
  @Test public void testGroupByDaySortLimit() {
    final String sql = "select \"brand_name\","
        + " cast(floor(\"timestamp\" to DAY) as timestamp) as d,"
        + " sum(\"unit_sales\") as s\n"
        + "from \"foodmart\"\n"
        + "group by \"brand_name\", floor(\"timestamp\" to DAY)\n"
        + "order by s desc limit 30";
    final String druidQueryPart1 = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default',"
        + "'dimension':'brand_name'},{'type':'extraction','dimension':'__time',"
        + "'outputName':'floor_day','extractionFn':{'type':'timeFormat'";
    final String druidQueryPart2 = "'limitSpec':{'type':'default','limit':30,"
        + "'columns':[{'dimension':'S','direction':'descending',"
        + "'dimensionOrder':'numeric'}]},'aggregations':[{'type':'longSum',"
        + "'name':'S','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String explain = "DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$2, FLOOR"
        + "($0, FLAG(DAY)), $89]], groups=[{0, 1}], aggs=[[SUM($2)]], sort0=[2], dir0=[DESC], "
        + "fetch=[30])";
    sql(sql)
        .runs()
        .returnsStartingWith("brand_name=Ebony; D=1997-07-27 00:00:00; S=135",
            "brand_name=Tri-State; D=1997-05-09 00:00:00; S=120",
            "brand_name=Hermanos; D=1997-05-09 00:00:00; S=115")
        .explainContains(explain)
        .queryContains(druidChecker(druidQueryPart1, druidQueryPart2));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1580">[CALCITE-1580]
   * Druid adapter: Wrong semantics for ordering within groupBy queries</a>. */
  @Test public void testGroupByDaySortDimension() {
    final String sql =
        "select \"brand_name\", cast(floor(\"timestamp\" to DAY) as timestamp) as d,"
        + " sum(\"unit_sales\") as s\n"
        + "from \"foodmart\"\n"
        + "group by \"brand_name\", floor(\"timestamp\" to DAY)\n"
        + "order by \"brand_name\"";
    final String subDruidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default',"
        + "'dimension':'brand_name'},{'type':'extraction','dimension':'__time',"
        + "'outputName':'floor_day','extractionFn':{'type':'timeFormat'";
    final String explain = "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$2, FLOOR"
        + "($0, FLAG(DAY)), $89]], groups=[{0, 1}], aggs=[[SUM($2)]], sort0=[0], dir0=[ASC])";
    sql(sql)
        .runs()
        .returnsStartingWith("brand_name=ADJ; D=1997-01-11 00:00:00; S=2",
            "brand_name=ADJ; D=1997-01-12 00:00:00; S=3",
            "brand_name=ADJ; D=1997-01-17 00:00:00; S=3")
        .explainContains(explain)
        .queryContains(druidChecker(subDruidQuery));
  }

  /** Tests a query that contains no GROUP BY and is therefore executed as a
   * Druid "select" query. */
  @Test public void testFilterSortDesc() {
    final String sql = "select \"product_name\" from \"foodmart\"\n"
        + "where \"product_id\" BETWEEN '1500' AND '1502'\n"
        + "order by \"state_province\" desc, \"product_id\"";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'filter':{'type':'and','fields':["
        + "{'type':'bound','dimension':'product_id','lower':'1500','lowerStrict':false,'ordering':'lexicographic'},"
        + "{'type':'bound','dimension':'product_id','upper':'1502','upperStrict':false,'ordering':'lexicographic'}]},"
        + "'columns':['product_name','state_province','product_id'],'granularity':'all',"
        + "'resultFormat':'compactedList'";
    sql(sql)
        .limit(4)
        .returns(
            new Function<ResultSet, Void>() {
              public Void apply(ResultSet resultSet) {
                try {
                  for (int i = 0; i < 4; i++) {
                    assertTrue(resultSet.next());
                    assertThat(resultSet.getString("product_name"),
                        is("Fort West Dried Apricots"));
                  }
                  assertFalse(resultSet.next());
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            })
        .queryContains(druidChecker(druidQuery));
  }

  /** As {@link #testFilterSortDesc()} but the bounds are numeric. */
  @Test public void testFilterSortDescNumeric() {
    final String sql = "select \"product_name\" from \"foodmart\"\n"
        + "where \"product_id\" BETWEEN 1500 AND 1502\n"
        + "order by \"state_province\" desc, \"product_id\"";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'filter':{'type':'and','fields':["
        + "{'type':'bound','dimension':'product_id','lower':'1500','lowerStrict':false,'ordering':'numeric'},"
        + "{'type':'bound','dimension':'product_id','upper':'1502','upperStrict':false,'ordering':'numeric'}]},"
        + "'columns':['product_name','state_province','product_id'],'granularity':'all',"
        + "'resultFormat':'compactedList'";
    sql(sql)
        .limit(4)
        .returns(
            new Function<ResultSet, Void>() {
              public Void apply(ResultSet resultSet) {
                try {
                  for (int i = 0; i < 4; i++) {
                    assertTrue(resultSet.next());
                    assertThat(resultSet.getString("product_name"),
                        is("Fort West Dried Apricots"));
                  }
                  assertFalse(resultSet.next());
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            })
        .queryContains(druidChecker(druidQuery));
  }

  /** Tests a query whose filter removes all rows. */
  @Test public void testFilterOutEverything() {
    final String sql = "select \"product_name\" from \"foodmart\"\n"
        + "where \"product_id\" = -1";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'filter':{'type':'selector','dimension':'product_id','value':'-1'},"
        + "'columns':['product_name'],'granularity':'all',"
        + "'resultFormat':'compactedList'}";
    sql(sql)
        .limit(4)
        .returnsUnordered()
        .queryContains(druidChecker(druidQuery));
  }

  /** As {@link #testFilterSortDescNumeric()} but with a filter that cannot
   * be pushed down to Druid. */
  @Test public void testNonPushableFilterSortDesc() {
    final String sql = "select \"product_name\" from \"foodmart\"\n"
        + "where cast(\"product_id\" as integer) - 1500 BETWEEN 0 AND 2\n"
        + "order by \"state_province\" desc, \"product_id\"";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'columns':['product_id','product_name','state_province'],'granularity':'all',"
        + "'resultFormat':'compactedList'}";
    sql(sql)
        .limit(4)
        .returns(
            new Function<ResultSet, Void>() {
              public Void apply(ResultSet resultSet) {
                try {
                  for (int i = 0; i < 4; i++) {
                    assertTrue(resultSet.next());
                    assertThat(resultSet.getString("product_name"),
                        is("Fort West Dried Apricots"));
                  }
                  assertFalse(resultSet.next());
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            })
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testUnionPlan() {
    final String sql = "select distinct \"gender\" from \"foodmart\"\n"
        + "union all\n"
        + "select distinct \"marital_status\" from \"foodmart\"";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableUnion(all=[true])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{39}], aggs=[[]])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{37}], aggs=[[]])";
    sql(sql)
        .explainContains(explain)
        .returnsUnordered(
            "gender=F",
            "gender=M",
            "gender=M",
            "gender=S");
  }

  @Test public void testFilterUnionPlan() {
    final String sql = "select * from (\n"
        + "  select distinct \"gender\" from \"foodmart\"\n"
        + "  union all\n"
        + "  select distinct \"marital_status\" from \"foodmart\")\n"
        + "where \"gender\" = 'M'";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableFilter(condition=[=($0, 'M')])\n"
        + "    BindableUnion(all=[true])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{39}], aggs=[[]])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{37}], aggs=[[]])";
    sql(sql)
        .explainContains(explain)
        .returnsUnordered("gender=M",
            "gender=M");
  }

  @Test public void testCountGroupByEmpty() {
    final String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart',"
        + "'descending':false,'granularity':'all',"
        + "'aggregations':[{'type':'count','name':'EXPR$0'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':false}}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[]], groups=[{}], aggs=[[COUNT()]])";
    final String sql = "select count(*) from \"foodmart\"";
    sql(sql)
        .returns("EXPR$0=86829\n")
        .queryContains(druidChecker(druidQuery))
        .explainContains(explain);
  }

  @Test public void testGroupByOneColumnNotProjected() {
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
  @Test public void testGroupByTimeAndOneColumnNotProjectedWithLimit() {
    final String sql = "select count(*) as \"c\","
        + " cast(floor(\"timestamp\" to MONTH) as timestamp) as \"month\"\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH), \"state_province\"\n"
        + "order by \"c\" desc limit 3";
    sql(sql)
        .returnsOrdered("c=4070; month=1997-12-01 00:00:00",
            "c=4033; month=1997-11-01 00:00:00",
            "c=3511; month=1997-07-01 00:00:00")
        .queryContains(druidChecker("'queryType':'groupBy'"));
  }

  @Test public void testGroupByTimeAndOneMetricNotProjected() {
    final String sql =
            "select count(*) as \"c\", cast(floor(\"timestamp\" to MONTH) as timestamp) as \"month\", floor"
                    + "(\"store_sales\") as sales\n"
                    + "from \"foodmart\"\n"
                    + "group by floor(\"timestamp\" to MONTH), \"state_province\", floor"
                    + "(\"store_sales\")\n"
                    + "order by \"c\" desc limit 3";
    sql(sql).returnsOrdered("c=494; month=1997-11-01 00:00:00; SALES=5.0",
            "c=475; month=1997-12-01 00:00:00; SALES=5.0",
            "c=468; month=1997-03-01 00:00:00; SALES=5.0"
    ).queryContains(druidChecker("'queryType':'scan'"));
  }

  @Test public void testGroupByTimeAndOneColumnNotProjected() {
    final String sql = "select count(*) as \"c\",\n"
        + "  cast(floor(\"timestamp\" to MONTH) as timestamp) as \"month\"\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH), \"state_province\"\n"
        + "having count(*) > 3500";
    sql(sql)
        .returnsUnordered("c=3511; month=1997-07-01 00:00:00",
            "c=4033; month=1997-11-01 00:00:00",
            "c=4070; month=1997-12-01 00:00:00")
        .queryContains(druidChecker("'queryType':'groupBy'"));
  }

  @Test public void testOrderByOneColumnNotProjected() {
    // Result including state: CA=24441, OR=21610, WA=40778
    final String sql = "select count(*) as c from \"foodmart\"\n"
        + "group by \"state_province\" order by \"state_province\"";
    sql(sql)
        .returnsOrdered("C=24441",
            "C=21610",
            "C=40778");
  }

  @Test public void testGroupByOneColumn() {
    final String sql = "select \"state_province\", count(*) as c\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\"\n"
        + "order by \"state_province\"";
    String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{30}], "
        + "aggs=[[COUNT()]], sort0=[0], dir0=[ASC])";
    sql(sql)
        .limit(2)
        .returnsOrdered("state_province=CA; C=24441",
            "state_province=OR; C=21610")
        .explainContains(explain);
  }

  @Test public void testGroupByOneColumnReversed() {
    final String sql = "select count(*) as c, \"state_province\"\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\"\n"
        + "order by \"state_province\"";
    sql(sql)
        .limit(2)
        .returnsOrdered("C=24441; state_province=CA",
            "C=21610; state_province=OR");
  }

  @Test public void testGroupByAvgSumCount() {
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
        .queryContains(druidChecker("'queryType':'scan'"));
  }

  @Test public void testGroupByMonthGranularity() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH) order by s";
    String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart'";
    sql(sql)
        .limit(3)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableSort(sort0=[$0], dir0=[ASC])\n"
            + "    BindableProject(S=[$1], C=[$2])\n"
            + "      DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[FLOOR"
            + "($0, FLAG(MONTH)), $89, $71]], groups=[{0}], aggs=[[SUM($1), COUNT($2)]])")
        .returnsOrdered("S=19958; C=5606", "S=20179; C=5523", "S=20388; C=5591")
        .queryContains(druidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1577">[CALCITE-1577]
   * Druid adapter: Incorrect result - limit on timestamp disappears</a>. */
  @Test public void testGroupByMonthGranularitySort() {
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

  @Test public void testGroupByMonthGranularitySortLimit() {
    final String sql = "select cast(floor(\"timestamp\" to MONTH) as timestamp) as m,\n"
        + " sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH)\n"
        + "order by floor(\"timestamp\" to MONTH) limit 3";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableProject(M=[CAST($0):TIMESTAMP(0) NOT NULL], S=[$1], C=[$2], EXPR$3=[$0])\n"
        + "    BindableSort(sort0=[$0], dir0=[ASC], fetch=[3])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[FLOOR($0, "
        + "FLAG(MONTH)), $89, $71]], groups=[{0}], aggs=[[SUM($1), COUNT($2)]], sort0=[0], "
        + "dir0=[ASC])";
    sql(sql)
        .returnsOrdered("M=1997-01-01 00:00:00; S=21628; C=5957",
            "M=1997-02-01 00:00:00; S=20957; C=5842",
            "M=1997-03-01 00:00:00; S=23706; C=6528")
        .explainContains(explain);
  }

  @Test public void testGroupByDayGranularity() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to DAY) order by c desc";
    String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart'";
    sql(sql)
        .limit(3)
        .queryContains(druidChecker(druidQuery))
        .returnsOrdered("S=3850; C=1230", "S=3342; C=1071", "S=3219; C=1024");
  }

  @Test public void testGroupByMonthGranularityFiltered() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "where \"timestamp\" >= '1996-01-01 00:00:00 UTC' and "
        + " \"timestamp\" < '1998-01-01 00:00:00 UTC'\n"
        + "group by floor(\"timestamp\" to MONTH) order by s asc";
    String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart'";

    sql(sql)
        .limit(3)
        .returnsOrdered("S=19958; C=5606", "S=20179; C=5523", "S=20388; C=5591")
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testTopNMonthGranularity() {
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
        + "'dimension':'state_province'},{'type':'extraction','dimension':'__time',"
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
        .queryContains(druidChecker(druidQueryPart1, druidQueryPart2));
  }

  @Test public void testTopNDayGranularityFiltered() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + "max(\"unit_sales\") as m,\n"
        + "\"state_province\" as p\n"
        + "from \"foodmart\"\n"
        + "where \"timestamp\" >= '1997-01-01 00:00:00 UTC' and "
        + " \"timestamp\" < '1997-09-01 00:00:00 UTC'\n"
        + "group by \"state_province\", floor(\"timestamp\" to DAY)\n"
        + "order by s desc limit 6";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableProject(S=[$2], M=[$3], P=[$0])\n"
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
        .queryContains(druidChecker(druidQueryType, limitSpec));
  }

  @Test public void testGroupByHaving() {
    // Note: We don't push down HAVING yet
    final String sql = "select \"state_province\" as s, count(*) as c\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\" having count(*) > 23000 order by 1";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$0], dir0=[ASC])\n"
        + "    BindableFilter(condition=[>($1, 23000)])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{30}], aggs=[[COUNT()]])";
    sql(sql)
        .returnsOrdered("S=CA; C=24441",
            "S=WA; C=40778")
        .explainContains(explain);
  }

  @Test public void testGroupComposite() {
    // Note: We don't push down SORT-LIMIT yet
    final String sql = "select count(*) as c, \"state_province\", \"city\"\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\", \"city\"\n"
        + "order by c desc limit 2";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableProject(C=[$2], state_province=[$1], city=[$0])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{29, 30}], aggs=[[COUNT()]], sort0=[2], dir0=[DESC], fetch=[2])";
    sql(sql)
        .returnsOrdered("C=7394; state_province=WA; city=Spokane",
            "C=3958; state_province=WA; city=Olympia")
        .explainContains(explain);
  }

  /** Tests that distinct-count is pushed down to Druid and evaluated using
   * "cardinality". The result is approximate, but gives the correct result in
   * this example when rounded down using FLOOR. */
  @Test public void testDistinctCount() {
    final String sql = "select \"state_province\",\n"
        + " floor(count(distinct \"city\")) as cdc\n"
        + "from \"foodmart\"\n"
        + "group by \"state_province\"\n"
        + "order by 2 desc limit 2";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$1], dir0=[DESC], fetch=[2])\n"
        + "    BindableProject(state_province=[$0], CDC=[FLOOR($1)])\n"
        + "      BindableAggregate(group=[{1}], agg#0=[COUNT($0)])\n"
        + "        DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{29, 30}], "
        + "aggs=[[]])";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default','dimension':'city'},"
        + "{'type':'default','dimension':'state_province'}],"
        + "'limitSpec':{'type':'default'},'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql)
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery))
        .returnsUnordered("state_province=CA; CDC=45",
            "state_province=WA; CDC=22");
  }

  /** Tests that projections of columns are pushed into the DruidQuery, and
   * projections of expressions that Druid cannot handle (in this case, a
   * literal 0) stay up. */
  @Test public void testProject() {
    final String sql = "select \"product_name\", 0 as zero\n"
        + "from \"foodmart\"\n"
        + "order by \"product_name\"";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableProject(product_name=[$0], ZERO=[0])\n"
        + "    BindableSort(sort0=[$0], dir0=[ASC])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$3]])";
    sql(sql)
        .limit(2)
        .explainContains(explain)
        .returnsUnordered("product_name=ADJ Rosy Sunglasses; ZERO=0",
            "product_name=ADJ Rosy Sunglasses; ZERO=0");
  }

  @Test public void testFilterDistinct() {
    final String sql = "select distinct \"state_province\", \"city\",\n"
        + "  \"product_name\"\n"
        + "from \"foodmart\"\n"
        + "where \"product_name\" = 'High Top Dried Mushrooms'\n"
        + "and \"quarter\" in ('Q2', 'Q3')\n"
        + "and \"state_province\" = 'WA'";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
        + "'dimensions':[{'type':'default','dimension':'state_province'},"
        + "{'type':'default','dimension':'city'},"
        + "{'type':'default','dimension':'product_name'}],'limitSpec':{'type':'default'},"
        + "'filter':{'type':'and','fields':[{'type':'selector','dimension':'product_name',"
        + "'value':'High Top Dried Mushrooms'},{'type':'or','fields':[{'type':'selector',"
        + "'dimension':'quarter','value':'Q2'},{'type':'selector','dimension':'quarter',"
        + "'value':'Q3'}]},{'type':'selector','dimension':'state_province','value':'WA'}]},"
        + "'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]],"
        + " filter=[AND(=($3, 'High Top Dried Mushrooms'),"
        + " OR(=($87, 'Q2'),"
        + " =($87, 'Q3')),"
        + " =($30, 'WA'))],"
        + " projects=[[$30, $29, $3]], groups=[{0, 1, 2}], aggs=[[]])\n";
    sql(sql)
        .queryContains(druidChecker(druidQuery))
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

  @Test public void testFilter() {
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
        + "'granularity':'all',"
        + "'resultFormat':'compactedList'}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[AND(=($3, 'High Top Dried Mushrooms'), "
        + "OR(=($87, 'Q2'), =($87, 'Q3')), =($30, 'WA'))], "
        + "projects=[[$30, $29, $3]])\n";
    sql(sql)
        .queryContains(druidChecker(druidQuery))
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
  @Test public void testFilterTimestamp() {
    String sql = "select count(*) as c\n"
        + "from \"foodmart\"\n"
        + "where extract(year from \"timestamp\") = 1997\n"
        + "and extract(month from \"timestamp\") in (4, 6)\n";
    final String explain = "DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[AND(=(EXTRACT(FLAG(YEAR), $0), 1997), OR(=(EXTRACT(FLAG(MONTH), $0), 4), "
        + "=(EXTRACT(FLAG(MONTH), $0), 6)))], groups=[{}], aggs=[[COUNT()]])";
    sql(sql)
        .explainContains(explain)
        .returnsUnordered("C=13500");
  }

  @Test public void testFilterSwapped() {
    String sql = "select \"state_province\"\n"
        + "from \"foodmart\"\n"
        + "where 'High Top Dried Mushrooms' = \"product_name\"";
    final String explain = "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[=('High Top Dried Mushrooms', $3)], projects=[[$30]])";
    final String druidQuery = "'filter':{'type':'selector','dimension':'product_name',"
        + "'value':'High Top Dried Mushrooms'}";
    sql(sql)
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  /** Tests a query that exposed several bugs in the interpreter. */
  @Test public void testWhereGroupBy() {
    String sql = "select \"wikiticker\".\"countryName\" as \"c0\",\n"
        + " sum(\"wikiticker\".\"count\") as \"m1\",\n"
        + " sum(\"wikiticker\".\"deleted\") as \"m2\",\n"
        + " sum(\"wikiticker\".\"delta\") as \"m3\"\n"
        + "from \"wiki\" as \"wikiticker\"\n"
        + "where (\"wikiticker\".\"countryName\" in ('Colombia', 'France',\n"
        + " 'Germany', 'India', 'Italy', 'Russia', 'United Kingdom',\n"
        + " 'United States') or \"wikiticker\".\"countryName\" is null)\n"
        + "group by \"wikiticker\".\"countryName\"";
    String druidQuery = "{'type':'selector','dimension':'countryName','value':null}";
    sql(sql, WIKI)
        .queryContains(druidChecker(druidQuery))
        .returnsCount(9);
  }


  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1656">[CALCITE-1656]
   * Improve cost function in DruidQuery to encourage early column
   * pruning</a>. */
  @Test public void testFieldBasedCostColumnPruning() {
    // A query where filter cannot be pushed to Druid but
    // the project can still be pushed in order to prune extra columns.
    String sql = "select \"countryName\", ceil(CAST(\"time\" AS TIMESTAMP) to DAY),\n"
        + "  cast(count(*) as integer) as c\n"
        + "from \"wiki\"\n"
        + "where ceil(\"time\" to DAY) >= '1997-01-01 00:00:00 UTC'\n"
        + "and ceil(\"time\" to DAY) < '1997-09-01 00:00:00 UTC'\n"
        + "and \"time\" + INTERVAL '1' DAY > '1997-01-01'\n"
        + "group by \"countryName\", ceil(CAST(\"time\" AS TIMESTAMP) TO DAY)\n"
        + "order by c limit 5";
    String plan = "BindableProject(countryName=[$0], EXPR$1=[$1], C=[CAST($2):INTEGER NOT NULL])\n"
        + "    BindableSort(sort0=[$2], dir0=[ASC], fetch=[5])\n"
        + "      BindableAggregate(group=[{0, 1}], agg#0=[COUNT()])\n"
        + "        BindableProject(countryName=[$1], EXPR$1=[CEIL(CAST($0):TIMESTAMP(0) NOT NULL, FLAG(DAY))])\n"
        + "          BindableFilter(condition=[AND(>($0, 1996-12-31 00:00:00), <=($0, 1997-08-31 00:00:00), >(+($0, 86400000), CAST('1997-01-01'):TIMESTAMP_WITH_LOCAL_TIME_ZONE(0) NOT NULL))])\n"
        + "            DruidQuery(table=[[wiki, wiki]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$0, $5]])";
    // NOTE: Druid query only has countryName as the dimension
    // being queried after project is pushed to druid query.
    String druidQuery = "{'queryType':'scan',"
        + "'dataSource':'wikiticker',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'columns':['__time','countryName'],"
        + "'granularity':'all',"
        + "'resultFormat':'compactedList'";
    sql(sql, WIKI).explainContains(plan);
    sql(sql, WIKI).queryContains(druidChecker(druidQuery));
  }

  @Test public void testGroupByMetricAndExtractTime() {
    final String sql =
        "SELECT count(*), cast(floor(\"timestamp\" to DAY) as timestamp), \"store_sales\" "
        + "FROM \"foodmart\"\n"
        + "GROUP BY \"store_sales\", floor(\"timestamp\" to DAY)\n ORDER BY \"store_sales\" DESC\n"
        + "LIMIT 10\n";
    sql(sql).queryContains(druidChecker("{\"queryType\":\"groupBy\""));
  }

  @Test public void testFilterOnDouble() {
    String sql = "select \"product_id\" from \"foodmart\"\n"
        + "where cast(\"product_id\" as double) < 0.41024 and \"product_id\" < 12223";
    sql(sql).queryContains(
        druidChecker("'type':'bound','dimension':'product_id','upper':'0.41024'",
            "'upper':'12223'"));
  }

  @Test public void testPushAggregateOnTime() {
    String sql = "select \"product_id\", cast(\"timestamp\" as timestamp) as \"time\" "
        + "from \"foodmart\" "
        + "where \"product_id\" = 1016 "
        + "and \"timestamp\" < '1997-01-03 00:00:00 UTC' "
        + "and \"timestamp\" > '1990-01-01 00:00:00 UTC' "
        + "group by \"timestamp\", \"product_id\" ";
    String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'extraction',"
        + "'dimension':'__time','outputName':'extract',"
        + "'extractionFn':{'type':'timeFormat','format':'yyyy-MM-dd";
    sql(sql)
        .queryContains(druidChecker(druidQuery))
        .returnsUnordered("product_id=1016; time=1997-01-02 00:00:00");
  }

  @Test public void testPushAggregateOnTimeWithExtractYear() {
    String sql = "select EXTRACT( year from \"timestamp\") as \"year\",\"product_id\" from "
        + "\"foodmart\" where \"product_id\" = 1016 and "
        + "\"timestamp\" < cast('1999-01-02' as timestamp) and \"timestamp\" > cast"
        + "('1997-01-01' as timestamp)" + " group by "
        + " EXTRACT( year from \"timestamp\"), \"product_id\" ";
    sql(sql)
        .queryContains(
            druidChecker(
                ",'granularity':'all'",
                "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_year',"
                    + "'extractionFn':{'type':'timeFormat','format':'yyyy',"
                    + "'timeZone':'UTC','locale':'en-US'}}"))
        .returnsUnordered("year=1997; product_id=1016");
  }

  @Test public void testPushAggregateOnTimeWithExtractMonth() {
    String sql = "select EXTRACT( month from \"timestamp\") as \"month\",\"product_id\" from "
        + "\"foodmart\" where \"product_id\" = 1016 and "
        + "\"timestamp\" < cast('1997-06-02' as timestamp) and \"timestamp\" > cast"
        + "('1997-01-01' as timestamp)" + " group by "
        + " EXTRACT( month from \"timestamp\"), \"product_id\" ";
    sql(sql)
        .queryContains(
            druidChecker(
                ",'granularity':'all'",
                "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_month',"
                    + "'extractionFn':{'type':'timeFormat','format':'M',"
                    + "'timeZone':'UTC','locale':'en-US'}}"))
        .returnsUnordered("month=1; product_id=1016", "month=2; product_id=1016",
            "month=3; product_id=1016", "month=4; product_id=1016", "month=5; product_id=1016");
  }

  @Test public void testPushAggregateOnTimeWithExtractDay() {
    String sql = "select EXTRACT( day from \"timestamp\") as \"day\","
        + "\"product_id\" from \"foodmart\""
        + " where \"product_id\" = 1016 and "
        + "\"timestamp\" < cast('1997-01-20' as timestamp) and \"timestamp\" > cast"
        + "('1997-01-01' as timestamp)" + " group by "
        + " EXTRACT( day from \"timestamp\"), \"product_id\" ";
    sql(sql)
        .queryContains(
            druidChecker(
                ",'granularity':'all'",
                "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_day',"
                    + "'extractionFn':{'type':'timeFormat','format':'d',"
                    + "'timeZone':'UTC','locale':'en-US'}}"))
        .returnsUnordered("day=2; product_id=1016", "day=10; product_id=1016",
            "day=13; product_id=1016", "day=16; product_id=1016");
  }

  // Calcite rewrite the extract function in the query as:
  // rel#85:BindableProject.BINDABLE.[](input=rel#69:Subset#1.BINDABLE.[],
  // hourOfDay=/INT(MOD(Reinterpret($0), 86400000), 3600000),product_id=$1).
  // Currently 'EXTRACT( hour from \"timestamp\")' is not pushed to Druid.
  @Ignore @Test public void testPushAggregateOnTimeWithExtractHourOfDay() {
    String sql =
        "select EXTRACT( hour from \"timestamp\") as \"hourOfDay\",\"product_id\"  from "
            + "\"foodmart\" where \"product_id\" = 1016 and "
            + "\"timestamp\" < cast('1997-06-02' as timestamp) and \"timestamp\" > cast"
            + "('1997-01-01' as timestamp)" + " group by "
            + " EXTRACT( hour from \"timestamp\"), \"product_id\" ";
    sql(sql)
        .queryContains(
            druidChecker(
                ",'granularity':'all'",
                "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_0',"
                    + "'extractionFn':{'type':'timeFormat','format':'H',"
                    + "'timeZone':'UTC'}}"))
        .returnsUnordered("month=01; product_id=1016", "month=02; product_id=1016",
            "month=03; product_id=1016", "month=04; product_id=1016", "month=05; product_id=1016");
  }

  @Test public void testPushAggregateOnTimeWithExtractYearMonthDay() {
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
            druidChecker(
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
            + "filter=[=($1, 1016)], projects=[[EXTRACT(FLAG(DAY), $0), EXTRACT(FLAG(MONTH), $0), "
            + "EXTRACT(FLAG(YEAR), $0), $1]], groups=[{0, 1, 2, 3}], aggs=[[]])\n")
        .returnsUnordered("day=2; month=1; year=1997; product_id=1016",
            "day=10; month=1; year=1997; product_id=1016",
            "day=13; month=1; year=1997; product_id=1016",
            "day=16; month=1; year=1997; product_id=1016");
  }

  @Test public void testPushAggregateOnTimeWithExtractYearMonthDayWithOutRenaming() {
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
            druidChecker(
                ",'granularity':'all'", "{'type':'extraction',"
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
            + "filter=[=($1, 1016)], projects=[[EXTRACT(FLAG(DAY), $0), EXTRACT(FLAG(MONTH), $0), "
            + "EXTRACT(FLAG(YEAR), $0), $1]], groups=[{0, 1, 2, 3}], aggs=[[]])\n")
        .returnsUnordered("EXPR$0=2; EXPR$1=1; EXPR$2=1997; product_id=1016",
            "EXPR$0=10; EXPR$1=1; EXPR$2=1997; product_id=1016",
            "EXPR$0=13; EXPR$1=1; EXPR$2=1997; product_id=1016",
            "EXPR$0=16; EXPR$1=1; EXPR$2=1997; product_id=1016");
  }

  @Test public void testPushAggregateOnTimeWithExtractWithOutRenaming() {
    String sql = "select EXTRACT( day from \"timestamp\"), "
        + "\"product_id\" as \"dayOfMonth\" from \"foodmart\" "
        + "where \"product_id\" = 1016 and \"timestamp\" < cast('1997-01-20' as timestamp) "
        + "and \"timestamp\" > cast('1997-01-01' as timestamp)"
        + " group by "
        + " EXTRACT( day from \"timestamp\"), EXTRACT( day from \"timestamp\"),"
        + " \"product_id\" ";
    sql(sql)
        .queryContains(
            druidChecker(
                ",'granularity':'all'", "{'type':'extraction',"
                    + "'dimension':'__time','outputName':'extract_day',"
                    + "'extractionFn':{'type':'timeFormat','format':'d',"
                    + "'timeZone':'UTC','locale':'en-US'}}"))
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1997-01-01T00:00:00.001Z/1997-01-20T00:00:00.000Z]], "
            + "filter=[=($1, 1016)], projects=[[EXTRACT(FLAG(DAY), $0), $1]], "
            + "groups=[{0, 1}], aggs=[[]])\n")
        .returnsUnordered("EXPR$0=2; dayOfMonth=1016", "EXPR$0=10; dayOfMonth=1016",
            "EXPR$0=13; dayOfMonth=1016", "EXPR$0=16; dayOfMonth=1016");
  }

  @Test public void testPushComplexFilter() {
    String sql = "select sum(\"store_sales\") from \"foodmart\" "
        + "where EXTRACT( year from \"timestamp\") = 1997 and "
        + "\"cases_per_pallet\" >= 8 and \"cases_per_pallet\" <= 10 and "
        + "\"units_per_case\" < 15 ";
    String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart',"
        + "'descending':false,'granularity':'all','filter':{'type':'and',"
        + "'fields':[{'type':'bound','dimension':'cases_per_pallet','lower':'8',"
        + "'lowerStrict':false,'ordering':'numeric'},{'type':'bound',"
        + "'dimension':'cases_per_pallet','upper':'10','upperStrict':false,"
        + "'ordering':'numeric'},{'type':'bound','dimension':'units_per_case',"
        + "'upper':'15','upperStrict':true,'ordering':'numeric'},"
        + "{'type':'selector','dimension':'__time','value':'1997',"
        + "'extractionFn':{'type':'timeFormat','format':'yyyy','timeZone':'UTC',"
        + "'locale':'en-US'}}]},'aggregations':[{'type':'doubleSum',"
        + "'name':'EXPR$0','fieldName':'store_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'context':{'skipEmptyBuckets':true}}";
    sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "filter=[AND(>=(CAST($11):BIGINT, 8), <=(CAST($11):BIGINT, 10), "
            + "<(CAST($10):BIGINT, 15), =(EXTRACT(FLAG(YEAR), $0), 1997))], groups=[{}], "
            + "aggs=[[SUM($90)]])\n")
        .queryContains(druidChecker(druidQuery))
        .returnsUnordered("EXPR$0=75364.1");
  }

  @Test public void testPushOfFilterExtractionOnDayAndMonth() {
    String sql = "SELECT \"product_id\" , EXTRACT(day from \"timestamp\"), EXTRACT(month from "
        + "\"timestamp\") from \"foodmart\" WHERE  EXTRACT(day from \"timestamp\") >= 30 AND "
        + "EXTRACT(month from \"timestamp\") = 11 "
        + "AND  \"product_id\" >= 1549 group by \"product_id\", EXTRACT(day from "
        + "\"timestamp\"), EXTRACT(month from \"timestamp\")";
    sql(sql)
        .queryContains(
            druidChecker("{'queryType':'groupBy','dataSource':'foodmart',"
                + "'granularity':'all','dimensions':[{'type':'default',"
                + "'dimension':'product_id'},{'type':'extraction','dimension':'__time',"
                + "'outputName':'extract_day','extractionFn':{'type':'timeFormat',"
                + "'format':'d','timeZone':'UTC','locale':'en-US'}},{'type':'extraction',"
                + "'dimension':'__time','outputName':'extract_month',"
                + "'extractionFn':{'type':'timeFormat','format':'M','timeZone':'UTC',"
                + "'locale':'en-US'}}],'limitSpec':{'type':'default'},"
                + "'filter':{'type':'and','fields':[{'type':'bound',"
                + "'dimension':'product_id','lower':'1549','lowerStrict':false,"
                + "'ordering':'numeric'},{'type':'bound','dimension':'__time',"
                + "'lower':'30','lowerStrict':false,'ordering':'numeric',"
                + "'extractionFn':{'type':'timeFormat','format':'d','timeZone':'UTC',"
                + "'locale':'en-US'}},{'type':'selector','dimension':'__time',"
                + "'value':'11','extractionFn':{'type':'timeFormat','format':'M',"
                + "'timeZone':'UTC','locale':'en-US'}}]},'aggregations':[],"
                + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}"))
        .returnsUnordered("product_id=1549; EXPR$1=30; EXPR$2=11",
            "product_id=1553; EXPR$1=30; EXPR$2=11");
  }

  @Test public void testPushOfFilterExtractionOnDayAndMonthAndYear() {
    String sql = "SELECT \"product_id\" , EXTRACT(day from \"timestamp\"), EXTRACT(month from "
        + "\"timestamp\") , EXTRACT(year from \"timestamp\") from \"foodmart\" "
        + "WHERE  EXTRACT(day from \"timestamp\") >= 30 AND EXTRACT(month from \"timestamp\") = 11 "
        + "AND  \"product_id\" >= 1549 AND EXTRACT(year from \"timestamp\") = 1997"
        + "group by \"product_id\", EXTRACT(day from \"timestamp\"), "
        + "EXTRACT(month from \"timestamp\"), EXTRACT(year from \"timestamp\")";
    sql(sql)
        .queryContains(
            druidChecker("{'queryType':'groupBy','dataSource':'foodmart',"
                + "'granularity':'all','dimensions':[{'type':'default',"
                + "'dimension':'product_id'},{'type':'extraction','dimension':'__time',"
                + "'outputName':'extract_day','extractionFn':{'type':'timeFormat',"
                + "'format':'d','timeZone':'UTC','locale':'en-US'}},{'type':'extraction',"
                + "'dimension':'__time','outputName':'extract_month',"
                + "'extractionFn':{'type':'timeFormat','format':'M','timeZone':'UTC',"
                + "'locale':'en-US'}},{'type':'extraction','dimension':'__time',"
                + "'outputName':'extract_year','extractionFn':{'type':'timeFormat',"
                + "'format':'yyyy','timeZone':'UTC','locale':'en-US'}}],"
                + "'limitSpec':{'type':'default'},'filter':{'type':'and',"
                + "'fields':[{'type':'bound','dimension':'product_id','lower':'1549',"
                + "'lowerStrict':false,'ordering':'numeric'},{'type':'bound',"
                + "'dimension':'__time','lower':'30','lowerStrict':false,"
                + "'ordering':'numeric','extractionFn':{'type':'timeFormat','format':'d',"
                + "'timeZone':'UTC','locale':'en-US'}},{'type':'selector',"
                + "'dimension':'__time','value':'11','extractionFn':{'type':'timeFormat',"
                + "'format':'M','timeZone':'UTC','locale':'en-US'}},{'type':'selector',"
                + "'dimension':'__time','value':'1997','extractionFn':{'type':'timeFormat',"
                + "'format':'yyyy','timeZone':'UTC','locale':'en-US'}}]},"
                + "'aggregations':[],"
                + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}"))
        .returnsUnordered("product_id=1549; EXPR$1=30; EXPR$2=11; EXPR$3=1997",
            "product_id=1553; EXPR$1=30; EXPR$2=11; EXPR$3=1997");
  }

  @Test public void testFilterExtractionOnMonthWithBetween() {
    String sqlQuery = "SELECT \"product_id\", EXTRACT(month from \"timestamp\") FROM \"foodmart\""
        + " WHERE EXTRACT(month from \"timestamp\") BETWEEN 10 AND 11 AND  \"product_id\" >= 1558"
        + " GROUP BY \"product_id\", EXTRACT(month from \"timestamp\")";
    String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default',"
        + "'dimension':'product_id'},{'type':'extraction','dimension':'__time',"
        + "'outputName':'extract_month','extractionFn':{'type':'timeFormat',"
        + "'format':'M','timeZone':'UTC','locale':'en-US'}}],"
        + "'limitSpec':{'type':'default'},'filter':{'type':'and',"
        + "'fields':[{'type':'bound','dimension':'product_id','lower':'1558',"
        + "'lowerStrict':false,'ordering':'numeric'},{'type':'bound',"
        + "'dimension':'__time','lower':'10','lowerStrict':false,"
        + "'ordering':'numeric','extractionFn':{'type':'timeFormat','format':'M',"
        + "'timeZone':'UTC','locale':'en-US'}},{'type':'bound',"
        + "'dimension':'__time','upper':'11','upperStrict':false,"
        + "'ordering':'numeric','extractionFn':{'type':'timeFormat','format':'M',"
        + "'timeZone':'UTC','locale':'en-US'}}]},'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sqlQuery)
        .returnsUnordered("product_id=1558; EXPR$1=10", "product_id=1558; EXPR$1=11",
            "product_id=1559; EXPR$1=11")
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testFilterExtractionOnMonthWithIn() {
    String sqlQuery = "SELECT \"product_id\", EXTRACT(month from \"timestamp\") FROM \"foodmart\""
        + " WHERE EXTRACT(month from \"timestamp\") IN (10, 11) AND  \"product_id\" >= 1558"
        + " GROUP BY \"product_id\", EXTRACT(month from \"timestamp\")";
    sql(sqlQuery)
        .queryContains(
            druidChecker("{'queryType':'groupBy',"
                + "'dataSource':'foodmart','granularity':'all',"
                + "'dimensions':[{'type':'default','dimension':'product_id'},"
                + "{'type':'extraction','dimension':'__time','outputName':'extract_month',"
                + "'extractionFn':{'type':'timeFormat','format':'M','timeZone':'UTC',"
                + "'locale':'en-US'}}],'limitSpec':{'type':'default'},"
                + "'filter':{'type':'and','fields':[{'type':'bound',"
                + "'dimension':'product_id','lower':'1558','lowerStrict':false,"
                + "'ordering':'numeric'},{'type':'or','fields':[{'type':'selector',"
                + "'dimension':'__time','value':'10','extractionFn':{'type':'timeFormat',"
                + "'format':'M','timeZone':'UTC','locale':'en-US'}},{'type':'selector',"
                + "'dimension':'__time','value':'11','extractionFn':{'type':'timeFormat',"
                + "'format':'M','timeZone':'UTC','locale':'en-US'}}]}]},"
                + "'aggregations':[],"
                + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}"))
        .returnsUnordered("product_id=1558; EXPR$1=10", "product_id=1558; EXPR$1=11",
            "product_id=1559; EXPR$1=11");
  }

  @Test public void testPushofOrderByWithMonthExtract() {
    String sqlQuery = "SELECT  extract(month from \"timestamp\") as m , \"product_id\", SUM"
        + "(\"unit_sales\") as s FROM \"foodmart\""
        + " WHERE \"product_id\" >= 1558"
        + " GROUP BY extract(month from \"timestamp\"), \"product_id\" order by m, s, "
        + "\"product_id\"";
    sql(sqlQuery).queryContains(
        druidChecker("{'queryType':'groupBy','dataSource':'foodmart',"
            + "'granularity':'all','dimensions':[{'type':'extraction',"
            + "'dimension':'__time','outputName':'extract_month',"
            + "'extractionFn':{'type':'timeFormat','format':'M','timeZone':'UTC',"
            + "'locale':'en-US'}},{'type':'default','dimension':'product_id'}],"
            + "'limitSpec':{'type':'default','columns':[{'dimension':'extract_month',"
            + "'direction':'ascending','dimensionOrder':'numeric'},{'dimension':'S',"
            + "'direction':'ascending','dimensionOrder':'numeric'},"
            + "{'dimension':'product_id','direction':'ascending',"
            + "'dimensionOrder':'alphanumeric'}]},'filter':{'type':'bound',"
            + "'dimension':'product_id','lower':'1558','lowerStrict':false,"
            + "'ordering':'numeric'},'aggregations':[{'type':'longSum','name':'S',"
            + "'fieldName':'unit_sales'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}"))
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "filter=[>=(CAST($1):BIGINT, 1558)], projects=[[EXTRACT(FLAG(MONTH), $0), $1, $89]], "
            + "groups=[{0, 1}], aggs=[[SUM($2)]], sort0=[0], sort1=[2], sort2=[1], "
            + "dir0=[ASC], dir1=[ASC], dir2=[ASC])");
  }


  @Test public void testGroupByFloorTimeWithoutLimit() {
    final String sql = "select cast(floor(\"timestamp\" to MONTH) as timestamp) as \"month\"\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH)\n"
        + "order by \"month\" DESC";
    sql(sql)
        .explainContains("DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[FLOOR($0, "
        + "FLAG(MONTH))]], groups=[{0}], aggs=[[]], sort0=[0], dir0=[DESC])")
        .queryContains(druidChecker("'queryType':'timeseries'", "'descending':true"));
  }

  @Test public void testGroupByFloorTimeWithLimit() {
    final String sql =
        "select cast(floor(\"timestamp\" to MONTH) as timestamp) as \"floor_month\"\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH)\n"
        + "order by \"floor_month\" DESC LIMIT 3";
    final String explain =
        "    BindableSort(sort0=[$0], dir0=[DESC], fetch=[3])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "projects=[[FLOOR($0, FLAG(MONTH))]], groups=[{0}], aggs=[[]], "
        + "sort0=[0], dir0=[DESC])";
    sql(sql).explainContains(explain)
        .queryContains(druidChecker("'queryType':'timeseries'", "'descending':true"))
        .returnsOrdered("floor_month=1997-12-01 00:00:00", "floor_month=1997-11-01 00:00:00",
            "floor_month=1997-10-01 00:00:00");
  }

  @Test public void testPushofOrderByYearWithYearMonthExtract() {
    String sqlQuery = "SELECT year(\"timestamp\") as y, extract(month from \"timestamp\") as m , "
        + "\"product_id\", SUM"
        + "(\"unit_sales\") as s FROM \"foodmart\""
        + " WHERE \"product_id\" >= 1558"
        + " GROUP BY year(\"timestamp\"), extract(month from \"timestamp\"), \"product_id\" order"
        + " by y DESC, m ASC, s DESC, \"product_id\" LIMIT 3";
    final String expectedPlan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[>=(CAST($1):BIGINT, 1558)], projects=[[EXTRACT(FLAG(YEAR), $0), "
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
        + "'dimension':'product_id'}],'limitSpec':{'type':'default','limit':3,"
        + "'columns':[{'dimension':'extract_year','direction':'descending',"
        + "'dimensionOrder':'numeric'},{'dimension':'extract_month',"
        + "'direction':'ascending','dimensionOrder':'numeric'},{'dimension':'S',"
        + "'direction':'descending','dimensionOrder':'numeric'},"
        + "{'dimension':'product_id','direction':'ascending',"
        + "'dimensionOrder':'alphanumeric'}]},'filter':{'type':'bound',"
        + "'dimension':'product_id','lower':'1558','lowerStrict':false,"
        + "'ordering':'numeric'},'aggregations':[{'type':'longSum','name':'S',"
        + "'fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sqlQuery).explainContains(expectedPlan).queryContains(druidChecker(expectedDruidQuery))
        .returnsOrdered("Y=1997; M=1; product_id=1558; S=6", "Y=1997; M=1; product_id=1559; S=6",
            "Y=1997; M=2; product_id=1558; S=24");
  }

  @Test public void testPushofOrderByMetricWithYearMonthExtract() {
    String sqlQuery = "SELECT year(\"timestamp\") as y, extract(month from \"timestamp\") as m , "
        + "\"product_id\", SUM(\"unit_sales\") as s FROM \"foodmart\""
        + " WHERE \"product_id\" >= 1558"
        + " GROUP BY year(\"timestamp\"), extract(month from \"timestamp\"), \"product_id\" order"
        + " by s DESC, m DESC, \"product_id\" LIMIT 3";
    final String expectedPlan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[>=(CAST($1):BIGINT, 1558)], projects=[[EXTRACT(FLAG(YEAR), $0), "
        + "EXTRACT(FLAG(MONTH), $0), $1, $89]], groups=[{0, 1, 2}], aggs=[[SUM($3)]], "
        + "sort0=[3], sort1=[1], sort2=[2], dir0=[DESC], dir1=[DESC], dir2=[ASC], fetch=[3])";
    final String expectedDruidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'extraction',"
        + "'dimension':'__time','outputName':'extract_year',"
        + "'extractionFn':{'type':'timeFormat','format':'yyyy','timeZone':'UTC',"
        + "'locale':'en-US'}},{'type':'extraction','dimension':'__time',"
        + "'outputName':'extract_month','extractionFn':{'type':'timeFormat',"
        + "'format':'M','timeZone':'UTC','locale':'en-US'}},{'type':'default',"
        + "'dimension':'product_id'}],'limitSpec':{'type':'default','limit':3,"
        + "'columns':[{'dimension':'S','direction':'descending',"
        + "'dimensionOrder':'numeric'},{'dimension':'extract_month',"
        + "'direction':'descending','dimensionOrder':'numeric'},"
        + "{'dimension':'product_id','direction':'ascending',"
        + "'dimensionOrder':'alphanumeric'}]},'filter':{'type':'bound',"
        + "'dimension':'product_id','lower':'1558','lowerStrict':false,"
        + "'ordering':'numeric'},'aggregations':[{'type':'longSum','name':'S',"
        + "'fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sqlQuery).explainContains(expectedPlan).queryContains(druidChecker(expectedDruidQuery))
        .returnsOrdered("Y=1997; M=12; product_id=1558; S=30", "Y=1997; M=3; product_id=1558; S=29",
            "Y=1997; M=5; product_id=1558; S=27");
  }

  @Test public void testGroupByTimeSortOverMetrics() {
    final String sqlQuery = "SELECT count(*) as c , SUM(\"unit_sales\") as s,"
        + " cast(floor(\"timestamp\" to month) as timestamp)"
        + " FROM \"foodmart\" group by floor(\"timestamp\" to month) order by s DESC";
    sql(sqlQuery)
        .explainContains("PLAN=EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$1], dir0=[DESC])\n"
        + "    BindableProject(C=[$1], S=[$2], EXPR$2=[CAST($0):TIMESTAMP(0) NOT NULL])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[FLOOR($0, "
        + "FLAG(MONTH)), $89]], groups=[{0}], aggs=[[COUNT(), SUM($1)]])")
        .queryContains(druidChecker("'queryType':'timeseries'"))
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
        "C=6478; S=19958; EXPR$2=1997-10-01 00:00:00");
  }

  @Test public void testNumericOrderingOfOrderByOperatorFullTime() {
    final String sqlQuery = "SELECT cast(\"timestamp\" as timestamp) as \"timestamp\","
        + " count(*) as c, SUM(\"unit_sales\") as s FROM "
        + "\"foodmart\" group by \"timestamp\" order by \"timestamp\" DESC, c DESC, s LIMIT 5";
    final String druidSubQuery = "'limitSpec':{'type':'default','limit':5,"
        + "'columns':[{'dimension':'extract','direction':'descending',"
        + "'dimensionOrder':'alphanumeric'},{'dimension':'C',"
        + "'direction':'descending','dimensionOrder':'numeric'},{'dimension':'S',"
        + "'direction':'ascending','dimensionOrder':'numeric'}]},"
        + "'aggregations':[{'type':'count','name':'C'},{'type':'longSum',"
        + "'name':'S','fieldName':'unit_sales'}]";
    sql(sqlQuery).returnsOrdered("timestamp=1997-12-30 00:00:00; C=22; S=36\ntimestamp=1997-12-29"
        + " 00:00:00; C=321; S=982\ntimestamp=1997-12-28 00:00:00; C=480; "
        + "S=1496\ntimestamp=1997-12-27 00:00:00; C=363; S=1156\ntimestamp=1997-12-26 00:00:00; "
        + "C=144; S=420").queryContains(druidChecker(druidSubQuery));

  }

  @Test public void testNumericOrderingOfOrderByOperatorTimeExtract() {
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
        + "Y=1997; C=137; S=422").queryContains(druidChecker(druidSubQuery));

  }

  @Test public void testNumericOrderingOfOrderByOperatorStringDims() {
    final String sqlQuery = "SELECT \"brand_name\", count(*) as c, SUM(\"unit_sales\")  "
        + "as s FROM "
        + "\"foodmart\" group by \"brand_name\" order by \"brand_name\"  DESC LIMIT 5";
    final String druidSubQuery = "'limitSpec':{'type':'default','limit':5,"
        + "'columns':[{'dimension':'brand_name','direction':'descending',"
        + "'dimensionOrder':'alphanumeric'}]}";
    sql(sqlQuery).returnsOrdered("brand_name=Washington; C=576; S=1775\nbrand_name=Walrus; C=457;"
        + " S=1399\nbrand_name=Urban; C=299; S=924\nbrand_name=Tri-State; C=2339; "
        + "S=7270\nbrand_name=Toucan; C=123; S=380").queryContains(druidChecker(druidSubQuery));

  }

  @Test public void testGroupByWeekExtract() {
    final String sql = "SELECT extract(week from \"timestamp\") from \"foodmart\" where "
        + "\"product_id\" = 1558 and extract(week from \"timestamp\") IN (10, 11)group by extract"
        + "(week from \"timestamp\")";

    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'extraction',"
        + "'dimension':'__time','outputName':'extract_week',"
        + "'extractionFn':{'type':'timeFormat','format':'w','timeZone':'UTC',"
        + "'locale':'en-US'}}],'limitSpec':{'type':'default'},"
        + "'filter':{'type':'and','fields':[{'type':'selector',"
        + "'dimension':'product_id','value':'1558'},{'type':'or',"
        + "'fields':[{'type':'selector','dimension':'__time','value':'10',"
        + "'extractionFn':{'type':'timeFormat','format':'w','timeZone':'UTC',"
        + "'locale':'en-US'}},{'type':'selector','dimension':'__time',"
        + "'value':'11','extractionFn':{'type':'timeFormat','format':'w',"
        + "'timeZone':'UTC','locale':'en-US'}}]}]},"
        + "'aggregations':[],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql).returnsOrdered("EXPR$0=10\nEXPR$0=11").queryContains(druidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1765">[CALCITE-1765]
   * Druid adapter: Gracefully handle granularity that cannot be pushed to
   * extraction function</a>. */
  @Test public void testTimeExtractThatCannotBePushed() {
    final String sql = "SELECT extract(CENTURY from \"timestamp\") from \"foodmart\" where "
        + "\"product_id\" = 1558 group by extract(CENTURY from \"timestamp\")";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  BindableAggregate(group=[{0}])\n"
        + "    BindableProject(EXPR$0=[EXTRACT(FLAG(CENTURY), $0)])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[=($1, 1558)], projects=[[$0]])\n";
    sql(sql).explainContains(plan).queryContains(druidChecker("'queryType':'scan'"))
        .returnsUnordered("EXPR$0=20");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1770">[CALCITE-1770]
   * Druid adapter: CAST(NULL AS ...) gives NPE</a>. */
  @Test public void testPushCast() {
    final String sql = "SELECT \"product_id\"\n"
        + "from \"foodmart\"\n"
        + "where \"product_id\" = cast(NULL as varchar)\n"
        + "group by \"product_id\" order by \"product_id\" limit 5";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$0], dir0=[ASC], fetch=[5])\n"
        + "    BindableFilter(condition=[=($0, null)])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals="
        + "[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{1}], aggs=[[]])";
    final String query = "{\"queryType\":\"groupBy\"";
    sql(sql)
        .explainContains(plan)
        .queryContains(druidChecker(query));
  }

  @Test public void testFalseFilter() {
    String sql = "Select count(*) as c from \"foodmart\" where false";
    sql(sql)
        .queryContains(
            druidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"1 == 2\"}"))
        .returnsUnordered("C=0");
  }

  @Test public void testTrueFilter() {
    String sql = "Select count(*) as c from \"foodmart\" where true";
    sql(sql).returnsUnordered("C=86829");
  }

  @Test public void testFalseFilterCaseConjectionWithTrue() {
    String sql = "Select count(*) as c from \"foodmart\" where "
        + "\"product_id\" = 1558 and (true or false)";
    sql(sql).returnsUnordered("C=60").queryContains(druidChecker("'queryType':'timeseries'"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1769">[CALCITE-1769]
   * Druid adapter: Push down filters involving numeric cast of literals</a>. */
  @Test public void testPushCastNumeric() {
    String druidQuery = "'filter':{'type':'bound','dimension':'product_id',"
        + "'upper':'10','upperStrict':true,'ordering':'numeric'}";
    sql("?")
        .withRel(new Function<RelBuilder, RelNode>() {
          public RelNode apply(RelBuilder b) {
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
                            ImmutableList.<RexNode>of(b.field("product_id"))),
                        b.getRexBuilder().makeCall(intType,
                            SqlStdOperatorTable.CAST,
                            ImmutableList.of(b.literal("10")))))
                .project(b.field("product_id"))
                .build();
          }
        })
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testPushFieldEqualsLiteral() {
    sql("?")
        .withRel(new Function<RelBuilder, RelNode>() {
          public RelNode apply(RelBuilder b) {
            // select count(*) as c
            // from foodmart.foodmart
            // where product_id = 'id'
            return b.scan("foodmart", "foodmart")
                .filter(
                    b.call(SqlStdOperatorTable.EQUALS, b.field("product_id"),
                        b.literal("id")))
                .aggregate(b.groupKey(), b.countStar("c"))
                .build();
          }
        })
        // Should return one row, "c=0"; logged
        // [CALCITE-1775] "GROUP BY ()" on empty relation should return 1 row
        .returnsUnordered("c=0")
        .queryContains(druidChecker("'queryType':'timeseries'"));
  }

  @Test public void testPlusArithmeticOperation() {
    final String sqlQuery = "select sum(\"store_sales\") + sum(\"store_cost\") as a, "
        + "\"store_state\" from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "'postAggregations':[{'type':'arithmetic','name':'postagg#0','fn':'+',"
        + "'fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'},{'type':'fieldAccess','"
        + "name':'','fieldName':'$f2'}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "groups=[{63}], aggs=[[SUM($90), SUM($91)]], post_projects=[[+($1, $2), $0]], "
        + "sort0=[0], dir0=[DESC]";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(postAggString))
        .returnsOrdered("A=369117.52790000016; store_state=WA",
            "A=222698.26509999996; store_state=CA",
            "A=199049.57059999998; store_state=OR");
  }

  @Test public void testDivideArithmeticOperation() {
    final String sqlQuery = "select \"store_state\", sum(\"store_sales\") / sum(\"store_cost\") "
        + "as a from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "'postAggregations':[{'type':'arithmetic','name':'postagg#0',"
        + "'fn':'quotient','fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'},"
        + "{'type':'fieldAccess','name':'','fieldName':'$f2'}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "groups=[{63}], aggs=[[SUM($90), SUM($91)]], post_projects=[[$0, /($1, $2)]], "
            + "sort0=[1], dir0=[DESC]";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(postAggString))
        .returnsOrdered("store_state=OR; A=2.506091302943239",
            "store_state=CA; A=2.505379741272971",
            "store_state=WA; A=2.5045806163801996");
  }

  @Test public void testMultiplyArithmeticOperation() {
    final String sqlQuery = "select \"store_state\", sum(\"store_sales\") * sum(\"store_cost\") "
        + "as a from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "'postAggregations':[{'type':'arithmetic','name':'postagg#0',"
        + "'fn':'*','fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'},"
        + "{'type':'fieldAccess','name':'','fieldName':'$f2'}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "groups=[{63}], aggs=[[SUM($90), SUM($91)]], post_projects=[[$0, *($1, $2)]], "
            + "sort0=[1], dir0=[DESC]";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(postAggString))
        .returnsOrdered("store_state=WA; A=2.7783838325212463E10",
            "store_state=CA; A=1.0112000537448784E10",
            "store_state=OR; A=8.077425041941243E9");
  }

  @Test public void testMinusArithmeticOperation() {
    final String sqlQuery = "select \"store_state\", sum(\"store_sales\") - sum(\"store_cost\") "
        + "as a from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "'postAggregations':[{'type':'arithmetic','name':'postagg#0',"
        + "'fn':'-','fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'},"
        + "{'type':'fieldAccess','name':'','fieldName':'$f2'}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "groups=[{63}], aggs=[[SUM($90), SUM($91)]], post_projects=[[$0, -($1, $2)]], "
            + "sort0=[1], dir0=[DESC]";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(postAggString))
        .returnsOrdered("store_state=WA; A=158468.91210000002",
            "store_state=CA; A=95637.41489999992",
            "store_state=OR; A=85504.56939999988");
  }

  @Test public void testConstantPostAggregator() {
    final String sqlQuery = "select \"store_state\", sum(\"store_sales\") + 100 as a from "
        + "\"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "{'type':'constant','name':'','value':100.0}";
    final String plan = "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "groups=[{63}], aggs=[[SUM($90)]], post_projects=[[$0, +($1, 100)]], "
            + "sort0=[1], dir0=[DESC]";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(postAggString))
        .returnsOrdered("store_state=WA; A=263893.2200000001",
            "store_state=CA; A=159267.83999999994",
            "store_state=OR; A=142377.06999999992");
  }

  @Test public void testRecursiveArithmeticOperation() {
    final String sqlQuery = "select \"store_state\", -1 * (a + b) as c from (select "
        + "(sum(\"store_sales\")-sum(\"store_cost\")) / (count(*) * 3) "
        + "AS a,sum(\"unit_sales\") AS b, \"store_state\"  from \"foodmart\"  group "
        + "by \"store_state\") order by c desc";
    String postAggString = "'postAggregations':[{'type':'arithmetic','name':'postagg#0',"
        + "'fn':'*','fields':[{'type':'constant','name':'','value':-1.0},{'type':"
        + "'arithmetic','name':'','fn':'+','fields':[{'type':'arithmetic','name':"
        + "'','fn':'quotient','fields':[{'type':'arithmetic','name':'','fn':'-',"
        + "'fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'},{'type':"
        + "'fieldAccess','name':'','fieldName':'$f2'}]},{'type':'arithmetic','name':"
        + "'','fn':'*','fields':[{'type':'fieldAccess','name':'','fieldName':'$f3'},"
        + "{'type':'constant','name':'','value':3.0}]}]},{'type':'fieldAccess','name'"
        + ":'','fieldName':'B'}]}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{63}], "
            + "aggs=[[SUM($90), SUM($91), COUNT(), SUM($89)]], "
            + "post_projects=[[$0, *(-1, +(/(-($1, $2), *($3, 3)), $4))]], sort0=[1], dir0=[DESC])";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(postAggString))
        .returnsOrdered("store_state=OR; C=-67660.31890435601",
            "store_state=CA; C=-74749.30433035882",
            "store_state=WA; C=-124367.29537914316");
  }

  /**
   * Turn on now count(distinct )
   */
  @Test public void testHyperUniquePostAggregator() {
    final String sqlQuery = "select \"store_state\", sum(\"store_cost\") / count(distinct "
        + "\"brand_name\") as a from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "'postAggregations':[{'type':'arithmetic','name':'postagg#0','fn':"
        + "'quotient','fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'},"
        + "{'type':'hyperUniqueCardinality','name':'','fieldName':'$f2'}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals="
        + "[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{63}], ";
    foodmartApprox(sqlQuery)
        .runs()
        .explainContains(plan)
        .queryContains(druidChecker(postAggString));
  }

  @Test public void testExtractFilterWorkWithPostAggregations() {
    final String sql = "SELECT \"store_state\", \"brand_name\", sum(\"store_sales\") - "
        + "sum(\"store_cost\") as a  from \"foodmart\" where extract (week from \"timestamp\")"
        + " IN (10,11) and \"brand_name\"='Bird Call' group by \"store_state\", \"brand_name\"";
    final String druidQuery = "'filter':{'type':'and','fields':[{'type':'selector','dimension'"
        + ":'brand_name','value':'Bird Call'},{'type':'or','fields':[{'type':'selector',"
        + "'dimension':'__time','value':'10','extractionFn':{'type':'timeFormat','format'"
        + ":'w','timeZone':'UTC','locale':'en-US'}},{'type':'selector','dimension':'__time'"
        + ",'value':'11','extractionFn':{'type':'timeFormat','format':'w','timeZone':'UTC'"
        + ",'locale':'en-US'}}]}]},'aggregations':[{'type':'doubleSum','name':'$f2',"
        + "'fieldName':'store_sales'},{'type':'doubleSum','name':'$f3','fieldName':"
        + "'store_cost'}],'postAggregations':[{'type':'arithmetic','name':'postagg#0'"
        + ",'fn':'-','fields':[{'type':'fieldAccess','name':'','fieldName':'$f2'},"
        + "{'type':'fieldAccess','name':'','fieldName':'$f3'}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[AND(=(";
    sql(sql, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(druidQuery))
        .returnsOrdered("store_state=CA; brand_name=Bird Call; A=34.364599999999996",
            "store_state=OR; brand_name=Bird Call; A=39.16359999999999",
            "store_state=WA; brand_name=Bird Call; A=53.742500000000014");
  }

  @Test public void testExtractFilterWorkWithPostAggregationsWithConstant() {
    final String sql = "SELECT \"store_state\", 'Bird Call' as \"brand_name\", "
        + "sum(\"store_sales\") - sum(\"store_cost\") as a  from \"foodmart\" "
        + "where extract (week from \"timestamp\")"
        + " IN (10,11) and \"brand_name\"='Bird Call' group by \"store_state\"";
    final String druidQuery = "'aggregations':[{'type':'doubleSum','name':'$f1','fieldName':"
        + "'store_sales'},{'type':'doubleSum','name':'$f2','fieldName':'store_cost'}],"
        + "'postAggregations':[{'type':'arithmetic','name':'postagg#0','fn':'-',"
        + "'fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'},{'type':'fieldAccess',"
        + "'name':'','fieldName':'$f2'}]}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  BindableProject(store_state=[$0], brand_name=['Bird Call'], A=[$1])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[AND(=(";
    sql(sql, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(druidQuery))
        .returnsOrdered("store_state=CA; brand_name=Bird Call; A=34.364599999999996",
            "store_state=OR; brand_name=Bird Call; A=39.16359999999999",
            "store_state=WA; brand_name=Bird Call; A=53.742500000000014");
  }

  @Test public void testSingleAverageFunction() {
    final String sqlQuery = "select \"store_state\", sum(\"store_cost\") / count(*) as a from "
        + "\"foodmart\" group by \"store_state\" order by a desc";
    String postAggString = "'aggregations':[{'type':'doubleSum','name':'$f1','fieldName':"
        + "'store_cost'},{'type':'count','name':'$f2'}],"
        + "'postAggregations':[{'type':'arithmetic','name':'postagg#0','fn':'quotient'"
        + ",'fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'},"
        + "{'type':'fieldAccess','name':'','fieldName':'$f2'}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "groups=[{63}], aggs=[[SUM($91), COUNT()]], post_projects=[[$0, /($1, $2)]], "
        + "sort0=[1], dir0=[DESC]";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(postAggString))
        .returnsOrdered("store_state=OR; A=2.6271402406293403",
            "store_state=CA; A=2.599338206292706",
            "store_state=WA; A=2.5828708592868717");
  }

  @Test public void testPartiallyPostAggregation() {
    final String sqlQuery = "select \"store_state\", sum(\"store_sales\") / sum(\"store_cost\")"
            + " as a, case when sum(\"unit_sales\")=0 then 1.0 else sum(\"unit_sales\") "
            + "end as b from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "'postAggregations':[{'type':'arithmetic','name':'postagg#0',"
            + "'fn':'quotient','fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'}"
            + ",{'type':'fieldAccess','name':'','fieldName':'$f2'}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
            + "  BindableProject(store_state=[$0], A=[$1], B=[CASE(=($2, 0), "
            + "1.0, CAST($2):DECIMAL(19, 0))])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "groups=[{63}], aggs=[[SUM($90), SUM($91), SUM($89)]], "
            + "post_projects=[[$0, /($1, $2), $3]], sort0=[1], dir0=[DESC]";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(postAggString))
        .returnsOrdered("store_state=OR; A=2.506091302943239; B=67659",
            "store_state=CA; A=2.505379741272971; B=74748",
            "store_state=WA; A=2.5045806163801996; B=124366");
  }

  @Test public void testDuplicateReferenceOnPostAggregation() {
    final String sqlQuery = "select \"store_state\", a, a - b as c from (select \"store_state\", "
        + "sum(\"store_sales\") + 100 as a, sum(\"store_cost\") as b from \"foodmart\"  group by "
        + "\"store_state\") order by a desc";
    String postAggString = "'postAggregations':[{'type':'arithmetic','name':'postagg#0',"
        + "'fn':'+','fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'},"
        + "{'type':'constant','name':'','value':100.0}]},{'type':'arithmetic',"
        + "'name':'postagg#1','fn':'-','fields':[{'type':'arithmetic','name':'',"
        + "'fn':'+','fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'},"
        + "{'type':'constant','name':'','value':100.0}]},{'type':'fieldAccess',"
        + "'name':'','fieldName':'B'}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{63}], "
        + "aggs=[[SUM($90), SUM($91)]], post_projects=[[$0, +($1, 100), -(+($1, 100), $2)]], "
        + "sort0=[1], dir0=[DESC]";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(postAggString))
        .returnsOrdered("store_state=WA; A=263893.2200000001; C=158568.91210000002",
            "store_state=CA; A=159267.83999999994; C=95737.41489999992",
            "store_state=OR; A=142377.06999999992; C=85604.56939999988");
  }

  @Test public void testDivideByZeroDoubleTypeInfinity() {
    final String sqlQuery = "select \"store_state\", sum(\"store_cost\") / 0 as a from "
        + "\"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "'postAggregations':[{'type':'arithmetic','name':'postagg#0',"
        + "'fn':'quotient','fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'},"
        + "{'type':'constant','name':'','value':0.0}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "groups=[{63}], aggs=[[SUM($91)]], post_projects=[[$0, /($1, 0)]]";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(postAggString))
        .returnsOrdered("store_state=CA; A=Infinity",
            "store_state=OR; A=Infinity",
            "store_state=WA; A=Infinity");
  }

  @Test public void testDivideByZeroDoubleTypeNegInfinity() {
    final String sqlQuery = "select \"store_state\", -1.0 * sum(\"store_cost\") / 0 as "
        + "a from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "'postAggregations':[{'type':'arithmetic','name':'postagg#0',"
        + "'fn':'quotient','fields':[{'type':'arithmetic','name':'',"
        + "'fn':'*','fields':[{'type':'constant','name':'','value':-1.0},"
        + "{'type':'fieldAccess','name':'','fieldName':'$f1'}]},"
        + "{'type':'constant','name':'','value':0.0}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "groups=[{63}], aggs=[[SUM($91)]], post_projects=[[$0, /(*(-1.0, $1), 0)]]";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(postAggString))
        .returnsOrdered("store_state=CA; A=-Infinity",
            "store_state=OR; A=-Infinity",
            "store_state=WA; A=-Infinity");
  }

  @Test public void testDivideByZeroDoubleTypeNaN() {
    final String sqlQuery = "select \"store_state\", (sum(\"store_cost\") - sum(\"store_cost\")) "
        + "/ 0 as a from \"foodmart\"  group by \"store_state\" order by a desc";
    String postAggString = "'postAggregations':[{'type':'arithmetic','name':'postagg#0',"
        + "'fn':'quotient','fields':[{'type':'arithmetic','name':'','fn':'-',"
        + "'fields':[{'type':'fieldAccess','name':'','fieldName':'$f1'},"
        + "{'type':'fieldAccess','name':'','fieldName':'$f1'}]},"
        + "{'type':'constant','name':'','value':0.0}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "groups=[{63}], aggs=[[SUM($91)]], post_projects=[[$0, /(-($1, $1), 0)]], "
        + "sort0=[1], dir0=[DESC]";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(postAggString))
        .returnsOrdered("store_state=CA; A=NaN",
            "store_state=OR; A=NaN",
            "store_state=WA; A=NaN");
  }

  @Test public void testDivideByZeroIntegerType() {
    final String sqlQuery = "select \"store_state\", (count(*) - "
            + "count(*)) / 0 as a from \"foodmart\"  group by \"store_state\" "
            + "order by a desc";
    final String plan = "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "groups=[{63}], aggs=[[COUNT()]], post_projects=[[$0, /(-($1, $1), 0)]]";
    sql(sqlQuery, FOODMART)
            .explainContains(plan)
            .throws_("/ by zero");
  }

  @Test public void testInterleaveBetweenAggregateAndGroupOrderByOnMetrics() {
    final String sqlQuery = "select \"store_state\", \"brand_name\", \"A\" from (\n"
            + "  select sum(\"store_sales\")-sum(\"store_cost\") as a, \"store_state\""
            + ", \"brand_name\"\n"
            + "  from \"foodmart\"\n"
            + "  group by \"store_state\", \"brand_name\" ) subq\n"
            + "order by \"A\" limit 5";
    String postAggString = "'limitSpec':{'type':'default','limit':5,'columns':[{'dimension':"
            + "'postagg#0','direction':'ascending','dimensionOrder':'numeric'}]},"
            + "'aggregations':[{'type':'doubleSum','name':'$f2','fieldName':'store_sales'},"
            + "{'type':'doubleSum','name':'$f3','fieldName':'store_cost'}],'postAggregations':"
            + "[{'type':'arithmetic','name':'postagg#0','fn':'-','fields':"
            + "[{'type':'fieldAccess','name':'','fieldName':'$f2'},"
            + "{'type':'fieldAccess','name':'','fieldName':'$f3'}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "groups=[{2, 63}], aggs=[[SUM($90), SUM($91)]], "
            + "post_projects=[[$1, $0, -($2, $3)]], sort0=[2], dir0=[ASC], fetch=[5]";
    sql(sqlQuery, FOODMART)
            .explainContains(plan)
            .queryContains(druidChecker(postAggString))
            .returnsOrdered("store_state=CA; brand_name=King; A=21.4632",
                    "store_state=OR; brand_name=Symphony; A=32.176",
                    "store_state=CA; brand_name=Toretti; A=32.24650000000001",
                    "store_state=WA; brand_name=King; A=34.6104",
                    "store_state=OR; brand_name=Toretti; A=36.3");
  }

  @Test public void testInterleaveBetweenAggregateAndGroupOrderByOnDimension() {
    final String sqlQuery = "select \"store_state\", \"brand_name\", \"A\" from \n"
            + "(select \"store_state\", sum(\"store_sales\")+sum(\"store_cost\") "
            + "as a, \"brand_name\" from \"foodmart\" group by \"store_state\", \"brand_name\") "
            + "order by \"brand_name\", \"store_state\" limit 5";
    String postAggString = "'limitSpec':{'type':'default','limit':5,'columns':[{'dimension':"
            + "'brand_name','direction':'ascending','dimensionOrder':'alphanumeric'},{'dimension':"
            + "'store_state','direction':'ascending','dimensionOrder':'alphanumeric'}]},"
            + "'aggregations':[{'type':'doubleSum','name':'$f2','fieldName':'store_sales'},"
            + "{'type':'doubleSum','name':'$f3','fieldName':'store_cost'}],'postAggregations':"
            + "[{'type':'arithmetic','name':'postagg#0','fn':'+','fields':"
            + "[{'type':'fieldAccess','name':'','fieldName':'$f2'},"
            + "{'type':'fieldAccess','name':'','fieldName':'$f3'}]}]";
    final String plan = "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "projects=[[$63, $2, $90, $91]], "
            + "groups=[{0, 1}], aggs=[[SUM($2), SUM($3)]], "
            + "post_projects=[[$0, $1, +($2, $3)]], sort0=[1], sort1=[0], dir0=[ASC], dir1=[ASC]";
    sql(sqlQuery, FOODMART)
            .explainContains(plan)
            .queryContains(druidChecker(postAggString))
            .returnsOrdered("store_state=CA; brand_name=ADJ; A=222.1524",
                    "store_state=OR; brand_name=ADJ; A=186.60359999999997",
                    "store_state=WA; brand_name=ADJ; A=216.9912",
                    "store_state=CA; brand_name=Akron; A=250.349",
                    "store_state=OR; brand_name=Akron; A=278.69720000000007");
  }

  @Test public void testOrderByOnMetricsInSelectDruidQuery() {
    final String sqlQuery = "select \"store_sales\" as a, \"store_cost\" as b, \"store_sales\" - "
            + "\"store_cost\" as c from \"foodmart\" where \"timestamp\" "
            + ">= '1997-01-01 00:00:00 UTC' and \"timestamp\" < '1997-09-01 00:00:00 UTC' order by c "
            + "limit 5";
    String postAggString = "'queryType':'scan'";
    final String plan = "PLAN=EnumerableInterpreter\n"
            + "  BindableSort(sort0=[$2], dir0=[ASC], fetch=[5])\n"
            + "    BindableProject(A=[$0], B=[$1], C=[-($0, $1)])\n"
            + "      DruidQuery(";
    sql(sqlQuery, FOODMART)
            .explainContains(plan)
            .queryContains(druidChecker(postAggString))
            .returnsOrdered("A=0.51; B=0.2448; C=0.2652",
                    "A=0.51; B=0.2397; C=0.2703",
                    "A=0.57; B=0.285; C=0.285",
                    "A=0.5; B=0.21; C=0.29000000000000004",
                    "A=0.57; B=0.2793; C=0.29069999999999996");
  }

  /**
   * Tests whether an aggregate with a filter clause has it's filter factored out
   * when there is no outer filter
   */
  @Test public void testFilterClauseFactoredOut() {
    // Logically equivalent to
    // select sum("store_sales") from "foodmart" where "the_year" >= 1997
    String sql = "select sum(\"store_sales\") "
            + "filter (where \"the_year\" >= 1997) from \"foodmart\"";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
            + "'granularity':'all','filter':{'type':'bound','dimension':'the_year','lower':'1997',"
            + "'lowerStrict':false,'ordering':'numeric'},'aggregations':[{'type':'doubleSum','name'"
            + ":'EXPR$0','fieldName':'store_sales'}],'intervals':['1900-01-09T00:00:00.000Z/2992-01"
            + "-10T00:00:00.000Z'],'context':{'skipEmptyBuckets':true}}";

    sql(sql).queryContains(druidChecker(expectedQuery));
  }

  /**
   * Tests whether filter clauses with filters that are always true disappear or not
   */
  @Test public void testFilterClauseAlwaysTrueGone() {
    // Logically equivalent to
    // select sum("store_sales") from "foodmart"
    String sql = "select sum(\"store_sales\") filter (where 1 = 1) from \"foodmart\"";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
            + "'granularity':'all','aggregations':[{'type':'doubleSum','name':'EXPR$0','fieldName':"
            + "'store_sales'}],'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':true}}";

    sql(sql).queryContains(druidChecker(expectedQuery));
  }

  /**
   * Tests whether filter clauses with filters that are always true disappear in the presence
   * of another aggregate without a filter clause
   */
  @Test public void testFilterClauseAlwaysTrueWithAggGone1() {
    // Logically equivalent to
    // select sum("store_sales"), sum("store_cost") from "foodmart"
    String sql = "select sum(\"store_sales\") filter (where 1 = 1), "
            + "sum(\"store_cost\") from \"foodmart\"";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
            + "'granularity':'all','aggregations':[{'type':'doubleSum','name':'EXPR$0','fieldName':"
            + "'store_sales'},{'type':'doubleSum','name':'EXPR$1','fieldName':'store_cost'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':true}}";

    sql(sql).queryContains(druidChecker(expectedQuery));
  }

  /**
   * Tests whether filter clauses with filters that are always true disappear in the presence
   * of another aggregate with a filter clause
   */
  @Test public void testFilterClauseAlwaysTrueWithAggGone2() {
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
            + "'context':{'skipEmptyBuckets':true}}";

    sql(sql).queryContains(druidChecker(expectedQuery));
  }

  /**
   * Tests whether an existing outer filter is untouched when an aggregate has a filter clause
   * that is always true
   */
  @Test public void testOuterFilterRemainsWithAlwaysTrueClause() {
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
            + "'context':{'skipEmptyBuckets':true}}";

    sql(sql).queryContains(druidChecker(expectedQuery));
  }

  /**
   * Tests that an aggregate with a filter clause that is always false does not get pushed in
   */
  @Test public void testFilterClauseAlwaysFalseNotPushed() {
    String sql = "select sum(\"store_sales\") filter (where 1 > 1) from \"foodmart\"";
    // Calcite takes care of the unsatisfiable filter
    String expectedSubExplain =
            "  BindableAggregate(group=[{}], EXPR$0=[SUM($0) FILTER $1])\n"
            + "    BindableProject(store_sales=[$0], $f1=[false])\n";
    sql(sql).explainContains(expectedSubExplain);
  }

  /**
   * Tests that an aggregate with a filter clause that is always false does not get pushed when
   * there is already an outer filter
   */
  @Test public void testFilterClauseAlwaysFalseNotPushedWithFilter() {
    String sql = "select sum(\"store_sales\") filter (where 1 > 1) "
            + "from \"foodmart\" where \"store_city\" = 'Seattle'";
    String expectedSubExplain =
            "  BindableAggregate(group=[{}], EXPR$0=[SUM($0) FILTER $1])\n"
            + "    BindableProject(store_sales=[$0], $f1=[false])\n"
            + "      DruidQuery(table=[[foodmart, foodmart]], "
                    + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
                    // Make sure the original filter is still there
                    + "filter=[=($62, 'Seattle')], projects=[[$90]])";

    sql(sql).explainContains(expectedSubExplain);

  }

  /**
   * Tests that an aggregate with a filter clause that is the same as the outer filter has no
   * references to that filter, and that the original outer filter remains
   */
  @Test public void testFilterClauseSameAsOuterFilterGone() {
    // Logically equivalent to
    // select sum("store_sales") from "foodmart" where "store_city" = 'Seattle'
    String sql = "select sum(\"store_sales\") filter (where \"store_city\" = 'Seattle') "
            + "from \"foodmart\" where \"store_city\" = 'Seattle'";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
            + "'granularity':'all','filter':{'type':'selector','dimension':'store_city','value':"
            + "'Seattle'},'aggregations':[{'type':'doubleSum','name':'EXPR$0','fieldName':"
            + "'store_sales'}],'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':true}}";

    sql(sql)
        .queryContains(druidChecker(expectedQuery))
        .returnsUnordered("EXPR$0=52644.07000000001");
  }

  /**
   * Test to ensure that an aggregate with a filter clause in the presence of another aggregate
   * without a filter clause does not have it's filter factored out into the outer filter
   */
  @Test public void testFilterClauseNotFactoredOut1() {
    String sql = "select sum(\"store_sales\") filter (where \"store_state\" = 'CA'), "
            + "sum(\"store_cost\") from \"foodmart\"";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
            + "'granularity':'all','aggregations':[{'type':'filtered','filter':{'type':'selector',"
            + "'dimension':'store_state','value':'CA'},'aggregator':{'type':'doubleSum','name':"
            + "'EXPR$0','fieldName':'store_sales'}},{'type':'doubleSum','name':'EXPR$1','fieldName'"
            + ":'store_cost'}],'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':true}}";

    sql(sql).queryContains(druidChecker(expectedQuery));
  }

  /**
   * Test to ensure that an aggregate with a filter clause in the presence of another aggregate
   * without a filter clause, and an outer filter does not have it's
   * filter factored out into the outer filter
   */
  @Test public void testFilterClauseNotFactoredOut2() {
    String sql = "select sum(\"store_sales\") filter (where \"store_state\" = 'CA'), "
            + "sum(\"store_cost\") from \"foodmart\" where \"the_year\" >= 1997";
    String expectedQuery = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
            + "'granularity':'all','filter':{'type':'bound','dimension':'the_year','lower':'1997',"
            + "'lowerStrict':false,'ordering':'numeric'},'aggregations':[{'type':'filtered',"
            + "'filter':{'type':'selector','dimension':'store_state','value':'CA'},'aggregator':{"
            + "'type':'doubleSum','name':'EXPR$0','fieldName':'store_sales'}},{'type':'doubleSum',"
            + "'name':'EXPR$1','fieldName':'store_cost'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':true}}";

    sql(sql).queryContains(druidChecker(expectedQuery));
  }

  /**
   * Test to ensure that multiple aggregates with filter clauses have their filters extracted to
   * the outer filter field for data pruning
   */
  @Test public void testFilterClausesFactoredForPruning1() {
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
            + "'context':{'skipEmptyBuckets':true}}";

    sql(sql)
        .queryContains(druidChecker(expectedQuery))
        .returnsUnordered("EXPR$0=159167.83999999994; EXPR$1=263793.2200000001");
  }

  /**
   * Test to ensure that multiple aggregates with filter clauses have their filters extracted to
   * the outer filter field for data pruning in the presence of an outer filter
   */
  @Test public void testFilterClausesFactoredForPruning2() {
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
            + "'context':{'skipEmptyBuckets':true}}";

    sql(sql)
        .queryContains(druidChecker(expectedQuery))
        .returnsUnordered("EXPR$0=2600.01; EXPR$1=4486.4400000000005");
  }

  /**
   * Test to ensure that multiple aggregates with the same filter clause have them factored
   * out in the presence of an outer filter, and that they no longer refer to those filters
   */
  @Test public void testMultipleFiltersFactoredOutWithOuterFilter() {
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
            + "'context':{'skipEmptyBuckets':true}}";

    sql(sql)
        .queryContains(druidChecker(expectedQuery))
        .explainContains(expectedAggregateExplain)
        .returnsUnordered("EXPR$0=2600.01; EXPR$1=1013.162");
  }

  /**
   * Tests that when the resulting filter from factoring filter clauses out is always false,
   * that they are still pushed to Druid to handle.
   */
  @Test public void testOuterFilterFalseAfterFactorSimplification() {
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
        .queryContains(druidChecker(expectedFilter))
        .returnsUnordered("");
  }

  /**
   * Test to ensure that aggregates with filter clauses that Druid cannot handle are not pushed in
   * as filtered aggregates.
   */
  @Test public void testFilterClauseNotPushable() {
    // Currently the adapter does not support the LIKE operator
    String sql = "select sum(\"store_sales\") "
            + "filter (where \"the_year\" like '199_') from \"foodmart\"";
    String expectedSubExplain =
            "  BindableAggregate(group=[{}], EXPR$0=[SUM($0) FILTER $1])\n"
            + "    BindableProject(store_sales=[$1], $f1=[IS TRUE(LIKE($0, '199_'))])";

    sql(sql).explainContains(expectedSubExplain);
  }

  @Test public void testFilterClauseWithMetricRef() {
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
            druidChecker("\"queryType\":\"timeseries\"", "\"filter\":{\"type\":\"bound\","
                + "\"dimension\":\"store_cost\",\"lower\":\"10\",\"lowerStrict\":true,"
                + "\"ordering\":\"numeric\"}"))
        .returnsUnordered("EXPR$0=25.060000000000002");
  }

  @Test public void testFilterClauseWithMetricRefAndAggregates() {
    String sql = "select sum(\"store_sales\"), \"product_id\" "
        + "from \"foodmart\" where \"product_id\" > 1553 and \"store_cost\" > 5 group by \"product_id\"";
    String expectedSubExplain =
        "PLAN=EnumerableInterpreter\n"
            + "  BindableProject(EXPR$0=[$1], product_id=[$0])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[AND(>"
            + "(CAST($1):BIGINT, 1553), >($91, 5))], groups=[{1}], aggs=[[SUM($90)]])";

    sql(sql)
        .explainContains(expectedSubExplain)
        .queryContains(
            druidChecker("\"queryType\":\"groupBy\"", "{\"type\":\"bound\","
                + "\"dimension\":\"store_cost\",\"lower\":\"5\",\"lowerStrict\":true,"
                + "\"ordering\":\"numeric\"}"))
        .returnsUnordered("EXPR$0=10.16; product_id=1554\n"
            + "EXPR$0=45.05; product_id=1556\n"
            + "EXPR$0=88.5; product_id=1555");
  }

  @Test public void testFilterClauseWithMetricAndTimeAndAggregates() {
    String sql = "select sum(\"store_sales\"), \"product_id\""
        + "from \"foodmart\" where \"product_id\" > 1555 and \"store_cost\" > 5 and extract(year "
        + "from \"timestamp\") = 1997 "
        + "group by floor(\"timestamp\" to DAY),\"product_id\"";
    sql(sql)
        .queryContains(
            druidChecker("\"queryType\":\"groupBy\"", "{\"type\":\"bound\","
                + "\"dimension\":\"store_cost\",\"lower\":\"5\",\"lowerStrict\":true,"
                + "\"ordering\":\"numeric\"}"))
        .returnsUnordered("EXPR$0=10.6; product_id=1556\n"
            + "EXPR$0=10.6; product_id=1556\n"
            + "EXPR$0=10.6; product_id=1556\n"
            + "EXPR$0=13.25; product_id=1556");
  }

  /**
   * Test to ensure that an aggregate with a nested filter clause has it's filter factored out
   */
  @Test public void testNestedFilterClauseFactored() {
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
            .queryContains(druidChecker(expectedFilterJson))
            .queryContains(druidChecker(expectedAggregateJson))
            .returnsUnordered("EXPR$0=301444.9099999999");
  }

  /**
   * Test to ensure that aggregates with nested filters have their filters factored out
   * into the outer filter for data pruning while still holding a reference to the filter clause
   */
  @Test public void testNestedFilterClauseInAggregates() {
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
            .queryContains(druidChecker(expectedFilterJson))
            .queryContains(druidChecker(expectedAggregatesJson))
            .returnsUnordered("EXPR$0=13077.789999999992; EXPR$1=9830.7799");
  }

  /**
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1805">[CALCITE-1805]
   * Druid adapter cannot handle count column without adding support for nested queries</a>.
   */
  @Test public void testCountColumn() {
    final String sql = "SELECT count(\"countryName\") FROM (SELECT \"countryName\" FROM "
        + "\"wikiticker\" WHERE \"countryName\"  IS NOT NULL) as a";
    sql(sql, WIKI_AUTO2)
        .returnsUnordered("EXPR$0=3799");

    final String sql2 = "SELECT count(\"countryName\") FROM (SELECT \"countryName\" FROM "
        + "\"wikiticker\") as a";
    final String plan2 = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[wiki, wikiticker]], "
        + "intervals=[[1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z]], projects=[[$7]], "
        + "groups=[{}], aggs=[[COUNT($0)]])";
    sql(sql2, WIKI_AUTO2)
        .returnsUnordered("EXPR$0=3799")
        .explainContains(plan2);

    final String sql3 = "SELECT count(*), count(\"countryName\") FROM \"wikiticker\"";
    final String plan3 = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[wiki, wikiticker]], "
        + "intervals=[[1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z]], projects=[[$7]], "
        + "groups=[{}], aggs=[[COUNT(), COUNT($0)]])";
    sql(sql3, WIKI_AUTO2)
        .explainContains(plan3);
  }


  @Test public void testCountColumn2() {
    final String sql = "SELECT count(\"countryName\") FROM (SELECT \"countryName\" FROM "
        + "\"wikiticker\" WHERE \"countryName\"  IS NOT NULL) as a";
    sql(sql, WIKI_AUTO2)
        .queryContains(druidChecker("timeseries"))
        .returnsUnordered("EXPR$0=3799");
  }

  @Test public void testCountWithNonNull() {
    final String sql = "select count(\"timestamp\") from \"foodmart\"\n";
    final String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart'";
    sql(sql)
        .returnsUnordered("EXPR$0=86829")
        .queryContains(druidChecker(druidQuery));
  }

  /**
   * Test to make sure the "not" filter has only 1 field, rather than an array of fields.
   */
  @Test public void testNotFilterForm() {
    String sql = "select count(distinct \"the_month\") from "
            + "\"foodmart\" where \"the_month\" <> \'October\'";
    String druidFilter = "'filter':{'type':'not',"
            + "'field':{'type':'selector','dimension':'the_month','value':'October'}}";
    // Check that the filter actually worked, and that druid was responsible for the filter
    sql(sql, FOODMART)
            .queryContains(druidChecker(druidFilter))
            .returnsOrdered("EXPR$0=11");
  }

  /**
   * Test to ensure that count(distinct ...) gets pushed to Druid when approximate results are
   * acceptable
   * */
  @Test public void testDistinctCountWhenApproxResultsAccepted() {
    String sql = "select count(distinct \"store_state\") from \"foodmart\"";
    String expectedSubExplain = "DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00"
        + ":00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{}], aggs=[[COUNT(DISTINCT $63)]])";
    String expectedAggregate = "{'type':'cardinality','name':"
        + "'EXPR$0','fieldNames':['store_state']}";

    testCountWithApproxDistinct(true, sql, expectedSubExplain, expectedAggregate);
  }

  /**
   * Test to ensure that count(distinct ...) doesn't get pushed to Druid when approximate results
   * are not acceptable
   */
  @Test public void testDistinctCountWhenApproxResultsNotAccepted() {
    String sql = "select count(distinct \"store_state\") from \"foodmart\"";
    String expectedSubExplain = "  BindableAggregate(group=[{}], EXPR$0=[COUNT($0)])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "groups=[{63}], aggs=[[]])";

    testCountWithApproxDistinct(false, sql, expectedSubExplain);
  }

  /**
   * Test to ensure that a count distinct on metric does not get pushed into Druid
   */
  @Test public void testDistinctCountOnMetric() {
    String sql = "select count(distinct \"store_sales\") from \"foodmart\" "
        + "where \"store_state\" = 'WA'";
    String expectedSubExplain = "PLAN=EnumerableInterpreter\n"
        + "  BindableAggregate(group=[{}], EXPR$0=[COUNT($0)])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[=($63, 'WA')"
        + "], groups=[{90}], aggs=[[]])";

    testCountWithApproxDistinct(true, sql, expectedSubExplain, "\"queryType\":\"groupBy\"");
    testCountWithApproxDistinct(false, sql, expectedSubExplain, "\"queryType\":\"groupBy\"");
  }

  /**
   * Test to ensure that a count on a metric does not get pushed into Druid
   */
  @Test public void testCountOnMetric() {
    String sql = "select \"brand_name\", count(\"store_sales\") from \"foodmart\" "
        + "group by \"brand_name\"";
    String expectedSubExplain = "  BindableAggregate(group=[{0}], EXPR$1=[COUNT($1)])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$2, $90]])";

    testCountWithApproxDistinct(true, sql, expectedSubExplain);
    testCountWithApproxDistinct(false, sql, expectedSubExplain);
  }

  /**
   * Test to ensure that count(*) is pushed into Druid
   */
  @Test public void testCountStar() {
    String sql = "select count(*) from \"foodmart\"";
    String expectedSubExplain = "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "projects=[[]], groups=[{}], aggs=[[COUNT()]])";

    sql(sql).explainContains(expectedSubExplain);
  }

  /**
   * Test to ensure that count() aggregates with metric columns are not pushed into Druid
   * even when the metric column has been renamed
   */
  @Test public void testCountOnMetricRenamed() {
    String sql = "select \"B\", count(\"A\") from "
        + "(select \"unit_sales\" as \"A\", \"store_state\" as \"B\" from \"foodmart\") "
        + "group by \"B\"";
    String expectedSubExplain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$89, "
        + "$63]], groups=[{1}], aggs=[[COUNT($0)]]";

    testCountWithApproxDistinct(true, sql, expectedSubExplain);
    testCountWithApproxDistinct(false, sql, expectedSubExplain);
  }

  @Test public void testDistinctCountOnMetricRenamed() {
    String sql = "select \"B\", count(distinct \"A\") from "
        + "(select \"unit_sales\" as \"A\", \"store_state\" as \"B\" from \"foodmart\") "
        + "group by \"B\"";
    String expectedSubExplain = "PLAN=EnumerableInterpreter\n"
        + "  BindableAggregate(group=[{0}], EXPR$1=[COUNT($1)])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{63, 89}], "
        + "aggs=[[]])";

    testCountWithApproxDistinct(true, sql, expectedSubExplain, "\"queryType\":\"groupBy\"");
    testCountWithApproxDistinct(false, sql, expectedSubExplain, "\"queryType\":\"groupBy\"");
  }

  private void testCountWithApproxDistinct(boolean approx, String sql, String expectedExplain) {
    testCountWithApproxDistinct(approx, sql, expectedExplain, "");
  }

  private void testCountWithApproxDistinct(boolean approx, String sql, String expectedExplain,
      String expectedDruidQuery) {
    CalciteAssert.that()
        .enable(enabled())
        .with(ImmutableMap.of("model", FOODMART.getPath()))
        .with(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT.camelName(), approx)
        .query(sql)
        .runs()
        .explainContains(expectedExplain)
        .queryContains(druidChecker(expectedDruidQuery));
  }

  /**
   * Tests the use of count(distinct ...) on a complex metric column in SELECT
   * */
  @Test public void testCountDistinctOnComplexColumn() {
    // Because approximate distinct count has not been enabled
    sql("select count(distinct \"user_id\") from \"wiki\"", WIKI)
            .failsAtValidation("Rolled up column 'user_id' is not allowed in COUNT");

    foodmartApprox("select count(distinct \"customer_id\") from \"foodmart\"")
            // customer_id gets transformed into it's actual underlying sketch column,
            // customer_id_ts. The thetaSketch aggregation is used to compute the count distinct.
            .queryContains(
                    druidChecker("{'queryType':'timeseries','dataSource':"
                            + "'foodmart','descending':false,'granularity':'all','aggregations':[{'type':"
                            + "'thetaSketch','name':'EXPR$0','fieldName':'customer_id_ts'}],"
                            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
                            + "'context':{'skipEmptyBuckets':true}}"))
            .returnsUnordered("EXPR$0=5581");

    foodmartApprox("select sum(\"store_sales\"), "
            + "count(distinct \"customer_id\") filter (where \"store_state\" = 'CA') "
            + "from \"foodmart\" where \"the_month\" = 'October'")
            // Check that filtered aggregations work correctly
            .queryContains(
                    druidChecker("{'type':'filtered','filter':"
                            + "{'type':'selector','dimension':'store_state','value':'CA'},'aggregator':"
                            + "{'type':'thetaSketch','name':'EXPR$1','fieldName':'customer_id_ts'}}]"))
            .returnsUnordered("EXPR$0=42342.26999999995; EXPR$1=459");
  }

  /**
   * Tests the use of other aggregations with complex columns
   * */
  @Test public void testAggregationsWithComplexColumns() {
    wikiApprox("select count(\"user_id\") from \"wiki\"")
            .failsAtValidation("Rolled up column 'user_id' is not allowed in COUNT");

    wikiApprox("select sum(\"user_id\") from \"wiki\"")
            .failsAtValidation("Cannot apply 'SUM' to arguments of type "
                    + "'SUM(<VARBINARY>)'. Supported form(s): 'SUM(<NUMERIC>)'");

    wikiApprox("select avg(\"user_id\") from \"wiki\"")
            .failsAtValidation("Cannot apply 'AVG' to arguments of type "
                    + "'AVG(<VARBINARY>)'. Supported form(s): 'AVG(<NUMERIC>)'");

    wikiApprox("select max(\"user_id\") from \"wiki\"")
            .failsAtValidation("Rolled up column 'user_id' is not allowed in MAX");

    wikiApprox("select min(\"user_id\") from \"wiki\"")
            .failsAtValidation("Rolled up column 'user_id' is not allowed in MIN");
  }

  /**
   * Test post aggregation support with +, -, /, * operators
   * */
  @Test public void testPostAggregationWithComplexColumns() {
    foodmartApprox("select "
            + "(count(distinct \"customer_id\") * 2) + "
            + "count(distinct \"customer_id\") - "
            + "(3 * count(distinct \"customer_id\")) "
            + "from \"foodmart\"")
            .queryContains(
                    druidChecker("'aggregations':[{'type':'thetaSketch','name':'$f0',"
                            + "'fieldName':'customer_id_ts'}],'postAggregations':[{'type':"
                            + "'arithmetic','name':'postagg#0','fn':'-','fields':[{'type':"
                            + "'arithmetic','name':'','fn':'+','fields':[{'type':'arithmetic','"
                            + "name':'','fn':'*','fields':[{'type':'thetaSketchEstimate','name':"
                            + "'','field':{'type':'fieldAccess','name':'','fieldName':'$f0'}},"
                            + "{'type':'constant','name':'','value':2.0}]},{'type':"
                            + "'thetaSketchEstimate','name':'','field':{'type':'fieldAccess',"
                            + "'name':'','fieldName':'$f0'}}]},{'type':'arithmetic','name':'',"
                            + "'fn':'*','fields':[{'type':'constant','name':'','value':3.0},"
                            + "{'type':'thetaSketchEstimate','name':'','field':{'type':"
                            + "'fieldAccess','name':'','fieldName':'$f0'}}]}]}]"))
            .returnsUnordered("EXPR$0=0");

    foodmartApprox("select "
            + "\"the_month\" as \"month\", "
            + "sum(\"store_sales\") / count(distinct \"customer_id\") as \"avg$\" "
            + "from \"foodmart\" group by \"the_month\"")
            .queryContains(
                    druidChecker("'aggregations':[{'type':'doubleSum','name':"
                            + "'$f1','fieldName':'store_sales'},{'type':'thetaSketch','name':'$f2',"
                            + "'fieldName':'customer_id_ts'}],'postAggregations':[{'type':'arithmetic',"
                            + "'name':'postagg#0','fn':'quotient','fields':[{'type':'fieldAccess','name':"
                            + "'','fieldName':'$f1'},{'type':'thetaSketchEstimate','name':'','field':"
                            + "{'type':'fieldAccess','name':'','fieldName':'$f2'}}]}]"))
            .returnsUnordered(
                    "month=January; avg$=32.62155444126063",
                    "month=February; avg$=33.102021036814484",
                    "month=March; avg$=33.84970906630567",
                    "month=April; avg$=32.557517084282296",
                    "month=May; avg$=32.42617797228287",
                    "month=June; avg$=33.93093562874239",
                    "month=July; avg$=34.36859097127213",
                    "month=August; avg$=32.81181818181806",
                    "month=September; avg$=33.327733840304155",
                    "month=October; avg$=32.74730858468674",
                    "month=November; avg$=34.51727684346705",
                    "month=December; avg$=33.62788665879565");

    final String druid = "'aggregations':[{'type':'hyperUnique','name':'$f0',"
        + "'fieldName':'user_unique'}],'postAggregations':[{'type':"
        + "'arithmetic','name':'postagg#0','fn':'-','fields':[{'type':"
        + "'arithmetic','name':'','fn':'+','fields':[{'type':"
        + "'hyperUniqueCardinality','name':'','fieldName':'$f0'},"
        + "{'type':'constant','name':'','value':100.0}]},{'type':"
        + "'arithmetic','name':'','fn':'*','fields':[{'type':"
        + "'hyperUniqueCardinality','name':'','fieldName':'$f0'},"
        + "{'type':'constant','name':'','value':2.0}]}]}]";
    final String sql = "select (count(distinct \"user_id\") + 100) - "
        + "(count(distinct \"user_id\") * 2) from \"wiki\"";
    wikiApprox(sql)
        .queryContains(druidChecker(druid))
        .returnsUnordered("EXPR$0=-10590");

    // Change COUNT(DISTINCT ...) to APPROX_COUNT_DISTINCT(...) and get
    // same result even if approximation is off by default.
    final String sql2 = "select (approx_count_distinct(\"user_id\") + 100) - "
        + "(approx_count_distinct(\"user_id\") * 2) from \"wiki\"";
    sql(sql2, WIKI)
        .queryContains(druidChecker(druid))
        .returnsUnordered("EXPR$0=-10590");
  }

  /**
   * Test to make sure that if a complex metric is also a dimension, then
   * {@link org.apache.calcite.adapter.druid.DruidTable} should allow it to be used like any other
   * column.
   * */
  @Test public void testComplexMetricAlsoDimension() {
    foodmartApprox("select \"customer_id\" from \"foodmart\"")
            .runs();

    foodmartApprox("select count(distinct \"the_month\"), \"customer_id\" "
            + "from \"foodmart\" group by \"customer_id\"")
            .queryContains(
                    druidChecker("{'queryType':'groupBy','dataSource':'foodmart',"
                            + "'granularity':'all','dimensions':[{'type':'default','dimension':"
                            + "'customer_id'}],'limitSpec':{'type':'default'},'aggregations':[{"
                            + "'type':'cardinality','name':'EXPR$0','fieldNames':['the_month']}],"
                            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}"));
  }

  /**
   * Test to make sure that SELECT * doesn't fail, and that the rolled up column is not requested
   * in the JSON query.
   * */
  @Test public void testSelectStarWithRollUp() {
    final String sql = "select * from \"wiki\" limit 5";
    sql(sql, WIKI)
        // make sure user_id column is not present
        .queryContains(
            druidChecker("{'queryType':'scan','dataSource':'wikiticker',"
                + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
                + "'columns':['__time','channel','cityName','comment','countryIsoCode','countryName',"
                + "'isAnonymous','isMinor','isNew','isRobot','isUnpatrolled','metroCode',"
                + "'namespace','page','regionIsoCode','regionName','count','added',"
                + "'deleted','delta'],'granularity':'all',"
                + "'resultFormat':'compactedList','limit':5"));
  }

  /**
   * Test to make sure that the mapping from a Table name to a Table returned from
   * {@link org.apache.calcite.adapter.druid.DruidSchema} is always the same Java object.
   * */
  @Test public void testTableMapReused() {
    AbstractSchema schema = new DruidSchema("http://localhost:8082", "http://localhost:8081", true);
    assertSame(schema.getTable("wikiticker"), schema.getTable("wikiticker"));
  }

  @Test public void testPushEqualsCastDimension() {
    final String sqlQuery = "select sum(\"store_cost\") as a "
        + "from \"foodmart\" "
        + "where cast(\"product_id\" as double) = 1016.0";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[=(CAST($1):DOUBLE, 1016.0)], groups=[{}], aggs=[[SUM($91)]])";
    final String druidQuery =
        "{'queryType':'timeseries','dataSource':'foodmart','descending':false,'granularity':'all',"
            + "'filter':{'type':'bound','dimension':'product_id','lower':'1016.0',"
            + "'lowerStrict':false,'upper':'1016.0','upperStrict':false,'ordering':'numeric'},"
            + "'aggregations':[{'type':'doubleSum','name':'A','fieldName':'store_cost'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':true}}";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(druidQuery))
        .returnsUnordered("A=85.31639999999999");

    final String sqlQuery2 = "select sum(\"store_cost\") as a "
        + "from \"foodmart\" "
        + "where cast(\"product_id\" as double) <= 1016.0 "
        + "and cast(\"product_id\" as double) >= 1016.0";
    sql(sqlQuery2, FOODMART)
        .returnsUnordered("A=85.31639999999999");
  }

  @Test public void testPushNotEqualsCastDimension() {
    final String sqlQuery = "select sum(\"store_cost\") as a "
        + "from \"foodmart\" "
        + "where cast(\"product_id\" as double) <> 1016.0";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[<>(CAST($1):DOUBLE, 1016.0)], groups=[{}], aggs=[[SUM($91)]])";
    final String druidQuery =
        "{'queryType':'timeseries','dataSource':'foodmart','descending':false,'granularity':'all',"
            + "'filter':{'type':'or','fields':[{'type':'bound','dimension':'product_id','lower':'1016.0',"
            + "'lowerStrict':true,'ordering':'numeric'},{'type':'bound','dimension':'product_id',"
            + "'upper':'1016.0','upperStrict':true,'ordering':'numeric'}]},"
            + "'aggregations':[{'type':'doubleSum','name':'A','fieldName':'store_cost'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':true}}";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(druidChecker(druidQuery))
        .returnsUnordered("A=225541.91720000014");

    final String sqlQuery2 = "select sum(\"store_cost\") as a "
        + "from \"foodmart\" "
        + "where cast(\"product_id\" as double) < 1016.0 "
        + "or cast(\"product_id\" as double) > 1016.0";
    sql(sqlQuery2, FOODMART)
        .returnsUnordered("A=225541.91720000014");
  }

  @Test public void testIsNull() {
    final String sql = "select count(*) as c "
        + "from \"foodmart\" "
        + "where \"product_id\" is null";
    final String druidQuery =
        "{'queryType':'timeseries','dataSource':'foodmart','descending':false,'granularity':'all',"
            + "'filter':{'type':'selector','dimension':'product_id','value':null},"
            + "'aggregations':[{'type':'count','name':'C'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':false}}";
    sql(sql, FOODMART)
        .queryContains(druidChecker(druidQuery))
        .returnsUnordered("C=0")
        .returnsCount(1);
  }

  @Test public void testIsNotNull() {
    final String sql = "select count(*) as c "
        + "from \"foodmart\" "
        + "where \"product_id\" is not null";
    final String druidQuery =
        "{'queryType':'timeseries','dataSource':'foodmart','descending':false,'granularity':'all',"
            + "'filter':{'type':'not','field':{'type':'selector','dimension':'product_id','value':null}},"
            + "'aggregations':[{'type':'count','name':'C'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':false}}";
    sql(sql, FOODMART)
        .queryContains(druidChecker(druidQuery))
        .returnsUnordered("C=86829");
  }

  @Test public void testFilterWithFloorOnTime() {
    // Test filter on floor on time column is pushed to druid
    final String sql =
        "Select cast(floor(\"timestamp\" to MONTH) as timestamp) as t from \"foodmart\" where "
            + "floor(\"timestamp\" to MONTH) between '1997-01-01 00:00:00 UTC'"
            + "and '1997-03-01 00:00:00 UTC' order by t limit 2";

    final String druidQueryPart1 = "\"filter\":{\"type\":\"and\",\"fields\":"
        + "[{\"type\":\"bound\",\"dimension\":\"__time\",\"lower\":\"1997-01-01T00:00:00.000Z\","
        + "\"lowerStrict\":false,\"ordering\":\"lexicographic\","
        + "\"extractionFn\":{\"type\":\"timeFormat\",\"format\":\"yyyy-MM-dd";
    final String druidQueryPart2 = "HH:mm:ss.SSS";
    final String druidQueryPart3 = ",\"granularity\":\"month\",\"timeZone\":\"UTC\","
        + "\"locale\":\"en-US\"}},{\"type\":\"bound\",\"dimension\":\"__time\""
        + ",\"upper\":\"1997-03-01T00:00:00.000Z\",\"upperStrict\":false,"
        + "\"ordering\":\"lexicographic\",\"extractionFn\":{\"type\":\"timeFormat\"";
    final String druidQueryPart4 = "\"columns\":[\"__time\"],\"granularity\":\"all\"";

    sql(sql, FOODMART)
        .queryContains(
            druidChecker(druidQueryPart1, druidQueryPart2, druidQueryPart3, druidQueryPart4))
        .returnsOrdered("T=1997-01-01 00:00:00", "T=1997-01-01 00:00:00");
  }

  @Test public void testSelectFloorOnTimeWithFilterOnFloorOnTime() {
    final String sql = "Select cast(floor(\"timestamp\" to MONTH) as timestamp) as t from "
        + "\"foodmart\" where floor(\"timestamp\" to MONTH) >= '1997-05-01 00:00:00 UTC' order by t"
        + " limit 1";
    final String druidQueryPart1 = "filter\":{\"type\":\"bound\",\"dimension\":\"__time\","
        + "\"lower\":\"1997-05-01T00:00:00.000Z\",\"lowerStrict\":false,"
        + "\"ordering\":\"lexicographic\",\"extractionFn\":{\"type\":\"timeFormat\","
        + "\"format\":\"yyyy-MM-dd";
    final String druidQueryPart2 = "\"granularity\":\"month\",\"timeZone\":\"UTC\","
        + "\"locale\":\"en-US\"}},\"columns\":[\"__time\"],\"granularity\":\"all\"";

    sql(sql, FOODMART).queryContains(druidChecker(druidQueryPart1, druidQueryPart2))
        .returnsOrdered("T=1997-05-01 00:00:00");
  }

  @Test public void testTmeWithFilterOnFloorOnTimeAndCastToTimestamp() {
    final String sql = "Select cast(floor(\"timestamp\" to MONTH) as timestamp) as t from "
        + "\"foodmart\" where floor(\"timestamp\" to MONTH) >= cast('1997-05-01 00:00:00' as TIMESTAMP) order by t"
        + " limit 1";
    final String druidQueryPart1 = "filter\":{\"type\":\"bound\",\"dimension\":\"__time\","
        + "\"lower\":\"1997-05-01T00:00:00.000Z\",\"lowerStrict\":false,"
        + "\"ordering\":\"lexicographic\",\"extractionFn\":{\"type\":\"timeFormat\","
        + "\"format\":\"yyyy-MM-dd";
    final String druidQueryPart2 = "\"granularity\":\"month\",\"timeZone\":\"UTC\","
        + "\"locale\":\"en-US\"}},\"columns\":[\"__time\"],\"granularity\":\"all\"";

    sql(sql, FOODMART).queryContains(druidChecker(druidQueryPart1, druidQueryPart2))
        .returnsOrdered("T=1997-05-01 00:00:00");
  }

  @Test public void testTmeWithFilterOnFloorOnTimeWithTimezone() {
    final String sql = "Select cast(floor(\"timestamp\" to MONTH) as timestamp) as t from "
        + "\"foodmart\" where floor(\"timestamp\" to MONTH) >= cast('1997-05-01 00:00:00'"
        + " as TIMESTAMP) order by t limit 1";
    final String druidQueryPart1 = "filter\":{\"type\":\"bound\",\"dimension\":\"__time\","
        + "\"lower\":\"1997-05-01T00:00:00.000Z\",\"lowerStrict\":false,"
        + "\"ordering\":\"lexicographic\",\"extractionFn\":{\"type\":\"timeFormat\","
        + "\"format\":\"yyyy-MM-dd";
    final String druidQueryPart2 = "\"granularity\":\"month\",\"timeZone\":\"IST\","
        + "\"locale\":\"en-US\"}},\"columns\":[\"__time\"],\"granularity\":\"all\"";

    CalciteAssert.that()
        .enable(enabled())
        .with(ImmutableMap.of("model", FOODMART.getPath()))
        .with(CalciteConnectionProperty.TIME_ZONE.camelName(), "IST")
        .query(sql)
        .runs()
        .queryContains(druidChecker(druidQueryPart1, druidQueryPart2))
        // NOTE: this return value is not as expected
        // see https://issues.apache.org/jira/browse/CALCITE-2107
        .returnsOrdered("T=1997-05-01 05:30:00");
  }

  @Test public void testTmeWithFilterOnFloorOnTimeWithTimezoneConversion() {
    final String sql = "Select cast(floor(\"timestamp\" to MONTH) as timestamp) as t from "
        + "\"foodmart\" where floor(\"timestamp\" to MONTH) >= '1997-04-30 18:30:00 UTC' order by t"
        + " limit 1";
    final String druidQueryPart1 = "filter\":{\"type\":\"bound\",\"dimension\":\"__time\","
        + "\"lower\":\"1997-05-01T00:00:00.000Z\",\"lowerStrict\":false,"
        + "\"ordering\":\"lexicographic\",\"extractionFn\":{\"type\":\"timeFormat\","
        + "\"format\":\"yyyy-MM-dd";
    final String druidQueryPart2 = "\"granularity\":\"month\",\"timeZone\":\"IST\","
        + "\"locale\":\"en-US\"}},\"columns\":[\"__time\"],\"granularity\":\"all\"";

    CalciteAssert.that()
        .enable(enabled())
        .with(ImmutableMap.of("model", FOODMART.getPath()))
        .with(CalciteConnectionProperty.TIME_ZONE.camelName(), "IST")
        .query(sql)
        .runs()
        .queryContains(druidChecker(druidQueryPart1, druidQueryPart2))
        // NOTE: this return value is not as expected
        // see https://issues.apache.org/jira/browse/CALCITE-2107
        .returnsOrdered("T=1997-05-01 05:30:00");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2122">[CALCITE-2122]
   * DateRangeRules issues</a>. */
  @Test public void testCombinationOfValidAndNotValidAndInterval() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\" "
        + "WHERE  \"timestamp\" < CAST('1998-01-02' as TIMESTAMP) AND "
        + "EXTRACT(MONTH FROM \"timestamp\") = 01 AND EXTRACT(YEAR FROM \"timestamp\") = 1996 ";
    sql(sql, FOODMART)
        .runs()
        .queryContains(druidChecker("{\"queryType\":\"timeseries\""));
  }
}

// End DruidAdapterIT.java
