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
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
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
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], "
        + "filter=[=($13, 'Jeremy Corbyn')], groups=[{5}], aggs=[[]])\n";
    checkSelectDistinctWiki(WIKI, "wiki")
        .explainContains(explain);
  }

  @Test public void testSelectDistinctWikiNoColumns() {
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[wiki, wiki]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], "
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
        + "intervals=[[1900-01-01T00:00:00.000/3000-01-01T00:00:00.000]], "
        + "filter=[=($17, 'Jeremy Corbyn')], groups=[{7}], aggs=[[]])\n";
    final String druidQuery = "{'queryType':'groupBy',"
        + "'dataSource':'wikiticker','granularity':'all',"
        + "'dimensions':[{'type':'default','dimension':'countryName'}],'limitSpec':{'type':'default'},"
        + "'filter':{'type':'selector','dimension':'page','value':'Jeremy Corbyn'},"
        + "'aggregations':[{'type':'longSum','name':'dummy_agg','fieldName':'dummy_agg'}],"
        + "'intervals':['1900-01-01T00:00:00.000/3000-01-01T00:00:00.000']}";
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
        + "    DruidQuery(table=[[wiki, wikiticker]], intervals=[[1900-01-01T00:00:00.000/3000-01-01T00:00:00.000]], projects=[[FLOOR($0, FLAG(DAY)), $1]], groups=[{0}], aggs=[[SUM($1)]])\n";
    final String druidQuery = "{'queryType':'timeseries',"
        + "'dataSource':'wikiticker','descending':false,'granularity':'day',"
        + "'aggregations':[{'type':'longSum','name':'EXPR$0','fieldName':'added'}],"
        + "'intervals':['1900-01-01T00:00:00.000/3000-01-01T00:00:00.000'],"
        + "'context':{'skipEmptyBuckets':true}}";
    sql(sql, WIKI_AUTO2)
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testSelectTimestampColumnNoTables2() {
    // Since columns are not explicitly declared, we use the default time
    // column in the query.
    final String sql = "select \"__time\"\n"
        + "from \"wikiticker\"\n"
        + "limit 1\n";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[wiki, wikiticker]], intervals=[[1900-01-01T00:00:00.000/3000-01-01T00:00:00.000]], projects=[[$0]], fetch=[1])\n";
    final String druidQuery = "{'queryType':'select',"
        + "'dataSource':'wikiticker','descending':false,"
        + "'intervals':['1900-01-01T00:00:00.000/3000-01-01T00:00:00.000'],"
        + "'dimensions':[],'metrics':[],'granularity':'all','pagingSpec':{'threshold':1,'fromNext':true},"
        + "'context':{'druid.query.fetch':true}}";
    sql(sql, WIKI_AUTO2)
        .returnsUnordered("__time=2015-09-12 00:46:58")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testSelectTimestampColumnNoTables3() {
    // Since columns are not explicitly declared, we use the default time
    // column in the query.
    final String sql = "select floor(\"__time\" to DAY) as \"day\", sum(\"added\")\n"
        + "from \"wikiticker\"\n"
        + "group by floor(\"__time\" to DAY)";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[wiki, wikiticker]], intervals=[[1900-01-01T00:00:00.000/3000-01-01T00:00:00.000]], projects=[[FLOOR($0, FLAG(DAY)), $1]], groups=[{0}], aggs=[[SUM($1)]])\n";
    final String druidQuery = "{'queryType':'timeseries',"
        + "'dataSource':'wikiticker','descending':false,'granularity':'day',"
        + "'aggregations':[{'type':'longSum','name':'EXPR$1','fieldName':'added'}],"
        + "'intervals':['1900-01-01T00:00:00.000/3000-01-01T00:00:00.000'],"
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
        + "floor(\"__time\" to DAY) as \"day\"\n"
        + "from \"wikiticker\"\n"
        + "group by \"page\", floor(\"__time\" to DAY)\n"
        + "order by \"s\" desc";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableProject(s=[$2], page=[$0], day=[$1])\n"
        + "    DruidQuery(table=[[wiki, wikiticker]], "
        + "intervals=[[1900-01-01T00:00:00.000/3000-01-01T00:00:00.000]], projects=[[$17, FLOOR"
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
    final String sql = "select floor(\"__time\" to SECOND) as \"second\", sum(\"added\")\n"
        + "from \"wikiticker\"\n"
        + "where \"page\" = 'Jeremy Corbyn'\n"
        + "group by floor(\"__time\" to SECOND)";
    final String druidQuery = "{'queryType':'timeseries',"
        + "'dataSource':'wikiticker','descending':false,'granularity':'second',"
        + "'filter':{'type':'selector','dimension':'page','value':'Jeremy Corbyn'},"
        + "'aggregations':[{'type':'longSum','name':'EXPR$1','fieldName':'added'}],"
        + "'intervals':['1900-01-01T00:00:00.000/3000-01-01T00:00:00.000'],"
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
        + "'aggregations':[{'type':'longSum','name':'dummy_agg','fieldName':'dummy_agg'}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
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
    final String sql = "select \"__time\"\n"
        + "from \"wikiticker\"\n"
        + "where \"__time\" < '2015-10-12 00:00:00'";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[wiki, wikiticker]], "
        + "intervals=[[1900-01-01T00:00:00.000/2015-10-12T00:00:00.000]], "
        + "projects=[[$0]])\n";
    final String druidQuery = "{'queryType':'select',"
        + "'dataSource':'wikiticker','descending':false,"
        + "'intervals':['1900-01-01T00:00:00.000/2015-10-12T00:00:00.000'],"
        + "'dimensions':[],'metrics':[],'granularity':'all',"
        + "'pagingSpec':{'threshold':16384,'fromNext':true},"
        + "'context':{'druid.query.fetch':false}}";
    sql(sql, WIKI_AUTO2)
        .limit(2)
        .returnsUnordered("__time=2015-09-12 00:46:58",
            "__time=2015-09-12 00:47:00")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testFilterTimeDistinct() {
    final String sql = "select distinct \"__time\"\n"
        + "from \"wikiticker\"\n"
        + "where \"__time\" < '2015-10-12 00:00:00'";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[wiki, wikiticker]], "
        + "intervals=[[1900-01-01T00:00:00.000/2015-10-12T00:00:00.000]], "
        + "groups=[{0}], aggs=[[]])\n";
    final String subDruidQuery = "{'queryType':'groupBy','dataSource':'wikiticker',"
        + "'granularity':'all','dimensions':[{'type':'extraction',"
        + "'dimension':'__time','outputName':'extract',"
        + "'extractionFn':{'type':'timeFormat'";
    sql(sql, WIKI_AUTO2)
        .limit(2)
        .returnsUnordered("__time=2015-09-12 00:46:58",
            "__time=2015-09-12 00:47:00")
        .explainContains(explain)
        .queryContains(druidChecker(subDruidQuery));
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
                  // 1 timestamp, 2 float measure, 1 int measure, 88 dimensions
                  assertThat(map.keySet().size(), is(4));
                  assertThat(map.values().size(), is(92));
                  assertThat(map.get("TIMESTAMP(0)").size(), is(1));
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
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], groups=[{30}], aggs=[[]])";
    final String sql = "select distinct \"state_province\" from \"foodmart\"";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
        + "'dimensions':[{'type':'default','dimension':'state_province'}],'limitSpec':{'type':'default'},"
        + "'aggregations':[{'type':'longSum','name':'dummy_agg','fieldName':'dummy_agg'}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
    sql(sql)
        .returnsUnordered("state_province=CA",
            "state_province=OR",
            "state_province=WA")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  @Ignore("TODO: fix invalid cast from Integer to Long")
  @Test public void testSelectGroupBySum() {
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], projects=[[$29, CAST($88):INTEGER]], groups=[{0}], aggs=[[SUM($1)]])";
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
    final String plan = "PLAN=EnumerableInterpreter\n  BindableAggregate(group=[{0, 1}])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], filter=[=($1, 1020)],"
            + " projects=[[$90, $1]])\n";
    final String druidQuery = "{'queryType':'select','dataSource':'foodmart','descending':false,"
            + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
            + "'filter':{'type':'selector','dimension':'product_id','value':'1020'},"
            + "'dimensions':['product_id'],'metrics':['store_sales'],'granularity':'all',"
            + "'pagingSpec':{'threshold':16384,'fromNext':true},"
            + "'context':{'druid.query.fetch':false}}";
    sql(sql)
        .explainContains(plan)
        .queryContains(druidChecker(druidQuery))
        .returnsUnordered("store_sales=0.5099999904632568; product_id=1020",
            "store_sales=1.0199999809265137; product_id=1020",
            "store_sales=1.5299999713897705; product_id=1020",
            "store_sales=2.0399999618530273; product_id=1020",
            "store_sales=2.549999952316284; product_id=1020");
  }

  @Test public void testPushSimpleGroupBy() {
    final String sql = "select \"product_id\" from \"foodmart\" where "
            + "\"product_id\" = 1020 group by \"product_id\"";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
            + "'granularity':'all','dimensions':[{'type':'default',"
            + "'dimension':'product_id'}],"
            + "'limitSpec':{'type':'default'},'filter':{'type':'selector',"
            + "'dimension':'product_id','value':'1020'},"
            + "'aggregations':[{'type':'longSum','name':'dummy_agg',"
            + "'fieldName':'dummy_agg'}],"
            + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
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
            + "'aggregations':[{'type':'longSum','name':'dummy_agg','fieldName':'dummy_agg'}],"
            + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
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
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[$39, $30]], "
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
                + "'aggregations':[{'type':'longSum','name':'dummy_agg',"
                + "'fieldName':'dummy_agg'}],"
                + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}"))
        .explainContains(explain);
  }

  @Test public void testSortLimit() {
    final String explain = "PLAN=EnumerableLimit(offset=[2], fetch=[3])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[$39, $30]], "
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
    final String druidQuery = "{'queryType':'select','dataSource':'foodmart',"
        + "'descending':false,'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'dimensions':['state_province','product_name'],'metrics':[],'granularity':'all',"
        + "'pagingSpec':{'threshold':16384,'fromNext':true},'context':{'druid.query.fetch':false}}";
    sql(sql)
        .runs()
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testLimit() {
    final String sql = "select \"gender\", \"state_province\"\n"
        + "from \"foodmart\" fetch next 3 rows only";
    final String druidQuery = "{'queryType':'select','dataSource':'foodmart',"
        + "'descending':false,'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'dimensions':['gender','state_province'],'metrics':[],'granularity':'all',"
        + "'pagingSpec':{'threshold':3,'fromNext':true},'context':{'druid.query.fetch':true}}";
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
        + "'aggregations':[{'type':'longSum','name':'dummy_agg','fieldName':'dummy_agg'}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[$39, $30]], "
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
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], "
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
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'threshold':3}";
    final String exactDruid = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default',"
        + "'dimension':'brand_name'}],'limitSpec':{'type':'default','limit':3,"
        + "'columns':[{'dimension':'S','direction':'descending',"
        + "'dimensionOrder':'numeric'}]},'aggregations':[{'type':'longSum',"
        + "'name':'S','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
    final String druidQuery = approx ? approxDruid : exactDruid;
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], "
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
    final String sql = "select \"brand_name\", floor(\"timestamp\" to DAY) as d,"
        + " sum(\"unit_sales\") as s\n"
        + "from \"foodmart\"\n"
        + "group by \"brand_name\", floor(\"timestamp\" to DAY)\n"
        + "order by s desc limit 30";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'day','dimensions':[{'type':'default','dimension':'brand_name'}],"
        + "'limitSpec':{'type':'default'},"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[$2, FLOOR"
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
    final String sql = "select \"brand_name\", floor(\"timestamp\" to DAY) as d,"
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
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[$2, FLOOR"
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
    final String sql = "select \"brand_name\", floor(\"timestamp\" to DAY) as d,"
        + " sum(\"unit_sales\") as s\n"
        + "from \"foodmart\"\n"
        + "group by \"brand_name\", floor(\"timestamp\" to DAY)\n"
        + "order by \"brand_name\"";
    final String subDruidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':[{'type':'default',"
        + "'dimension':'brand_name'},{'type':'extraction','dimension':'__time',"
        + "'outputName':'floor_day','extractionFn':{'type':'timeFormat'";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[$2, FLOOR"
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
    final String sql = "select * from \"foodmart\"\n"
        + "where \"product_id\" BETWEEN '1500' AND '1502'\n"
        + "order by \"state_province\" desc, \"product_id\"";
    final String druidQuery = "{'queryType':'select','dataSource':'foodmart',"
        + "'descending':false,'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'filter':{'type':'and','fields':["
        + "{'type':'bound','dimension':'product_id','lower':'1500','lowerStrict':false,'ordering':'lexicographic'},"
        + "{'type':'bound','dimension':'product_id','upper':'1502','upperStrict':false,'ordering':'lexicographic'}]},"
        + "'dimensions':['product_id','brand_name','product_name','SKU','SRP','gross_weight','net_weight',"
        + "'recyclable_package','low_fat','units_per_case','cases_per_pallet','shelf_width','shelf_height',"
        + "'shelf_depth','product_class_id','product_subcategory','product_category','product_department',"
        + "'product_family','customer_id','account_num','lname','fname','mi','address1','address2','address3',"
        + "'address4','city','state_province','postal_code','country','customer_region_id','phone1','phone2',"
        + "'birthdate','marital_status','yearly_income','gender','total_children','num_children_at_home',"
        + "'education','date_accnt_opened','member_card','occupation','houseowner','num_cars_owned',"
        + "'fullname','promotion_id','promotion_district_id','promotion_name','media_type','cost','start_date',"
        + "'end_date','store_id','store_type','region_id','store_name','store_number','store_street_address',"
        + "'store_city','store_state','store_postal_code','store_country','store_manager','store_phone',"
        + "'store_fax','first_opened_date','last_remodel_date','store_sqft','grocery_sqft','frozen_sqft',"
        + "'meat_sqft','coffee_bar','video_store','salad_bar','prepared_food','florist','time_id','the_day',"
        + "'the_month','the_year','day_of_month','week_of_year','month_of_year','quarter','fiscal_period'],"
        + "'metrics':['unit_sales','store_sales','store_cost'],'granularity':'all',"
        + "'pagingSpec':{'threshold':16384,'fromNext':true},'context':{'druid.query.fetch':false}}";
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
    final String sql = "select * from \"foodmart\"\n"
        + "where \"product_id\" BETWEEN 1500 AND 1502\n"
        + "order by \"state_province\" desc, \"product_id\"";
    final String druidQuery = "{'queryType':'select','dataSource':'foodmart',"
        + "'descending':false,'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'filter':{'type':'and','fields':["
        + "{'type':'bound','dimension':'product_id','lower':'1500','lowerStrict':false,'ordering':'numeric'},"
        + "{'type':'bound','dimension':'product_id','upper':'1502','upperStrict':false,'ordering':'numeric'}]},"
        + "'dimensions':['product_id','brand_name','product_name','SKU','SRP','gross_weight','net_weight',"
        + "'recyclable_package','low_fat','units_per_case','cases_per_pallet','shelf_width','shelf_height',"
        + "'shelf_depth','product_class_id','product_subcategory','product_category','product_department',"
        + "'product_family','customer_id','account_num','lname','fname','mi','address1','address2','address3',"
        + "'address4','city','state_province','postal_code','country','customer_region_id','phone1','phone2',"
        + "'birthdate','marital_status','yearly_income','gender','total_children','num_children_at_home',"
        + "'education','date_accnt_opened','member_card','occupation','houseowner','num_cars_owned',"
        + "'fullname','promotion_id','promotion_district_id','promotion_name','media_type','cost','start_date',"
        + "'end_date','store_id','store_type','region_id','store_name','store_number','store_street_address',"
        + "'store_city','store_state','store_postal_code','store_country','store_manager','store_phone',"
        + "'store_fax','first_opened_date','last_remodel_date','store_sqft','grocery_sqft','frozen_sqft',"
        + "'meat_sqft','coffee_bar','video_store','salad_bar','prepared_food','florist','time_id','the_day',"
        + "'the_month','the_year','day_of_month','week_of_year','month_of_year','quarter','fiscal_period'],"
        + "'metrics':['unit_sales','store_sales','store_cost'],'granularity':'all',"
        + "'pagingSpec':{'threshold':16384,'fromNext':true},'context':{'druid.query.fetch':false}}";
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
    final String sql = "select * from \"foodmart\"\n"
        + "where \"product_id\" = -1";
    final String druidQuery = "{'queryType':'select','dataSource':'foodmart',"
        + "'descending':false,'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'filter':{'type':'selector','dimension':'product_id','value':'-1'},"
        + "'dimensions':['product_id','brand_name','product_name','SKU','SRP',"
        + "'gross_weight','net_weight','recyclable_package','low_fat','units_per_case',"
        + "'cases_per_pallet','shelf_width','shelf_height','shelf_depth',"
        + "'product_class_id','product_subcategory','product_category',"
        + "'product_department','product_family','customer_id','account_num',"
        + "'lname','fname','mi','address1','address2','address3','address4',"
        + "'city','state_province','postal_code','country','customer_region_id',"
        + "'phone1','phone2','birthdate','marital_status','yearly_income','gender',"
        + "'total_children','num_children_at_home','education','date_accnt_opened',"
        + "'member_card','occupation','houseowner','num_cars_owned','fullname',"
        + "'promotion_id','promotion_district_id','promotion_name','media_type','cost',"
        + "'start_date','end_date','store_id','store_type','region_id','store_name',"
        + "'store_number','store_street_address','store_city','store_state',"
        + "'store_postal_code','store_country','store_manager','store_phone',"
        + "'store_fax','first_opened_date','last_remodel_date','store_sqft','grocery_sqft',"
        + "'frozen_sqft','meat_sqft','coffee_bar','video_store','salad_bar','prepared_food',"
        + "'florist','time_id','the_day','the_month','the_year','day_of_month',"
        + "'week_of_year','month_of_year','quarter','fiscal_period'],"
        + "'metrics':['unit_sales','store_sales','store_cost'],'granularity':'all',"
        + "'pagingSpec':{'threshold':16384,'fromNext':true},'context':{'druid.query.fetch':false}}";
    sql(sql)
        .limit(4)
        .returnsUnordered()
        .queryContains(druidChecker(druidQuery));
  }

  /** As {@link #testFilterSortDescNumeric()} but with a filter that cannot
   * be pushed down to Druid. */
  @Test public void testNonPushableFilterSortDesc() {
    final String sql = "select * from \"foodmart\"\n"
        + "where cast(\"product_id\" as integer) - 1500 BETWEEN 0 AND 2\n"
        + "order by \"state_province\" desc, \"product_id\"";
    final String druidQuery = "{'queryType':'select','dataSource':'foodmart',"
        + "'descending':false,'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'dimensions':['product_id','brand_name','product_name','SKU','SRP','gross_weight',"
        + "'net_weight','recyclable_package','low_fat','units_per_case','cases_per_pallet',"
        + "'shelf_width','shelf_height','shelf_depth','product_class_id','product_subcategory',"
        + "'product_category','product_department','product_family','customer_id','account_num',"
        + "'lname','fname','mi','address1','address2','address3','address4','city','state_province',"
        + "'postal_code','country','customer_region_id','phone1','phone2','birthdate','marital_status',"
        + "'yearly_income','gender','total_children','num_children_at_home','education',"
        + "'date_accnt_opened','member_card','occupation','houseowner','num_cars_owned','fullname',"
        + "'promotion_id','promotion_district_id','promotion_name','media_type','cost','start_date',"
        + "'end_date','store_id','store_type','region_id','store_name','store_number','store_street_address',"
        + "'store_city','store_state','store_postal_code','store_country','store_manager','store_phone',"
        + "'store_fax','first_opened_date','last_remodel_date','store_sqft','grocery_sqft','frozen_sqft',"
        + "'meat_sqft','coffee_bar','video_store','salad_bar','prepared_food','florist','time_id','the_day',"
        + "'the_month','the_year','day_of_month','week_of_year','month_of_year','quarter','fiscal_period'],"
        + "'metrics':['unit_sales','store_sales','store_cost'],'granularity':'all',"
        + "'pagingSpec':{'threshold':16384,'fromNext':true},'context':{'druid.query.fetch':false}}";
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
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], groups=[{39}], aggs=[[]])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], groups=[{37}], aggs=[[]])";
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
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], groups=[{39}], aggs=[[]])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], groups=[{37}], aggs=[[]])";
    sql(sql)
        .explainContains(explain)
        .returnsUnordered("gender=M",
            "gender=M");
  }

  @Test public void testCountGroupByEmpty() {
    final String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart',"
        + "'descending':false,'granularity':'all',"
        + "'aggregations':[{'type':'count','name':'EXPR$0'}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'context':{'skipEmptyBuckets':true}}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[]], groups=[{}], aggs=[[COUNT()]])";
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
    final String sql = "select count(*) as \"c\", floor(\"timestamp\" to MONTH) as \"month\"\n"
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
            "select count(*) as \"c\", floor(\"timestamp\" to MONTH) as \"month\", floor"
                    + "(\"store_sales\") as sales\n"
                    + "from \"foodmart\"\n"
                    + "group by floor(\"timestamp\" to MONTH), \"state_province\", floor"
                    + "(\"store_sales\")\n"
                    + "order by \"c\" desc limit 3";
    sql(sql).returnsOrdered("c=494; month=1997-11-01 00:00:00; SALES=5.0",
            "c=475; month=1997-12-01 00:00:00; SALES=5.0",
            "c=468; month=1997-03-01 00:00:00; SALES=5.0"
    ).queryContains(druidChecker("'queryType':'select'"));
  }

  @Test public void testGroupByTimeAndOneColumnNotProjected() {
    final String sql = "select count(*) as \"c\",\n"
        + "  floor(\"timestamp\" to MONTH) as \"month\"\n"
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
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], groups=[{30}], "
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
    String druidQuery = "'aggregations':["
        + "{'type':'longSum','name':'$f1','fieldName':'unit_sales'},"
        + "{'type':'count','name':'$f2','fieldName':'unit_sales'},"
        + "{'type':'count','name':'C','fieldName':'store_sqft'},"
        + "{'type':'count','name':'C0'}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
    sql(sql)
        .limit(2)
        .returnsUnordered("state_province=CA; A=3; S=74748; C=24441; C0=24441",
            "state_province=OR; A=3; S=67659; C=21610; C0=21610")
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testGroupByMonthGranularity() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH)";
    String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart',"
        + "'descending':false,'granularity':'month',"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'},"
        + "{'type':'count','name':'C','fieldName':'store_sqft'}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'context':{'skipEmptyBuckets':true}}";
    sql(sql)
        .limit(3)
        .returnsUnordered("S=20957; C=6844", "S=21628; C=7033", "S=23706; C=7710")
        .queryContains(druidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1577">[CALCITE-1577]
   * Druid adapter: Incorrect result - limit on timestamp disappears</a>. */
  @Test public void testGroupByMonthGranularitySort() {
    final String sql = "select floor(\"timestamp\" to MONTH) as m,\n"
        + " sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH)\n"
        + "order by floor(\"timestamp\" to MONTH) ASC";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[FLOOR($0, "
        + "FLAG(MONTH)), $89, $71]], groups=[{0}], aggs=[[SUM($1), COUNT($2)]], sort0=[0], "
        + "dir0=[ASC])";
    sql(sql)
        .returnsOrdered("M=1997-01-01 00:00:00; S=21628; C=7033",
            "M=1997-02-01 00:00:00; S=20957; C=6844",
            "M=1997-03-01 00:00:00; S=23706; C=7710",
            "M=1997-04-01 00:00:00; S=20179; C=6588",
            "M=1997-05-01 00:00:00; S=21081; C=6865",
            "M=1997-06-01 00:00:00; S=21350; C=6912",
            "M=1997-07-01 00:00:00; S=23763; C=7752",
            "M=1997-08-01 00:00:00; S=21697; C=7038",
            "M=1997-09-01 00:00:00; S=20388; C=6662",
            "M=1997-10-01 00:00:00; S=19958; C=6478",
            "M=1997-11-01 00:00:00; S=25270; C=8231",
            "M=1997-12-01 00:00:00; S=26796; C=8716")
        .explainContains(explain);
  }

  @Test public void testGroupByMonthGranularitySortLimit() {
    final String sql = "select floor(\"timestamp\" to MONTH) as m,\n"
        + " sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH)\n"
        + "order by floor(\"timestamp\" to MONTH) limit 3";
    final String explain = "PLAN=EnumerableLimit(fetch=[3])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[FLOOR($0, "
        + "FLAG(MONTH)), $89, $71]], groups=[{0}], aggs=[[SUM($1), COUNT($2)]], sort0=[0], "
        + "dir0=[ASC])";
    sql(sql)
        .returnsOrdered("M=1997-01-01 00:00:00; S=21628; C=7033",
            "M=1997-02-01 00:00:00; S=20957; C=6844",
            "M=1997-03-01 00:00:00; S=23706; C=7710")
        .explainContains(explain);
  }

  @Test public void testGroupByDayGranularity() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to DAY)";
    String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart',"
        + "'descending':false,'granularity':'day',"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'},"
        + "{'type':'count','name':'C','fieldName':'store_sqft'}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'context':{'skipEmptyBuckets':true}}";
    sql(sql)
        .limit(3)
        .returnsUnordered("S=348; C=117", "S=589; C=189", "S=635; C=206")
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testGroupByMonthGranularityFiltered() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "where \"timestamp\" >= '1996-01-01 00:00:00' and "
        + " \"timestamp\" < '1998-01-01 00:00:00'\n"
        + "group by floor(\"timestamp\" to MONTH)";
    String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart',"
        + "'descending':false,'granularity':'month',"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'},"
        + "{'type':'count','name':'C','fieldName':'store_sqft'}],"
        + "'intervals':['1996-01-01T00:00:00.000/1998-01-01T00:00:00.000'],"
        + "'context':{'skipEmptyBuckets':true}}";
    sql(sql)
        .limit(3)
        .returnsUnordered("S=20957; C=6844", "S=21628; C=7033", "S=23706; C=7710")
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
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableProject(S=[$2], M=[$3], P=[$0])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[$30, FLOOR"
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
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
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
        + "where \"timestamp\" >= '1997-01-01 00:00:00' and "
        + " \"timestamp\" < '1997-09-01 00:00:00'\n"
        + "group by \"state_province\", floor(\"timestamp\" to DAY)\n"
        + "order by s desc limit 6";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableProject(S=[$2], M=[$3], P=[$0])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1997-01-01T00:00:00.000/1997-09-01T00:00:00.000]], projects=[[$30, FLOOR"
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
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], groups=[{30}], aggs=[[COUNT()]])";
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
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], groups=[{29, 30}], aggs=[[COUNT()]], sort0=[2], dir0=[DESC], fetch=[2])";
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
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$1], dir0=[DESC], fetch=[2])\n"
        + "    BindableProject(state_province=[$0], CDC=[FLOOR($1)])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], groups=[{30}], aggs=[[COUNT(DISTINCT $29)]])\n";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all',"
        + "'dimensions':[{'type':'default','dimension':'state_province'}],"
        + "'limitSpec':{'type':'default'},"
        + "'aggregations':[{'type':'cardinality','name':'$f1','fieldNames':['city']}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
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
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[$3]])";
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
        + "'aggregations':[{'type':'longSum','name':'dummy_agg','fieldName':'dummy_agg'}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]],"
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
    final String druidQuery = "{'queryType':'select',"
        + "'dataSource':'foodmart',"
        + "'descending':false,"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'filter':{'type':'and','fields':["
        + "{'type':'selector','dimension':'product_name','value':'High Top Dried Mushrooms'},"
        + "{'type':'or','fields':["
        + "{'type':'selector','dimension':'quarter','value':'Q2'},"
        + "{'type':'selector','dimension':'quarter','value':'Q3'}]},"
        + "{'type':'selector','dimension':'state_province','value':'WA'}]},"
        + "'dimensions':['state_province','city','product_name'],"
        + "'metrics':[],"
        + "'granularity':'all',"
        + "'pagingSpec':{'threshold':16384,'fromNext':true},'context':{'druid.query.fetch':false}}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], "
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
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], filter=[AND(="
        + "(EXTRACT_DATE(FLAG(YEAR), /INT(Reinterpret($0), 86400000)), 1997), OR(=(EXTRACT_DATE"
        + "(FLAG(MONTH), /INT(Reinterpret($0), 86400000)), 4), =(EXTRACT_DATE(FLAG(MONTH), /INT"
        + "(Reinterpret($0), 86400000)), 6)))], groups=[{}], aggs=[[COUNT()]])";
    sql(sql)
        .explainContains(explain)
        .returnsUnordered("C=13500");
  }

  @Test public void testFilterSwapped() {
    String sql = "select \"state_province\"\n"
        + "from \"foodmart\"\n"
        + "where 'High Top Dried Mushrooms' = \"product_name\"";
    final String explain = "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], filter=[=('High Top Dried Mushrooms', $3)], projects=[[$30]])";
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
    sql(sql, WIKI)
        .returnsCount(9);
  }


  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1656">[CALCITE-1656]
   * Improve cost function in DruidQuery to encourage early column
   * pruning</a>. */
  @Test public void testFieldBasedCostColumnPruning() {
    // A query where filter cannot be pushed to Druid but
    // the project can still be pushed in order to prune extra columns.
    String sql = "select \"countryName\", floor(\"time\" to DAY),\n"
        + "  cast(count(*) as integer) as c\n"
        + "from \"wiki\"\n"
        + "where floor(\"time\" to DAY) >= '1997-01-01 00:00:00'\n"
        + "and floor(\"time\" to DAY) < '1997-09-01 00:00:00'\n"
        + "group by \"countryName\", floor(\"time\" TO DAY)\n"
        + "order by c limit 5";
    String plan = "BindableProject(countryName=[$0], EXPR$1=[$1], C=[CAST($2):INTEGER NOT NULL])\n"
        + "    BindableSort(sort0=[$2], dir0=[ASC], fetch=[5])\n"
        + "      BindableAggregate(group=[{0, 1}], agg#0=[COUNT()])\n"
        + "        BindableProject(countryName=[$1], EXPR$1=[FLOOR($0, FLAG(DAY))])\n"
        + "          BindableFilter(condition=[AND(>=(FLOOR($0, FLAG(DAY)), 1997-01-01 00:00:00), <(FLOOR($0, FLAG(DAY)), 1997-09-01 00:00:00))])\n"
        + "            DruidQuery(table=[[wiki, wiki]], intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[$0, $5]])";
    // NOTE: Druid query only has countryName as the dimension
    // being queried after project is pushed to druid query.
    String druidQuery = "{'queryType':'select',"
        + "'dataSource':'wikiticker',"
        + "'descending':false,"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'dimensions':['countryName'],"
        + "'metrics':[],"
        + "'granularity':'all',"
        + "'pagingSpec':{'threshold':16384,'fromNext':true},"
        + "'context':{'druid.query.fetch':false}}";
    sql(sql, WIKI).explainContains(plan);
    sql(sql, WIKI).queryContains(druidChecker(druidQuery));
  }

  @Test public void testGroupByMetricAndExtractTime() {
    final String sql = "SELECT count(*), floor(\"timestamp\" to DAY), \"store_sales\" "
            + "FROM \"foodmart\"\n"
            + "GROUP BY \"store_sales\", floor(\"timestamp\" to DAY)\n ORDER BY \"store_sales\" DESC\n"
            + "LIMIT 10\n";
    sql(sql).queryContains(druidChecker("{\"queryType\":\"select\""));
  }

  @Test public void testFilterOnDouble() {
    String sql = "select \"product_id\" from \"foodmart\"\n"
        + "where cast(\"product_id\" as double) < 0.41024 and \"product_id\" < 12223";
    sql(sql).queryContains(
        druidChecker("'type':'bound','dimension':'product_id','upper':'0.41024'",
            "'upper':'12223'"));
  }

  @Test public void testPushAggregateOnTime() {
    String sql = "select \"product_id\", \"timestamp\" as \"time\" from \"foodmart\" "
        + "where \"product_id\" = 1016 "
        + "and \"timestamp\" < cast('1997-01-03' as timestamp) "
        + "and \"timestamp\" > cast('1990-01-01' as timestamp) "
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
            + "intervals=[[1997-01-01T00:00:00.001/1997-01-20T00:00:00.000]], filter=[=($1, 1016)"
            + "], projects=[[EXTRACT_DATE(FLAG(DAY), /INT(Reinterpret($0), 86400000)), "
            + "EXTRACT_DATE(FLAG(MONTH), /INT(Reinterpret($0), 86400000)), EXTRACT_DATE(FLAG"
            + "(YEAR), /INT(Reinterpret($0), 86400000)), $1]], groups=[{0, 1, 2, 3}], aggs=[[]])\n")
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
            + "intervals=[[1997-01-01T00:00:00.001/1997-01-20T00:00:00.000]], filter=[=($1, 1016)"
            + "], projects=[[EXTRACT_DATE(FLAG(DAY), /INT(Reinterpret($0), 86400000)), "
            + "EXTRACT_DATE(FLAG(MONTH), /INT(Reinterpret($0), 86400000)), EXTRACT_DATE(FLAG"
            + "(YEAR), /INT(Reinterpret($0), 86400000)), $1]], groups=[{0, 1, 2, 3}], aggs=[[]])\n")
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
            + "intervals=[[1997-01-01T00:00:00.001/1997-01-20T00:00:00.000]], filter=[=($1, 1016)], "
            + "projects=[[EXTRACT_DATE(FLAG(DAY), /INT(Reinterpret($0), 86400000)), $1]], "
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
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000'],"
        + "'context':{'skipEmptyBuckets':true}}";
    sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], filter=[AND(>=(CAST"
            + "($11):BIGINT, 8), <=(CAST($11):BIGINT, 10), <(CAST($10):BIGINT, 15), =(EXTRACT_DATE"
            + "(FLAG(YEAR), /INT(Reinterpret($0), 86400000)), 1997))], groups=[{}], "
            + "aggs=[[SUM($90)]])")
        .queryContains(druidChecker(druidQuery))
        .returnsUnordered("EXPR$0=75364.09998679161");
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
                + "'timeZone':'UTC','locale':'en-US'}}]},'aggregations':[{'type':'longSum',"
                + "'name':'dummy_agg','fieldName':'dummy_agg'}],"
                + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}"))
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
                + "'aggregations':[{'type':'longSum','name':'dummy_agg',"
                + "'fieldName':'dummy_agg'}],"
                + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}"))
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
        + "'timeZone':'UTC','locale':'en-US'}}]},'aggregations':[{'type':'longSum',"
        + "'name':'dummy_agg','fieldName':'dummy_agg'}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
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
                + "'aggregations':[{'type':'longSum','name':'dummy_agg',"
                + "'fieldName':'dummy_agg'}],"
                + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}"))
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
            + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}"))
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], filter=[>=(CAST($1)"
            + ":BIGINT, 1558)], projects=[[EXTRACT_DATE(FLAG(MONTH), /INT(Reinterpret($0), "
            + "86400000)), $1, $89]], groups=[{0, 1}], aggs=[[SUM($2)]], sort0=[0], sort1=[2], "
            + "sort2=[1], dir0=[ASC], dir1=[ASC], dir2=[ASC])");
  }


  @Test public void testGroupByFloorTimeWithoutLimit() {
    final String sql = "select  floor(\"timestamp\" to MONTH) as \"month\"\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH)\n"
        + "order by \"month\" DESC";
    sql(sql)
        .explainContains("PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[FLOOR($0, "
        + "FLAG(MONTH))]], groups=[{0}], aggs=[[]], sort0=[0], dir0=[DESC])")
        .queryContains(druidChecker("'queryType':'timeseries'", "'descending':true"));
  }

  @Test public void testGroupByFloorTimeWithLimit() {
    final String sql = "select  floor(\"timestamp\" to MONTH) as \"floor_month\"\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH)\n"
        + "order by \"floor_month\" DESC LIMIT 3";
    sql(sql).explainContains("PLAN=EnumerableLimit(fetch=[3])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[FLOOR($0, "
        + "FLAG(MONTH))]], groups=[{0}], aggs=[[]], sort0=[0], dir0=[DESC])")
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
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], filter=[>=(CAST($1)"
        + ":BIGINT, 1558)], projects=[[EXTRACT_DATE(FLAG(YEAR), /INT(Reinterpret($0), 86400000)),"
        + " EXTRACT_DATE(FLAG(MONTH), /INT(Reinterpret($0), 86400000)), $1, $89]], groups=[{0, 1,"
        + " 2}], aggs=[[SUM($3)]], sort0=[0], sort1=[1], sort2=[3], sort3=[2], dir0=[DESC], "
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
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
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
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], filter=[>=(CAST($1)"
        + ":BIGINT, 1558)], projects=[[EXTRACT_DATE(FLAG(YEAR), /INT(Reinterpret($0), 86400000)),"
        + " EXTRACT_DATE(FLAG(MONTH), /INT(Reinterpret($0), 86400000)), $1, $89]], groups=[{0, 1,"
        + " 2}], aggs=[[SUM($3)]], sort0=[3], sort1=[1], sort2=[2], dir0=[DESC], dir1=[DESC], "
        + "dir2=[ASC], fetch=[3])";
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
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
    sql(sqlQuery).explainContains(expectedPlan).queryContains(druidChecker(expectedDruidQuery))
        .returnsOrdered("Y=1997; M=12; product_id=1558; S=30", "Y=1997; M=3; product_id=1558; S=29",
            "Y=1997; M=5; product_id=1558; S=27");
  }

  @Test public void testGroupByTimeSortOverMetrics() {
    final String sqlQuery = "SELECT count(*) as c , SUM(\"unit_sales\") as s, floor(\"timestamp\""
        + " to month) FROM \"foodmart\" group by floor(\"timestamp\" to month) order by s DESC";
    sql(sqlQuery)
        .explainContains("PLAN=EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$1], dir0=[DESC])\n"
        + "    BindableProject(C=[$1], S=[$2], EXPR$2=[$0])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[FLOOR($0, "
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
    final String sqlQuery = "SELECT \"timestamp\", count(*) as c, SUM(\"unit_sales\")  "
        + "as s FROM "
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
        + "'aggregations':[{'type':'longSum','name':'dummy_agg',"
        + "'fieldName':'dummy_agg'}],"
        + "'intervals':['1900-01-09T00:00:00.000/2992-01-10T00:00:00.000']}";
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
        + "    BindableProject(EXPR$0=[/INT(EXTRACT_DATE(FLAG(YEAR), /INT(Reinterpret($0), "
        + "86400000)), 100)])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], filter=[=($1, 1558)], "
        + "projects=[[$0]])";
    sql(sql).explainContains(plan).queryContains(druidChecker("'queryType':'select'"))
        .returnsUnordered("EXPR$0=19");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1770">[CALCITE-1770]
   * Druid adapter: CAST(NULL AS ...) gives NPE</a>. */
  @Test public void testPushCast() {
    final String sql = "SELECT \"product_id\"\n"
        + "from \"foodmart\"\n"
        + "where \"product_id\" = cast(NULL as varchar)\n"
        + "group by \"product_id\"";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  BindableAggregate(group=[{0}])\n"
        + "    BindableFilter(condition=[=($0, null)])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000/2992-01-10T00:00:00.000]], projects=[[$1]])";
    sql(sql).explainContains(plan);
  }

  @Test public void testFalseFilter() {
    String sql = "Select count(*) as c from \"foodmart\" where false";
    sql(sql).returnsUnordered("C=0");
  }

  @Test public void testFalseFilterCaseConjectionWithTrue() {
    String sql = "Select count(*) as c from \"foodmart\" where "
        + "\"product_id\" = 1558 and (true or false)";
    sql(sql).returnsUnordered("C=60").queryContains(druidChecker("'queryType':'timeseries'"));
  }
}

// End DruidAdapterIT.java
