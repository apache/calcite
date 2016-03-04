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
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
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
 * git clone https://github.com/vlsi/test-dataset
 * cd test-dataset
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
  /** Connection factory based on the "druid-foodmart" model. */
  public static final ImmutableMap<String, String> FOODMART =
      ImmutableMap.of("model",
          DruidAdapterIT.class.getResource("/druid-foodmart-model.json")
              .getPath());

  /** Connection factory based on the "druid-wiki" model
   * and the "wikiticker" data set. */
  public static final ImmutableMap<String, String> WIKI =
      ImmutableMap.of("model",
          DruidAdapterIT.class.getResource("/druid-wiki-model.json")
              .getPath());

  /** Whether to run Druid tests. Enabled by default, however test is only
   * included if "it" profile is activated ({@code -Pit}). To disable,
   * specify {@code -Dcalcite.test.druid=false} on the Java command line. */
  public static final boolean ENABLED =
      Util.getBooleanProperty("calcite.test.druid", true);

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
          assertThat(querySpec.queryString, containsString(s));
        }
        return null;
      }
    };
  }

  /** Similar to {@link CalciteAssert#checkResultUnordered}, but filters strings
   * before comparing them. */
  static Function<ResultSet, Void> checkResultUnordered(
      final String... lines) {
    return new Function<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          final List<String> expectedList = Lists.newArrayList(lines);
          Collections.sort(expectedList);

          final List<String> actualList = Lists.newArrayList();
          CalciteAssert.toStringList(resultSet, actualList);
          for (int i = 0; i < actualList.size(); i++) {
            String s = actualList.get(i);
            actualList.set(i,
                s.replaceAll("\\.0;", ";").replaceAll("\\.0$", ""));
          }
          Collections.sort(actualList);

          assertThat(actualList, equalTo(expectedList));
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  /** Creates a query against the {@link #FOODMART} data set. */
  private CalciteAssert.AssertQuery sql(String sql) {
    return CalciteAssert.that()
        .enable(enabled())
        .with(FOODMART)
        .query(sql);
  }

  /** Creates a query against the {@link #WIKI} data set. */
  private CalciteAssert.AssertQuery wiki(String sql) {
    return CalciteAssert.that()
        .enable(enabled())
        .with(WIKI)
        .query(sql);
  }

  /** Tests a query against the {@link #WIKI} data set.
   *
   * <p>Most of the others in this suite are against {@link #FOODMART},
   * but our examples in "druid-adapter.md" use wikiticker. */
  @Test public void testSelectDistinctWiki() {
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[wiki, wiki]], filter=[=(CAST($12):VARCHAR(13) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'Jeremy Corbyn')], groups=[{4}], aggs=[[]])\n";
    final String sql = "select distinct \"countryName\"\n"
        + "from \"wiki\"\n"
        + "where \"page\" = 'Jeremy Corbyn'";
    final String druidQuery = "{'queryType':'groupBy',"
        + "'dataSource':'wikiticker','granularity':'all',"
        + "'dimensions':['countryName'],"
        + "'filter':{'type':'selector','dimension':'page','value':'Jeremy Corbyn'},"
        + "'aggregations':[{'type':'longSum','name':'unit_sales','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    wiki(sql)
        .returnsUnordered("countryName=United Kingdom",
            "countryName=null")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testSelectDistinct() {
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], groups=[{29}], aggs=[[]])";
    final String sql = "select distinct \"state_province\" from \"foodmart\"";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
        + "'dimensions':['state_province'],"
        + "'aggregations':[{'type':'longSum','name':'unit_sales','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
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

  @Test public void testSort() {
    // Note: We do not push down SORT yet
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], projects=[[$38, $29]], groups=[{0, 1}], aggs=[[]])";
    final String sql = "select distinct \"gender\", \"state_province\"\n"
        + "from \"foodmart\" order by 2, 1 desc";
    sql(sql)
        .returnsOrdered("gender=M; state_province=CA",
            "gender=F; state_province=CA",
            "gender=M; state_province=OR",
            "gender=F; state_province=OR",
            "gender=M; state_province=WA",
            "gender=F; state_province=WA")
        .explainContains(explain);
  }

  @Test public void testSortLimit() {
    // Note: We do not push down SORT-LIMIT into Druid "groupBy" query yet
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC], offset=[2], fetch=[3])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], projects=[[$38, $29]], groups=[{0, 1}], aggs=[[]])";
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
        + "'descending':'false','intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'dimensions':['state_province','product_name'],'metrics':[],'granularity':'all',"
        + "'pagingSpec':{'threshold':16384}}";
    sql(sql)
        .runs()
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testLimit() {
    // We do not yet push LIMIT into a Druid "select" query as a "threshold".
    final String sql = "select \"gender\", \"state_province\"\n"
        + "from \"foodmart\" fetch next 3 rows only";
    final String druidQuery = "{'queryType':'select','dataSource':'foodmart',"
        + "'descending':'false','intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'dimensions':['gender','state_province'],'metrics':[],'granularity':'all',"
        + "'pagingSpec':{'threshold':16384}";
    sql(sql)
        .runs()
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testGroupByLimit() {
    // We do not yet push LIMIT into a Druid "groupBy" query.
    final String sql = "select distinct \"gender\", \"state_province\"\n"
        + "from \"foodmart\" fetch next 3 rows only";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':['gender','state_province'],"
        + "'aggregations':[{'type':'longSum','name':'unit_sales','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String explain = "PLAN="
        + "EnumerableLimit(fetch=[3])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], projects=[[$38, $29]], groups=[{0, 1}], aggs=[[]])";
    sql(sql)
        .runs()
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  /** Tests a query that contains no GROUP BY and is therefore executed as a
   * Druid "select" query. */
  @Test public void testFilterSortDesc() {
    final String sql = "select * from \"foodmart\"\n"
        + "where \"product_id\" BETWEEN 1500 AND 1502\n"
        + "order by \"state_province\" desc, \"product_id\"";
    final String druidQuery = "{'queryType':'select','dataSource':'foodmart',"
        + "'descending':'false','intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'filter':{'type':'and','fields':["
        + "{'type':'bound','dimension':'product_id','lower':'1500','lowerStrict':false,'alphaNumeric':false},"
        + "{'type':'bound','dimension':'product_id','upper':'1502','upperStrict':false,'alphaNumeric':false}]},"
        + "'dimensions':[],'metrics':[],'granularity':'all',"
        + "'pagingSpec':{'threshold':16384}}";
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
                  throw Throwables.propagate(e);
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
        + "'descending':'false','intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'filter':{'type':'selector','dimension':'product_id','value':'-1'},"
        + "'dimensions':[],'metrics':[],'granularity':'all',"
        + "'pagingSpec':{'threshold':16384}}";
    sql(sql)
        .limit(4)
        .returnsUnordered()
        .queryContains(druidChecker(druidQuery));
  }

  /** As {@link #testFilterSortDesc} but with a filter that cannot be pushed
   * down to Druid. */
  @Test public void testNonPushableFilterSortDesc() {
    final String sql = "select * from \"foodmart\"\n"
        + "where cast(\"product_id\" as integer) - 1500 BETWEEN 0 AND 2\n"
        + "order by \"state_province\" desc, \"product_id\"";
    final String druidQuery = "{'queryType':'select','dataSource':'foodmart',"
        + "'descending':'false','intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'dimensions':[],'metrics':[],'granularity':'all',"
        + "'pagingSpec':{'threshold':16384}}";
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
                  throw Throwables.propagate(e);
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
        + "    DruidQuery(table=[[foodmart, foodmart]], groups=[{38}], aggs=[[]])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], groups=[{36}], aggs=[[]])";
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
        + "      DruidQuery(table=[[foodmart, foodmart]], groups=[{38}], aggs=[[]])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], groups=[{36}], aggs=[[]])";
    sql(sql)
        .explainContains(explain)
        .returnsUnordered("gender=M",
            "gender=M");
  }

  @Test public void testCountGroupByEmpty() {
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
        + "'dimensions':[],"
        + "'aggregations':[{'type':'count','name':'EXPR$0'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], projects=[[]], groups=[{}], aggs=[[COUNT()]])";
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
        + "  BindableSort(sort0=[$0], dir0=[ASC])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], groups=[{29}], aggs=[[COUNT()]])";
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

  @Ignore("TODO: fix invalid cast from Integer to Long")
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
        + "{'type':'count','name':'$f2'}]";
    sql(sql)
        .limit(2)
        .returnsUnordered("state_province=CA; A=3; S=74748; C=23190; C0=23190",
            "state_province=OR; A=3; S=67659; C=19027; C0=19027")
        .queryContains(druidChecker(druidQuery));
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
        + "      DruidQuery(table=[[foodmart, foodmart]], groups=[{29}], aggs=[[COUNT()]])";
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
        + "  BindableSort(sort0=[$0], dir0=[DESC], fetch=[2])\n"
        + "    BindableProject(C=[$2], state_province=[$1], city=[$0])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], groups=[{28, 29}], aggs=[[COUNT()]])";
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
        + "      DruidQuery(table=[[foodmart, foodmart]], groups=[{29}], aggs=[[COUNT(DISTINCT $28)]])";
    sql(sql)
        .explainContains(explain)
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
        + "      DruidQuery(table=[[foodmart, foodmart]], projects=[[$2]])";
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
    final String druidQuery = "{'queryType':'groupBy',"
        + "'dataSource':'foodmart',"
        + "'granularity':'all',"
        + "'dimensions':['state_province','city','product_name'],"
        + "'filter':{'type':'and','fields':["
        + "{'type':'selector','dimension':'product_name','value':'High Top Dried Mushrooms'},"
        + "{'type':'or','fields':["
        + "{'type':'selector','dimension':'quarter','value':'Q2'},"
        + "{'type':'selector','dimension':'quarter','value':'Q3'}]},"
        + "{'type':'selector','dimension':'state_province','value':'WA'}]},"
        + "'aggregations':[{'type':'longSum','name':'unit_sales','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]],"
        + " filter=[AND(=(CAST($2):VARCHAR(24) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'High Top Dried Mushrooms'),"
        + " OR(=($86, CAST('Q2'):VARCHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL),"
        + " =($86, CAST('Q3'):VARCHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL)),"
        + " =(CAST($29):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'WA'))],"
        + " projects=[[$29, $28, $2]], groups=[{0, 1, 2}], aggs=[[]])\n";
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
        + "'descending':'false',"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'filter':{'type':'and','fields':["
        + "{'type':'selector','dimension':'product_name','value':'High Top Dried Mushrooms'},"
        + "{'type':'or','fields':["
        + "{'type':'selector','dimension':'quarter','value':'Q2'},"
        + "{'type':'selector','dimension':'quarter','value':'Q3'}]},"
        + "{'type':'selector','dimension':'state_province','value':'WA'}]},"
        + "'dimensions':['state_province','city','product_name'],"
        + "'metrics':[],"
        + "'granularity':'all',"
        + "'pagingSpec':{'threshold':16384}}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]],"
        + " filter=[AND(=(CAST($2):VARCHAR(24) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'High Top Dried Mushrooms'),"
        + " OR(=($86, CAST('Q2'):VARCHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL),"
        + " =($86, CAST('Q3'):VARCHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL)),"
        + " =(CAST($29):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'WA'))],"
        + " projects=[[$29, $28, $2]])\n";
    sql(sql)
        .queryContains(druidChecker(druidQuery))
        .explainContains(explain)
        .returnsUnordered(
            "state_province=WA; city=Bremerton; product_name=High Top Dried Mushrooms",
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
}

// End DruidAdapterIT.java
