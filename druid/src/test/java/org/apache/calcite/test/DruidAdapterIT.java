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

import org.junit.Ignore;
import org.junit.Test;

import java.net.URL;
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
        + "  DruidQuery(table=[[wiki, wiki]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[=(CAST($13):VARCHAR(13) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'Jeremy Corbyn')], groups=[{5}], aggs=[[]])\n";
    checkSelectDistinctWiki(WIKI, "wiki")
        .explainContains(explain);
  }

  @Test public void testSelectDistinctWikiNoColumns() {
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[wiki, wiki]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[=(CAST($17):VARCHAR(13) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'Jeremy Corbyn')], groups=[{7}], aggs=[[]])\n";
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
            + "  DruidQuery(table=[[wiki, wikiticker]], intervals=[[1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z]], filter=[=(CAST($17):VARCHAR(13) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'Jeremy Corbyn')], groups=[{7}], aggs=[[]])\n";
    final String druidQuery = "{'queryType':'groupBy',"
        + "'dataSource':'wikiticker','granularity':'all',"
        + "'dimensions':['countryName'],'limitSpec':{'type':'default'},"
        + "'filter':{'type':'selector','dimension':'page','value':'Jeremy Corbyn'},"
        + "'aggregations':[{'type':'longSum','name':'dummy_agg','fieldName':'dummy_agg'}],"
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
        + "'dataSource':'wikiticker','descending':false,'granularity':'DAY',"
        + "'aggregations':[{'type':'longSum','name':'EXPR$0','fieldName':'added'}],"
        + "'intervals':['1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z']}";
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
        + "  DruidQuery(table=[[wiki, wikiticker]], intervals=[[1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z]], projects=[[$0]], fetch=[1])\n";
    final String druidQuery = "{'queryType':'select',"
        + "'dataSource':'wikiticker','descending':false,"
        + "'intervals':['1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z'],"
        + "'dimensions':[],'metrics':[],'granularity':'all','pagingSpec':{'threshold':1},"
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
        + "  DruidQuery(table=[[wiki, wikiticker]], intervals=[[1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z]], projects=[[FLOOR($0, FLAG(DAY)), $1]], groups=[{0}], aggs=[[SUM($1)]])\n";
    final String druidQuery = "{'queryType':'timeseries',"
        + "'dataSource':'wikiticker','descending':false,'granularity':'DAY',"
        + "'aggregations':[{'type':'longSum','name':'EXPR$1','fieldName':'added'}],"
        + "'intervals':['1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z']}";
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
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$0], dir0=[DESC])\n"
        + "    BindableProject(s=[$2], page=[$0], day=[$1])\n"
        + "      DruidQuery(table=[[wiki, wikiticker]], intervals=[[1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z]], projects=[[$17, FLOOR($0, FLAG(DAY)), $1]], groups=[{0, 1}], aggs=[[SUM($2)]])\n";
    final String druidQuery = "{'queryType':'groupBy',"
        + "'dataSource':'wikiticker','granularity':'DAY','dimensions':['page'],"
        + "'limitSpec':{'type':'default'},"
        + "'aggregations':[{'type':'longSum','name':'s','fieldName':'added'}],"
        + "'intervals':['1900-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z']}";
    sql(sql, WIKI_AUTO2)
        .limit(1)
        .returnsUnordered("s=199818; page=User:QuackGuru/Electronic cigarettes 1; "
            + "day=2015-09-12 00:00:00")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  private CalciteAssert.AssertQuery checkSelectDistinctWiki(URL url, String tableName) {
    final String sql = "select distinct \"countryName\"\n"
        + "from \"" + tableName + "\"\n"
        + "where \"page\" = 'Jeremy Corbyn'";
    final String druidQuery = "{'queryType':'groupBy',"
        + "'dataSource':'wikiticker','granularity':'all',"
        + "'dimensions':['countryName'],'limitSpec':{'type':'default'},"
        + "'filter':{'type':'selector','dimension':'page','value':'Jeremy Corbyn'},"
        + "'aggregations':[{'type':'longSum','name':'dummy_agg','fieldName':'dummy_agg'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    return sql(sql, url)
        .returnsUnordered("countryName=United Kingdom",
            "countryName=null")
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testSelectDistinct() {
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{30}], aggs=[[]])";
    final String sql = "select distinct \"state_province\" from \"foodmart\"";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
        + "'dimensions':['state_province'],'limitSpec':{'type':'default'},"
        + "'aggregations':[{'type':'longSum','name':'dummy_agg','fieldName':'dummy_agg'}],"
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
              throw Throwables.propagate(e);
            }
          }
        });
  }

  @Test public void testSort() {
    // Note: We do not push down SORT yet
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$39, $30]], groups=[{0, 1}], aggs=[[]])";
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
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$39, $30]], groups=[{0, 1}], aggs=[[]])";
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
        + "'descending':false,'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'dimensions':['state_province','product_name'],'metrics':[],'granularity':'all',"
        + "'pagingSpec':{'threshold':16384},'context':{'druid.query.fetch':false}}";
    sql(sql)
        .runs()
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testLimit() {
    final String sql = "select \"gender\", \"state_province\"\n"
        + "from \"foodmart\" fetch next 3 rows only";
    final String druidQuery = "{'queryType':'select','dataSource':'foodmart',"
        + "'descending':false,'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'dimensions':['gender','state_province'],'metrics':[],'granularity':'all',"
        + "'pagingSpec':{'threshold':3},'context':{'druid.query.fetch':true}}";
    sql(sql)
        .runs()
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testGroupByLimit() {
    // We do not yet push LIMIT into a Druid "groupBy" query.
    final String sql = "select distinct \"gender\", \"state_province\"\n"
        + "from \"foodmart\" fetch next 3 rows only";
    final String druidQuery = "{'queryType':'groupBy','dataSource':'foodmart',"
        + "'granularity':'all','dimensions':['gender','state_province'],'limitSpec':{'type':'default'},"
        + "'aggregations':[{'type':'longSum','name':'dummy_agg','fieldName':'dummy_agg'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String explain = "PLAN="
        + "EnumerableLimit(fetch=[3])\n"
        + "  EnumerableInterpreter\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$39, $30]], groups=[{0, 1}], aggs=[[]])";
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
        + "'descending':false,'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
        + "'filter':{'type':'and','fields':["
        + "{'type':'bound','dimension':'product_id','lower':'1500','lowerStrict':false,'alphaNumeric':false},"
        + "{'type':'bound','dimension':'product_id','upper':'1502','upperStrict':false,'alphaNumeric':false}]},"
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
        + "'pagingSpec':{'threshold':16384},'context':{'druid.query.fetch':false}}";
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
        + "'descending':false,'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
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
        + "'pagingSpec':{'threshold':16384},'context':{'druid.query.fetch':false}}";
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
        + "'descending':false,'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
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
        + "'pagingSpec':{'threshold':16384},'context':{'druid.query.fetch':false}}";
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
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
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

  @Test public void testGroupByTimeAndOneColumnNotProjected() {
    final String sql = "select count(*) as \"c\", floor(\"timestamp\" to MONTH) as \"month\"\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to MONTH), \"state_province\"\n"
        + "order by \"c\" desc limit 3";
    sql(sql)
        .returnsOrdered("c=3072; month=1997-01-01 00:00:00",
            "c=2231; month=1997-01-01 00:00:00",
            "c=1730; month=1997-01-01 00:00:00")
        .queryContains(druidChecker("'queryType':'topN'"));
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
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{30}], aggs=[[COUNT()]])";
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
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
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
        + "'descending':false,'granularity':'MONTH',"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'},"
        + "{'type':'count','name':'C','fieldName':'store_sqft'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    sql(sql)
        .limit(3)
        .returnsUnordered("S=20957; C=6844", "S=21628; C=7033", "S=23706; C=7710")
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testGroupByDayGranularity() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + " count(\"store_sqft\") as c\n"
        + "from \"foodmart\"\n"
        + "group by floor(\"timestamp\" to DAY)";
    String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart',"
        + "'descending':false,'granularity':'DAY',"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'},"
        + "{'type':'count','name':'C','fieldName':'store_sqft'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
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
        + "'descending':false,'granularity':'MONTH',"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'},"
        + "{'type':'count','name':'C','fieldName':'store_sqft'}],"
        + "'intervals':['1996-01-01T00:00:00.000Z/1998-01-01T00:00:00.000Z']}";
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
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableProject(S=[$2], M=[$3], P=[$0])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$30, FLOOR($0, FLAG(MONTH)), $89]], groups=[{0, 1}], aggs=[[SUM($2), MAX($2)]], sort0=[2], dir0=[DESC], fetch=[3])";
    final String druidQuery = "{'queryType':'topN','dataSource':'foodmart',"
        + "'granularity':'MONTH','dimension':'state_province','metric':'S',"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'},"
        + "{'type':'longMax','name':'M','fieldName':'unit_sales'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],'threshold':3}";
    sql(sql)
        .returnsOrdered("S=9342; M=6; P=WA", "S=6909; M=6; P=OR", "S=5377; M=7; P=CA")
        .explainContains(explain)
        .queryContains(druidChecker(druidQuery));
  }

  @Test public void testTopNDayGranularityFiltered() {
    final String sql = "select sum(\"unit_sales\") as s,\n"
        + "max(\"unit_sales\") as m,\n"
        + "\"state_province\" as p\n"
        + "from \"foodmart\"\n"
        + "where \"timestamp\" >= '1997-01-01 00:00:00' and "
        + " \"timestamp\" < '1997-09-01 00:00:00'\n"
        + "group by \"state_province\", floor(\"timestamp\" to DAY)\n"
        + "order by s desc limit 3";
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableProject(S=[$2], M=[$3], P=[$0])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1997-01-01T00:00:00.000Z/1997-09-01T00:00:00.000Z]], projects=[[$30, FLOOR($0, FLAG(DAY)), $89]], groups=[{0, 1}], aggs=[[SUM($2), MAX($2)]], sort0=[2], dir0=[DESC], fetch=[3])";
    final String druidQuery = "{'queryType':'topN','dataSource':'foodmart',"
        + "'granularity':'DAY','dimension':'state_province','metric':'S',"
        + "'aggregations':[{'type':'longSum','name':'S','fieldName':'unit_sales'},"
        + "{'type':'longMax','name':'M','fieldName':'unit_sales'}],"
        + "'intervals':['1997-01-01T00:00:00.000Z/1997-09-01T00:00:00.000Z'],'threshold':3}";
    sql(sql)
        .returnsOrdered("S=348; M=5; P=CA")
        .explainContains(explain)
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
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$1], dir0=[DESC], fetch=[2])\n"
        + "    BindableProject(state_province=[$0], CDC=[FLOOR($1)])\n"
        + "      BindableAggregate(group=[{1}], agg#0=[COUNT($0)])\n"
        + "        DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{29, 30}], aggs=[[]])";
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
        + "'dimensions':['state_province','city','product_name'],'limitSpec':{'type':'default'},"
        + "'filter':{'type':'and','fields':[{'type':'selector','dimension':'product_name',"
        + "'value':'High Top Dried Mushrooms'},{'type':'or','fields':[{'type':'selector',"
        + "'dimension':'quarter','value':'Q2'},{'type':'selector','dimension':'quarter',"
        + "'value':'Q3'}]},{'type':'selector','dimension':'state_province','value':'WA'}]},"
        + "'aggregations':[{'type':'longSum','name':'dummy_agg','fieldName':'dummy_agg'}],"
        + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]],"
        + " filter=[AND(=(CAST($3):VARCHAR(24) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'High Top Dried Mushrooms'),"
        + " OR(=($87, 'Q2'),"
        + " =($87, 'Q3')),"
        + " =(CAST($30):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'WA'))],"
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
        + "'pagingSpec':{'threshold':16384},'context':{'druid.query.fetch':false}}";
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]],"
        + " filter=[AND(=(CAST($3):VARCHAR(24) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'High Top Dried Mushrooms'),"
        + " OR(=($87, 'Q2'),"
        + " =($87, 'Q3')),"
        + " =(CAST($30):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'WA'))],"
        + " projects=[[$30, $29, $3]])\n";
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
    final String explain = "EnumerableInterpreter\n"
        + "  BindableAggregate(group=[{}], C=[COUNT()])\n"
        + "    BindableFilter(condition=[AND(>=(/INT(Reinterpret($0), 86400000), 1997-01-01), <(/INT(Reinterpret($0), 86400000), 1998-01-01), >=(/INT(Reinterpret($0), 86400000), 1997-04-01), <(/INT(Reinterpret($0), 86400000), 1997-05-01))])\n"
        + "      DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]])";
    sql(sql)
        .explainContains(explain)
        .returnsUnordered("C=6588");
  }

  @Test public void testFilterSwapped() {
    String sql = "select \"state_province\"\n"
        + "from \"foodmart\"\n"
        + "where 'High Top Dried Mushrooms' = \"product_name\"";
    final String explain = "EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[=('High Top Dried Mushrooms', CAST($3):VARCHAR(24) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\")], projects=[[$30]])";
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
}

// End DruidAdapterIT.java
