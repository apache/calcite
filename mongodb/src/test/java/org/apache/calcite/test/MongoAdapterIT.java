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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@code org.apache.calcite.adapter.mongodb} package.
 *
 * <p>Before calling this test, you need to populate MongoDB with the "zips"
 * data set (as described in howto.md)
 * and "foodmart" data set, as follows:</p>
 *
 * <blockquote><code>
 * git clone https://github.com/vlsi/test-dataset
 * cd test-dataset
 * mvn install
 * </code></blockquote>
 *
 * This will create a virtual machine with MongoDB and test dataset.
 */
public class MongoAdapterIT {
  public static final String MONGO_FOODMART_SCHEMA = "     {\n"
      + "       type: 'custom',\n"
      + "       name: '_foodmart',\n"
      + "       factory: 'org.apache.calcite.adapter.mongodb.MongoSchemaFactory',\n"
      + "       operand: {\n"
      + "         host: 'localhost',\n"
      + "         database: 'foodmart'\n"
      + "       }\n"
      + "     },\n"
      + "     {\n"
      + "       name: 'foodmart',\n"
      + "       tables: [\n"
      + "         {\n"
      + "           name: 'sales_fact_1997',\n"
      + "           type: 'view',\n"
      + "           sql: 'select cast(_MAP[\\'product_id\\'] AS double) AS \"product_id\" from \"_foodmart\".\"sales_fact_1997\"'\n"
      + "         },\n"
      + "         {\n"
      + "           name: 'sales_fact_1998',\n"
      + "           type: 'view',\n"
      + "           sql: 'select cast(_MAP[\\'product_id\\'] AS double) AS \"product_id\" from \"_foodmart\".\"sales_fact_1998\"'\n"
      + "         },\n"
      + "         {\n"
      + "           name: 'store',\n"
      + "           type: 'view',\n"
      + "           sql: 'select cast(_MAP[\\'store_id\\'] AS double) AS \"store_id\", cast(_MAP[\\'store_name\\'] AS varchar(20)) AS \"store_name\" from \"_foodmart\".\"store\"'\n"
      + "         },\n"
      + "         {\n"
      + "           name: 'warehouse',\n"
      + "           type: 'view',\n"
      + "           sql: 'select cast(_MAP[\\'warehouse_id\\'] AS double) AS \"warehouse_id\", cast(_MAP[\\'warehouse_state_province\\'] AS varchar(20)) AS \"warehouse_state_province\" from \"_foodmart\".\"warehouse\"'\n"
      + "         }\n"
      + "       ]\n"
      + "     }\n";

  public static final String MONGO_FOODMART_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'foodmart',\n"
      + "   schemas: [\n"
      + MONGO_FOODMART_SCHEMA
      + "   ]\n"
      + "}";

  /** Connection factory based on the "mongo-zips" model. */
  public static final ImmutableMap<String, String> ZIPS =
      ImmutableMap.of("model",
          MongoAdapterIT.class.getResource("/mongo-zips-model.json")
              .getPath());

  /** Connection factory based on the "mongo-zips" model. */
  public static final ImmutableMap<String, String> FOODMART =
      ImmutableMap.of("model",
          MongoAdapterIT.class.getResource("/mongo-foodmart-model.json")
              .getPath());

  /** Whether to run Mongo tests. Enabled by default, however test is only
   * included if "it" profile is activated ({@code -Pit}). To disable,
   * specify {@code -Dcalcite.test.mongodb=false} on the Java command line. */
  public static final boolean ENABLED =
      Boolean.valueOf(System.getProperty("calcite.test.mongodb", "true"));

  /** Whether to run this test. */
  protected boolean enabled() {
    return ENABLED;
  }

  /** Returns a function that checks that a particular MongoDB pipeline is
   * generated to implement a query. */
  private static Function<List, Void> mongoChecker(final String... strings) {
    return new Function<List, Void>() {
      public Void apply(List actual) {
        Object[] actualArray =
            actual == null || actual.isEmpty()
                ? null
                : ((List) actual.get(0)).toArray();
        CalciteAssert.assertArrayEqual("expected MongoDB query not found",
            strings, actualArray);
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

  @Test public void testSort() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select * from zips order by state")
        .returnsCount(29353)
        .explainContains("PLAN=MongoToEnumerableConverter\n"
            + "  MongoSort(sort0=[$4], dir0=[ASC])\n"
            + "    MongoProject(CITY=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], LONGITUDE=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], LATITUDE=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], POP=[CAST(ITEM($0, 'pop')):INTEGER], STATE=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], ID=[CAST(ITEM($0, '_id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "      MongoTableScan(table=[[mongo_raw, zips]])");
  }

  @Test public void testSortLimit() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, id from zips\n"
            + "order by state, id offset 2 rows fetch next 3 rows only")
        .returns("STATE=AK; ID=99503\n"
            + "STATE=AK; ID=99504\n"
            + "STATE=AK; ID=99505\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state', ID: '$_id'}}",
                "{$sort: {STATE: 1, ID: 1}}",
                "{$skip: 2}",
                "{$limit: 3}"));
  }

  @Test public void testOffsetLimit() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, id from zips\n"
            + "offset 2 fetch next 3 rows only")
        .runs()
        .queryContains(
            mongoChecker(
                "{$skip: 2}",
                "{$limit: 3}",
                "{$project: {STATE: '$state', ID: '$_id'}}"));
  }

  @Test public void testLimit() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, id from zips\n"
            + "fetch next 3 rows only")
        .runs()
        .queryContains(
            mongoChecker(
                "{$limit: 3}",
                "{$project: {STATE: '$state', ID: '$_id'}}"));
  }

  @Ignore
  @Test public void testFilterSort() {
    // LONGITUDE and LATITUDE are null because of CALCITE-194.
    Util.discard(Bug.CALCITE_194_FIXED);
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select * from zips\n"
            + "where city = 'SPRINGFIELD' and id >= '70000'\n"
            + "order by state, id")
        .returns(""
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=752; STATE=AR; ID=72157\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=1992; STATE=CO; ID=81073\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=5597; STATE=LA; ID=70462\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=32384; STATE=OR; ID=97477\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=27521; STATE=OR; ID=97478\n")
        .queryContains(
            mongoChecker(
                "{\n"
                    + "  $match: {\n"
                    + "    city: \"SPRINGFIELD\",\n"
                    + "    _id: {\n"
                    + "      $gte: \"70000\"\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
                "{$project: {CITY: '$city', LONGITUDE: '$loc[0]', LATITUDE: '$loc[1]', POP: '$pop', STATE: '$state', ID: '$_id'}}",
                "{$sort: {STATE: 1, ID: 1}}"))
        .explainContains("PLAN=MongoToEnumerableConverter\n"
            + "  MongoSort(sort0=[$4], sort1=[$5], dir0=[ASC], dir1=[ASC])\n"
            + "    MongoProject(CITY=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], LONGITUDE=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], LATITUDE=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], POP=[CAST(ITEM($0, 'pop')):INTEGER], STATE=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], ID=[CAST(ITEM($0, '_id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "      MongoFilter(condition=[AND(=(CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'SPRINGFIELD'), >=(CAST(ITEM($0, '_id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", '70000'))])\n"
            + "        MongoTableScan(table=[[mongo_raw, zips]])");
  }

  @Test public void testFilterSortDesc() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select * from zips\n"
            + "where pop BETWEEN 20000 AND 20100\n"
            + "order by state desc, pop")
        .limit(4)
        .returns(""
            + "CITY=SHERIDAN; LONGITUDE=null; LATITUDE=null; POP=20025; STATE=WY; ID=82801\n"
            + "CITY=MOUNTLAKE TERRAC; LONGITUDE=null; LATITUDE=null; POP=20059; STATE=WA; ID=98043\n"
            + "CITY=FALMOUTH; LONGITUDE=null; LATITUDE=null; POP=20039; STATE=VA; ID=22405\n"
            + "CITY=FORT WORTH; LONGITUDE=null; LATITUDE=null; POP=20012; STATE=TX; ID=76104\n");
  }

  @Test public void testUnionPlan() {
    CalciteAssert.that()
        .enable(enabled())
        .withModel(MONGO_FOODMART_MODEL)
        .query("select * from \"sales_fact_1997\"\n"
            + "union all\n"
            + "select * from \"sales_fact_1998\"")
        .explainContains("PLAN=EnumerableUnion(all=[true])\n"
            + "  MongoToEnumerableConverter\n"
            + "    MongoProject(product_id=[CAST(ITEM($0, 'product_id')):DOUBLE])\n"
            + "      MongoTableScan(table=[[_foodmart, sales_fact_1997]])\n"
            + "  MongoToEnumerableConverter\n"
            + "    MongoProject(product_id=[CAST(ITEM($0, 'product_id')):DOUBLE])\n"
            + "      MongoTableScan(table=[[_foodmart, sales_fact_1998]])")
        .limit(2)
        .returns(
            checkResultUnordered(
                "product_id=337", "product_id=1512"));
  }

  @Ignore(
      "java.lang.ClassCastException: java.lang.Integer cannot be cast to java.lang.Double")
  @Test public void testFilterUnionPlan() {
    CalciteAssert.that()
        .enable(enabled())
        .withModel(MONGO_FOODMART_MODEL)
        .query("select * from (\n"
            + "  select * from \"sales_fact_1997\"\n"
            + "  union all\n"
            + "  select * from \"sales_fact_1998\")\n"
            + "where \"product_id\" = 1")
        .runs();
  }

  /** Tests that we don't generate multiple constraints on the same column.
   * MongoDB doesn't like it. If there is an '=', it supersedes all other
   * operators. */
  @Test public void testFilterRedundant() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query(
            "select * from zips where state > 'CA' and state < 'AZ' and state = 'OK'")
        .runs()
        .queryContains(
            mongoChecker(
                "{\n"
                    + "  $match: {\n"
                    + "    state: \"OK\"\n"
                    + "  }\n"
                    + "}",
                "{$project: {CITY: '$city', LONGITUDE: '$loc[0]', LATITUDE: '$loc[1]', POP: '$pop', STATE: '$state', ID: '$_id'}}"));
  }

  @Test public void testSelectWhere() {
    CalciteAssert.that()
        .enable(enabled())
        .withModel(MONGO_FOODMART_MODEL)
        .query(
            "select * from \"warehouse\" where \"warehouse_state_province\" = 'CA'")
        .explainContains("PLAN=MongoToEnumerableConverter\n"
            + "  MongoProject(warehouse_id=[CAST(ITEM($0, 'warehouse_id')):DOUBLE], warehouse_state_province=[CAST(ITEM($0, 'warehouse_state_province')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "    MongoFilter(condition=[=(CAST(ITEM($0, 'warehouse_state_province')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'CA')])\n"
            + "      MongoTableScan(table=[[_foodmart, warehouse]])")
        .returns(
            checkResultUnordered(
                "warehouse_id=6; warehouse_state_province=CA",
                "warehouse_id=7; warehouse_state_province=CA",
                "warehouse_id=14; warehouse_state_province=CA",
                "warehouse_id=24; warehouse_state_province=CA"))
        .queryContains(
            // Per https://issues.apache.org/jira/browse/CALCITE-164,
            // $match must occur before $project for good performance.
            mongoChecker(
                "{\n"
                    + "  $match: {\n"
                    + "    warehouse_state_province: \"CA\"\n"
                    + "  }\n"
                    + "}",
                "{$project: {warehouse_id: 1, warehouse_state_province: 1}}"));
  }

  @Test public void testInPlan() {
    CalciteAssert.that()
        .enable(enabled())
        .withModel(MONGO_FOODMART_MODEL)
        .query("select \"store_id\", \"store_name\" from \"store\"\n"
            + "where \"store_name\" in ('Store 1', 'Store 10', 'Store 11', 'Store 15', 'Store 16', 'Store 24', 'Store 3', 'Store 7')")
        .returns(
            checkResultUnordered(
                "store_id=1; store_name=Store 1",
                "store_id=3; store_name=Store 3",
                "store_id=7; store_name=Store 7",
                "store_id=10; store_name=Store 10",
                "store_id=11; store_name=Store 11",
                "store_id=15; store_name=Store 15",
                "store_id=16; store_name=Store 16",
                "store_id=24; store_name=Store 24"))
        .queryContains(
            mongoChecker(
                "{\n"
                    + "  $match: {\n"
                    + "    $or: [\n"
                    + "      {\n"
                    + "        store_name: \"Store 1\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        store_name: \"Store 10\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        store_name: \"Store 11\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        store_name: \"Store 15\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        store_name: \"Store 16\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        store_name: \"Store 24\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        store_name: \"Store 3\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        store_name: \"Store 7\"\n"
                    + "      }\n"
                    + "    ]\n"
                    + "  }\n"
                    + "}",
                "{$project: {store_id: 1, store_name: 1}}"));
  }

  /** Simple query based on the "mongo-zips" model. */
  @Test public void testZips() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, city from zips")
        .returnsCount(29353);
  }

  @Test public void testCountGroupByEmpty() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select count(*) from zips")
        .returns("EXPR$0=29353\n")
        .explainContains("PLAN=MongoToEnumerableConverter\n"
            + "  MongoAggregate(group=[{}], EXPR$0=[COUNT()])\n"
            + "    MongoTableScan(table=[[mongo_raw, zips]])")
        .queryContains(
            mongoChecker(
                "{$group: {_id: {}, 'EXPR$0': {$sum: 1}}}"));
  }

  @Test public void testCountGroupByEmptyMultiplyBy2() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select count(*)*2 from zips")
        .returns("EXPR$0=58706\n")
        .queryContains(
            mongoChecker(
                "{$group: {_id: {}, _0: {$sum: 1}}}",
                "{$project: {'EXPR$0': {$multiply: ['$_0', {$literal: 2}]}}}"));
  }

  @Test public void testGroupByOneColumnNotProjected() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select count(*) from zips group by state order by 1")
        .limit(2)
        .returns("EXPR$0=24\n"
            + "EXPR$0=53\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state'}}",
                "{$group: {_id: '$STATE', 'EXPR$0': {$sum: 1}}}",
                "{$project: {STATE: '$_id', 'EXPR$0': '$EXPR$0'}}",
                "{$project: {'EXPR$0': 1}}",
                "{$sort: {EXPR$0: 1}}"));
  }

  @Test public void testGroupByOneColumn() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query(
            "select state, count(*) as c from zips group by state order by state")
        .limit(2)
        .returns("STATE=AK; C=195\n"
            + "STATE=AL; C=567\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state'}}",
                "{$group: {_id: '$STATE', C: {$sum: 1}}}",
                "{$project: {STATE: '$_id', C: '$C'}}",
                "{$sort: {STATE: 1}}"));
  }

  @Test public void testGroupByOneColumnReversed() {
    // Note extra $project compared to testGroupByOneColumn.
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query(
            "select count(*) as c, state from zips group by state order by state")
        .limit(2)
        .returns("C=195; STATE=AK\n"
            + "C=567; STATE=AL\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state'}}",
                "{$group: {_id: '$STATE', C: {$sum: 1}}}",
                "{$project: {STATE: '$_id', C: '$C'}}",
                "{$project: {C: 1, STATE: 1}}",
                "{$sort: {STATE: 1}}"));
  }

  @Test public void testGroupByAvg() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query(
            "select state, avg(pop) as a from zips group by state order by state")
        .limit(2)
        .returns("STATE=AK; A=2793.3230769230768\n"
            + "STATE=AL; A=7126.255731922399\n")
        .queryContains(
            mongoChecker(
                "{$project: {POP: '$pop', STATE: '$state'}}",
                "{$group: {_id: '$STATE', A: {$avg: '$POP'}}}",
                "{$project: {STATE: '$_id', A: '$A'}}",
                "{$sort: {STATE: 1}}"));
  }

  @Test public void testGroupByAvgSumCount() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query(
            "select state, avg(pop) as a, sum(pop) as s, count(pop) as c from zips group by state order by state")
        .limit(2)
        .returns("STATE=AK; A=2793.3230769230768; S=544698; C=195\n"
            + "STATE=AL; A=7126.255731922399; S=4040587; C=567\n")
        .queryContains(
            mongoChecker(
                "{$project: {POP: '$pop', STATE: '$state'}}",
                "{$group: {_id: '$STATE', _1: {$sum: '$POP'}, _2: {$sum: {$cond: [ {$eq: ['POP', null]}, 0, 1]}}}}",
                "{$project: {STATE: '$_id', _1: '$_1', _2: '$_2'}}",
                "{$sort: {STATE: 1}}",
                "{$project: {STATE: 1, A: {$divide: [{$cond:[{$eq: ['$_2', {$literal: 0}]},null,'$_1']}, '$_2']}, S: {$cond:[{$eq: ['$_2', {$literal: 0}]},null,'$_1']}, C: '$_2'}}"));
  }

  @Test public void testGroupByHaving() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, count(*) as c from zips\n"
            + "group by state having count(*) > 1500 order by state")
        .returns("STATE=CA; C=1516\n"
            + "STATE=NY; C=1595\n"
            + "STATE=TX; C=1671\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state'}}",
                "{$group: {_id: '$STATE', C: {$sum: 1}}}",
                "{$project: {STATE: '$_id', C: '$C'}}",
                "{\n"
                    + "  $match: {\n"
                    + "    C: {\n"
                    + "      $gt: 1500\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
                "{$sort: {STATE: 1}}"));
  }

  @Ignore("https://issues.apache.org/jira/browse/CALCITE-270")
  @Test public void testGroupByHaving2() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, count(*) as c from zips\n"
            + "group by state having sum(pop) > 12000000")
        .returns("STATE=NY; C=1596\n"
            + "STATE=TX; C=1676\n"
            + "STATE=FL; C=826\n"
            + "STATE=CA; C=1523\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state', POP: '$pop'}}",
                "{$group: {_id: '$STATE', C: {$sum: 1}, _2: {$sum: '$POP'}}}",
                "{$project: {STATE: '$_id', C: '$C', _2: '$_2'}}",
                "{\n"
                    + "  $match: {\n"
                    + "    _2: {\n"
                    + "      $gt: 12000000\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
                "{$project: {STATE: 1, C: 1}}"));
  }

  @Test public void testGroupByMinMaxSum() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select count(*) as c, state,\n"
            + " min(pop) as min_pop, max(pop) as max_pop, sum(pop) as sum_pop\n"
            + "from zips group by state order by state")
        .limit(2)
        .returns("C=195; STATE=AK; MIN_POP=0; MAX_POP=32383; SUM_POP=544698\n"
            + "C=567; STATE=AL; MIN_POP=0; MAX_POP=44165; SUM_POP=4040587\n")
        .queryContains(
            mongoChecker(
                "{$project: {POP: '$pop', STATE: '$state'}}",
                "{$group: {_id: '$STATE', C: {$sum: 1}, MIN_POP: {$min: '$POP'}, MAX_POP: {$max: '$POP'}, SUM_POP: {$sum: '$POP'}}}",
                "{$project: {STATE: '$_id', C: '$C', MIN_POP: '$MIN_POP', MAX_POP: '$MAX_POP', SUM_POP: '$SUM_POP'}}",
                "{$project: {C: 1, STATE: 1, MIN_POP: 1, MAX_POP: 1, SUM_POP: 1}}",
                "{$sort: {STATE: 1}}"));
  }

  @Test public void testGroupComposite() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select count(*) as c, state, city from zips\n"
            + "group by state, city order by c desc limit 2")
        .returns("C=93; STATE=TX; CITY=HOUSTON\n"
            + "C=56; STATE=CA; CITY=LOS ANGELES\n")
        .queryContains(
            mongoChecker(
                "{$project: {CITY: '$city', STATE: '$state'}}",
                "{$group: {_id: {CITY: '$CITY', STATE: '$STATE'}, C: {$sum: 1}}}",
                "{$project: {_id: 0, CITY: '$_id.CITY', STATE: '$_id.STATE', C: '$C'}}",
                "{$sort: {C: -1}}",
                "{$limit: 2}",
                "{$project: {C: 1, STATE: 1, CITY: 1}}"));
  }

  @Test public void testDistinctCount() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, count(distinct city) as cdc from zips\n"
            + "where state in ('CA', 'TX') group by state order by state")
        .returns("STATE=CA; CDC=1072\n"
            + "STATE=TX; CDC=1233\n")
        .queryContains(
            mongoChecker(
                "{\n"
                    + "  $match: {\n"
                    + "    $or: [\n"
                    + "      {\n"
                    + "        state: \"CA\"\n"
                    + "      },\n"
                    + "      {\n"
                    + "        state: \"TX\"\n"
                    + "      }\n"
                    + "    ]\n"
                    + "  }\n"
                    + "}",
                "{$project: {CITY: '$city', STATE: '$state'}}",
                "{$group: {_id: {CITY: '$CITY', STATE: '$STATE'}}}",
                "{$project: {_id: 0, CITY: '$_id.CITY', STATE: '$_id.STATE'}}",
                "{$group: {_id: '$STATE', CDC: {$sum: {$cond: [ {$eq: ['CITY', null]}, 0, 1]}}}}",
                "{$project: {STATE: '$_id', CDC: '$CDC'}}",
                "{$sort: {STATE: 1}}"));
  }

  @Test public void testDistinctCountOrderBy() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, count(distinct city) as cdc\n"
            + "from zips\n"
            + "group by state\n"
            + "order by cdc desc limit 5")
        .returns("STATE=NY; CDC=1370\n"
            + "STATE=PA; CDC=1369\n"
            + "STATE=TX; CDC=1233\n"
            + "STATE=IL; CDC=1148\n"
            + "STATE=CA; CDC=1072\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state', CITY: '$city'}}",
                "{$group: {_id: {STATE: '$STATE', CITY: '$CITY'}}}",
                "{$project: {_id: 0, STATE: '$_id.STATE', CITY: '$_id.CITY'}}",
                "{$group: {_id: '$STATE', CDC: {$sum: {$cond: [ {$eq: ['CITY', null]}, 0, 1]}}}}",
                "{$project: {STATE: '$_id', CDC: '$CDC'}}",
                "{$sort: {CDC: -1}}",
                "{$limit: 5}"));
  }

  @Test public void testProject() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, city, 0 as zero from zips order by state, city")
        .limit(2)
        .returns("STATE=AK; CITY=AKHIOK; ZERO=0\n"
            + "STATE=AK; CITY=AKIACHAK; ZERO=0\n")
        .queryContains(
            mongoChecker(
                "{$project: {CITY: '$city', STATE: '$state'}}",
                "{$sort: {STATE: 1, CITY: 1}}",
                "{$project: {STATE: 1, CITY: 1, ZERO: {$literal: 0}}}"));
  }

  @Test public void testFilter() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, city from zips where state = 'CA'")
        .limit(2)
        .returns("STATE=CA; CITY=LOS ANGELES\n"
            + "STATE=CA; CITY=LOS ANGELES\n")
        .explainContains("PLAN=MongoToEnumerableConverter\n"
            + "  MongoProject(STATE=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], CITY=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "    MongoFilter(condition=[=(CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'CA')])\n"
            + "      MongoTableScan(table=[[mongo_raw, zips]])");
  }

  /** MongoDB's predicates are handed (they can only accept literals on the
   * right-hand size) so it's worth testing that we handle them right both
   * ways around. */
  @Test public void testFilterReversed() {
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, city from zips where 'WI' < state")
        .limit(2)
        .returns("STATE=WV; CITY=BLUEWELL\n"
            + "STATE=WV; CITY=ATHENS\n");
    CalciteAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, city from zips where state > 'WI'")
        .limit(2)
        .returns("STATE=WV; CITY=BLUEWELL\n"
            + "STATE=WV; CITY=ATHENS\n");
  }

  @Ignore
  @Test public void testFoodmartQueries() {
    final List<Pair<String, String>> queries = JdbcTest.getFoodmartQueries();
    for (Ord<Pair<String, String>> query : Ord.zip(queries)) {
//      if (query.i != 29) continue;
      if (query.e.left.contains("agg_")) {
        continue;
      }
      final CalciteAssert.AssertQuery query1 =
          CalciteAssert.that()
              .enable(enabled())
              .with(FOODMART)
              .query(query.e.left);
      if (query.e.right != null) {
        query1.returns(query.e.right);
      } else {
        query1.runs();
      }
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-286">CALCITE-286</a>,
   * "Error casting MongoDB date". */
  @Test public void testDate() {
    // Assumes that you have created the following collection before running
    // this test:
    //
    // $ mongo
    // > use test
    // switched to db test
    // > db.createCollection("datatypes")
    // { "ok" : 1 }
    // > db.datatypes.insert( {
    //     "_id" : ObjectId("53655599e4b0c980df0a8c27"),
    //     "_class" : "com.ericblue.Test",
    //     "date" : ISODate("2012-09-05T07:00:00Z"),
    //     "value" : 1231,
    //     "ownerId" : "531e7789e4b0853ddb861313"
    //   } )
    CalciteAssert.that()
        .enable(enabled())
        .withModel("{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'test',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       name: 'test',\n"
            + "       factory: 'org.apache.calcite.adapter.mongodb.MongoSchemaFactory',\n"
            + "       operand: {\n"
            + "         host: 'localhost',\n"
            + "         database: 'test'\n"
            + "       }\n"
            + "     }\n"
            + "   ]\n"
            + "}")
        .query("select cast(_MAP['date'] as DATE) from \"datatypes\"")
        .returnsUnordered("EXPR$0=2012-09-05");
  }
}

// End MongoAdapterIT.java
