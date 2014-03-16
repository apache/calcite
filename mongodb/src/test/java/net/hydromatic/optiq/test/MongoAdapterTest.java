/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.test;

import net.hydromatic.linq4j.Ord;
import net.hydromatic.linq4j.function.Function1;

import org.eigenbase.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@code net.hydromatic.optiq.impl.mongodb} package.
 *
 * <p>Before calling this test, you need to populate MongoDB with the "zips"
 * data set (as described in HOWTO.md)
 * and "foodmart" data set, as follows:</p>
 *
 * <blockquote><code>
 * JAR=~/.m2/repository/pentaho/mondrian-data-foodmart-json/
 * 0.3/mondrian-data-foodmart-json-0.3.jar<br>
 * mkdir /tmp/foodmart<br>
 * cd /tmp/foodmart<br>
 * jar xvf $JAR<br>
 * for i in *.json; do<br>
 * &nbsp;&nbsp;mongoimport --db foodmart --collection ${i/.json/} --file $i<br>
 * done<br>
 * </code></blockquote>
 */
public class MongoAdapterTest {
  public static final String MONGO_FOODMART_SCHEMA =
      "     {\n"
      + "       type: 'custom',\n"
      + "       name: '_foodmart',\n"
      + "       factory: 'net.hydromatic.optiq.impl.mongodb.MongoSchemaFactory',\n"
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

  public static final String MONGO_FOODMART_MODEL =
      "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'foodmart',\n"
      + "   schemas: [\n"
      + MONGO_FOODMART_SCHEMA
      + "   ]\n"
      + "}";

  /** Connection factory based on the "mongo-zips" model. */
  public static final ImmutableMap<String, String> ZIPS =
      ImmutableMap.of("model",
          MongoAdapterTest.class.getResource("/mongo-zips-model.json")
              .getPath());

  /** Connection factory based on the "mongo-zips" model. */
  public static final ImmutableMap<String, String> FOODMART =
      ImmutableMap.of("model",
          MongoAdapterTest.class.getResource("/mongo-foodmart-model.json")
              .getPath());

  /** Whether to run Mongo tests. Disabled by default, because we do not expect
   * Mongo to be installed and populated with the FoodMart data set. To enable,
   * specify {@code -Doptiq.test.mongodb=true} on the Java command line. */
  public static final boolean ENABLED =
      Boolean.getBoolean("optiq.test.mongodb");

  /** Whether to run this test. */
  protected boolean enabled() {
    return ENABLED;
  }

  /** Returns a function that checks that a particular MongoDB pipeline is
   * generated to implement a query. */
  private static Function1<List, Void> mongoChecker(final String... strings) {
    return new Function1<List, Void>() {
      public Void apply(List actual) {
        if (!actual.contains(ImmutableList.copyOf(strings))) {
          Assert.fail("expected MongoDB query not found; actual: " + actual);
        }
        return null;
      }
    };
  }

  static Function1<ResultSet, Void> checkResultUnordered(
      final String... lines) {
    return new Function1<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          final List<String> expectedList = new ArrayList<String>();
          Collections.addAll(expectedList, lines);
          Collections.sort(expectedList);

          final List<String> actualList = new ArrayList<String>();
          OptiqAssert.toStringList(resultSet, actualList);
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
    OptiqAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select * from zips order by state")
        .returnsCount(29467)
        .explainContains(
            "PLAN=MongoToEnumerableConverter\n"
            + "  MongoSortRel(sort0=[$4], dir0=[ASC])\n"
            + "    MongoProjectRel(CITY=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], LONGITUDE=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT NOT NULL], LATITUDE=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT NOT NULL], POP=[CAST(ITEM($0, 'pop')):INTEGER], STATE=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], ID=[CAST(ITEM($0, '_id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "      MongoTableScan(table=[[mongo_raw, zips]])");
  }

  @Test public void testFilterSort() {
    // LONGITUDE and LATITUDE are null because of OPTIQ-194.
    Util.discard(Bug.OPTIQ194_FIXED);
    OptiqAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query(
            "select * from zips\n"
            + "where city = 'SPRINGFIELD' and id between '20000' and '30000'\n"
            + "order by state")
        .returns(
            "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=2184; STATE=SC; ID=29146\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=16811; STATE=VA; ID=22150\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=32161; STATE=VA; ID=22153\n"
            + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=1321; STATE=WV; ID=26763\n")
        .queryContains(
            mongoChecker(
                "{\n"
                + "  $match: {\n"
                + "    city: \"SPRINGFIELD\",\n"
                + "    _id: {\n"
                + "      $lte: \"30000\",\n"
                + "      $gte: \"20000\"\n"
                + "    }\n"
                + "  }\n"
                + "}",
                "{$project: {CITY: '$city', LONGITUDE: '$loc[0]', LATITUDE: '$loc[1]', POP: '$pop', STATE: '$state', ID: '$_id'}}",
                "{$sort: {STATE: 1}}"))
        .explainContains(
            "PLAN=MongoToEnumerableConverter\n"
            + "  MongoSortRel(sort0=[$4], dir0=[ASC])\n"
            + "    MongoProjectRel(CITY=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], LONGITUDE=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT NOT NULL], LATITUDE=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT NOT NULL], POP=[CAST(ITEM($0, 'pop')):INTEGER], STATE=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], ID=[CAST(ITEM($0, '_id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "      MongoFilterRel(condition=[AND(=(CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'SPRINGFIELD'), >=(CAST(ITEM($0, '_id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", '20000'), <=(CAST(ITEM($0, '_id')):VARCHAR(5) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", '30000'))])\n"
            + "        MongoTableScan(table=[[mongo_raw, zips]])");
  }

  @Test public void testFilterSortDesc() {
    OptiqAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query(
            "select * from zips\n"
            + "where pop BETWEEN 20000 AND 20100\n"
            + "order by state desc, pop")
        .limit(4)
        .returns(
            "CITY=SHERIDAN; LONGITUDE=null; LATITUDE=null; POP=20025; STATE=WY; ID=82801\n"
                + "CITY=MOUNTLAKE TERRAC; LONGITUDE=null; LATITUDE=null; POP=20059; STATE=WA; ID=98043\n"
                + "CITY=FALMOUTH; LONGITUDE=null; LATITUDE=null; POP=20039; STATE=VA; ID=22405\n"
                + "CITY=FORT WORTH; LONGITUDE=null; LATITUDE=null; POP=20012; STATE=TX; ID=76104\n");
  }

  @Test public void testUnionPlan() {
    OptiqAssert.that()
        .enable(enabled())
        .withModel(MONGO_FOODMART_MODEL)
        .query(
            "select * from \"sales_fact_1997\"\n"
                + "union all\n"
                + "select * from \"sales_fact_1998\"")
        .explainContains(
            "PLAN=EnumerableUnionRel(all=[true])\n"
                + "  MongoToEnumerableConverter\n"
                + "    MongoProjectRel(product_id=[CAST(ITEM($0, 'product_id')):DOUBLE])\n"
                + "      MongoTableScan(table=[[_foodmart, sales_fact_1997]])\n"
                + "  MongoToEnumerableConverter\n"
                + "    MongoProjectRel(product_id=[CAST(ITEM($0, 'product_id')):DOUBLE])\n"
                + "      MongoTableScan(table=[[_foodmart, sales_fact_1998]])\n")
        .limit(2)
        .returns(
            checkResultUnordered(
                "product_id=337", "product_id=1512"));
  }

  @Test public void testFilterUnionPlan() {
    OptiqAssert.that()
        .enable(enabled())
        .withModel(MONGO_FOODMART_MODEL)
        .query(
            "select * from (\n"
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
    OptiqAssert.that()
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
    OptiqAssert.that()
        .enable(enabled())
        .withModel(MONGO_FOODMART_MODEL)
        .query(
            "select * from \"warehouse\" where \"warehouse_state_province\" = 'CA'")
        .explainContains(
            "PLAN=MongoToEnumerableConverter\n"
                + "  MongoProjectRel(warehouse_id=[CAST(ITEM($0, 'warehouse_id')):DOUBLE], warehouse_state_province=[CAST(ITEM($0, 'warehouse_state_province')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
                + "    MongoFilterRel(condition=[=(CAST(ITEM($0, 'warehouse_state_province')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'CA')])\n"
                + "      MongoTableScan(table=[[_foodmart, warehouse]])")
        .returns(
            checkResultUnordered(
                "warehouse_id=6; warehouse_state_province=CA",
                "warehouse_id=7; warehouse_state_province=CA",
                "warehouse_id=14; warehouse_state_province=CA",
                "warehouse_id=24; warehouse_state_province=CA"))
        .queryContains(
            // Per https://github.com/julianhyde/optiq/issues/164,
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
    OptiqAssert.that()
        .enable(enabled())
        .withModel(MONGO_FOODMART_MODEL)
        .query(
            "select \"store_id\", \"store_name\" from \"store\"\n"
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

  /** Query based on the "mongo-zips" model. */
  @Test public void testZips() {
    OptiqAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select count(*) from zips")
        .returns("EXPR$0=29467\n")
        .explainContains(
            "PLAN=EnumerableAggregateRel(group=[{}], EXPR$0=[COUNT()])\n"
                + "  MongoToEnumerableConverter\n"
                + "    MongoProjectRel(DUMMY=[0])\n"
                + "      MongoTableScan(table=[[mongo_raw, zips]])");
  }

  @Test public void testProject() {
    OptiqAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, city, 0 as zero from zips")
        .limit(2)
        .returns(
            "STATE=AL; CITY=ACMAR; ZERO=0\n"
            + "STATE=AL; CITY=ADAMSVILLE; ZERO=0\n")
        .queryContains(
            mongoChecker(
                "{$project: {STATE: '$state', CITY: '$city', ZERO: {$ifNull: [null, 0]}}}"));
  }

  @Test public void testFilter() {
    OptiqAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, city from zips where state = 'CA'")
        .limit(2)
        .returns(
            "STATE=CA; CITY=LOS ANGELES\n"
            + "STATE=CA; CITY=LOS ANGELES\n")
        .explainContains(
            "PLAN=MongoToEnumerableConverter\n"
            + "  MongoProjectRel(STATE=[CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], CITY=[CAST(ITEM($0, 'city')):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "    MongoFilterRel(condition=[=(CAST(ITEM($0, 'state')):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'CA')])\n"
            + "      MongoTableScan(table=[[mongo_raw, zips]])");
  }

  /** MongoDB's predicates are handed (they can only accept literals on the
   * right-hand size) so it's worth testing that we handle them right both
   * ways around. */
  @Test public void testFilterReversed() {
    OptiqAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, city from zips where 'WI' < state")
        .limit(2)
        .returns(
            "STATE=WV; CITY=BLUEWELL\n"
            + "STATE=WV; CITY=ATHENS\n");
    OptiqAssert.that()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, city from zips where state > 'WI'")
        .limit(2)
        .returns(
            "STATE=WV; CITY=BLUEWELL\n" + "STATE=WV; CITY=ATHENS\n");
  }

  @Ignore
  @Test public void testFoodmartQueries() {
    final List<Pair<String, String>> queries = JdbcTest.getFoodmartQueries();
    for (Ord<Pair<String, String>> query : Ord.zip(queries)) {
//      if (query.i != 29) continue;
      if (query.e.left.contains("agg_")) {
        continue;
      }
      final OptiqAssert.AssertQuery query1 =
          OptiqAssert.that()
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
}

// End MongoAdapterTest.java
