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
import net.hydromatic.optiq.jdbc.OptiqConnection;

import junit.framework.TestCase;

import org.eigenbase.util.Pair;

import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

/**
 * Tests for the {@link net.hydromatic.optiq.impl.mongodb} package.
 */
public class MongoAdapterTest extends TestCase {
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
  public static final OptiqAssert.ConnectionFactory ZIPS =
      new OptiqAssert.ConnectionFactory() {
        public OptiqConnection createConnection() throws Exception {
          Class.forName("net.hydromatic.optiq.jdbc.Driver");
          final Properties info = new Properties();
          info.setProperty("model",
              "target/test-classes/mongo-zips-model.json");
          return (OptiqConnection)
              DriverManager.getConnection("jdbc:optiq:", info);
        }
      };

  /** Connection factory based on the "mongo-zips" model. */
  public static final OptiqAssert.ConnectionFactory FOODMART =
      new OptiqAssert.ConnectionFactory() {
        public OptiqConnection createConnection() throws Exception {
          Class.forName("net.hydromatic.optiq.jdbc.Driver");
          final Properties info = new Properties();
          info.setProperty("model",
              "target/test-classes/mongo-foodmart-model.json");
          return (OptiqConnection)
              DriverManager.getConnection("jdbc:optiq:", info);
        }
      };

  /** Disabled by default, because we do not expect Mongo to be installed and
   * populated with the FoodMart data set. */
  private boolean enabled() {
    return true;
  }

  public void testUnionPlan() {
    OptiqAssert.assertThat()
        .enable(enabled())
        .withModel(MONGO_FOODMART_MODEL)
        .query(
            "select * from \"sales_fact_1997\"\n"
            + "union all\n"
            + "select * from \"sales_fact_1998\"")
        .explainContains(
            "PLAN=EnumerableUnionRel(all=[true])\n"
            + "  EnumerableCalcRel(expr#0=[{inputs}], product_id=[$t0])\n"
            + "    MongoToEnumerableConverter\n"
            + "      MongoTableScan(table=[[_foodmart, sales_fact_1997]], ops=[[<{product_id: 1}, {$project ...}>]])\n"
            + "  EnumerableCalcRel(expr#0=[{inputs}], product_id=[$t0])\n"
            + "    MongoToEnumerableConverter\n"
            + "      MongoTableScan(table=[[_foodmart, sales_fact_1998]], ops=[[<{product_id: 1}, {$project ...}>]])")
        .limit(2)
        .returns(
            "product_id=337.0\n"
            + "product_id=1512.0\n");
  }

  public void testFilterUnionPlan() {
    OptiqAssert.assertThat()
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

  public void testSelectWhere() {
    OptiqAssert.assertThat()
        .enable(enabled())
        .withModel(MONGO_FOODMART_MODEL)
        .query(
            "select * from \"warehouse\" where \"warehouse_state_province\" = 'CA'")
        .explainContains(
            "PLAN=EnumerableCalcRel(expr#0..1=[{inputs}], expr#2=['CA'], expr#3=[=($t1, $t2)], proj#0..1=[{exprs}], $condition=[$t3])\n"
            + "  MongoToEnumerableConverter\n"
            + "    MongoTableScan(table=[[_foodmart, warehouse]], ops=[[<{warehouse_id: 1, warehouse_state_province: 1}, {$project ...}>]])")
        .returns(
            "warehouse_id=6.0; warehouse_state_province=CA\n"
            + "warehouse_id=7.0; warehouse_state_province=CA\n"
            + "warehouse_id=14.0; warehouse_state_province=CA\n"
            + "warehouse_id=24.0; warehouse_state_province=CA\n");
  }

  public void testInPlan() {
    OptiqAssert.assertThat()
        .enable(enabled())
        .withModel(MONGO_FOODMART_MODEL)
        .query(
            "select \"store_id\", \"store_name\" from \"store\"\n"
            + "where \"store_name\" in ('Store 1', 'Store 10', 'Store 11', 'Store 15', 'Store 16', 'Store 24', 'Store 3', 'Store 7')")
        .returns(
            "store_id=1.0; store_name=Store 1\n"
            + "store_id=3.0; store_name=Store 3\n"
            + "store_id=7.0; store_name=Store 7\n"
            + "store_id=10.0; store_name=Store 10\n"
            + "store_id=11.0; store_name=Store 11\n"
            + "store_id=15.0; store_name=Store 15\n"
            + "store_id=16.0; store_name=Store 16\n"
            + "store_id=24.0; store_name=Store 24\n");
  }

  /** Query based on the "mongo-zips" model. */
  public void testZips() {
    OptiqAssert.assertThat()
        .enable(enabled())
        .with(ZIPS)
        .query("select count(*) from zips")
        .returns("EXPR$0=29467\n")
        .explainContains(
            "PLAN=EnumerableAggregateRel(group=[{}], EXPR$0=[COUNT()])\n"
            + "  EnumerableCalcRel(expr#0..4=[{inputs}], expr#5=[0], $f0=[$t5])\n"
            + "    MongoToEnumerableConverter\n"
            + "      MongoTableScan(table=[[mongo_raw, zips]], ops=[[<{city: 1, loc: 1, pop: 1, state: 1, _id: 1}, {$project ...}>]])");
  }

  public void testProject() {
    OptiqAssert.assertThat()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, city from zips")
        .limit(2)
        .returns(
            "STATE=AL; CITY=ACMAR\n"
            + "STATE=AL; CITY=ADAMSVILLE\n")
        .explainContains(
            "PLAN=EnumerableCalcRel(expr#0..4=[{inputs}], STATE=[$t3], CITY=[$t0])\n"
            + "  MongoToEnumerableConverter\n"
            + "    MongoTableScan(table=[[mongo_raw, zips]], ops=[[<{city: 1, loc: 1, pop: 1, state: 1, _id: 1}, {$project ...}>]])");
  }

  public void testFilter() {
    OptiqAssert.assertThat()
        .enable(enabled())
        .with(ZIPS)
        .query("select state, city from zips where state = 'CA'")
        .limit(2)
        .returns(
            "STATE=CA; CITY=LOS ANGELES\n"
            + "STATE=CA; CITY=LOS ANGELES\n")
        .explainContains(
            "PLAN=EnumerableCalcRel(expr#0..4=[{inputs}], expr#5=['CA'], expr#6=[=($t3, $t5)], STATE=[$t3], CITY=[$t0], $condition=[$t6])\n"
            + "  MongoToEnumerableConverter\n"
            + "    MongoTableScan(table=[[mongo_raw, zips]], ops=[[<{city: 1, loc: 1, pop: 1, state: 1, _id: 1}, {$project ...}>]])");
  }

  public void _testFoodmartQueries() {
    final List<Pair<String, String>> queries = JdbcTest.getFoodmartQueries();
    for (Ord<Pair<String, String>> query : Ord.zip(queries)) {
//      if (query.i != 29) continue;
      if (query.e.left.contains("agg_")) {
        continue;
      }
      final OptiqAssert.AssertQuery query1 =
          OptiqAssert.assertThat()
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
