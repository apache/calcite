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

import net.hydromatic.optiq.jdbc.OptiqConnection;

import junit.framework.TestCase;

import java.sql.DriverManager;
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

  /** Disabled by default, because we do not expect Mongo to be installed and
   * populated with the FoodMart data set. */
  private boolean enabled() {
    return true;
  }

  public void testUnionPlan() {
    if (!enabled()) {
      return;
    }
    OptiqAssert.assertThat()
        .withModel(MONGO_FOODMART_MODEL)
        .query(
            "select * from \"sales_fact_1997\"\n"
            + "union all\n"
            + "select * from \"sales_fact_1998\"")
        .explainContains(
            "PLAN=EnumerableUnionRel(all=[true])\n"
            + "  EnumerableCalcRel(expr#0=[{inputs}], expr#1=['product_id'], expr#2=[ITEM($t0, $t1)], expr#3=[CAST($t2):DOUBLE NOT NULL], product_id=[$t3])\n"
            + "    EnumerableTableAccessRel(table=[[_foodmart, sales_fact_1997]])\n"
            + "  EnumerableCalcRel(expr#0=[{inputs}], expr#1=['product_id'], expr#2=[ITEM($t0, $t1)], expr#3=[CAST($t2):DOUBLE NOT NULL], product_id=[$t3])\n"
            + "    EnumerableTableAccessRel(table=[[_foodmart, sales_fact_1998]])")
        .runs();
  }

  public void testFilterUnionPlan() {
    if (!enabled()) {
      return;
    }
    OptiqAssert.assertThat()
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
    if (!enabled()) {
      return;
    }
    OptiqAssert.assertThat()
        .withModel(MONGO_FOODMART_MODEL)
        .query(
            "select * from \"warehouse\" where \"warehouse_state_province\" = 'CA'")
        .explainContains(
            "PLAN=EnumerableCalcRel(expr#0=[{inputs}], expr#1=['warehouse_id'], expr#2=[ITEM($t0, $t1)], expr#3=[CAST($t2):DOUBLE NOT NULL], expr#4=['warehouse_state_province'], expr#5=[ITEM($t0, $t4)], expr#6=[CAST($t5):VARCHAR(20) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL], expr#7=['CA'], expr#8=[=($t6, $t7)], warehouse_id=[$t3], warehouse_state_province=[$t6], $condition=[$t8])\n"
            + "  EnumerableTableAccessRel(table=[[_foodmart, warehouse]])")
        .returns(
            "warehouse_id=6.0; warehouse_state_province=CA\n"
            + "warehouse_id=7.0; warehouse_state_province=CA\n"
            + "warehouse_id=14.0; warehouse_state_province=CA\n"
            + "warehouse_id=24.0; warehouse_state_province=CA\n");
  }

  public void testInPlan() {
    if (!enabled()) {
      return;
    }
    OptiqAssert.assertThat()
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
    if (!enabled()) {
      return;
    }
    OptiqAssert.assertThat()
        .with(
            new OptiqAssert.ConnectionFactory() {
              public OptiqConnection createConnection() throws Exception {
                Class.forName("net.hydromatic.optiq.jdbc.Driver");
                final Properties info = new Properties();
                info.setProperty("model",
                    "target/test-classes/mongo-zips-model.json");
                return (OptiqConnection) DriverManager.getConnection(
                    "jdbc:optiq:", info);
              }
            })
        .query("select count(*) from zips")
        .returns("EXPR$0=29467\n")
        .explainContains(
            "PLAN=EnumerableAggregateRel(group=[{}], EXPR$0=[COUNT()])\n"
            + "  EnumerableCalcRel(expr#0=[{inputs}], expr#1=[0], $f0=[$t1])\n"
            + "    MongoToEnumerableConverter\n"
            + "      MongoTableScan(table=[[mongo_raw, zips]], ops=[[]])");
  }
}

// End MongoAdapterTest.java
