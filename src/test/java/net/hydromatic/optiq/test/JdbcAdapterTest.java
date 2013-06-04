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

import junit.framework.TestCase;

/**
 * Tests for the {@link net.hydromatic.optiq.impl.jdbc} package.
 */
public class JdbcAdapterTest extends TestCase {
  public void testUnionPlan() {
    OptiqAssert.assertThat()
        .withModel(JdbcTest.FOODMART_MODEL)
        .query(
            "select * from \"sales_fact_1997\"\n"
            + "union all\n"
            + "select * from \"sales_fact_1998\"")
        .explainContains(
            "PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcUnionRel(all=[true])\n"
            + "    JdbcTableScan(table=[[foodmart, sales_fact_1997]])\n"
            + "    JdbcTableScan(table=[[foodmart, sales_fact_1998]])")
        .planHasSql(
            "SELECT * FROM `foodmart`.`sales_fact_1997`\n"
            + "UNION ALL \n"
            + "SELECT * FROM `foodmart`.`sales_fact_1998`")
        .runs();
  }

  public void testFilterUnionPlan() {
    OptiqAssert.assertThat()
        .withModel(JdbcTest.FOODMART_MODEL)
        .query(
            "select * from (\n"
            + "  select * from \"sales_fact_1997\"\n"
            + "  union all\n"
            + "  select * from \"sales_fact_1998\")\n"
            + "where \"product_id\" = 1")
        .planHasSql(
            "SELECT *\n"
            + "FROM (\n"
            + "    SELECT * FROM `foodmart`.`sales_fact_1997`) AS `t`\n"
            + "WHERE `product_id` = 1\n"
            + "UNION ALL \n"
            + "SELECT *\n"
            + "FROM (\n"
            + "    SELECT * FROM `foodmart`.`sales_fact_1998`) AS `t`\n"
            + "WHERE `product_id` = 1")
        .runs();
  }

  public void testInPlan() {
    OptiqAssert.assertThat()
        .withModel(JdbcTest.FOODMART_MODEL)
        .query(
            "select \"store_id\", \"store_name\" from \"store\"\n"
            + "where \"store_name\" in ('Store 1', 'Store 10', 'Store 11', 'Store 15', 'Store 16', 'Store 24', 'Store 3', 'Store 7')")
        .planHasSql(
            "SELECT `store_id` AS `store_id`, `store_name` AS `store_name`\n"
            + "FROM (\n"
            + "    SELECT * FROM `foodmart`.`store`) AS `t`\n"
            + "WHERE `store_name` = 'Store 1' OR `store_name` = 'Store 10' OR `store_name` = 'Store 11' OR `store_name` = 'Store 15' OR `store_name` = 'Store 16' OR `store_name` = 'Store 24' OR `store_name` = 'Store 3' OR `store_name` = 'Store 7'")
        .returns(
            "store_id=1; store_name=Store 1\n"
            + "store_id=3; store_name=Store 3\n"
            + "store_id=7; store_name=Store 7\n"
            + "store_id=10; store_name=Store 10\n"
            + "store_id=11; store_name=Store 11\n"
            + "store_id=15; store_name=Store 15\n"
            + "store_id=16; store_name=Store 16\n"
            + "store_id=24; store_name=Store 24\n");
  }
}

// End JdbcAdapterTest.java
