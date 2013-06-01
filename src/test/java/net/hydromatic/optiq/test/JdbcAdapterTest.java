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
            "select * from sales_fact_1997\n"
            + "union all\n"
            + "select * from sales_fact_1998")
        .explainContains(
            "PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcUnionRel(all=[true])\n"
            + "    JdbcTableScan(table=[[foodmart, SALES_FACT_1997]])\n"
            + "    JdbcTableScan(table=[[foodmart, SALES_FACT_1998]])")
        .planHasSql(
            "SELECT * FROM `foodmart`.`SALES_FACT_1997`\n"
            + "UNION ALL \n"
            + "SELECT * FROM `foodmart`.`SALES_FACT_1998`")
        .runs();
  }

  public void testFilterUnionPlan() {
    OptiqAssert.assertThat()
        .withModel(JdbcTest.FOODMART_MODEL)
        .query(
            "select * from (\n"
            + "  select * from sales_fact_1997\n"
            + "  union all\n"
            + "  select * from sales_fact_1998)\n"
            + "where \"product_id\" = 1")
        .planHasSql(
            "SELECT *\n"
            + "FROM (\n"
            + "    SELECT * FROM `foodmart`.`SALES_FACT_1997`) AS `t`\n"
            + "WHERE `product_id` = 1\n"
            + "UNION ALL \n"
            + "SELECT *\n"
            + "FROM (\n"
            + "    SELECT * FROM `foodmart`.`SALES_FACT_1998`) AS `t`\n"
            + "WHERE `product_id` = 1")
        .runs();
  }
}

// End JdbcAdapterTest.java
