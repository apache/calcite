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

import org.apache.calcite.util.Bug;

import org.junit.Ignore;
import org.junit.Test;

import static org.apache.calcite.test.CalciteAssert.that;

/**
 * Tests for a JDBC front-end and JDBC back-end where the processing is not
 * pushed down to JDBC (as in {@link JdbcFrontJdbcBackTest}) but is executed
 * in a pipeline of linq4j operators.
 */
public class JdbcFrontJdbcBackLinqMiddleTest {

  @Test public void testTable() {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select * from \"foodmart\".\"days\"")
        .returns("day=1; week_day=Sunday\n"
            + "day=2; week_day=Monday\n"
            + "day=5; week_day=Thursday\n"
            + "day=4; week_day=Wednesday\n"
            + "day=3; week_day=Tuesday\n"
            + "day=6; week_day=Friday\n"
            + "day=7; week_day=Saturday\n");
  }

  @Test public void testWhere() {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select * from \"foodmart\".\"days\" where \"day\" < 3")
        .returns("day=1; week_day=Sunday\n"
            + "day=2; week_day=Monday\n");
  }

  @Test public void testWhere2() {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select * from \"foodmart\".\"days\"\n"
            + "where not (lower(\"week_day\") = 'wednesday')")
        .returns("day=1; week_day=Sunday\n"
            + "day=2; week_day=Monday\n"
            + "day=5; week_day=Thursday\n"
            + "day=3; week_day=Tuesday\n"
            + "day=6; week_day=Friday\n"
            + "day=7; week_day=Saturday\n");
  }

  @Test public void testCase() {
    that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"day\",\n"
            + " \"week_day\",\n"
            + " case when \"day\" < 3 then upper(\"week_day\")\n"
            + "      when \"day\" < 5 then lower(\"week_day\")\n"
            + "      else \"week_day\" end as d\n"
            + "from \"foodmart\".\"days\"\n"
            + "where \"day\" <> 1\n"
            + "order by \"day\"")
        .returns("day=2; week_day=Monday; D=MONDAY\n"
            + "day=3; week_day=Tuesday; D=tuesday\n"
            + "day=4; week_day=Wednesday; D=wednesday\n"
            + "day=5; week_day=Thursday; D=Thursday\n"
            + "day=6; week_day=Friday; D=Friday\n"
            + "day=7; week_day=Saturday; D=Saturday\n");
  }

  @Test public void testGroup() {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select s, count(*) as c, min(\"week_day\") as mw from (\n"
            + "select \"week_day\",\n"
            + "  substring(\"week_day\" from 1 for 1) as s\n"
            + "from \"foodmart\".\"days\")\n"
            + "group by s")
        .returnsUnordered(
            "S=T; C=2; MW=Thursday",
            "S=F; C=1; MW=Friday",
            "S=W; C=1; MW=Wednesday",
            "S=S; C=2; MW=Saturday",
            "S=M; C=1; MW=Monday");
  }

  @Test public void testGroupEmpty() {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select count(*) as c\n"
            + "from \"foodmart\".\"days\"")
        .returns("C=7\n");
  }

  /** Tests a theta join: a join whose condition cannot be decomposed
   * into input0.x = input1.x and ... input0.z = input1.z.
   *
   * <p>Currently, the query can be planned, but the plan is not efficient (uses
   * cartesian product).</p>
   */
  @Ignore("non-deterministic on JDK 1.7 vs 1.8")
  @Test public void testJoinTheta() {
    that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select count(*) from (\n"
            + "  select *\n"
            + "  from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "  join \"foodmart\".\"customer\" as c\n"
            + "  on s.\"customer_id\" - c.\"customer_id\" = 0)")
        .explainContains("EnumerableAggregate(group=[{}], EXPR$0=[COUNT()])\n"
            + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[0], expr#3=[-($t0, $t1)], expr#4=[=($t3, $t2)], DUMMY=[$t2], $condition=[$t4])\n"
            + "    EnumerableHashJoin(condition=[true], joinType=[inner])\n"
            + "      JdbcToEnumerableConverter\n"
            + "        JdbcProject(customer_id=[$2])\n"
            + "          JdbcTableScan(table=[[foodmart, sales_fact_1997]])\n"
            + "      JdbcToEnumerableConverter\n"
            + "        JdbcProject(customer_id=[$0])\n"
            + "          JdbcTableScan(table=[[foodmart, customer]])");
  }

  @Test public void testJoinGroupByEmpty() {
    if (CalciteAssert.DB == CalciteAssert.DatabaseInstance.MYSQL
        && !Bug.CALCITE_673_FIXED) {
      return;
    }
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select count(*) from (\n"
            + "  select *\n"
            + "  from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "  join \"foodmart\".\"customer\" as c\n"
            + "  on s.\"customer_id\" = c.\"customer_id\")")
        .returns("EXPR$0=86837\n");
  }

  @Test public void testJoinGroupByOrderBy() {
    if (CalciteAssert.DB == CalciteAssert.DatabaseInstance.MYSQL
        && !Bug.CALCITE_673_FIXED) {
      return;
    }
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select count(*), c.\"state_province\",\n"
            + "  sum(s.\"unit_sales\") as s\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "  join \"foodmart\".\"customer\" as c\n"
            + "  on s.\"customer_id\" = c.\"customer_id\"\n"
            + "group by c.\"state_province\"\n"
            + "order by c.\"state_province\"")
        .returns2("EXPR$0=24442; state_province=CA; S=74748\n"
            + "EXPR$0=21611; state_province=OR; S=67659\n"
            + "EXPR$0=40784; state_province=WA; S=124366\n");
  }

  @Test public void testCompositeGroupBy() {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select count(*) as c, c.\"state_province\"\n"
            + "from \"foodmart\".\"customer\" as c\n"
            + "group by c.\"state_province\", c.\"country\"\n"
            + "order by c, 1")
        .returns("C=78; state_province=Sinaloa\n"
            + "C=90; state_province=Oaxaca\n"
            + "C=93; state_province=Veracruz\n"
            + "C=97; state_province=Mexico\n"
            + "C=99; state_province=Yucatan\n"
            + "C=104; state_province=Jalisco\n"
            + "C=106; state_province=Guerrero\n"
            + "C=191; state_province=Zacatecas\n"
            + "C=347; state_province=DF\n"
            + "C=1051; state_province=OR\n"
            + "C=1717; state_province=BC\n"
            + "C=2086; state_province=WA\n"
            + "C=4222; state_province=CA\n");
  }

  @Ignore
  @Test public void testDistinctCount() {
    // Complicating factors:
    // Composite GROUP BY key
    // Order by select item, referenced by ordinal
    // Distinct count
    // Not all GROUP columns are projected
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select c.\"state_province\",\n"
            + "  sum(s.\"unit_sales\") as s,\n"
            + "  count(distinct c.\"customer_id\") as dc\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "  join \"foodmart\".\"customer\" as c\n"
            + "  on s.\"customer_id\" = c.\"customer_id\"\n"
            + "group by c.\"state_province\", c.\"country\"\n"
            + "order by c.\"state_province\", 2")
        .planHasSql("SELECT `state_province`, `S`, `DC`\n"
            + "FROM (SELECT `customer`.`state_province`, `customer`.`country`, SUM(`sales_fact_1997`.`unit_sales`) AS `S`, COUNT(DISTINCT `customer`.`customer_id`) AS `DC`\n"
            + "FROM `foodmart`.`sales_fact_1997`\n"
            + "INNER JOIN `foodmart`.`customer` ON `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "GROUP BY `customer`.`state_province`, `customer`.`country`) AS `t0`\n"
            + "ORDER BY `state_province`, `S`")
        .returns("state_province=CA; S=74748.0000; DC=2716\n"
            + "state_province=OR; S=67659.0000; DC=1037\n"
            + "state_province=WA; S=124366.0000; DC=1828\n");
  }

  @Ignore
  @Test public void testPlan() {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select c.\"state_province\"\n"
            + "from \"foodmart\".\"customer\" as c\n"
            + "where c.\"state_province\" = 'USA'")
        .planContains("            public boolean moveNext() {\n"
            + "              while (inputEnumerator.moveNext()) {\n"
            + "                final String v = (String) ((Object[]) inputEnumerator.current())[10];\n"
            + "                if (v != null && org.apache.calcite.runtime.SqlFunctions.eq(v, \"USA\")) {\n"
            + "                  return true;\n"
            + "                }\n"
            + "              }\n"
            + "              return false;\n"
            + "            }\n");
  }

  @Ignore
  @Test public void testPlan2() {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .withDefaultSchema("foodmart")
        .query("select \"customer\".\"state_province\" as \"c0\",\n"
            + " \"customer\".\"country\" as \"c1\"\n"
            + "from \"customer\" as \"customer\"\n"
            + "where (\"customer\".\"country\" = 'USA')\n"
            + "and UPPER(\"customer\".\"state_province\") = UPPER('CA')\n"
            + "group by \"customer\".\"state_province\", \"customer\".\"country\"\n"
            + "order by \"customer\".\"state_province\" ASC")
        .planContains("          public boolean moveNext() {\n"
            + "            while (inputEnumerator.moveNext()) {\n"
            + "              final Object[] current12 = (Object[]) inputEnumerator.current();\n"
            + "              final String v1 = (String) current12[10];\n"
            + "              if (org.apache.calcite.runtime.SqlFunctions.eq((String) current12[12], \"USA\") && (v1 != null && org.apache.calcite.runtime.SqlFunctions.eq(org.apache.calcite.runtime.SqlFunctions.upper(v1), org.apache.calcite.runtime.SqlFunctions.trim(org.apache.calcite.runtime.SqlFunctions.upper(\"CA\"))))) {\n"
            + "                return true;\n"
            + "              }\n"
            + "            }\n"
            + "            return false;\n"
            + "          }\n");
  }

  @Test public void testPlan3() {
    // Plan should contain 'join'. If it doesn't, maybe int-vs-Integer
    // data type incompatibility has caused it to use a cartesian product
    // instead, and that would be wrong.
    //
    // inventory_fact_1997 is on the LHS because it is larger than store.
    that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(
            "select \"store\".\"store_country\" as \"c0\", sum(\"inventory_fact_1997\".\"supply_time\") as \"m0\" from \"store\" as \"store\", \"inventory_fact_1997\" as \"inventory_fact_1997\" where \"inventory_fact_1997\".\"store_id\" = \"store\".\"store_id\" group by \"store\".\"store_country\"")
        .planContains(
            " left.hashJoin(right, new org.apache.calcite.linq4j.function.Function1() {\n");
  }
}

// End JdbcFrontJdbcBackLinqMiddleTest.java
