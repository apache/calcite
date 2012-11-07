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

import static net.hydromatic.optiq.test.OptiqAssert.assertThat;

/**
 * Tests for a JDBC front-end and JDBC back-end where the processing is not
 * pushed down to JDBC (as in {@link JdbcFrontJdbcBackTest}) but is executed
 * in a pipeline of linq4j operators.
 *
 * @author jhyde
 */
public class JdbcFrontJdbcBackLinqMiddleTest extends TestCase {

    public void testTable() {
        assertThat()
            .inJdbcFoodmart()
            .query("select * from \"foodmart\".\"days\"")
            .returns(
                "day=1; week_day=Sunday\n"
                + "day=2; week_day=Monday\n"
                + "day=5; week_day=Thursday\n"
                + "day=4; week_day=Wednesday\n"
                + "day=3; week_day=Tuesday\n"
                + "day=6; week_day=Friday\n"
                + "day=7; week_day=Saturday\n");
    }

    public void testWhere() {
        assertThat()
            .inJdbcFoodmart()
            .query("select * from \"foodmart\".\"days\" where \"day\" < 3")
            .returns(
                "day=1; week_day=Sunday\n"
                + "day=2; week_day=Monday\n");
    }

    public void testWhere2() {
        assertThat()
            .inJdbcFoodmart()
            .query(
                "select * from \"foodmart\".\"days\"\n"
                + "where not (lower(\"week_day\") = 'wednesday')")
            .returns(
                "day=1; week_day=Sunday\n"
                + "day=2; week_day=Monday\n"
                + "day=5; week_day=Thursday\n"
                + "day=3; week_day=Tuesday\n"
                + "day=6; week_day=Friday\n"
                + "day=7; week_day=Saturday\n");
    }

    public void testCase() {
        assertThat()
            .inJdbcFoodmart()
            .query(
                "select \"day\",\n"
                + " \"week_day\",\n"
                + " case when \"day\" < 3 then upper(\"week_day\")\n"
                + "      when \"day\" < 5 then lower(\"week_day\")\n"
                + "      else \"week_day\" end as d\n"
                + "from \"foodmart\".\"days\"\n"
                + "where \"day\" <> 1\n"
                + "order by \"day\"")
            .returns(
                "day=2; week_day=Monday; D=MONDAY\n"
                + "day=3; week_day=Tuesday; D=tuesday\n"
                + "day=4; week_day=Wednesday; D=wednesday\n"
                + "day=5; week_day=Thursday; D=Thursday\n"
                + "day=6; week_day=Friday; D=Friday\n"
                + "day=7; week_day=Saturday; D=Saturday\n");
    }

    public void testGroup() {
        assertThat()
            .inJdbcFoodmart()
            .query(
                "select s, count(*) as c, min(\"week_day\") as mw from (\n"
                + "select \"week_day\",\n"
                + "  substring(\"week_day\" from 1 for 1) as s\n"
                + "from \"foodmart\".\"days\")\n"
                + "group by s")
            .returns(
                "S=T; C=2; MW=Thursday\n"
                + "S=F; C=1; MW=Friday\n"
                + "S=W; C=1; MW=Wednesday\n"
                + "S=S; C=2; MW=Saturday\n"
                + "S=M; C=1; MW=Monday\n");
    }

    public void testGroupEmpty() {
        assertThat()
            .inJdbcFoodmart()
            .query(
                "select count(*) as c\n"
                + "from \"foodmart\".\"days\"")
            .returns("C=7\n");
    }

    public void testJoinGroupByEmpty() {
        assertThat()
            .inJdbcFoodmart()
            .query(
                "select count(*) from (\n"
                + "  select *\n"
                + "  from \"foodmart\".\"sales_fact_1997\" as s\n"
                + "  join \"foodmart\".\"customer\" as c\n"
                + "  on s.\"customer_id\" = c.\"customer_id\")")
            .returns("EXPR$0=86837\n");
    }

    public void testJoinGroupByOrderBy() {
        assertThat()
            .inJdbcFoodmart()
            .query(
                "select count(*), c.\"state_province\", sum(s.\"unit_sales\") as s\n"
                + "from \"foodmart\".\"sales_fact_1997\" as s\n"
                + "  join \"foodmart\".\"customer\" as c\n"
                + "  on s.\"customer_id\" = c.\"customer_id\"\n"
                + "group by c.\"state_province\"\n"
                + "order by c.\"state_province\"")
            .returns(
                "EXPR$0=24442; state_province=CA; S=74748\n"
                + "EXPR$0=21611; state_province=OR; S=67659\n"
                + "EXPR$0=40784; state_province=WA; S=124366\n");
    }

    public void testCompositeGroupBy() {
        assertThat()
            .inJdbcFoodmart()
            .query(
                "select count(*) as c, c.\"state_province\"\n"
                + "from \"foodmart\".\"customer\" as c\n"
                + "group by c.\"state_province\", c.\"country\"\n"
                + "order by c.\"state_province\", 1")
            .returns(
                "C=1717; state_province=BC\n"
                + "C=4222; state_province=CA\n"
                + "C=347; state_province=DF\n"
                + "C=106; state_province=Guerrero\n"
                + "C=104; state_province=Jalisco\n"
                + "C=97; state_province=Mexico\n"
                + "C=1051; state_province=OR\n"
                + "C=90; state_province=Oaxaca\n"
                + "C=78; state_province=Sinaloa\n"
                + "C=93; state_province=Veracruz\n"
                + "C=2086; state_province=WA\n"
                + "C=99; state_province=Yucatan\n"
                + "C=191; state_province=Zacatecas\n");
    }

    public void testDistinctCount() {
        // Complicating factors:
        // Composite GROUP BY key
        // Order by select item, referenced by ordinal
        // Distinct count
        // Not all GROUP columns are projected
        assertThat()
            .inJdbcFoodmart()
            .query(
                "select c.\"state_province\",\n"
                + "  sum(s.\"unit_sales\") as s,\n"
                + "  count(distinct c.\"customer_id\") as dc\n"
                + "from \"foodmart\".\"sales_fact_1997\" as s\n"
                + "  join \"foodmart\".\"customer\" as c\n"
                + "  on s.\"customer_id\" = c.\"customer_id\"\n"
                + "group by c.\"state_province\", c.\"country\"\n"
                + "order by c.\"state_province\", 2")
            .returns(
                "state_province=CA; S=74748; DC=24442\n"
                + "state_province=OR; S=67659; DC=21611\n"
                + "state_province=WA; S=124366; DC=40784\n");
    }
}

// End JdbcFrontJdbcBackLinqMiddleTest.java
