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
package org.apache.calcite.adapter.geode.rel;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.test.CalciteAssert;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Tests based on {@code zips-min.json} dataset. Runs automatically as part of CI.
 */
public class GeodeZipsTest extends AbstractGeodeTest {

  @BeforeClass
  public static void setUp() throws Exception {
    Cache cache = POLICY.cache();
    Region<?, ?> region =  cache.<String, Object>createRegionFactory().create("zips");
    new JsonLoader(region).loadClasspathResource("/zips-mini.json");
    createTestRegion();
  }

  private static void createTestRegion() throws Exception {
    Cache cache = POLICY.cache();
    Region<?, ?> region =  cache.<String, Object>createRegionFactory().create("TestRegion");

    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("{\"foo\": \"abc\", \"bar\": [123, 1.5899], \"baz\": true}");
    stringBuilder.append("\n");
    stringBuilder.append("{\"foo\": \"def\", \"bar\": [456, 9.675344], \"baz\": false}");
    stringBuilder.append("\n");
    stringBuilder.append("{\"foo\": \"ghi\", \"bar\": [789, 89.7899], \"baz\": true}");
    stringBuilder.append("\n");
    stringBuilder.append("{\"foo\": \"jkl\", \"bar\": [572, 8.9234], \"baz\": null}");
    stringBuilder.append("\n");

    final Reader reader = new StringReader(stringBuilder.toString());
    new JsonLoader(region).load(reader);
  }

  private CalciteAssert.ConnectionFactory newConnectionFactory() {
    return new CalciteAssert.ConnectionFactory() {
      @Override public Connection createConnection() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
        final SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();

        root.add("geode", new GeodeSchema(POLICY.cache(), Arrays.asList("zips", "TestRegion")));

        // add calcite view programmatically
        final String viewSql =  "select \"_id\" AS \"id\", \"city\", \"loc\", "
            + "cast(\"pop\" AS integer) AS \"pop\", cast(\"state\" AS varchar(2)) AS \"state\" "
            + "from \"geode\".\"zips\"";


        ViewTableMacro macro = ViewTable.viewMacro(root, viewSql,
            Collections.singletonList("geode"), Arrays.asList("geode", "view"), false);
        root.add("view", macro);

        return connection;
      }
    };
  }

  private CalciteAssert.AssertThat calciteAssert() {
    return CalciteAssert.that()
        .with(newConnectionFactory());
  }

  @Test
  public void testGroupByView() {
    calciteAssert()
        .query("SELECT state, SUM(pop) FROM view GROUP BY state")
        .returnsCount(51)
        .queryContains(
            GeodeAssertions.query("SELECT state AS state, "
                + "SUM(pop) AS EXPR$1 FROM /zips GROUP BY state"));
  }

  @Test
  @Ignore("Currently fails")
  public void testGroupByViewWithAliases() {
    calciteAssert()
        .query("SELECT state as st, SUM(pop) po "
            + "FROM view GROUP BY state")
        .queryContains(
            GeodeAssertions.query("SELECT state, SUM(pop) AS po FROM /zips GROUP BY state"))
        .returnsCount(51)
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{1}], po=[SUM($0)])\n"
            + "    GeodeProject(pop=[CAST($3):INTEGER], state=[CAST($4):VARCHAR(2) CHARACTER SET"
            + " \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "      GeodeTableScan(table=[[geode, zips]])\n");
  }

  @Test
  public void testGroupByRaw() {
    calciteAssert()
        .query("SELECT state as st, SUM(pop) po "
            + "FROM geode.zips GROUP BY state")
        .returnsCount(51)
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{4}], po=[SUM($3)])\n"
            + "    GeodeTableScan(table=[[geode, zips]])\n");
  }

  @Test
  public void testGroupByRawWithAliases() {
    calciteAssert()
        .query("SELECT state AS st, SUM(pop) AS po "
            + "FROM geode.zips GROUP BY state")
        .returnsCount(51)
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{4}], po=[SUM($3)])\n"
            + "    GeodeTableScan(table=[[geode, zips]])\n");
  }

  @Test
  public void testMaxRaw() {
    calciteAssert()
        .query("SELECT MAX(pop) FROM view")
        .returns("EXPR$0=112047\n")
        .queryContains(GeodeAssertions.query("SELECT MAX(pop) AS EXPR$0 FROM /zips"));
  }

  @Test
  @Ignore("Currently fails")
  public void testJoin() {
    calciteAssert()
        .query("SELECT r._id FROM geode.zips AS v "
            + "JOIN geode.zips AS r ON v._id = r._id LIMIT 1")
        .returnsCount(1)
        .explainContains("PLAN=EnumerableCalc(expr#0..2=[{inputs}], _id1=[$t0])\n"
            + "  EnumerableLimit(fetch=[1])\n"
            + "    EnumerableJoin(condition=[=($1, $2)], joinType=[inner])\n"
            + "      GeodeToEnumerableConverter\n"
            + "        GeodeProject(_id=[$0], _id0=[CAST($0):VARCHAR CHARACTER SET "
            + "\"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "          GeodeTableScan(table=[[geode, zips]])\n"
            + "      GeodeToEnumerableConverter\n"
            + "        GeodeProject(_id0=[CAST($0):VARCHAR CHARACTER SET \"ISO-8859-1\" COLLATE "
            + "\"ISO-8859-1$en_US$primary\"])\n"
            + "          GeodeTableScan(table=[[geode, zips]])\n");
  }

  @Test
  public void testSelectLocItem() {
    calciteAssert()
        .query("SELECT loc[0] as lat, loc[1] as lon "
            + "FROM view LIMIT 1")
        .returns("lat=-105.007985; lon=39.840562\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(lat=[ITEM($2, 0)], lon=[ITEM($2, 1)])\n"
            + "    GeodeSort(fetch=[1])\n"
            + "      GeodeTableScan(table=[[geode, zips]])\n");
  }

  @Test
  public void testItemPredicate() {
    calciteAssert()
        .query("SELECT loc[0] as lat, loc[1] as lon "
            + "FROM view WHERE loc[0] < 0 LIMIT 1")
        .returnsCount(1)
        .returns("lat=-105.007985; lon=39.840562\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(lat=[ITEM($2, 0)], lon=[ITEM($2, 1)])\n"
            + "    GeodeSort(fetch=[1])\n"
            + "      GeodeFilter(condition=[<(ITEM($2, 0), 0)])\n"
            + "        GeodeTableScan(table=[[geode, zips]])\n");

    calciteAssert()
        .query("SELECT loc[0] as lat, loc[1] as lon "
            + "FROM view WHERE loc[0] > 0 LIMIT 1")
        .returnsCount(0)
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(lat=[ITEM($2, 0)], lon=[ITEM($2, 1)])\n"
            + "    GeodeSort(fetch=[1])\n"
            + "      GeodeFilter(condition=[>(ITEM($2, 0), 0)])\n"
            + "        GeodeTableScan(table=[[geode, zips]])\n");
  }

  @Test
  public void testWhereWithOrForStringField() {
    String expectedQuery = "SELECT state AS state FROM /zips "
        + "WHERE state IN SET('RI', 'MA')";
    calciteAssert()
        .query("SELECT state as state "
            + "FROM view WHERE state = 'MA' OR state = 'RI'")
        .returnsCount(6)
        .queryContains(
            GeodeAssertions.query(expectedQuery));
  }

  @Test
  public void testWhereWithOrForNumericField() {
    calciteAssert()
        .query("SELECT pop as pop "
            + "FROM view WHERE pop = 34035 OR pop = 40173")
        .returnsCount(2)
        .queryContains(
            GeodeAssertions.query("SELECT pop AS pop FROM /zips WHERE pop IN SET(40173, 34035)"));
  }

  @Test
  public void testWhereWithOrForNestedNumericField() {
    String expectedQuery = "SELECT loc[1] AS lan FROM /zips "
        + "WHERE loc[1] IN SET(44.098538, 43.218525)";

    calciteAssert()
        .query("SELECT loc[1] as lan "
            + "FROM view WHERE loc[1] = 43.218525 OR loc[1] = 44.098538")
        .returnsCount(2)
        .queryContains(
            GeodeAssertions.query(expectedQuery));
  }

  @Test
  public void testWhereWithOrForLargeValueList() {
    String stateListPredicate = "state = 'IL' OR state = 'UT' OR state = 'NJ' OR state = 'AL' "
        + "OR state = 'TN' OR state = 'OH' OR state = 'MD' OR state = 'CT' OR state = 'PA' "
        + "OR state = 'SC' OR state = 'VA' OR state = 'ID' OR state = 'NV' OR state = 'MT' "
        + "OR state = 'WY' OR state = 'MI' OR state = 'ME' OR state = 'KY' OR state = 'NC' "
        + "OR state = 'MA' OR state = 'SD' OR state = 'IA' OR state = 'AZ' OR state = 'GA' "
        + "OR state = 'CA' OR state = 'DC' OR state = 'MN' OR state = 'IN' OR state = 'WV' "
        + "OR state = 'FL' OR state = 'VT' OR state = 'NH' OR state = 'ND' OR state = 'AR' "
        + "OR state = 'WI' OR state = 'WA' OR state = 'TX' OR state = 'OR' OR state = 'MO' "
        + "OR state = 'NM' OR state = 'KS' OR state = 'OK' OR state = 'AK' OR state = 'CO' "
        + "OR state = 'RI' OR state = 'NE' OR state = 'LA' OR state = 'NY' OR state = 'MS' "
        + "OR state = 'DE' OR state = 'HI'";

    String stateListStr = "'WA', 'FL', 'IN', 'MN', 'TX', 'OR', 'CA', 'DC', 'GA', 'AZ', "
        + "'MI', 'NC', 'ME', 'IA', 'AR', 'WI', 'MA', 'AK', 'CO', 'NY', 'MS', 'KS', 'MO', "
        + "'DE', 'HI', 'VT', 'WV', 'NH', 'ND', 'SC', 'VA', 'AL', 'NV', 'MT', 'IL', 'NM', 'OK', "
        + "'LA', 'RI', 'NE', 'CT', 'PA', 'KY', 'WY', 'UT', 'TN', 'OH', 'NJ', 'ID', 'MD', 'SD'";

    String queryToBeExecuted = "SELECT state as state FROM view WHERE " + stateListPredicate;

    String expectedQuery = "SELECT state AS state FROM /zips WHERE state "
        + "IN SET(" + stateListStr + ")";

    calciteAssert()
        .query(queryToBeExecuted)
        .returnsCount(149)
        .queryContains(
            GeodeAssertions.query(expectedQuery));
  }

  @Test
  public void testWhereWithOrForBooleanField() {
    String expectedQuery = "SELECT foo AS foo FROM /TestRegion "
        + "WHERE baz IN SET(true, false)";

    calciteAssert()
        .query("SELECT foo as foo "
            + "FROM geode.TestRegion WHERE baz = true OR baz = false")
        .returnsCount(3)
        .queryContains(
            GeodeAssertions.query(expectedQuery));
  }

  @Test
  public void testWhereWithMultipleOr() {
    String queryToBeExecuted = "SELECT foo as foo "
        + "FROM geode.TestRegion WHERE (foo = 'abc' OR foo = 'def') OR "
        + "(bar[0] = 789 OR bar[0] = 572) OR (bar[1] = 1.5899 AND bar[1] = 0.5234) OR "
        + "(baz = false OR baz = null)";

    String expectedQuery = "SELECT foo AS foo FROM /TestRegion WHERE foo "
        + "IN SET('def', 'abc') OR bar[0] IN SET(572, 789) OR "
        + "(bar[1] = 1.5899 AND bar[1] = 0.5234) OR baz IN SET(null, false)";

    calciteAssert()
        .query(queryToBeExecuted)
        .returnsCount(4)
        .queryContains(
            GeodeAssertions.query(expectedQuery));
  }
}

// End GeodeZipsTest.java
