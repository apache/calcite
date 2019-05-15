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
import org.apache.calcite.test.CalciteAssert;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Tests using {@code Bookshop} schema.
 */
public class GeodeBookstoreTest extends AbstractGeodeTest {

  @BeforeClass
  public static void setUp() throws Exception {
    Cache cache = POLICY.cache();
    Region<?, ?> bookMaster =  cache.<String, Object>createRegionFactory().create("BookMaster");
    new JsonLoader(bookMaster).loadClasspathResource("/book_master.json");

    Region<?, ?> bookCustomer =  cache.<String, Object>createRegionFactory().create("BookCustomer");
    new JsonLoader(bookCustomer).loadClasspathResource("/book_customer.json");

  }

  private CalciteAssert.ConnectionFactory newConnectionFactory() {
    return new CalciteAssert.ConnectionFactory() {
      @Override public Connection createConnection() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
        final SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
        root.add("geode",
            new GeodeSchema(POLICY.cache(), Arrays.asList("BookMaster", "BookCustomer")));
        return connection;
      }
    };
  }

  private CalciteAssert.AssertThat calciteAssert() {
    return CalciteAssert.that()
        .with(newConnectionFactory());
  }

  @Test
  public void testSelect() {
    calciteAssert()
        .query("select * from geode.BookMaster")
        .returnsCount(3);
  }

  @Test
  public void testWhereEqual() {
    String expectedQuery = "SELECT * FROM /BookMaster WHERE itemNumber = 123";

    calciteAssert()
        .query("select * from geode.BookMaster WHERE itemNumber = 123")
        .returnsCount(1)
        .returns("itemNumber=123; description=Run on sentences and drivel on all things mundane;"
            + " retailCost=34.99; yearPublished=2011; author=Daisy Mae West; title=A Treatise of "
            + "Treatises\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeFilter(condition=[=(CAST($0):INTEGER, 123)])\n"
            + "    GeodeTableScan(table=[[geode, BookMaster]])")
        .queryContains(GeodeAssertions.query(expectedQuery));
  }

  @Test
  public void testWhereWithAnd() {
    calciteAssert()
        .query("select * from geode.BookMaster WHERE itemNumber > 122 "
            + "AND itemNumber <= 123")
        .returnsCount(1)
        .returns("itemNumber=123; description=Run on sentences and drivel on all things mundane; "
            + "retailCost=34.99; yearPublished=2011; author=Daisy Mae West; title=A Treatise of "
            + "Treatises\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeFilter(condition=[AND(>($0, 122), <=($0, 123))])\n"
            + "    GeodeTableScan(table=[[geode, BookMaster]])")
        .queryContains(
            GeodeAssertions.query("SELECT * FROM /BookMaster "
                + "WHERE itemNumber > 122 AND itemNumber <= 123"));
  }

  @Test
  public void testWhereWithOr() {
    String expectedQuery = "SELECT author AS author FROM /BookMaster "
        + "WHERE itemNumber IN SET(123, 789)";

    calciteAssert()
        .query("select author from geode.BookMaster "
            + "WHERE itemNumber = 123 OR itemNumber = 789")
        .returnsCount(2)
        .returnsUnordered("author=Jim Heavisides", "author=Daisy Mae West")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(author=[$4])\n"
            + "    GeodeFilter(condition=[OR(=(CAST($0):INTEGER, 123), "
            + "=(CAST($0):INTEGER, 789))])\n"
            + "      GeodeTableScan(table=[[geode, BookMaster]])\n")
        .queryContains(
            GeodeAssertions.query(expectedQuery));
  }

  @Test
  public void testWhereWithAndOr() {
    calciteAssert()
        .query("SELECT author from geode.BookMaster "
            + "WHERE (itemNumber > 123 AND itemNumber = 789) "
            + "OR author='Daisy Mae West'")
        .returnsCount(2)
        .returnsUnordered("author=Jim Heavisides", "author=Daisy Mae West")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(author=[$4])\n"
            + "    GeodeFilter(condition=[OR(AND(>($0, 123), =(CAST($0):INTEGER, 789)), "
            + "=(CAST($4):VARCHAR, 'Daisy Mae West'))])\n"
            + "      GeodeTableScan(table=[[geode, BookMaster]])\n"
            + "\n")
        .queryContains(
            GeodeAssertions.query("SELECT author AS author FROM /BookMaster "
                + "WHERE (itemNumber > 123 AND itemNumber = 789) OR author = 'Daisy Mae West'"));
  }

  // TODO: Not supported YET
  @Test
  public void testWhereWithOrAnd() {
    calciteAssert()
        .query("SELECT author from geode.BookMaster "
            + "WHERE (itemNumber > 100 OR itemNumber = 789) "
            + "AND author='Daisy Mae West'")
        .returnsCount(1)
        .returnsUnordered("author=Daisy Mae West")
        .explainContains("");
  }

  @Test
  public void testProjectionsAndWhereGreatThan() {
    calciteAssert()
        .query("select author from geode.BookMaster WHERE itemNumber > 123")
        .returnsCount(2)
        .returns("author=Clarence Meeks\n"
            + "author=Jim Heavisides\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(author=[$4])\n"
            + "    GeodeFilter(condition=[>($0, 123)])\n"
            + "      GeodeTableScan(table=[[geode, BookMaster]])")
        .queryContains(
            GeodeAssertions.query("SELECT author AS author "
                + "FROM /BookMaster WHERE itemNumber > 123"));
  }

  @Test
  public void testLimit() {
    calciteAssert()
        .query("select * from geode.BookMaster LIMIT 1")
        .returnsCount(1)
        .returns("itemNumber=123; description=Run on sentences and drivel on all things mundane; "
            + "retailCost=34.99; yearPublished=2011; author=Daisy Mae West; title=A Treatise of "
            + "Treatises\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeSort(fetch=[1])\n"
            + "    GeodeTableScan(table=[[geode, BookMaster]])");
  }

  @Test
  public void testSortWithProjection() {
    calciteAssert()
        .query("select yearPublished from geode.BookMaster ORDER BY yearPublished ASC")
        .returnsCount(3)
        .returns("yearPublished=1971\n"
            + "yearPublished=2011\n"
            + "yearPublished=2011\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeSort(sort0=[$0], dir0=[ASC])\n"
            + "    GeodeProject(yearPublished=[$3])\n"
            + "      GeodeTableScan(table=[[geode, BookMaster]])\n");
  }

  @Test
  public void testSortWithProjectionAndLimit() {
    calciteAssert()
        .query("select yearPublished from geode.BookMaster ORDER BY yearPublished "
            + "LIMIT 2")
        .returnsCount(2)
        .returns("yearPublished=1971\n"
            + "yearPublished=2011\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(yearPublished=[$3])\n"
            + "    GeodeSort(sort0=[$3], dir0=[ASC], fetch=[2])\n"
            + "      GeodeTableScan(table=[[geode, BookMaster]])\n");
  }

  @Test
  public void testSortBy2Columns() {
    calciteAssert()
        .query("select yearPublished, itemNumber from geode.BookMaster ORDER BY "
            + "yearPublished ASC, itemNumber DESC")
        .returnsCount(3)
        .returns("yearPublished=1971; itemNumber=456\n"
            + "yearPublished=2011; itemNumber=789\n"
            + "yearPublished=2011; itemNumber=123\n")
        .queryContains(
            GeodeAssertions.query("SELECT yearPublished AS yearPublished, "
              + "itemNumber AS itemNumber "
              + "FROM /BookMaster ORDER BY yearPublished ASC, itemNumber DESC"));
  }

  //
  // geode Group By and Aggregation Function Support
  //

  /**
   * OQL Error: Query contains group by columns not present in projected fields
   * Solution: Automatically expand the projections to include all missing GROUP By columns.
   */
  @Test
  public void testAddMissingGroupByColumnToProjectedFields() {
    calciteAssert()
        .query("select yearPublished from geode.BookMaster GROUP BY  yearPublished, "
            + "author")
        .returnsCount(3)
        .returns("yearPublished=1971\n"
            + "yearPublished=2011\n"
            + "yearPublished=2011\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(yearPublished=[$0])\n"
            + "    GeodeAggregate(group=[{3, 4}])\n"
            + "      GeodeTableScan(table=[[geode, BookMaster]])");
  }

  /**
   * When the group by columns match the projected fields, the optimizers removes the projected
   * relation.
   */
  @Test
  public void testMissingProjectRelationOnGroupByColumnMatchingProjectedFields() {
    calciteAssert()
        .query("select yearPublished from geode.BookMaster GROUP BY yearPublished")
        .returnsCount(2)
        .returns("yearPublished=1971\n"
            + "yearPublished=2011\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{3}])\n"
            + "    GeodeTableScan(table=[[geode, BookMaster]])");
  }

  /**
   * When the group by columns match the projected fields, the optimizers removes the projected
   * relation.
   */
  @Test
  public void testMissingProjectRelationOnGroupByColumnMatchingProjectedFields2() {
    calciteAssert()
        .query("select yearPublished, MAX(retailCost) from geode.BookMaster GROUP BY "
            + "yearPublished")
        .returnsCount(2)
        .returns("yearPublished=1971; EXPR$1=11.99\n"
            + "yearPublished=2011; EXPR$1=59.99\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{3}], EXPR$1=[MAX($2)])\n"
            + "    GeodeTableScan(table=[[geode, BookMaster]])");
  }

  @Test
  public void testCount() {
    calciteAssert()
        .query("select COUNT(retailCost) from geode.BookMaster")
        .returnsCount(1)
        .returns("EXPR$0=3\n")
        .returnsValue("3")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{}], EXPR$0=[COUNT($2)])\n"
            + "    GeodeTableScan(table=[[geode, BookMaster]])\n");
  }

  @Test
  public void testCountStar() {
    calciteAssert()
        .query("select COUNT(*) from geode.BookMaster")
        .returnsCount(1)
        .returns("EXPR$0=3\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{}], EXPR$0=[COUNT()])\n"
            + "    GeodeTableScan(table=[[geode, BookMaster]])\n");
  }

  @Test
  public void testCountInGroupBy() {
    calciteAssert()
        .query("select yearPublished, COUNT(retailCost) from geode.BookMaster GROUP BY "
            + "yearPublished")
        .returnsCount(2)
        .returns("yearPublished=1971; EXPR$1=1\n"
            + "yearPublished=2011; EXPR$1=2\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{3}], EXPR$1=[COUNT($2)])\n"
            + "    GeodeTableScan(table=[[geode, BookMaster]])\n");
  }

  @Test
  public void testMaxMinSumAvg() {
    calciteAssert()
        .query("select MAX(retailCost), MIN(retailCost), SUM(retailCost), AVG"
            + "(retailCost) from geode.BookMaster")
        .returnsCount(1)
        .returns("EXPR$0=59.99; EXPR$1=11.99; EXPR$2=106.97000122070312; "
            + "EXPR$3=35.65666580200195\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{}], EXPR$0=[MAX($2)], EXPR$1=[MIN($2)], EXPR$2=[SUM($2)"
            + "], EXPR$3=[AVG($2)])\n"
            + "    GeodeTableScan(table=[[geode, BookMaster]])\n");
  }

  @Test
  public void testMaxMinSumAvgInGroupBy() {
    calciteAssert()
        .query("select yearPublished, MAX(retailCost), MIN(retailCost), SUM"
            + "(retailCost), AVG(retailCost) from geode.BookMaster "
            + "GROUP BY  yearPublished")
        .returnsCount(2)
        .returns("yearPublished=2011; EXPR$1=59.99; EXPR$2=34.99; EXPR$3=94.9800033569336; "
            + "EXPR$4=47.4900016784668\n"
            + "yearPublished=1971; EXPR$1=11.99; EXPR$2=11.99; EXPR$3=11.989999771118164; "
            + "EXPR$4=11.989999771118164\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{3}], EXPR$1=[MAX($2)], EXPR$2=[MIN($2)], EXPR$3=[SUM($2)"
            + "], EXPR$4=[AVG($2)])\n"
            + "    GeodeTableScan(table=[[geode, BookMaster]])\n");
  }

  @Test
  public void testGroupBy() {
    calciteAssert()
        .query("select yearPublished, MAX(retailCost) AS MAXCOST, author from "
            + "geode.BookMaster GROUP BY yearPublished, author")
        .returnsCount(3)
        .returnsUnordered("yearPublished=2011; MAXCOST=59.99; author=Jim Heavisides",
            "yearPublished=1971; MAXCOST=11.99; author=Clarence Meeks",
            "yearPublished=2011; MAXCOST=34.99; author=Daisy Mae West")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(yearPublished=[$0], MAXCOST=[$2], author=[$1])\n"
            + "    GeodeAggregate(group=[{3, 4}], MAXCOST=[MAX($2)])\n"
            + "      GeodeTableScan(table=[[geode, BookMaster]])\n");
  }

  @Test
  public void testSelectWithNestedPdx() {
    calciteAssert()
        .query("select * from geode.BookCustomer limit 2")
        .returnsCount(2)
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeSort(fetch=[2])\n"
            + "    GeodeTableScan(table=[[geode, BookCustomer]])\n");
  }

  @Test
  public void testSelectWithNestedPdx2() {
    calciteAssert()
        .query("select primaryAddress from geode.BookCustomer limit 2")
        .returnsCount(2)
        .returns("primaryAddress=PDX[addressLine1,addressLine2,addressLine3,city,state,"
            + "postalCode,country,phoneNumber,addressTag]\n"
            + "primaryAddress=PDX[addressLine1,addressLine2,addressLine3,city,state,postalCode,"
            + "country,phoneNumber,addressTag]\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(primaryAddress=[$3])\n"
            + "    GeodeSort(fetch=[2])\n"
            + "      GeodeTableScan(table=[[geode, BookCustomer]])\n");
  }

  @Test
  public void testSelectWithNestedPdxFieldAccess() {
    calciteAssert()
        .query("select primaryAddress['city'] as city from geode.BookCustomer limit 2")
        .returnsCount(2)
        .returns("city=Topeka\n"
            + "city=San Francisco\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(city=[ITEM($3, 'city')])\n"
            + "    GeodeSort(fetch=[2])\n"
            + "      GeodeTableScan(table=[[geode, BookCustomer]])\n");
  }

  @Test
  public void testSelectWithNullFieldValue() {
    calciteAssert()
        .query("select primaryAddress['addressLine2'] from geode.BookCustomer limit"
            + " 2")
        .returnsCount(2)
        .returns("EXPR$0=null\n"
            + "EXPR$0=null\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(EXPR$0=[ITEM($3, 'addressLine2')])\n"
            + "    GeodeSort(fetch=[2])\n"
            + "      GeodeTableScan(table=[[geode, BookCustomer]])\n");
  }

  @Test
  public void testFilterWithNestedField() {
    calciteAssert()
        .query("SELECT primaryAddress['postalCode'] AS postalCode\n"
            + "FROM geode.BookCustomer\n"
            + "WHERE primaryAddress['postalCode'] > '0'\n")
        .returnsCount(3)
        .returns("postalCode=50505\n"
            + "postalCode=50505\n"
            + "postalCode=50505\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(postalCode=[ITEM($3, 'postalCode')])\n"
            + "    GeodeFilter(condition=[>(ITEM($3, 'postalCode'), '0')])\n"
            + "      GeodeTableScan(table=[[geode, BookCustomer]])\n")
        .queryContains(
            GeodeAssertions.query("SELECT primaryAddress.postalCode AS postalCode "
                + "FROM /BookCustomer WHERE primaryAddress.postalCode > '0'"));
  }

  @Test
  public void testSqlSimple() {
    calciteAssert()
        .query("SELECT itemNumber FROM geode.BookMaster WHERE itemNumber > 123")
        .runs()
        .queryContains(
            GeodeAssertions.query("SELECT itemNumber AS itemNumber "
                + "FROM /BookMaster WHERE itemNumber > 123"));
  }

  @Test
  public void testSqlSingleNumberWhereFilter() {
    calciteAssert().query("SELECT * FROM geode.BookMaster "
        + "WHERE itemNumber = 123")
        .runs()
        .queryContains(
            GeodeAssertions.query("SELECT * FROM /BookMaster "
                + "WHERE itemNumber = 123"));
  }

  @Test
  public void testSqlDistinctSort() {
    calciteAssert().query("SELECT DISTINCT itemNumber, author "
        + "FROM geode.BookMaster ORDER BY itemNumber, author").runs();
  }

  @Test
  public void testSqlDistinctSort2() {
    calciteAssert().query("SELECT itemNumber, author "
        + "FROM geode.BookMaster GROUP BY itemNumber, author ORDER BY itemNumber, "
        + "author").runs();
  }

  @Test
  public void testSqlDistinctSort3() {
    calciteAssert().query("SELECT DISTINCT * FROM geode.BookMaster").runs();
  }


  @Test
  public void testSqlLimit2() {
    calciteAssert().query("SELECT DISTINCT * FROM geode.BookMaster LIMIT 2").runs();
  }


  @Test
  public void testSqlDisjunction() {
    String expectedQuery = "SELECT author AS author FROM /BookMaster "
        + "WHERE itemNumber IN SET(789, 123)";

    calciteAssert().query("SELECT author FROM geode.BookMaster "
        + "WHERE itemNumber = 789 OR itemNumber = 123").runs()
        .queryContains(
            GeodeAssertions.query(expectedQuery));
  }

  @Test
  public void testSqlConjunction() {
    calciteAssert().query("SELECT author FROM geode.BookMaster "
        + "WHERE itemNumber = 789 AND author = 'Jim Heavisides'")
        .runs()
        .queryContains(
            GeodeAssertions.query("SELECT author AS author FROM /BookMaster "
                + "WHERE itemNumber = 789 AND author = 'Jim Heavisides'"));
  }

  @Test
  public void testSqlBookMasterWhere() {
    calciteAssert().query("select author, title from geode.BookMaster "
        + "WHERE author = 'Jim Heavisides' LIMIT 2")
        .runs()
        .queryContains(
            GeodeAssertions.query("SELECT author AS author, title AS title FROM /BookMaster "
                + "WHERE author = 'Jim Heavisides' LIMIT 2"));
  }

  @Test
  public void testSqlBookMasterCount() {
    calciteAssert().query("select count(*) from geode.BookMaster").runs();
  }

  @Test
  public void testInSetFilterWithNestedStringField() {
    String expectedQuery = "SELECT primaryAddress.city AS city FROM /BookCustomer "
        + "WHERE primaryAddress.city IN SET('Topeka', 'San Francisco')";

    calciteAssert()
        .query("SELECT primaryAddress['city'] AS city\n"
            + "FROM geode.BookCustomer\n"
            + "WHERE primaryAddress['city'] = 'Topeka' OR primaryAddress['city'] = 'San Francisco'\n")
        .returnsCount(3)
        .queryContains(
            GeodeAssertions.query(expectedQuery));
  }
}

// End GeodeBookstoreTest.java
