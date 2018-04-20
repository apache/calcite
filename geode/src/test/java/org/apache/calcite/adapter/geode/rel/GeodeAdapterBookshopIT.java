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

import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

/**
 * Tests for the {@code org.apache.calcite.adapter.geode} package.
 *
 * <p>Before calling this rel, you need to populate Geode, as follows:
 *
 * <blockquote><code>
 * git clone https://github.com/vlsi/calcite-test-dataset<br>
 * cd calcite-rel-dataset<br>
 * mvn install
 * </code></blockquote>
 *
 * <p>This will create a virtual machine with Geode and the "bookshop"
 * and "zips" rel dataset.
 */
public class GeodeAdapterBookshopIT {
  /**
   * Connection factory based on the "geode relational " model.
   */
  public static final ImmutableMap<String, String> GEODE =
      ImmutableMap.of("model",
          GeodeAdapterBookshopIT.class.getResource("/model-bookshop.json")
              .getPath());


  /**
   * Whether to run Geode tests. Enabled by default, however rel is only
   * included if "it" profile is activated ({@code -Pit}). To disable,
   * specify {@code -Dcalcite.rel.geode=false} on the Java command line.
   */
  public static final boolean ENABLED = Util.getBooleanProperty("calcite.rel.geode", true);

  /**
   * Whether to run this rel.
   */
  protected boolean enabled() {
    return ENABLED;
  }

  @Test
  public void testSelect() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select * from \"BookMaster\"")
        .returnsCount(3);
  }

  @Test
  public void testWhereEqual() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select * from \"BookMaster\" WHERE \"itemNumber\" = 123")
        .returnsCount(1)
        .returns("itemNumber=123; description=Run on sentences and drivel on all things mundane;"
            + " retailCost=34.99; yearPublished=2011; author=Daisy Mae West; title=A Treatise of "
            + "Treatises\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeFilter(condition=[=(CAST($0):INTEGER, 123)])\n"
            + "    GeodeTableScan(table=[[TEST, BookMaster]])");
  }

  @Test
  public void testWhereWithAnd() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select * from \"BookMaster\" WHERE \"itemNumber\" > 122 "
            + "AND \"itemNumber\" <= 123")
        .returnsCount(1)
        .returns("itemNumber=123; description=Run on sentences and drivel on all things mundane; "
            + "retailCost=34.99; yearPublished=2011; author=Daisy Mae West; title=A Treatise of "
            + "Treatises\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeFilter(condition=[AND(>($0, 122), <=($0, 123))])\n"
            + "    GeodeTableScan(table=[[TEST, BookMaster]])");
  }

  @Test
  public void testWhereWithOr() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"author\" from \"BookMaster\" "
            + "WHERE \"itemNumber\" = 123 OR \"itemNumber\" = 789")
        .returnsCount(2)
        .returnsUnordered("author=Jim Heavisides", "author=Daisy Mae West")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(author=[$4])\n"
            + "    GeodeFilter(condition=[OR(=(CAST($0):INTEGER, 123), "
            + "=(CAST($0):INTEGER, 789))])\n"
            + "      GeodeTableScan(table=[[TEST, BookMaster]])\n");
  }

  @Test
  public void testWhereWithAndOr() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("SELECT \"author\" from \"BookMaster\" "
            + "WHERE (\"itemNumber\" > 123 AND \"itemNumber\" = 789) "
            + "OR \"author\"='Daisy Mae West'")
        .returnsCount(2)
        .returnsUnordered("author=Jim Heavisides", "author=Daisy Mae West")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(author=[$4])\n"
            + "    GeodeFilter(condition=[OR(AND(>($0, 123), =(CAST($0):INTEGER, 789)), "
            + "=(CAST($4):VARCHAR CHARACTER SET \"ISO-8859-1\" "
            + "COLLATE \"ISO-8859-1$en_US$primary\", 'Daisy Mae West'))])\n"
            + "      GeodeTableScan(table=[[TEST, BookMaster]])\n"
            + "\n");
  }

  // TODO: Not supported YET
  @Test
  public void testWhereWithOrAnd() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("SELECT \"author\" from \"BookMaster\" "
            + "WHERE (\"itemNumber\" > 100 OR \"itemNumber\" = 789) "
            + "AND \"author\"='Daisy Mae West'")
        .returnsCount(1)
        .returnsUnordered("author=Daisy Mae West")
        .explainContains("");
  }

  @Test
  public void testProjectionsAndWhereGreatThan() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"author\" from \"BookMaster\" WHERE \"itemNumber\" > 123")
        .returnsCount(2)
        .returns("author=Clarence Meeks\n"
            + "author=Jim Heavisides\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(author=[$4])\n"
            + "    GeodeFilter(condition=[>($0, 123)])\n"
            + "      GeodeTableScan(table=[[TEST, BookMaster]])");
  }

  @Test
  public void testLimit() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select * from \"BookMaster\" LIMIT 1")
        .returnsCount(1)
        .returns("itemNumber=123; description=Run on sentences and drivel on all things mundane; "
            + "retailCost=34.99; yearPublished=2011; author=Daisy Mae West; title=A Treatise of "
            + "Treatises\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeSort(fetch=[1])\n"
            + "    GeodeTableScan(table=[[TEST, BookMaster]])");
  }

  @Test
  public void testSortWithProjection() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"yearPublished\" from \"BookMaster\" ORDER BY \"yearPublished\" ASC")
        .returnsCount(3)
        .returns("yearPublished=1971\n"
            + "yearPublished=2011\n"
            + "yearPublished=2011\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeSort(sort0=[$0], dir0=[ASC])\n"
            + "    GeodeProject(yearPublished=[$3])\n"
            + "      GeodeTableScan(table=[[TEST, BookMaster]])\n");
  }

  @Test
  public void testSortWithProjectionAndLimit() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"yearPublished\" from \"BookMaster\" ORDER BY \"yearPublished\" "
            + "LIMIT 2")
        .returnsCount(2)
        .returns("yearPublished=1971\n"
            + "yearPublished=2011\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(yearPublished=[$3])\n"
            + "    GeodeSort(sort0=[$3], dir0=[ASC], fetch=[2])\n"
            + "      GeodeTableScan(table=[[TEST, BookMaster]])\n");
  }

  @Test
  public void testSortBy2Columns() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"yearPublished\", \"itemNumber\" from \"BookMaster\" ORDER BY "
            + "\"yearPublished\" ASC, \"itemNumber\" DESC")
        .returnsCount(3)
        .returns("yearPublished=1971; itemNumber=456\n"
            + "yearPublished=2011; itemNumber=789\n"
            + "yearPublished=2011; itemNumber=123\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(yearPublished=[$3], itemNumber=[$0])\n"
            + "    GeodeSort(sort0=[$3], sort1=[$0], dir0=[ASC], dir1=[DESC])\n"
            + "      GeodeTableScan(table=[[TEST, BookMaster]])\n");
  }

  //
  // TEST Group By and Aggregation Function Support
  //

  /**
   * OQL Error: Query contains group by columns not present in projected fields
   * Solution: Automatically expand the projections to include all missing GROUP By columns.
   */
  @Test
  public void testAddMissingGroupByColumnToProjectedFields() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"yearPublished\" from \"BookMaster\" GROUP BY  \"yearPublished\", "
            + "\"author\"")
        .returnsCount(3)
        .returns("yearPublished=1971\n"
            + "yearPublished=2011\n"
            + "yearPublished=2011\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(yearPublished=[$0])\n"
            + "    GeodeAggregate(group=[{3, 4}])\n"
            + "      GeodeTableScan(table=[[TEST, BookMaster]])");
  }

  /**
   * When the group by columns match the projected fields, the optimizers removes the projected
   * relation.
   */
  @Test
  public void testMissingProjectRelationOnGroupByColumnMatchingProjectedFields() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"yearPublished\" from \"BookMaster\" GROUP BY \"yearPublished\"")
        .returnsCount(2)
        .returns("yearPublished=1971\n"
            + "yearPublished=2011\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{3}])\n"
            + "    GeodeTableScan(table=[[TEST, BookMaster]])");
  }

  /**
   * When the group by columns match the projected fields, the optimizers removes the projected
   * relation.
   */
  @Test
  public void testMissingProjectRelationOnGroupByColumnMatchingProjectedFields2() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"yearPublished\", MAX(\"retailCost\") from \"BookMaster\" GROUP BY "
            + "\"yearPublished\"")
        .returnsCount(2)
        .returns("yearPublished=1971; EXPR$1=11.99\n"
            + "yearPublished=2011; EXPR$1=59.99\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{3}], EXPR$1=[MAX($2)])\n"
            + "    GeodeTableScan(table=[[TEST, BookMaster]])");
  }

  @Test
  public void testCount() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select COUNT(\"retailCost\") from \"BookMaster\"")
        .returnsCount(1)
        .returns("EXPR$0=3\n")
        .returnsValue("3")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{}], EXPR$0=[COUNT($2)])\n"
            + "    GeodeTableScan(table=[[TEST, BookMaster]])\n");
  }

  @Test
  public void testCountStar() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select COUNT(*) from \"BookMaster\"")
        .returnsCount(1)
        .returns("EXPR$0=3\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{}], EXPR$0=[COUNT()])\n"
            + "    GeodeTableScan(table=[[TEST, BookMaster]])\n");
  }

  @Test
  public void testCountInGroupBy() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"yearPublished\", COUNT(\"retailCost\") from \"BookMaster\" GROUP BY "
            + "\"yearPublished\"")
        .returnsCount(2)
        .returns("yearPublished=1971; EXPR$1=1\n"
            + "yearPublished=2011; EXPR$1=2\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{3}], EXPR$1=[COUNT($2)])\n"
            + "    GeodeTableScan(table=[[TEST, BookMaster]])\n");
  }

  @Test
  public void testMaxMinSumAvg() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select MAX(\"retailCost\"), MIN(\"retailCost\"), SUM(\"retailCost\"), AVG"
            + "(\"retailCost\") from \"BookMaster\"")
        .returnsCount(1)
        .returns("EXPR$0=59.99; EXPR$1=11.99; EXPR$2=106.97000122070312; "
            + "EXPR$3=35.65666580200195\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{}], EXPR$0=[MAX($2)], EXPR$1=[MIN($2)], EXPR$2=[SUM($2)"
            + "], EXPR$3=[AVG($2)])\n"
            + "    GeodeTableScan(table=[[TEST, BookMaster]])\n");
  }

  @Test
  public void testMaxMinSumAvgInGroupBy() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"yearPublished\", MAX(\"retailCost\"), MIN(\"retailCost\"), SUM"
            + "(\"retailCost\"), AVG(\"retailCost\") from \"BookMaster\" "
            + "GROUP BY  \"yearPublished\"")
        .returnsCount(2)
        .returns("yearPublished=2011; EXPR$1=59.99; EXPR$2=34.99; EXPR$3=94.9800033569336; "
            + "EXPR$4=47.4900016784668\n"
            + "yearPublished=1971; EXPR$1=11.99; EXPR$2=11.99; EXPR$3=11.989999771118164; "
            + "EXPR$4=11.989999771118164\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{3}], EXPR$1=[MAX($2)], EXPR$2=[MIN($2)], EXPR$3=[SUM($2)"
            + "], EXPR$4=[AVG($2)])\n"
            + "    GeodeTableScan(table=[[TEST, BookMaster]])\n");
  }

  @Test
  public void testGroupBy() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"yearPublished\", MAX(\"retailCost\") AS MAXCOST, \"author\" from "
            + "\"BookMaster\" GROUP BY \"yearPublished\", \"author\"")
        .returnsCount(3)
        .returns("yearPublished=2011; MAXCOST=59.99; author=Jim Heavisides\n"
            + "yearPublished=1971; MAXCOST=11.99; author=Clarence Meeks\n"
            + "yearPublished=2011; MAXCOST=34.99; author=Daisy Mae West\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(yearPublished=[$0], MAXCOST=[$2], author=[$1])\n"
            + "    GeodeAggregate(group=[{3, 4}], MAXCOST=[MAX($2)])\n"
            + "      GeodeTableScan(table=[[TEST, BookMaster]])\n");
  }

  @Test
  public void testSelectWithNestedPdx() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select * from \"BookCustomer\" limit 2")
        .returnsCount(2)
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeSort(fetch=[2])\n"
            + "    GeodeTableScan(table=[[TEST, BookCustomer]])\n");
  }

  @Test
  public void testSelectWithNestedPdx2() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"primaryAddress\" from \"BookCustomer\" limit 2")
        .returnsCount(2)
        .returns("primaryAddress=PDX[addressLine1,addressLine2,addressLine3,city,state,"
            + "postalCode,country,phoneNumber,addressTag]\n"
            + "primaryAddress=PDX[addressLine1,addressLine2,addressLine3,city,state,postalCode,"
            + "country,phoneNumber,addressTag]\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(primaryAddress=[$3])\n"
            + "    GeodeSort(fetch=[2])\n"
            + "      GeodeTableScan(table=[[TEST, BookCustomer]])\n");
  }

  @Test
  public void testSelectWithNestedPdxFieldAccess() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"primaryAddress\"['city'] as \"city\" from \"BookCustomer\" limit 2")
        .returnsCount(2)
        .returns("city=Topeka\n"
            + "city=San Francisco\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(city=[ITEM($3, 'city')])\n"
            + "    GeodeSort(fetch=[2])\n"
            + "      GeodeTableScan(table=[[TEST, BookCustomer]])\n");
  }

  @Test
  public void testSelectWithNullFieldValue() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"primaryAddress\"['addressLine2'] from \"BookCustomer\" limit"
            + " 2")
        .returnsCount(2)
        .returns("EXPR$0=null\n"
            + "EXPR$0=null\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(EXPR$0=[ITEM($3, 'addressLine2')])\n"
            + "    GeodeSort(fetch=[2])\n"
            + "      GeodeTableScan(table=[[TEST, BookCustomer]])\n");
  }

  @Test
  public void testFilterWithNestedField() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("SELECT \"primaryAddress\"['postalCode'] AS \"postalCode\"\n"
            + "FROM \"TEST\".\"BookCustomer\"\n"
            + "WHERE \"primaryAddress\"['postalCode'] > '0'\n")
        .returnsCount(3)
        .returns("postalCode=50505\n"
            + "postalCode=50505\n"
            + "postalCode=50505\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(postalCode=[ITEM($3, 'postalCode')])\n"
            + "    GeodeFilter(condition=[>(ITEM($3, 'postalCode'), '0')])\n"
            + "      GeodeTableScan(table=[[TEST, BookCustomer]])\n");
  }

}

// End GeodeAdapterBookshopIT.java
