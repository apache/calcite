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
 * <p>This will create a virtual machine with Geode and the "bookshop" and "zips" rel dataset.
 */
public class GeodeZipsIT {
  /**
   * Connection factory based on the "geode relational " model.
   */
  public static final ImmutableMap<String, String> GEODE_ZIPS =
      ImmutableMap.of("CONFORMANCE", "LENIENT", "model",
          GeodeZipsIT.class.getResource("/model-zips.json")
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
  public void testGroupByView() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE_ZIPS)
        .query("SELECT \"state\", SUM(\"pop\") FROM \"geode\".\"ZIPS\" GROUP BY \"state\"")
        .returnsCount(51)
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{1}], EXPR$1=[SUM($0)])\n"
            + "    GeodeProject(pop=[CAST($3):INTEGER], state=[CAST($4):VARCHAR(2) CHARACTER SET"
            + " \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "      GeodeTableScan(table=[[geode_raw, Zips]])\n");
  }

  @Test
  public void testGroupByViewWithAliases() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE_ZIPS)
        .query("SELECT \"state\" as \"st\", SUM(\"pop\") \"po\" "
            + "FROM \"geode\".\"ZIPS\" GROUP BY "
            + "\"state\"")
        .returnsCount(51)
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{1}], po=[SUM($0)])\n"
            + "    GeodeProject(pop=[CAST($3):INTEGER], state=[CAST($4):VARCHAR(2) CHARACTER SET"
            + " \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "      GeodeTableScan(table=[[geode_raw, Zips]])\n");
  }

  @Test
  public void testGroupByRaw() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE_ZIPS)
        .query("SELECT \"state\" as \"st\", SUM(\"pop\") \"po\" "
            + "FROM \"geode_raw\".\"Zips\" GROUP"
            + " BY \"state\"")
        .returnsCount(51)
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{4}], po=[SUM($3)])\n"
            + "    GeodeTableScan(table=[[geode_raw, Zips]])\n");
  }

  @Test
  public void testGroupByRawWithAliases() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE_ZIPS)
        .query("SELECT \"state\" AS \"st\", SUM(\"pop\") AS \"po\" "
            + "FROM \"geode_raw\".\"Zips\" "
            + "GROUP BY \"state\"")
        .returnsCount(51)
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{4}], po=[SUM($3)])\n"
            + "    GeodeTableScan(table=[[geode_raw, Zips]])\n");
  }

  @Test
  public void testMaxRaw() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE_ZIPS)
        .query("SELECT MAX(\"pop\") FROM \"geode_raw\".\"Zips\"")
        .returnsCount(1)
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeAggregate(group=[{}], EXPR$0=[MAX($3)])\n"
            + "    GeodeTableScan(table=[[geode_raw, Zips]])\n");
  }

  @Test
  public void testJoin() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE_ZIPS)
        .query("SELECT \"r\".\"_id\" FROM \"geode_raw\".\"Zips\" AS \"v\" JOIN \"geode_raw\""
            + ".\"Zips\" AS \"r\" ON \"v\".\"_id\" = \"r\".\"_id\" LIMIT 1")
        .returnsCount(1)
        .explainContains("PLAN=EnumerableCalc(expr#0..2=[{inputs}], _id1=[$t0])\n"
            + "  EnumerableLimit(fetch=[1])\n"
            + "    EnumerableJoin(condition=[=($1, $2)], joinType=[inner])\n"
            + "      GeodeToEnumerableConverter\n"
            + "        GeodeProject(_id=[$0], _id0=[CAST($0):VARCHAR CHARACTER SET "
            + "\"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
            + "          GeodeTableScan(table=[[geode_raw, Zips]])\n"
            + "      GeodeToEnumerableConverter\n"
            + "        GeodeProject(_id0=[CAST($0):VARCHAR CHARACTER SET \"ISO-8859-1\" COLLATE "
            + "\"ISO-8859-1$en_US$primary\"])\n"
            + "          GeodeTableScan(table=[[geode_raw, Zips]])\n");
  }

  @Test
  public void testSelectLocItem() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE_ZIPS)
        .query("SELECT \"loc\"[0] as \"lat\", \"loc\"[1] as \"lon\" "
            + "FROM \"geode_raw\".\"Zips\" LIMIT 1")
        .returnsCount(1)
        .returns("lat=-74.700748; lon=41.65158\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(lat=[ITEM($2, 0)], lon=[ITEM($2, 1)])\n"
            + "    GeodeSort(fetch=[1])\n"
            + "      GeodeTableScan(table=[[geode_raw, Zips]])\n");
  }

  @Test
  public void testItemPredicate() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE_ZIPS)
        .query("SELECT \"loc\"[0] as \"lat\", \"loc\"[1] as \"lon\" "
            + "FROM \"geode_raw\".\"Zips\" WHERE \"loc\"[0] < 0 LIMIT 1")
        .returnsCount(1)
        .returns("lat=-74.700748; lon=41.65158\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(lat=[ITEM($2, 0)], lon=[ITEM($2, 1)])\n"
            + "    GeodeSort(fetch=[1])\n"
            + "      GeodeFilter(condition=[<(ITEM($2, 0), 0)])\n"
            + "        GeodeTableScan(table=[[geode_raw, Zips]])\n");

    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE_ZIPS)
        .query("SELECT \"loc\"[0] as \"lat\", \"loc\"[1] as \"lon\" "
            + "FROM \"geode_raw\".\"Zips\" WHERE \"loc\"[0] > 0 LIMIT 1")
        .returnsCount(0)
        .explainContains("PLAN=GeodeToEnumerableConverter\n"
            + "  GeodeProject(lat=[ITEM($2, 0)], lon=[ITEM($2, 1)])\n"
            + "    GeodeSort(fetch=[1])\n"
            + "      GeodeFilter(condition=[>(ITEM($2, 0), 0)])\n"
            + "        GeodeTableScan(table=[[geode_raw, Zips]])\n");
  }
}

// End GeodeZipsIT.java
