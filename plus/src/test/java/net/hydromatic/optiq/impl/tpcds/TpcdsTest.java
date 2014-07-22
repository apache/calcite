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
package net.hydromatic.optiq.impl.tpcds;

import net.hydromatic.optiq.prepare.Prepare;
import net.hydromatic.optiq.runtime.Hook;
import net.hydromatic.optiq.test.OptiqAssert;
import net.hydromatic.optiq.tools.Program;
import net.hydromatic.optiq.tools.Programs;

import org.eigenbase.util.Bug;
import org.eigenbase.util.Pair;

import com.google.common.base.Function;

import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Random;

import net.hydromatic.tpcds.query.Query;

/** Unit test for {@link net.hydromatic.optiq.impl.tpcds.TpcdsSchema}.
 *
 * <p>Only runs if {@code -Doptiq.test.slow=true} is specified on the
 * command-line.
 * (See {@link net.hydromatic.optiq.test.OptiqAssert#ENABLE_SLOW}.)</p> */
public class TpcdsTest {
  private static String schema(String name, String scaleFactor) {
    return "     {\n"
        + "       type: 'custom',\n"
        + "       name: '" + name + "',\n"
        + "       factory: 'net.hydromatic.optiq.impl.tpcds.TpcdsSchemaFactory',\n"
        + "       operand: {\n"
        + "         columnPrefix: true,\n"
        + "         scale: " + scaleFactor + "\n"
        + "       }\n"
        + "     }";
  }

  public static final String TPCDS_MODEL =
      "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'TPCDS',\n"
      + "   schemas: [\n"
      + schema("TPCDS", "1.0") + ",\n"
      + schema("TPCDS_01", "0.01") + ",\n"
      + schema("TPCDS_5", "5.0") + "\n"
      + "   ]\n"
      + "}";

  private OptiqAssert.AssertThat with() {
    return OptiqAssert.that()
        .withModel(TPCDS_MODEL)
        .enable(OptiqAssert.ENABLE_SLOW);
  }

  @Test public void testCallCenter() {
    with()
        .query("select * from tpcds.call_center")
        .returnsUnordered();
  }

  @Ignore("add tests like this that count each table")
  @Test public void testLineItem() {
    with()
        .query("select * from tpcds.lineitem")
        .returnsCount(6001215);
  }

  /** Tests the customer table with scale factor 5. */
  @Ignore("add tests like this that count each table")
  @Test public void testCustomer5() {
    with()
        .query("select * from tpcds_5.customer")
        .returnsCount(750000);
  }

  @Test public void testQuery01() {
    checkQuery(1).runs();
  }

  @Test public void testQuery17Plan() {
    //noinspection unchecked
    Hook.Closeable closeable = Hook.PROGRAM.addThread(
        new Function<Pair<List<Prepare.Materialization>, Program[]>, Void>() {
          public Void apply(Pair<List<Prepare.Materialization>, Program[]> a0) {
            a0.right[0] = Programs.heuristicJoinOrder(Programs.RULE_SET, true);
            return null;
          }
        });
    try {
      checkQuery(17).explainMatches("including all attributes ",
          OptiqAssert.checkMaskedResultContains(""
              + "EnumerableProjectRel(I_ITEM_ID=[$0], I_ITEM_DESC=[$1], S_STATE=[$2], STORE_SALES_QUANTITYCOUNT=[$3], STORE_SALES_QUANTITYAVE=[$4], STORE_SALES_QUANTITYSTDEV=[$5], STORE_SALES_QUANTITYCOV=[/($5, $4)], AS_STORE_RETURNS_QUANTITYCOUNT=[$6], AS_STORE_RETURNS_QUANTITYAVE=[$7], AS_STORE_RETURNS_QUANTITYSTDEV=[$8], STORE_RETURNS_QUANTITYCOV=[/($8, $7)], CATALOG_SALES_QUANTITYCOUNT=[$9], CATALOG_SALES_QUANTITYAVE=[$10], CATALOG_SALES_QUANTITYSTDEV=[/($11, $10)], CATALOG_SALES_QUANTITYCOV=[/($11, $10)]): rowcount = 5.434029018852197E26, cumulative cost = {1.618185849567114E30 rows, 6.412154242245593E28 cpu, 0.0 io}\n"
              + "  EnumerableSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC]): rowcount = 5.434029018852197E26, cumulative cost = {1.6176424466652288E30 rows, 5.597049889417763E28 cpu, 0.0 io}\n"
              + "    EnumerableProjectRel(I_ITEM_ID=[$0], I_ITEM_DESC=[$1], S_STATE=[$2], STORE_SALES_QUANTITYCOUNT=[$3], STORE_SALES_QUANTITYAVE=[CAST(/($4, $5)):JavaType(class java.lang.Integer)], STORE_SALES_QUANTITYSTDEV=[CAST(POWER(/(-($6, /(*($4, $4), $5)), CASE(=($5, 1), null, -($5, 1))), 0.5)):JavaType(class java.lang.Integer)], AS_STORE_RETURNS_QUANTITYCOUNT=[$7], AS_STORE_RETURNS_QUANTITYAVE=[CAST(/($8, $7)):JavaType(class java.lang.Integer)], AS_STORE_RETURNS_QUANTITYSTDEV=[CAST(POWER(/(-($9, /(*($8, $8), $7)), CASE(=($7, 1), null, -($7, 1))), 0.5)):JavaType(class java.lang.Integer)], CATALOG_SALES_QUANTITYCOUNT=[$10], CATALOG_SALES_QUANTITYAVE=[CAST(/($11, $10)):JavaType(class java.lang.Integer)], $f11=[CAST(POWER(/(-($12, /(*($11, $11), $10)), CASE(=($10, 1), null, -($10, 1))), 0.5)):JavaType(class java.lang.Integer)]): rowcount = 5.434029018852197E26, cumulative cost = {1.1954863841615548E28 rows, 5.5427095992292415E28 cpu, 0.0 io}\n"
              + "      EnumerableAggregateRel(group=[{0, 1, 2}], STORE_SALES_QUANTITYCOUNT=[COUNT()], agg#1=[SUM($3)], agg#2=[COUNT($3)], agg#3=[SUM($6)], AS_STORE_RETURNS_QUANTITYCOUNT=[COUNT($4)], agg#5=[SUM($4)], agg#6=[SUM($7)], CATALOG_SALES_QUANTITYCOUNT=[COUNT($5)], agg#8=[SUM($5)], agg#9=[SUM($8)]): rowcount = 5.434029018852197E26, cumulative cost = {1.1411460939730328E28 rows, 4.890626116966977E28 cpu, 0.0 io}\n"
              + "        EnumerableProjectRel(I_ITEM_ID=[$58], I_ITEM_DESC=[$61], S_STATE=[$24], SS_QUANTITY=[$89], SR_RETURN_QUANTITY=[$140], CS_QUANTITY=[$196], $f6=[*($89, $89)], $f7=[*($140, $140)], $f8=[*($196, $196)]): rowcount = 5.434029018852197E27, cumulative cost = {1.0868058037845108E28 rows, 4.890626116966977E28 cpu, 0.0 io}\n"
              + "          EnumerableJoinRel(condition=[AND(=($82, $133), =($81, $132), =($88, $139))], joinType=[inner]): rowcount = 5.434029018852197E27, cumulative cost = {5.434029018992911E27 rows, 5065780.0 cpu, 0.0 io}\n"
              + "            EnumerableJoinRel(condition=[=($0, $86)], joinType=[inner]): rowcount = 2.3008402586892598E13, cumulative cost = {4.8588854672852766E13 rows, 3044518.0 cpu, 0.0 io}\n"
              + "              EnumerableTableAccessRel(table=[[TPCDS, STORE]]): rowcount = 12.0, cumulative cost = {12.0 rows, 13.0 cpu, 0.0 io}\n"
              + "              EnumerableJoinRel(condition=[=($0, $50)], joinType=[inner]): rowcount = 1.2782445881607E13, cumulative cost = {1.279800620431134E13 rows, 3044505.0 cpu, 0.0 io}\n"
              + "                EnumerableFilterRel(condition=[=(CAST($15):VARCHAR(6) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", '1998Q1')]): rowcount = 10957.35, cumulative cost = {84006.35 rows, 146099.0 cpu, 0.0 io}\n"
              + "                  EnumerableTableAccessRel(table=[[TPCDS, DATE_DIM]]): rowcount = 73049.0, cumulative cost = {73049.0 rows, 73050.0 cpu, 0.0 io}\n"
              + "                EnumerableJoinRel(condition=[=($0, $24)], joinType=[inner]): rowcount = 7.7770908E9, cumulative cost = {7.783045975286664E9 rows, 2898406.0 cpu, 0.0 io}\n"
              + "                  EnumerableTableAccessRel(table=[[TPCDS, ITEM]]): rowcount = 18000.0, cumulative cost = {18000.0 rows, 18001.0 cpu, 0.0 io}\n"
              + "                  EnumerableTableAccessRel(table=[[TPCDS, STORE_SALES]]): rowcount = 2880404.0, cumulative cost = {2880404.0 rows, 2880405.0 cpu, 0.0 io}\n"
              + "            EnumerableJoinRel(condition=[AND(=($31, $79), =($30, $91))], joinType=[inner]): rowcount = 6.9978029381741304E16, cumulative cost = {6.9978054204658736E16 rows, 2021262.0 cpu, 0.0 io}\n"
              + "              EnumerableJoinRel(condition=[=($28, $0)], joinType=[inner]): rowcount = 7.87597881975E8, cumulative cost = {7.884434222216867E8 rows, 433614.0 cpu, 0.0 io}\n"
              + "                EnumerableFilterRel(condition=[OR(=($15, '1998Q1'), =($15, '1998Q2'), =($15, '1998Q3'))]): rowcount = 18262.25, cumulative cost = {91311.25 rows, 146099.0 cpu, 0.0 io}\n"
              + "                  EnumerableTableAccessRel(table=[[TPCDS, DATE_DIM]]): rowcount = 73049.0, cumulative cost = {73049.0 rows, 73050.0 cpu, 0.0 io}\n"
              + "                EnumerableTableAccessRel(table=[[TPCDS, STORE_RETURNS]]): rowcount = 287514.0, cumulative cost = {287514.0 rows, 287515.0 cpu, 0.0 io}\n"
              + "              EnumerableJoinRel(condition=[=($28, $0)], joinType=[inner]): rowcount = 3.94888649445E9, cumulative cost = {3.9520401026966867E9 rows, 1587648.0 cpu, 0.0 io}\n"
              + "                EnumerableFilterRel(condition=[OR(=($15, '1998Q1'), =($15, '1998Q2'), =($15, '1998Q3'))]): rowcount = 18262.25, cumulative cost = {91311.25 rows, 146099.0 cpu, 0.0 io}\n"
              + "                  EnumerableTableAccessRel(table=[[TPCDS, DATE_DIM]]): rowcount = 73049.0, cumulative cost = {73049.0 rows, 73050.0 cpu, 0.0 io}\n"
              + "                EnumerableTableAccessRel(table=[[TPCDS, CATALOG_SALES]]): rowcount = 1441548.0, cumulative cost = {1441548.0 rows, 1441549.0 cpu, 0.0 io}\n"));
    } finally {
      closeable.close();
    }
  }

  @Test public void testQuery58() {
    checkQuery(58).explainContains("PLAN").runs();
  }

  @Ignore("takes too long to optimize")
  @Test public void testQuery72() {
    checkQuery(72).runs();
  }

  @Ignore("work in progress")
  @Test public void testQuery72Plan() {
    checkQuery(72).planContains("xx");
  }

  private OptiqAssert.AssertQuery checkQuery(int i) {
    final Query query = Query.of(i);
    String sql = query.sql(-1, new Random(0));
    switch (i) {
    case 58:
      if (Bug.upgrade("new TPC-DS generator")) {
        // Work around bug: Support '<DATE>  = <character literal>'.
        sql = sql.replace(" = '", " = DATE '");
      } else {
        // Until TPC-DS generator can handle date(...).
        sql = sql.replace("'date([YEAR]+\"-01-01\",[YEAR]+\"-07-24\",sales)'",
            "DATE '1998-08-18'");
      }
      break;
    case 72:
      // Work around OPTIQ-304: Support '<DATE> + <INTEGER>'.
      sql = sql.replace("+ 5", "+ interval '5' day");
      break;
    }
    return with()
        .query(sql.replaceAll("tpcds\\.", "tpcds_01."));
  }
}

// End TpcdsTest.java
