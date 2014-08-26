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
package net.hydromatic.optiq.impl.tpcds;

import net.hydromatic.optiq.prepare.Prepare;
import net.hydromatic.optiq.runtime.Hook;
import net.hydromatic.optiq.test.OptiqAssert;
import net.hydromatic.optiq.tools.Program;
import net.hydromatic.optiq.tools.Programs;

import org.eigenbase.util.Bug;
import org.eigenbase.util.Holder;
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
  private static
  Function<Pair<List<Prepare.Materialization>, Holder<Program>>, Void>
  handler(final boolean bushy) {
    return new Function<Pair<List<Prepare.Materialization>, Holder<Program>>,
        Void>() {
      public Void apply(
          Pair<List<Prepare.Materialization>, Holder<Program>> pair) {
        pair.right.set(
            Programs.sequence(
                Programs.heuristicJoinOrder(Programs.RULE_SET, bushy),
                Programs.CALC_PROGRAM));
        return null;
      }
    };
  }

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
    checkQuery(17)
        .withHook(Hook.PROGRAM, handler(true))
        .explainMatches("including all attributes ",
            OptiqAssert.checkMaskedResultContains(""
                + "EnumerableCalcRel(expr#0..11=[{inputs}], expr#12=[/($t5, $t4)], expr#13=[/($t8, $t7)], expr#14=[/($t11, $t10)], proj#0..5=[{exprs}], STORE_SALES_QUANTITYCOV=[$t12], AS_STORE_RETURNS_QUANTITYCOUNT=[$t6], AS_STORE_RETURNS_QUANTITYAVE=[$t7], AS_STORE_RETURNS_QUANTITYSTDEV=[$t8], STORE_RETURNS_QUANTITYCOV=[$t13], CATALOG_SALES_QUANTITYCOUNT=[$t9], CATALOG_SALES_QUANTITYAVE=[$t10], CATALOG_SALES_QUANTITYSTDEV=[$t14], CATALOG_SALES_QUANTITYCOV=[$t14]): rowcount = 5.434029018852197E26, cumulative cost = {1.618185849567114E30 rows, 1.2672155671963324E30 cpu, 0.0 io}\n"
                + "  EnumerableSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC]): rowcount = 5.434029018852197E26, cumulative cost = {1.6176424466652288E30 rows, 1.2509134801397759E30 cpu, 0.0 io}\n"
                + "    EnumerableCalcRel(expr#0..12=[{inputs}], expr#13=[/($t4, $t5)], expr#14=[CAST($t13):JavaType(class java.lang.Integer)], expr#15=[*($t4, $t4)], expr#16=[/($t15, $t5)], expr#17=[-($t6, $t16)], expr#18=[1], expr#19=[=($t5, $t18)], expr#20=[null], expr#21=[-($t5, $t18)], expr#22=[CASE($t19, $t20, $t21)], expr#23=[/($t17, $t22)], expr#24=[0.5], expr#25=[POWER($t23, $t24)], expr#26=[CAST($t25):JavaType(class java.lang.Integer)], expr#27=[/($t8, $t7)], expr#28=[CAST($t27):JavaType(class java.lang.Integer)], expr#29=[*($t8, $t8)], expr#30=[/($t29, $t7)], expr#31=[-($t9, $t30)], expr#32=[=($t7, $t18)], expr#33=[-($t7, $t18)], expr#34=[CASE($t32, $t20, $t33)], expr#35=[/($t31, $t34)], expr#36=[POWER($t35, $t24)], expr#37=[CAST($t36):JavaType(class java.lang.Integer)], expr#38=[/($t11, $t10)], expr#39=[CAST($t38):JavaType(class java.lang.Integer)], expr#40=[*($t11, $t11)], expr#41=[/($t40, $t10)], expr#42=[-($t12, $t41)], expr#43=[=($t10, $t18)], expr#44=[-($t10, $t18)], expr#45=[CASE($t43, $t20, $t44)], expr#46=[/($t42, $t45)], expr#47=[POWER($t46, $t24)], expr#48=[CAST($t47):JavaType(class java.lang.Integer)], proj#0..3=[{exprs}], STORE_SALES_QUANTITYAVE=[$t14], STORE_SALES_QUANTITYSTDEV=[$t26], AS_STORE_RETURNS_QUANTITYCOUNT=[$t7], AS_STORE_RETURNS_QUANTITYAVE=[$t28], AS_STORE_RETURNS_QUANTITYSTDEV=[$t37], CATALOG_SALES_QUANTITYCOUNT=[$t10], CATALOG_SALES_QUANTITYAVE=[$t39], $f11=[$t48]): rowcount = 5.434029018852197E26, cumulative cost = {1.1954863841615548E28 rows, 1.2503700772378907E30 cpu, 0.0 io}\n"
                + "      EnumerableAggregateRel(group=[{0, 1, 2}], STORE_SALES_QUANTITYCOUNT=[COUNT()], agg#1=[SUM($3)], agg#2=[COUNT($3)], agg#3=[SUM($6)], AS_STORE_RETURNS_QUANTITYCOUNT=[COUNT($4)], agg#5=[SUM($4)], agg#6=[SUM($7)], CATALOG_SALES_QUANTITYCOUNT=[COUNT($5)], agg#8=[SUM($5)], agg#9=[SUM($8)]): rowcount = 5.434029018852197E26, cumulative cost = {1.1411460939730328E28 rows, 1.2172225002228922E30 cpu, 0.0 io}\n"
                + "        EnumerableCalcRel(expr#0..211=[{inputs}], expr#212=[*($t89, $t89)], expr#213=[*($t140, $t140)], expr#214=[*($t196, $t196)], I_ITEM_ID=[$t58], I_ITEM_DESC=[$t61], S_STATE=[$t24], SS_QUANTITY=[$t89], SR_RETURN_QUANTITY=[$t140], CS_QUANTITY=[$t196], $f6=[$t212], $f7=[$t213], $f8=[$t214]): rowcount = 5.434029018852197E27, cumulative cost = {1.0868058037845108E28 rows, 1.2172225002228922E30 cpu, 0.0 io}\n"
                + "          EnumerableJoinRel(condition=[AND(=($82, $133), =($81, $132), =($88, $139))], joinType=[inner]): rowcount = 5.434029018852197E27, cumulative cost = {5.434029018992911E27 rows, 1.8579845E7 cpu, 0.0 io}\n"
                + "            EnumerableJoinRel(condition=[=($0, $86)], joinType=[inner]): rowcount = 2.3008402586892598E13, cumulative cost = {4.8588854672853766E13 rows, 7354409.0 cpu, 0.0 io}\n"
                + "              EnumerableTableAccessRel(table=[[TPCDS, STORE]]): rowcount = 12.0, cumulative cost = {12.0 rows, 13.0 cpu, 0.0 io}\n"
                + "              EnumerableJoinRel(condition=[=($0, $50)], joinType=[inner]): rowcount = 1.2782445881607E13, cumulative cost = {1.279800620431234E13 rows, 7354396.0 cpu, 0.0 io}\n"
                + "                EnumerableCalcRel(expr#0..27=[{inputs}], expr#28=[CAST($t15):VARCHAR(6) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], expr#29=['1998Q1'], expr#30=[=($t28, $t29)], proj#0..27=[{exprs}], $condition=[$t30]): rowcount = 10957.35, cumulative cost = {84006.35 rows, 4455990.0 cpu, 0.0 io}\n"
                + "                  EnumerableTableAccessRel(table=[[TPCDS, DATE_DIM]]): rowcount = 73049.0, cumulative cost = {73049.0 rows, 73050.0 cpu, 0.0 io}\n"
                + "                EnumerableJoinRel(condition=[=($0, $24)], joinType=[inner]): rowcount = 7.7770908E9, cumulative cost = {7.783045975286664E9 rows, 2898406.0 cpu, 0.0 io}\n"
                + "                  EnumerableTableAccessRel(table=[[TPCDS, ITEM]]): rowcount = 18000.0, cumulative cost = {18000.0 rows, 18001.0 cpu, 0.0 io}\n"
                + "                  EnumerableTableAccessRel(table=[[TPCDS, STORE_SALES]]): rowcount = 2880404.0, cumulative cost = {2880404.0 rows, 2880405.0 cpu, 0.0 io}\n"
                + "            EnumerableJoinRel(condition=[AND(=($31, $79), =($30, $91))], joinType=[inner]): rowcount = 6.9978029381741304E16, cumulative cost = {6.9978054204658736E16 rows, 1.1225436E7 cpu, 0.0 io}\n"
                + "              EnumerableJoinRel(condition=[=($0, $28)], joinType=[inner]): rowcount = 7.87597881975E8, cumulative cost = {7.884434222216867E8 rows, 5035701.0 cpu, 0.0 io}\n"
                + "                EnumerableCalcRel(expr#0..27=[{inputs}], expr#28=['1998Q1'], expr#29=[=($t15, $t28)], expr#30=['1998Q2'], expr#31=[=($t15, $t30)], expr#32=['1998Q3'], expr#33=[=($t15, $t32)], expr#34=[OR($t29, $t31, $t33)], proj#0..27=[{exprs}], $condition=[$t34]): rowcount = 18262.25, cumulative cost = {91311.25 rows, 4748186.0 cpu, 0.0 io}\n"
                + "                  EnumerableTableAccessRel(table=[[TPCDS, DATE_DIM]]): rowcount = 73049.0, cumulative cost = {73049.0 rows, 73050.0 cpu, 0.0 io}\n"
                + "                EnumerableTableAccessRel(table=[[TPCDS, STORE_RETURNS]]): rowcount = 287514.0, cumulative cost = {287514.0 rows, 287515.0 cpu, 0.0 io}\n"
                + "              EnumerableJoinRel(condition=[=($0, $28)], joinType=[inner]): rowcount = 3.94888649445E9, cumulative cost = {3.9520401026966867E9 rows, 6189735.0 cpu, 0.0 io}\n"
                + "                EnumerableCalcRel(expr#0..27=[{inputs}], expr#28=['1998Q1'], expr#29=[=($t15, $t28)], expr#30=['1998Q2'], expr#31=[=($t15, $t30)], expr#32=['1998Q3'], expr#33=[=($t15, $t32)], expr#34=[OR($t29, $t31, $t33)], proj#0..27=[{exprs}], $condition=[$t34]): rowcount = 18262.25, cumulative cost = {91311.25 rows, 4748186.0 cpu, 0.0 io}\n"
                + "                  EnumerableTableAccessRel(table=[[TPCDS, DATE_DIM]]): rowcount = 73049.0, cumulative cost = {73049.0 rows, 73050.0 cpu, 0.0 io}\n"
                + "                EnumerableTableAccessRel(table=[[TPCDS, CATALOG_SALES]]): rowcount = 1441548.0, cumulative cost = {1441548.0 rows, 1441549.0 cpu, 0.0 io}\n"));
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
    checkQuery(72)
        .withHook(Hook.PROGRAM, handler(true))
        .planContains("xx");
  }

  @Test public void testQuery95() {
    checkQuery(95)
        .withHook(Hook.PROGRAM, handler(false))
        .runs();
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
    case 95:
      sql = sql.replace("60 days", "interval '60' day");
      sql = sql.replace("d_date between '", "d_date between date '");
      break;
    }
    return with()
        .query(sql.replaceAll("tpcds\\.", "tpcds_01."));
  }
}

// End TpcdsTest.java
