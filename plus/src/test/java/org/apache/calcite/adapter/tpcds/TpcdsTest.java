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
package org.apache.calcite.adapter.tpcds;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Holder;

import net.hydromatic.tpcds.query.Query;

import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.junit.Assert.assertThat;

/** Unit test for {@link org.apache.calcite.adapter.tpcds.TpcdsSchema}.
 *
 * <p>Only runs if {@link org.apache.calcite.config.CalciteSystemProperty#TEST_SLOW} is set.</p>
 */
public class TpcdsTest {
  private static Consumer<Holder<Program>> handler(
      final boolean bushy, final int minJoinCount) {
    return holder -> holder.set(
        Programs.sequence(
            Programs.heuristicJoinOrder(Programs.RULE_SET, bushy,
                minJoinCount),
            Programs.CALC_PROGRAM));
  }

  private static String schema(String name, String scaleFactor) {
    return "     {\n"
        + "       type: 'custom',\n"
        + "       name: '" + name + "',\n"
        + "       factory: 'org.apache.calcite.adapter.tpcds.TpcdsSchemaFactory',\n"
        + "       operand: {\n"
        + "         columnPrefix: true,\n"
        + "         scale: " + scaleFactor + "\n"
        + "       }\n"
        + "     }";
  }

  public static final String TPCDS_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'TPCDS',\n"
      + "   schemas: [\n"
      + schema("TPCDS", "1.0") + ",\n"
      + schema("TPCDS_01", "0.01") + ",\n"
      + schema("TPCDS_5", "5.0") + "\n"
      + "   ]\n"
      + "}";

  private CalciteAssert.AssertThat with() {
    return CalciteAssert.model(TPCDS_MODEL)
        .enable(CalciteSystemProperty.TEST_SLOW.value());
  }

  @Test public void testCallCenter() {
    final String[] strings = {
        "CC_CALL_CENTER_SK=1; CC_CALL_CENTER_ID=AAAAAAAABAAAAAAA; CC_REC_START_DATE=1998-01-01;"
            + " CC_REC_END_DATE=null; CC_CLOSED_DATE_SK=null; CC_OPEN_DATE_SK=2450952;"
            + " CC_NAME=NY Metro; CC_CLASS=large; CC_EMPLOYEES=2; CC_SQ_FT=1138;"
            + " CC_HOURS=8AM-4PM             ; CC_MANAGER=Bob Belcher; CC_MKT_ID=6;"
            + " CC_MKT_CLASS=More than other authori                           ;"
            + " CC_MKT_DESC=Shared others could not count fully dollars. New members ca;"
            + " CC_MARKET_MANAGER=Julius Tran; CC_DIVISION=3; CC_DIVISION_NAME=pri;"
            + " CC_COMPANY=6; CC_COMPANY_NAME=cally                                             ;"
            + " CC_STREET_NUMBER=730       ; CC_STREET_NAME=Ash Hill;"
            + " CC_STREET_TYPE=Boulevard      ; CC_SUITE_NUMBER=Suite 0   ; CC_CITY=Midway;"
            + " CC_COUNTY=Williamson County; CC_STATE=TN; CC_ZIP=31904     ;"
            + " CC_COUNTRY=United States; CC_GMT_OFFSET=-5; CC_TAX_PERCENTAGE=0.11",
        "CC_CALL_CENTER_SK=2; CC_CALL_CENTER_ID=AAAAAAAACAAAAAAA; CC_REC_START_DATE=1998-01-01;"
            + " CC_REC_END_DATE=2000-12-31; CC_CLOSED_DATE_SK=null; CC_OPEN_DATE_SK=2450806;"
            + " CC_NAME=Mid Atlantic; CC_CLASS=medium; CC_EMPLOYEES=6; CC_SQ_FT=2268;"
            + " CC_HOURS=8AM-8AM             ; CC_MANAGER=Felipe Perkins; CC_MKT_ID=2;"
            + " CC_MKT_CLASS=A bit narrow forms matter animals. Consist        ;"
            + " CC_MKT_DESC=Largely blank years put substantially deaf, new others. Question;"
            + " CC_MARKET_MANAGER=Julius Durham; CC_DIVISION=5; CC_DIVISION_NAME=anti;"
            + " CC_COMPANY=1; CC_COMPANY_NAME=ought                                             ;"
            + " CC_STREET_NUMBER=984       ; CC_STREET_NAME=Center Hill;"
            + " CC_STREET_TYPE=Way            ; CC_SUITE_NUMBER=Suite 70  ; CC_CITY=Midway;"
            + " CC_COUNTY=Williamson County; CC_STATE=TN; CC_ZIP=31904     ;"
            + " CC_COUNTRY=United States; CC_GMT_OFFSET=-5; CC_TAX_PERCENTAGE=0.12",
        "CC_CALL_CENTER_SK=3; CC_CALL_CENTER_ID=AAAAAAAACAAAAAAA; CC_REC_START_DATE=2001-01-01;"
            + " CC_REC_END_DATE=null; CC_CLOSED_DATE_SK=null; CC_OPEN_DATE_SK=2450806;"
            + " CC_NAME=Mid Atlantic; CC_CLASS=medium; CC_EMPLOYEES=6; CC_SQ_FT=4134;"
            + " CC_HOURS=8AM-4PM             ; CC_MANAGER=Mark Hightower; CC_MKT_ID=2;"
            + " CC_MKT_CLASS=Wrong troops shall work sometimes in a opti       ;"
            + " CC_MKT_DESC=Largely blank years put substantially deaf, new others. Question;"
            + " CC_MARKET_MANAGER=Julius Durham; CC_DIVISION=1; CC_DIVISION_NAME=ought;"
            + " CC_COMPANY=2; CC_COMPANY_NAME=able                                              ;"
            + " CC_STREET_NUMBER=984       ; CC_STREET_NAME=Center Hill;"
            + " CC_STREET_TYPE=Way            ; CC_SUITE_NUMBER=Suite 70  ; CC_CITY=Midway;"
            + " CC_COUNTY=Williamson County; CC_STATE=TN; CC_ZIP=31904     ;"
            + " CC_COUNTRY=United States; CC_GMT_OFFSET=-5; CC_TAX_PERCENTAGE=0.01",
        "CC_CALL_CENTER_SK=4; CC_CALL_CENTER_ID=AAAAAAAAEAAAAAAA; CC_REC_START_DATE=1998-01-01;"
            + " CC_REC_END_DATE=2000-01-01; CC_CLOSED_DATE_SK=null; CC_OPEN_DATE_SK=2451063;"
            + " CC_NAME=North Midwest; CC_CLASS=medium; CC_EMPLOYEES=1; CC_SQ_FT=649;"
            + " CC_HOURS=8AM-4PM             ; CC_MANAGER=Larry Mccray; CC_MKT_ID=2;"
            + " CC_MKT_CLASS=Dealers make most historical, direct students     ;"
            + " CC_MKT_DESC=Rich groups catch longer other fears; future,;"
            + " CC_MARKET_MANAGER=Matthew Clifton; CC_DIVISION=4; CC_DIVISION_NAME=ese;"
            + " CC_COMPANY=3; CC_COMPANY_NAME=pri                                               ;"
            + " CC_STREET_NUMBER=463       ; CC_STREET_NAME=Pine Ridge;"
            + " CC_STREET_TYPE=RD             ; CC_SUITE_NUMBER=Suite U   ; CC_CITY=Midway;"
            + " CC_COUNTY=Williamson County; CC_STATE=TN; CC_ZIP=31904     ;"
            + " CC_COUNTRY=United States; CC_GMT_OFFSET=-5; CC_TAX_PERCENTAGE=0.05",
        "CC_CALL_CENTER_SK=5; CC_CALL_CENTER_ID=AAAAAAAAEAAAAAAA; CC_REC_START_DATE=2000-01-02;"
            + " CC_REC_END_DATE=2001-12-31; CC_CLOSED_DATE_SK=null; CC_OPEN_DATE_SK=2451063;"
            + " CC_NAME=North Midwest; CC_CLASS=small; CC_EMPLOYEES=3; CC_SQ_FT=795;"
            + " CC_HOURS=8AM-8AM             ; CC_MANAGER=Larry Mccray; CC_MKT_ID=2;"
            + " CC_MKT_CLASS=Dealers make most historical, direct students     ;"
            + " CC_MKT_DESC=Blue, due beds come. Politicians would not make far thoughts. "
            + "Specifically new horses partic;"
            + " CC_MARKET_MANAGER=Gary Colburn; CC_DIVISION=4; CC_DIVISION_NAME=ese;"
            + " CC_COMPANY=3; CC_COMPANY_NAME=pri                                               ;"
            + " CC_STREET_NUMBER=463       ; CC_STREET_NAME=Pine Ridge;"
            + " CC_STREET_TYPE=RD             ; CC_SUITE_NUMBER=Suite U   ; CC_CITY=Midway;"
            + " CC_COUNTY=Williamson County; CC_STATE=TN; CC_ZIP=31904     ;"
            + " CC_COUNTRY=United States; CC_GMT_OFFSET=-5; CC_TAX_PERCENTAGE=0.12",
        "CC_CALL_CENTER_SK=6; CC_CALL_CENTER_ID=AAAAAAAAEAAAAAAA; CC_REC_START_DATE=2002-01-01;"
            + " CC_REC_END_DATE=null; CC_CLOSED_DATE_SK=null; CC_OPEN_DATE_SK=2451063;"
            + " CC_NAME=North Midwest; CC_CLASS=medium; CC_EMPLOYEES=7; CC_SQ_FT=3514;"
            + " CC_HOURS=8AM-4PM             ; CC_MANAGER=Larry Mccray; CC_MKT_ID=5;"
            + " CC_MKT_CLASS=Silly particles could pro                         ;"
            + " CC_MKT_DESC=Blue, due beds come. Politicians would not make far thoughts. "
            + "Specifically new horses partic;"
            + " CC_MARKET_MANAGER=Gary Colburn; CC_DIVISION=5; CC_DIVISION_NAME=anti;"
            + " CC_COMPANY=3; CC_COMPANY_NAME=pri                                               ;"
            + " CC_STREET_NUMBER=463       ; CC_STREET_NAME=Pine Ridge;"
            + " CC_STREET_TYPE=RD             ; CC_SUITE_NUMBER=Suite U   ; CC_CITY=Midway;"
            + " CC_COUNTY=Williamson County; CC_STATE=TN; CC_ZIP=31904     ;"
            + " CC_COUNTRY=United States; CC_GMT_OFFSET=-5; CC_TAX_PERCENTAGE=0.11"};
    with().query("select * from tpcds.call_center").returnsUnordered(strings);
  }

  @Test public void testTableCount() {
    final CalciteAssert.AssertThat with = with();
    foo(with, "CALL_CENTER", 6);
    foo(with, "CATALOG_PAGE", 11_718);
    foo(with, "CATALOG_RETURNS", 144_067);
    foo(with, "CATALOG_SALES", 1_441_548);
    foo(with, "CUSTOMER", 100_000);
    foo(with, "CUSTOMER_ADDRESS", 50_000);
    foo(with, "CUSTOMER_DEMOGRAPHICS", 1_920_800);
    foo(with, "DATE_DIM", 73_049);
    foo(with, "HOUSEHOLD_DEMOGRAPHICS", 7_200);
    foo(with, "INCOME_BAND", 20);
    foo(with, "INVENTORY", 11_745_000);
    foo(with, "ITEM", 18_000);
    foo(with, "PROMOTION", 300);
    foo(with, "REASON", 35);
    foo(with, "SHIP_MODE", 20);
    foo(with, "STORE", 12);
    foo(with, "STORE_RETURNS", 287_514);
    foo(with, "STORE_SALES", 2_880_404);
    foo(with, "TIME_DIM", 86_400);
    foo(with, "WAREHOUSE", 5);
    foo(with, "WEB_PAGE", 60);
    foo(with, "WEB_RETURNS", 71_763);
    foo(with, "WEB_SALES", 719_384);
    foo(with, "WEB_SITE", 30);
    foo(with, "DBGEN_VERSION", 1);
  }

  protected void foo(CalciteAssert.AssertThat with, String tableName,
      int expectedCount) {
    final String sql = "select * from tpcds." + tableName;
    with.query(sql).returnsCount(expectedCount);
  }

  /** Tests the customer table with scale factor 5. */
  @Ignore("add tests like this that count each table")
  @Test public void testCustomer5() {
    with()
        .query("select * from tpcds_5.customer")
        .returnsCount(750000);
  }

  @Ignore("throws 'RuntimeException: Cannot convert null to long'")
  @Test public void testQuery01() {
    checkQuery(1).runs();
  }

  @Test public void testQuery17Plan() {
    //noinspection unchecked
    checkQuery(17)
        .withHook(Hook.PROGRAM, handler(true, 2))
        .explainMatches("including all attributes ",
            CalciteAssert.checkMaskedResultContains(""
                + "EnumerableCalc(expr#0..9=[{inputs}], expr#10=[/($t4, $t3)], expr#11=[CAST($t10):INTEGER NOT NULL], expr#12=[*($t4, $t4)], expr#13=[/($t12, $t3)], expr#14=[-($t5, $t13)], expr#15=[1], expr#16=[=($t3, $t15)], expr#17=[null], expr#18=[-($t3, $t15)], expr#19=[CASE($t16, $t17, $t18)], expr#20=[/($t14, $t19)], expr#21=[0.5], expr#22=[POWER($t20, $t21)], expr#23=[CAST($t22):INTEGER NOT NULL], expr#24=[/($t23, $t11)], expr#25=[/($t6, $t3)], expr#26=[CAST($t25):INTEGER NOT NULL], expr#27=[*($t6, $t6)], expr#28=[/($t27, $t3)], expr#29=[-($t7, $t28)], expr#30=[/($t29, $t19)], expr#31=[POWER($t30, $t21)], expr#32=[CAST($t31):INTEGER NOT NULL], expr#33=[/($t32, $t26)], expr#34=[/($t8, $t3)], expr#35=[CAST($t34):INTEGER NOT NULL], expr#36=[*($t8, $t8)], expr#37=[/($t36, $t3)], expr#38=[-($t9, $t37)], expr#39=[/($t38, $t19)], expr#40=[POWER($t39, $t21)], expr#41=[CAST($t40):INTEGER NOT NULL], expr#42=[/($t41, $t35)], proj#0..3=[{exprs}], STORE_SALES_QUANTITYAVE=[$t11], STORE_SALES_QUANTITYSTDEV=[$t23], STORE_SALES_QUANTITYCOV=[$t24], AS_STORE_RETURNS_QUANTITYCOUNT=[$t3], AS_STORE_RETURNS_QUANTITYAVE=[$t26], AS_STORE_RETURNS_QUANTITYSTDEV=[$t32], STORE_RETURNS_QUANTITYCOV=[$t33], CATALOG_SALES_QUANTITYCOUNT=[$t3], CATALOG_SALES_QUANTITYAVE=[$t35], CATALOG_SALES_QUANTITYSTDEV=[$t42], CATALOG_SALES_QUANTITYCOV=[$t42]): rowcount = 100.0, cumulative cost = {1.2435775409784036E28 rows, 2.555295485909236E30 cpu, 0.0 io}\n"
                + "  EnumerableLimit(fetch=[100]): rowcount = 100.0, cumulative cost = {1.2435775409784036E28 rows, 2.555295485909236E30 cpu, 0.0 io}\n"
                + "    EnumerableSort(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC]): rowcount = 5.434029018852197E26, cumulative cost = {1.2435775409784036E28 rows, 2.555295485909236E30 cpu, 0.0 io}\n"
                + "      EnumerableAggregate(group=[{0, 1, 2}], STORE_SALES_QUANTITYCOUNT=[COUNT()], agg#1=[$SUM0($3)], agg#2=[$SUM0($6)], agg#3=[$SUM0($4)], agg#4=[$SUM0($7)], agg#5=[$SUM0($5)], agg#6=[$SUM0($8)]): rowcount = 5.434029018852197E26, cumulative cost = {1.1892372507898816E28 rows, 1.2172225002228922E30 cpu, 0.0 io}\n"
                + "        EnumerableCalc(expr#0..211=[{inputs}], expr#212=[*($t89, $t89)], expr#213=[*($t140, $t140)], expr#214=[*($t196, $t196)], I_ITEM_ID=[$t58], I_ITEM_DESC=[$t61], S_STATE=[$t24], SS_QUANTITY=[$t89], SR_RETURN_QUANTITY=[$t140], CS_QUANTITY=[$t196], $f6=[$t212], $f7=[$t213], $f8=[$t214]): rowcount = 5.434029018852197E27, cumulative cost = {1.0873492066864028E28 rows, 1.2172225002228922E30 cpu, 0.0 io}\n"
                + "          EnumerableHashJoin(condition=[AND(=($82, $133), =($81, $132), =($88, $139))], joinType=[inner]): rowcount = 5.434029018852197E27, cumulative cost = {5.439463048011832E27 rows, 1.8506796E7 cpu, 0.0 io}\n"
                + "            EnumerableHashJoin(condition=[=($0, $86)], joinType=[inner]): rowcount = 2.3008402586892598E13, cumulative cost = {4.8588854672854766E13 rows, 7281360.0 cpu, 0.0 io}\n"
                + "              EnumerableTableScan(table=[[TPCDS, STORE]]): rowcount = 12.0, cumulative cost = {12.0 rows, 13.0 cpu, 0.0 io}\n"
                + "              EnumerableHashJoin(condition=[=($0, $50)], joinType=[inner]): rowcount = 1.2782445881607E13, cumulative cost = {1.279800620431234E13 rows, 7281347.0 cpu, 0.0 io}\n"
                + "                EnumerableCalc(expr#0..27=[{inputs}], expr#28=['1998Q1'], expr#29=[=($t15, $t28)], proj#0..27=[{exprs}], $condition=[$t29]): rowcount = 10957.35, cumulative cost = {84006.35 rows, 4382941.0 cpu, 0.0 io}\n"
                + "                  EnumerableTableScan(table=[[TPCDS, DATE_DIM]]): rowcount = 73049.0, cumulative cost = {73049.0 rows, 73050.0 cpu, 0.0 io}\n"
                + "                EnumerableHashJoin(condition=[=($0, $24)], joinType=[inner]): rowcount = 7.7770908E9, cumulative cost = {7.783045975286664E9 rows, 2898406.0 cpu, 0.0 io}\n"
                + "                  EnumerableTableScan(table=[[TPCDS, ITEM]]): rowcount = 18000.0, cumulative cost = {18000.0 rows, 18001.0 cpu, 0.0 io}\n"
                + "                  EnumerableTableScan(table=[[TPCDS, STORE_SALES]]): rowcount = 2880404.0, cumulative cost = {2880404.0 rows, 2880405.0 cpu, 0.0 io}\n"
                + "            EnumerableHashJoin(condition=[AND(=($31, $79), =($30, $91))], joinType=[inner]): rowcount = 6.9978029381741304E16, cumulative cost = {7.0048032234040472E16 rows, 1.1225436E7 cpu, 0.0 io}\n"
                + "              EnumerableHashJoin(condition=[=($0, $28)], joinType=[inner]): rowcount = 7.87597881975E8, cumulative cost = {7.884434212216867E8 rows, 5035701.0 cpu, 0.0 io}\n"
                + "                EnumerableCalc(expr#0..27=[{inputs}], expr#28=['1998Q1'], expr#29=[=($t15, $t28)], expr#30=['1998Q2'], expr#31=[=($t15, $t30)], expr#32=['1998Q3'], expr#33=[=($t15, $t32)], expr#34=[OR($t29, $t31, $t33)], proj#0..27=[{exprs}], $condition=[$t34]): rowcount = 18262.25, cumulative cost = {91311.25 rows, 4748186.0 cpu, 0.0 io}\n"
                + "                  EnumerableTableScan(table=[[TPCDS, DATE_DIM]]): rowcount = 73049.0, cumulative cost = {73049.0 rows, 73050.0 cpu, 0.0 io}\n"
                + "                EnumerableTableScan(table=[[TPCDS, STORE_RETURNS]]): rowcount = 287514.0, cumulative cost = {287514.0 rows, 287515.0 cpu, 0.0 io}\n"
                + "              EnumerableHashJoin(condition=[=($0, $28)], joinType=[inner]): rowcount = 3.94888649445E9, cumulative cost = {3.9520401026966867E9 rows, 6189735.0 cpu, 0.0 io}\n"
                + "                EnumerableCalc(expr#0..27=[{inputs}], expr#28=['1998Q1'], expr#29=[=($t15, $t28)], expr#30=['1998Q2'], expr#31=[=($t15, $t30)], expr#32=['1998Q3'], expr#33=[=($t15, $t32)], expr#34=[OR($t29, $t31, $t33)], proj#0..27=[{exprs}], $condition=[$t34]): rowcount = 18262.25, cumulative cost = {91311.25 rows, 4748186.0 cpu, 0.0 io}\n"
                + "                  EnumerableTableScan(table=[[TPCDS, DATE_DIM]]): rowcount = 73049.0, cumulative cost = {73049.0 rows, 73050.0 cpu, 0.0 io}\n"
                + "                EnumerableTableScan(table=[[TPCDS, CATALOG_SALES]]): rowcount = 1441548.0, cumulative cost = {1441548.0 rows, 1441549.0 cpu, 0.0 io}\n"));
  }

  @Ignore("throws 'RuntimeException: Cannot convert null to long'")
  @Test public void testQuery27() {
    checkQuery(27).runs();
  }

  @Ignore("throws 'RuntimeException: Cannot convert null to long'")
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
        .withHook(Hook.PROGRAM, handler(true, 2))
        .planContains("xx");
  }

  @Ignore("throws 'java.lang.AssertionError: type mismatch'")
  @Test public void testQuery95() {
    checkQuery(95)
        .withHook(Hook.PROGRAM, handler(false, 6))
        .runs();
  }

  private CalciteAssert.AssertQuery checkQuery(int i) {
    final Query query = Query.of(i);
    String sql = query.sql(new Random(0));
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
      // Work around CALCITE-304: Support '<DATE> + <INTEGER>'.
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

  public Frameworks.ConfigBuilder config() throws Exception {
    final Holder<SchemaPlus> root = Holder.of(null);
    CalciteAssert.model(TPCDS_MODEL)
        .doWithConnection(connection -> {
          root.set(connection.getRootSchema().getSubSchema("TPCDS"));
        });
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(root.get())
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  /**
   * Builder query 27 using {@link RelBuilder}.
   *
   * <blockquote><pre>
   *   select  i_item_id,
   *         s_state, grouping(s_state) g_state,
   *         avg(ss_quantity) agg1,
   *         avg(ss_list_price) agg2,
   *         avg(ss_coupon_amt) agg3,
   *         avg(ss_sales_price) agg4
   * from store_sales, customer_demographics, date_dim, store, item
   * where ss_sold_date_sk = d_date_sk and
   *        ss_item_sk = i_item_sk and
   *        ss_store_sk = s_store_sk and
   *        ss_cdemo_sk = cd_demo_sk and
   *        cd_gender = 'dist(gender, 1, 1)' and
   *        cd_marital_status = 'dist(marital_status, 1, 1)' and
   *        cd_education_status = 'dist(education, 1, 1)' and
   *        d_year = 1998 and
   *        s_state in ('distmember(fips_county,[STATENUMBER.1], 3)',
   *              'distmember(fips_county,[STATENUMBER.2], 3)',
   *              'distmember(fips_county,[STATENUMBER.3], 3)',
   *              'distmember(fips_county,[STATENUMBER.4], 3)',
   *              'distmember(fips_county,[STATENUMBER.5], 3)',
   *              'distmember(fips_county,[STATENUMBER.6], 3)')
   *  group by rollup (i_item_id, s_state)
   *  order by i_item_id
   *          ,s_state
   *  LIMIT 100
   * </pre></blockquote>
   */
  @Test public void testQuery27Builder() throws Exception {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("STORE_SALES")
            .scan("CUSTOMER_DEMOGRAPHICS")
            .scan("DATE_DIM")
            .scan("STORE")
            .scan("ITEM")
            .join(JoinRelType.INNER)
            .join(JoinRelType.INNER)
            .join(JoinRelType.INNER)
            .join(JoinRelType.INNER)
            .filter(
                builder.equals(builder.field("SS_SOLD_DATE_SK"), builder.field("D_DATE_SK")),
                builder.equals(builder.field("SS_ITEM_SK"), builder.field("I_ITEM_SK")),
                builder.equals(builder.field("SS_STORE_SK"), builder.field("S_STORE_SK")),
                builder.equals(builder.field("SS_CDEMO_SK"), builder.field("CD_DEMO_SK")),
                builder.equals(builder.field("CD_GENDER"), builder.literal("M")),
                builder.equals(builder.field("CD_MARITAL_STATUS"), builder.literal("S")),
                builder.equals(builder.field("CD_EDUCATION_STATUS"),
                    builder.literal("HIGH SCHOOL")),
                builder.equals(builder.field("D_YEAR"), builder.literal(1998)),
                builder.call(SqlStdOperatorTable.IN,
                    builder.field("S_STATE"),
                    builder.call(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
                        builder.literal("CA"),
                        builder.literal("OR"),
                        builder.literal("WA"),
                        builder.literal("TX"),
                        builder.literal("OK"),
                        builder.literal("MD"))))
            .aggregate(builder.groupKey("I_ITEM_ID", "S_STATE"),
                builder.avg(false, "AGG1", builder.field("SS_QUANTITY")),
                builder.avg(false, "AGG2", builder.field("SS_LIST_PRICE")),
                builder.avg(false, "AGG3", builder.field("SS_COUPON_AMT")),
                builder.avg(false, "AGG4", builder.field("SS_SALES_PRICE")))
            .sortLimit(0, 100, builder.field("I_ITEM_ID"), builder.field("S_STATE"))
            .build();
    String expectResult = ""
        + "LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC], fetch=[100])\n"
        + "  LogicalAggregate(group=[{84, 90}], AGG1=[AVG($10)], AGG2=[AVG($12)], AGG3=[AVG($19)], AGG4=[AVG($13)])\n"
        + "    LogicalFilter(condition=[AND(=($0, $32), =($2, $89), =($7, $60), =($4, $23), =($24, 'M'), =($25, 'S'), =($26, 'HIGH SCHOOL'), =($38, 1998), IN($84, ARRAY('CA', 'OR', 'WA', 'TX', 'OK', 'MD')))])\n"
        + "      LogicalJoin(condition=[true], joinType=[inner])\n"
        + "        LogicalTableScan(table=[[TPCDS, STORE_SALES]])\n"
        + "        LogicalJoin(condition=[true], joinType=[inner])\n"
        + "          LogicalTableScan(table=[[TPCDS, CUSTOMER_DEMOGRAPHICS]])\n"
        + "          LogicalJoin(condition=[true], joinType=[inner])\n"
        + "            LogicalTableScan(table=[[TPCDS, DATE_DIM]])\n"
        + "            LogicalJoin(condition=[true], joinType=[inner])\n"
        + "              LogicalTableScan(table=[[TPCDS, STORE]])\n"
        + "              LogicalTableScan(table=[[TPCDS, ITEM]])\n";
    assertThat(root, hasTree(expectResult));
  }
}

// End TpcdsTest.java
