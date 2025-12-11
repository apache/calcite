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
package org.apache.calcite.adapter.tpch;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.calcite.test.Matchers.containsStringLinux;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/** Unit test for {@link org.apache.calcite.adapter.tpch.TpchSchema}.
 *
 * <p>Because the TPC-H data generator takes time and memory to instantiate,
 * tests only run as part of slow tests.
 */
class TpchTest {
  public static final boolean ENABLE = TestUtil.getJavaMajorVersion() >= 7;

  private static String schema(String name, String scaleFactor) {
    return "     {\n"
        + "       type: 'custom',\n"
        + "       name: '" + name + "',\n"
        + "       factory: 'org.apache.calcite.adapter.tpch.TpchSchemaFactory',\n"
        + "       operand: {\n"
        + "         columnPrefix: false,\n"
        + "         scale: " + scaleFactor + "\n"
        + "       }\n"
        + "     }";
  }

  public static final String TPCH_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'TPCH',\n"
      + "   schemas: [\n"
      + schema("TPCH", "1.0") + ",\n"
      + schema("TPCH_01", "0.01") + ",\n"
      + schema("TPCH_5", "5.0") + "\n"
      + "   ]\n"
      + "}";

  private static final String[] QUERY_ARRAY = {
      "select\n"
          + "  l_returnflag,\n"
          + "  l_linestatus,\n"
          + "  sum(l_quantity) as sum_qty,\n"
          + "  sum(l_extendedprice) as sum_base_price,\n"
          + "  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n"
          + "  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n"
          + "  avg(l_quantity) as avg_qty,\n"
          + "  avg(l_extendedprice) as avg_price,\n"
          + "  avg(l_discount) as avg_disc,\n"
          + "  count(*) as count_order\n"
          + "from\n"
          + "  tpch.lineitem\n"
          + "-- where\n"
          + "--  l_shipdate <= date '1998-12-01' - interval '120' day (3)\n"
          + "group by\n"
          + "  l_returnflag,\n"
          + "  l_linestatus\n"
          + "\n"
          + "order by\n"
          + "  l_returnflag,\n"
          + "  l_linestatus",

      // 02
      "select\n"
          + "  s.s_acctbal,\n"
          + "  s.s_name,\n"
          + "  n.n_name,\n"
          + "  p.p_partkey,\n"
          + "  p.p_mfgr,\n"
          + "  s.s_address,\n"
          + "  s.s_phone,\n"
          + "  s.s_comment\n"
          + "from\n"
          + "  tpch.part p,\n"
          + "  tpch.supplier s,\n"
          + "  tpch.partsupp ps,\n"
          + "  tpch.nation n,\n"
          + "  tpch.region r\n"
          + "where\n"
          + "  p.p_partkey = ps.ps_partkey\n"
          + "  and s.s_suppkey = ps.ps_suppkey\n"
          + "  and p.p_size = 41\n"
          + "  and p.p_type like '%NICKEL'\n"
          + "  and s.s_nationkey = n.n_nationkey\n"
          + "  and n.n_regionkey = r.r_regionkey\n"
          + "  and r.r_name = 'EUROPE'\n"
          + "  and ps.ps_supplycost = (\n"
          + "\n"
          + "    select\n"
          + "      min(ps.ps_supplycost)\n"
          + "\n"
          + "    from\n"
          + "      tpch.partsupp ps,\n"
          + "      tpch.supplier s,\n"
          + "      tpch.nation n,\n"
          + "      tpch.region r\n"
          + "    where\n"
          + "      p.p_partkey = ps.ps_partkey\n"
          + "      and s.s_suppkey = ps.ps_suppkey\n"
          + "      and s.s_nationkey = n.n_nationkey\n"
          + "      and n.n_regionkey = r.r_regionkey\n"
          + "      and r.r_name = 'EUROPE'\n"
          + "  )\n"
          + "\n"
          + "order by\n"
          + "  s.s_acctbal desc,\n"
          + "  n.n_name,\n"
          + "  s.s_name,\n"
          + "  p.p_partkey\n"
          + "limit 100",

      // 03
      "select\n"
          + "  l.l_orderkey,\n"
          + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
          + "  o.o_orderdate,\n"
          + "  o.o_shippriority\n"
          + "\n"
          + "from\n"
          + "  tpch.customer c,\n"
          + "  tpch.orders o,\n"
          + "  tpch.lineitem l\n"
          + "\n"
          + "where\n"
          + "  c.c_mktsegment = 'HOUSEHOLD'\n"
          + "  and c.c_custkey = o.o_custkey\n"
          + "  and l.l_orderkey = o.o_orderkey\n"
          + "--  and o.o_orderdate < date '1995-03-25'\n"
          + "--  and l.l_shipdate > date '1995-03-25'\n"
          + "\n"
          + "group by\n"
          + "  l.l_orderkey,\n"
          + "  o.o_orderdate,\n"
          + "  o.o_shippriority\n"
          + "order by\n"
          + "  revenue desc,\n"
          + "  o.o_orderdate\n"
          + "limit 10",

      // 04
      "select\n"
          + "  o_orderpriority,\n"
          + "  count(*) as order_count\n"
          + "from\n"
          + "  tpch.orders\n"
          + "\n"
          + "where\n"
          + "--  o_orderdate >= date '1996-10-01'\n"
          + "--  and o_orderdate < date '1996-10-01' + interval '3' month\n"
          + "--  and\n"
          + "  exists (\n"
          + "    select\n"
          + "      *\n"
          + "    from\n"
          + "      tpch.lineitem\n"
          + "    where\n"
          + "      l_orderkey = o_orderkey\n"
          + "      and l_commitdate < l_receiptdate\n"
          + "  )\n"
          + "group by\n"
          + "  o_orderpriority\n"
          + "order by\n"
          + "  o_orderpriority",

      // 05
      "select\n"
          + "  n.n_name,\n"
          + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue\n"
          + "\n"
          + "from\n"
          + "  tpch.customer c,\n"
          + "  tpch.orders o,\n"
          + "  tpch.lineitem l,\n"
          + "  tpch.supplier s,\n"
          + "  tpch.nation n,\n"
          + "  tpch.region r\n"
          + "\n"
          + "where\n"
          + "  c.c_custkey = o.o_custkey\n"
          + "  and l.l_orderkey = o.o_orderkey\n"
          + "  and l.l_suppkey = s.s_suppkey\n"
          + "  and c.c_nationkey = s.s_nationkey\n"
          + "  and s.s_nationkey = n.n_nationkey\n"
          + "  and n.n_regionkey = r.r_regionkey\n"
          + "  and r.r_name = 'EUROPE'\n"
          + "--  and o.o_orderdate >= date '1997-01-01'\n"
          + "--  and o.o_orderdate < date '1997-01-01' + interval '1' year\n"
          + "group by\n"
          + "  n.n_name\n"
          + "\n"
          + "order by\n"
          + "  revenue desc",

      // 06
      "select\n"
          + "  sum(l_extendedprice * l_discount) as revenue\n"
          + "from\n"
          + "  tpch.lineitem\n"
          + "where\n"
          + "--  l_shipdate >= date '1997-01-01'\n"
          + "--  and l_shipdate < date '1997-01-01' + interval '1' year\n"
          + "--  and\n"
          + "  l_discount between 0.03 - 0.01 and 0.03 + 0.01\n"
          + "  and l_quantity < 24",

      // 07
      "select\n"
          + "  supp_nation,\n"
          + "  cust_nation,\n"
          + "  l_year,\n"
          + "  sum(volume) as revenue\n"
          + "from\n"
          + "  (\n"
          + "    select\n"
          + "      n1.n_name as supp_nation,\n"
          + "      n2.n_name as cust_nation,\n"
          + "      extract(year from l.l_shipdate) as l_year,\n"
          + "      l.l_extendedprice * (1 - l.l_discount) as volume\n"
          + "    from\n"
          + "      tpch.supplier s,\n"
          + "      tpch.lineitem l,\n"
          + "      tpch.orders o,\n"
          + "      tpch.customer c,\n"
          + "      tpch.nation n1,\n"
          + "      tpch.nation n2\n"
          + "    where\n"
          + "      s.s_suppkey = l.l_suppkey\n"
          + "      and o.o_orderkey = l.l_orderkey\n"
          + "      and c.c_custkey = o.o_custkey\n"
          + "      and s.s_nationkey = n1.n_nationkey\n"
          + "      and c.c_nationkey = n2.n_nationkey\n"
          + "      and (\n"
          + "        (n1.n_name = 'EGYPT' and n2.n_name = 'UNITED STATES')\n"
          + "        or (n1.n_name = 'UNITED STATES' and n2.n_name = 'EGYPT')\n"
          + "      )\n"
          + "--      and l.l_shipdate between date '1995-01-01' and date '1996-12-31'\n"
          + "  ) as shipping\n"
          + "group by\n"
          + "  supp_nation,\n"
          + "  cust_nation,\n"
          + "  l_year\n"
          + "order by\n"
          + "  supp_nation,\n"
          + "  cust_nation,\n"
          + "  l_year",

      // 08
      "select\n"
          + "  o_year,\n"
          + "  sum(case\n"
          + "    when nation = 'EGYPT' then volume\n"
          + "    else 0\n"
          + "  end) / sum(volume) as mkt_share\n"
          + "from\n"
          + "  (\n"
          + "    select\n"
          + "      extract(year from o.o_orderdate) as o_year,\n"
          + "      l.l_extendedprice * (1 - l.l_discount) as volume,\n"
          + "      n2.n_name as nation\n"
          + "    from\n"
          + "      tpch.part p,\n"
          + "      tpch.supplier s,\n"
          + "      tpch.lineitem l,\n"
          + "      tpch.orders o,\n"
          + "      tpch.customer c,\n"
          + "      tpch.nation n1,\n"
          + "      tpch.nation n2,\n"
          + "      tpch.region r\n"
          + "    where\n"
          + "      p.p_partkey = l.l_partkey\n"
          + "      and s.s_suppkey = l.l_suppkey\n"
          + "      and l.l_orderkey = o.o_orderkey\n"
          + "      and o.o_custkey = c.c_custkey\n"
          + "      and c.c_nationkey = n1.n_nationkey\n"
          + "      and n1.n_regionkey = r.r_regionkey\n"
          + "      and r.r_name = 'MIDDLE EAST'\n"
          + "      and s.s_nationkey = n2.n_nationkey\n"
          + "      and o.o_orderdate between date '1995-01-01' and date '1996-12-31'\n"
          + "      and p.p_type = 'PROMO BRUSHED COPPER'\n"
          + "  ) as all_nations\n"
          + "group by\n"
          + "  o_year\n"
          + "order by\n"
          + "  o_year",

      // 09
      "select\n"
          + "  nation,\n"
          + "  o_year,\n"
          + "  sum(amount) as sum_profit\n"
          + "from\n"
          + "  (\n"
          + "    select\n"
          + "      n_name as nation,\n"
          + "      extract(year from o_orderdate) as o_year,\n"
          + "      l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity as amount\n"
          + "    from\n"
          + "      tpch.part p,\n"
          + "      tpch.supplier s,\n"
          + "      tpch.lineitem l,\n"
          + "      tpch.partsupp ps,\n"
          + "      tpch.orders o,\n"
          + "      tpch.nation n\n"
          + "    where\n"
          + "      s.s_suppkey = l.l_suppkey\n"
          + "      and ps.ps_suppkey = l.l_suppkey\n"
          + "      and ps.ps_partkey = l.l_partkey\n"
          + "      and p.p_partkey = l.l_partkey\n"
          + "      and o.o_orderkey = l.l_orderkey\n"
          + "      and s.s_nationkey = n.n_nationkey\n"
          + "      and p.p_name like '%yellow%'\n"
          + "  ) as profit\n"
          + "group by\n"
          + "  nation,\n"
          + "  o_year\n"
          + "order by\n"
          + "  nation,\n"
          + "  o_year desc",

      // 10
      "select\n"
          + "  c.c_custkey,\n"
          + "  c.c_name,\n"
          + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
          + "  c.c_acctbal,\n"
          + "  n.n_name,\n"
          + "  c.c_address,\n"
          + "  c.c_phone,\n"
          + "  c.c_comment\n"
          + "from\n"
          + "  tpch.customer c,\n"
          + "  tpch.orders o,\n"
          + "  tpch.lineitem l,\n"
          + "  tpch.nation n\n"
          + "where\n"
          + "  c.c_custkey = o.o_custkey\n"
          + "  and l.l_orderkey = o.o_orderkey\n"
          + "  and o.o_orderdate >= date '1994-03-01'\n"
          + "  and o.o_orderdate < date '1994-03-01' + interval '3' month\n"
          + "  and l.l_returnflag = 'R'\n"
          + "  and c.c_nationkey = n.n_nationkey\n"
          + "group by\n"
          + "  c.c_custkey,\n"
          + "  c.c_name,\n"
          + "  c.c_acctbal,\n"
          + "  c.c_phone,\n"
          + "  n.n_name,\n"
          + "  c.c_address,\n"
          + "  c.c_comment\n"
          + "order by\n"
          + "  revenue desc\n"
          + "limit 20",

      // 11
      "select\n"
          + "  ps.ps_partkey,\n"
          + "  sum(ps.ps_supplycost * ps.ps_availqty) as \"value\"\n"
          + "from\n"
          + "  tpch.partsupp ps,\n"
          + "  tpch.supplier s,\n"
          + "  tpch.nation n\n"
          + "where\n"
          + "  ps.ps_suppkey = s.s_suppkey\n"
          + "  and s.s_nationkey = n.n_nationkey\n"
          + "  and n.n_name = 'JAPAN'\n"
          + "group by\n"
          + "  ps.ps_partkey having\n"
          + "    sum(ps.ps_supplycost * ps.ps_availqty) > (\n"
          + "      select\n"
          + "        sum(ps.ps_supplycost * ps.ps_availqty) * 0.0001000000\n"
          + "      from\n"
          + "        tpch.partsupp ps,\n"
          + "        tpch.supplier s,\n"
          + "        tpch.nation n\n"
          + "      where\n"
          + "        ps.ps_suppkey = s.s_suppkey\n"
          + "        and s.s_nationkey = n.n_nationkey\n"
          + "        and n.n_name = 'JAPAN'\n"
          + "    )\n"
          + "order by\n"
          + "  \"value\" desc",

      // 12
      "select\n"
          + "  l.l_shipmode,\n"
          + "  sum(case\n"
          + "    when o.o_orderpriority = '1-URGENT'\n"
          + "      or o.o_orderpriority = '2-HIGH'\n"
          + "      then 1\n"
          + "    else 0\n"
          + "  end) as high_line_count,\n"
          + "  sum(case\n"
          + "    when o.o_orderpriority <> '1-URGENT'\n"
          + "      and o.o_orderpriority <> '2-HIGH'\n"
          + "      then 1\n"
          + "    else 0\n"
          + "  end) as low_line_count\n"
          + "from\n"
          + "  tpch.orders o,\n"
          + "  tpch.lineitem l\n"
          + "where\n"
          + "  o.o_orderkey = l.l_orderkey\n"
          + "  and l.l_shipmode in ('TRUCK', 'REG AIR')\n"
          + "  and l.l_commitdate < l.l_receiptdate\n"
          + "  and l.l_shipdate < l.l_commitdate\n"
          + "--  and l.l_receiptdate >= date '1994-01-01'\n"
          + "--  and l.l_receiptdate < date '1994-01-01' + interval '1' year\n"
          + "group by\n"
          + "  l.l_shipmode\n"
          + "order by\n"
          + "  l.l_shipmode",

      // 13
      "select\n"
          + "  c_count,\n"
          + "  count(*) as custdist\n"
          + "from\n"
          + "  (\n"
          + "    select\n"
          + "      c.c_custkey,\n"
          + "      count(o.o_orderkey)\n"
          + "    from\n"
          + "      tpch.customer c\n"
          + "      left outer join tpch.orders o\n"
          + "        on c.c_custkey = o.o_custkey\n"
          + "        and o.o_comment not like '%special%requests%'\n"
          + "    group by\n"
          + "      c.c_custkey\n"
          + "  ) as orders (c_custkey, c_count)\n"
          + "group by\n"
          + "  c_count\n"
          + "order by\n"
          + "  custdist desc,\n"
          + "  c_count desc",

      // 14
      "select\n"
          + "  100.00 * sum(case\n"
          + "    when p.p_type like 'PROMO%'\n"
          + "      then l.l_extendedprice * (1 - l.l_discount)\n"
          + "    else 0\n"
          + "  end) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue\n"
          + "from\n"
          + "  tpch.lineitem l,\n"
          + "  tpch.part p\n"
          + "where\n"
          + "  l.l_partkey = p.p_partkey\n"
          + "  and l.l_shipdate >= date '1994-08-01'\n"
          + "  and l.l_shipdate < date '1994-08-01' + interval '1' month",

      // 15
      "with revenue0 (supplier_no, total_revenue) as (\n"
          + "  select\n"
          + "    l_suppkey,\n"
          + "    sum(l_extendedprice * (1 - l_discount))\n"
          + "  from\n"
          + "    tpch.lineitem\n"
          + "  where\n"
          + "    l_shipdate >= date '1993-05-01'\n"
          + "    and l_shipdate < date '1993-05-01' + interval '3' month\n"
          + "  group by\n"
          + "    l_suppkey)\n"
          + "select\n"
          + "  s.s_suppkey,\n"
          + "  s.s_name,\n"
          + "  s.s_address,\n"
          + "  s.s_phone,\n"
          + "  r.total_revenue\n"
          + "from\n"
          + "  tpch.supplier s,\n"
          + "  revenue0 r\n"
          + "where\n"
          + "  s.s_suppkey = r.supplier_no\n"
          + "  and r.total_revenue = (\n"
          + "    select\n"
          + "      max(total_revenue)\n"
          + "    from\n"
          + "      revenue0\n"
          + "  )\n"
          + "order by\n"
          + "  s.s_suppkey",

      // 16
      "select\n"
          + "  p.p_brand,\n"
          + "  p.p_type,\n"
          + "  p.p_size,\n"
          + "  count(distinct ps.ps_suppkey) as supplier_cnt\n"
          + "from\n"
          + "  tpch.partsupp ps,\n"
          + "  tpch.part p\n"
          + "where\n"
          + "  p.p_partkey = ps.ps_partkey\n"
          + "  and p.p_brand <> 'Brand#21'\n"
          + "  and p.p_type not like 'MEDIUM PLATED%'\n"
          + "  and p.p_size in (38, 2, 8, 31, 44, 5, 14, 24)\n"
          + "  and ps.ps_suppkey not in (\n"
          + "    select\n"
          + "      s_suppkey\n"
          + "    from\n"
          + "      tpch.supplier\n"
          + "    where\n"
          + "      s_comment like '%Customer%Complaints%'\n"
          + "  )\n"
          + "group by\n"
          + "  p.p_brand,\n"
          + "  p.p_type,\n"
          + "  p.p_size\n"
          + "order by\n"
          + "  supplier_cnt desc,\n"
          + "  p.p_brand,\n"
          + "  p.p_type,\n"
          + "  p.p_size",

      // 17
      "select\n"
          + "  sum(l.l_extendedprice) / 7.0 as avg_yearly\n"
          + "from\n"
          + "  tpch.lineitem l,\n"
          + "  tpch.part p\n"
          + "where\n"
          + "  p.p_partkey = l.l_partkey\n"
          + "  and p.p_brand = 'Brand#13'\n"
          + "  and p.p_container = 'JUMBO CAN'\n"
          + "  and l.l_quantity < (\n"
          + "    select\n"
          + "      0.2 * avg(l2.l_quantity)\n"
          + "    from\n"
          + "      tpch.lineitem l2\n"
          + "    where\n"
          + "      l2.l_partkey = p.p_partkey\n"
          + "  )",

      // 18
      "select\n"
          + "  c.c_name,\n"
          + "  c.c_custkey,\n"
          + "  o.o_orderkey,\n"
          + "  o.o_orderdate,\n"
          + "  o.o_totalprice,\n"
          + "  sum(l.l_quantity)\n"
          + "from\n"
          + "  tpch.customer c,\n"
          + "  tpch.orders o,\n"
          + "  tpch.lineitem l\n"
          + "where\n"
          + "  o.o_orderkey in (\n"
          + "    select\n"
          + "      l_orderkey\n"
          + "    from\n"
          + "      tpch.lineitem\n"
          + "    group by\n"
          + "      l_orderkey having\n"
          + "        sum(l_quantity) > 313\n"
          + "  )\n"
          + "  and c.c_custkey = o.o_custkey\n"
          + "  and o.o_orderkey = l.l_orderkey\n"
          + "group by\n"
          + "  c.c_name,\n"
          + "  c.c_custkey,\n"
          + "  o.o_orderkey,\n"
          + "  o.o_orderdate,\n"
          + "  o.o_totalprice\n"
          + "order by\n"
          + "  o.o_totalprice desc,\n"
          + "  o.o_orderdate\n"
          + "limit 100",

      // 19
      "select\n"
          + "  sum(l.l_extendedprice* (1 - l.l_discount)) as revenue\n"
          + "from\n"
          + "  tpch.lineitem l,\n"
          + "  tpch.part p\n"
          + "where\n"
          + "  (\n"
          + "    p.p_partkey = l.l_partkey\n"
          + "    and p.p_brand = 'Brand#41'\n"
          + "    and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
          + "    and l.l_quantity >= 2 and l.l_quantity <= 2 + 10\n"
          + "    and p.p_size between 1 and 5\n"
          + "    and l.l_shipmode in ('AIR', 'AIR REG')\n"
          + "    and l.l_shipinstruct = 'DELIVER IN PERSON'\n"
          + "  )\n"
          + "  or\n"
          + "  (\n"
          + "    p.p_partkey = l.l_partkey\n"
          + "    and p.p_brand = 'Brand#13'\n"
          + "    and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
          + "    and l.l_quantity >= 14 and l.l_quantity <= 14 + 10\n"
          + "    and p.p_size between 1 and 10\n"
          + "    and l.l_shipmode in ('AIR', 'AIR REG')\n"
          + "    and l.l_shipinstruct = 'DELIVER IN PERSON'\n"
          + "  )\n"
          + "  or\n"
          + "  (\n"
          + "    p.p_partkey = l.l_partkey\n"
          + "    and p.p_brand = 'Brand#55'\n"
          + "    and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
          + "    and l.l_quantity >= 23 and l.l_quantity <= 23 + 10\n"
          + "    and p.p_size between 1 and 15\n"
          + "    and l.l_shipmode in ('AIR', 'AIR REG')\n"
          + "    and l.l_shipinstruct = 'DELIVER IN PERSON'\n"
          + "  )",

      // 20
      "select\n"
          + "  s.s_name,\n"
          + "  s.s_address\n"
          + "from\n"
          + "  tpch.supplier s,\n"
          + "  tpch.nation n\n"
          + "where\n"
          + "  s.s_suppkey in (\n"
          + "    select\n"
          + "      ps.ps_suppkey\n"
          + "    from\n"
          + "      tpch.partsupp ps\n"
          + "    where\n"
          + "      ps. ps_partkey in (\n"
          + "        select\n"
          + "          p.p_partkey\n"
          + "        from\n"
          + "          tpch.part p\n"
          + "        where\n"
          + "          p.p_name like 'antique%'\n"
          + "      )\n"
          + "      and ps.ps_availqty > (\n"
          + "        select\n"
          + "          0.5 * sum(l.l_quantity)\n"
          + "        from\n"
          + "          tpch.lineitem l\n"
          + "        where\n"
          + "          l.l_partkey = ps.ps_partkey\n"
          + "          and l.l_suppkey = ps.ps_suppkey\n"
          + "          and l.l_shipdate >= date '1993-01-01'\n"
          + "          and l.l_shipdate < date '1993-01-01' + interval '1' year\n"
          + "      )\n"
          + "  )\n"
          + "  and s.s_nationkey = n.n_nationkey\n"
          + "  and n.n_name = 'KENYA'\n"
          + "order by\n"
          + "  s.s_name",

      // 21
      "select\n"
          + "  s.s_name,\n"
          + "  count(*) as numwait\n"
          + "from\n"
          + "  tpch.supplier s,\n"
          + "  tpch.lineitem l1,\n"
          + "  tpch.orders o,\n"
          + "  tpch.nation n\n"
          + "where\n"
          + "  s.s_suppkey = l1.l_suppkey\n"
          + "  and o.o_orderkey = l1.l_orderkey\n"
          + "  and o.o_orderstatus = 'F'\n"
          + "  and l1.l_receiptdate > l1.l_commitdate\n"
          + "  and exists (\n"
          + "    select\n"
          + "      *\n"
          + "    from\n"
          + "      tpch.lineitem l2\n"
          + "    where\n"
          + "      l2.l_orderkey = l1.l_orderkey\n"
          + "      and l2.l_suppkey <> l1.l_suppkey\n"
          + "  )\n"
          + "  and not exists (\n"
          + "    select\n"
          + "      *\n"
          + "    from\n"
          + "      tpch.lineitem l3\n"
          + "    where\n"
          + "      l3.l_orderkey = l1.l_orderkey\n"
          + "      and l3.l_suppkey <> l1.l_suppkey\n"
          + "      and l3.l_receiptdate > l3.l_commitdate\n"
          + "  )\n"
          + "  and s.s_nationkey = n.n_nationkey\n"
          + "  and n.n_name = 'BRAZIL'\n"
          + "group by\n"
          + "  s.s_name\n"
          + "order by\n"
          + "  numwait desc,\n"
          + "  s.s_name\n"
          + "limit 100",

      // 22
      "select\n"
          + "  cntrycode,\n"
          + "  count(*) as numcust,\n"
          + "  sum(c_acctbal) as totacctbal\n"
          + "from\n"
          + "  (\n"
          + "    select\n"
          + "      substring(c_phone from 1 for 2) as cntrycode,\n"
          + "      c_acctbal\n"
          + "    from\n"
          + "      tpch.customer c\n"
          + "    where\n"
          + "      substring(c_phone from 1 for 2) in\n"
          + "        ('24', '31', '11', '16', '21', '20', '34')\n"
          + "      and c_acctbal > (\n"
          + "        select\n"
          + "          avg(c_acctbal)\n"
          + "        from\n"
          + "          tpch.customer\n"
          + "        where\n"
          + "          c_acctbal > 0.00\n"
          + "          and substring(c_phone from 1 for 2) in\n"
          + "            ('24', '31', '11', '16', '21', '20', '34')\n"
          + "      )\n"
          + "      and not exists (\n"
          + "        select\n"
          + "          *\n"
          + "        from\n"
          + "          tpch.orders o\n"
          + "        where\n"
          + "          o.o_custkey = c.c_custkey\n"
          + "      )\n"
          + "  ) as custsale\n"
          + "group by\n"
          + "  cntrycode\n"
          + "order by\n"
          + "  cntrycode"};

  static final List<String> QUERIES =
      ImmutableList.copyOf(QUERY_ARRAY);

  @Disabled("it's wasting time")
  @Test void testRegion() {
    with()
        .query("select * from tpch.region")
        .returnsUnordered(
            "R_REGIONKEY=0; R_NAME=AFRICA; R_COMMENT=lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to ",
            "R_REGIONKEY=1; R_NAME=AMERICA; R_COMMENT=hs use ironic, even requests. s",
            "R_REGIONKEY=2; R_NAME=ASIA; R_COMMENT=ges. thinly even pinto beans ca",
            "R_REGIONKEY=3; R_NAME=EUROPE; R_COMMENT=ly final courts cajole furiously final excuse",
            "R_REGIONKEY=4; R_NAME=MIDDLE EAST; R_COMMENT=uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl");
  }

  @Disabled("it's wasting time")
  @Test void testLineItem() {
    with()
        .query("select * from tpch.lineitem")
        .returnsCount(6001215);
  }

  @Disabled("it's wasting time")
  @Test void testOrders() {
    with()
        .query("select * from tpch.orders")
        .returnsCount(1500000);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1543">[CALCITE-1543]
   * Correlated scalar sub-query with multiple aggregates gives
   * AssertionError</a>. */
  @Disabled("planning succeeds, but gives OutOfMemoryError during execution")
  @Test void testDecorrelateScalarAggregate() {
    final String sql = "select sum(l_extendedprice)\n"
        + "from lineitem, part\n"
        + "where\n"
        + "     p_partkey = l_partkey\n"
        + "     and l_quantity > (\n"
        + "       select avg(l_quantity)\n"
        + "       from lineitem\n"
        + "       where l_partkey = p_partkey\n"
        + "    )\n";
    with().query(sql).runs();
  }

  @Disabled("it's wasting time")
  @Test void testCustomer() {
    with()
        .query("select * from tpch.customer")
        .returnsCount(150000);
  }

  private CalciteAssert.AssertThat with() {
    // Only run on JDK 1.7 or higher. The io.airlift.tpch library requires it.
    return CalciteAssert.model(TPCH_MODEL).enable(ENABLE);
  }

  /** Tests the customer table with scale factor 5. */
  @Disabled("it's wasting time")
  @Test void testCustomer5() {
    with()
        .query("select * from tpch_5.customer")
        .returnsCount(750000);
  }

  @Test void testQuery01() {
    checkQuery(1);
  }

  @Test void testQuery02() {
    checkQuery(2);
  }

  @Test void testQuery02Conversion() {
    query(2)
        .convertMatches(relNode -> {
          String s = RelOptUtil.toString(relNode);
          assertThat(s, not(containsString("Correlator")));
        });
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7319">[CALCITE-7319]
   * FILTER_INTO_JOIN rule loses correlation variable context in HepPlanner</a>. */
  @Test public void optimizeQuery2()
      throws SqlParseException, ValidationException, RelConversionException {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    TpchSchema tpchSchema = new TpchSchema(1.0, 0, 1, false);
    rootSchema.add("TPCH", tpchSchema);
    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(rootSchema)
        .build();

    Planner planner = Frameworks.getPlanner(config);

    SqlNode parsed = planner.parse(QUERY_ARRAY[1]);
    SqlNode validated = planner.validate(parsed);
    RelRoot root = planner.rel(validated);

    final HepProgramBuilder builder = HepProgram.builder();
    builder.addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
    builder.addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);
    builder.addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
    builder.addRuleInstance(CoreRules.FILTER_CORRELATE);
    // We are checking that this rule can push some predicates into joins
    // Prior to fixing [CALCITE-7319] (second improvement) many joins below
    // had a condition=[true].
    builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);

    RelOptPlanner optPlanner = new HepPlanner(builder.build());
    optPlanner.setRoot(root.rel);
    RelNode rel = optPlanner.findBestExp();
    final String expected = "LogicalSort(sort0=[$0], sort1=[$2], sort2=[$1], sort3=[$3], "
        + "dir0=[DESC], dir1=[ASC], dir2=[ASC], dir3=[ASC], fetch=[100])\n"
        + "  LogicalProject(S_ACCTBAL=[$14], S_NAME=[$10], N_NAME=[$22], P_PARTKEY=[$0], "
        + "P_MFGR=[$2], S_ADDRESS=[$11], S_PHONE=[$13], S_COMMENT=[$15])\n"
        + "    LogicalProject(P_PARTKEY=[$0], P_NAME=[$1], P_MFGR=[$2], P_BRAND=[$3], P_TYPE=[$4], "
        + "P_SIZE=[$5], P_CONTAINER=[$6], P_RETAILPRICE=[$7], P_COMMENT=[$8], S_SUPPKEY=[$9], "
        + "S_NAME=[$10], S_ADDRESS=[$11], S_NATIONKEY=[$12], S_PHONE=[$13], S_ACCTBAL=[$14], "
        + "S_COMMENT=[$15], PS_PARTKEY=[$16], PS_SUPPKEY=[$17], PS_AVAILQTY=[$18], "
        + "PS_SUPPLYCOST=[$19], PS_COMMENT=[$20], N_NATIONKEY=[$21], N_NAME=[$22], "
        + "N_REGIONKEY=[$23], N_COMMENT=[$24], R_REGIONKEY=[$25], R_NAME=[$26], R_COMMENT=[$27])\n"
        + "      LogicalFilter(condition=[=($19, $28)])\n"
        + "        LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
        + "          LogicalJoin(condition=[=($23, $25)], joinType=[inner])\n"
        + "            LogicalJoin(condition=[=($12, $21)], joinType=[inner])\n"
        + "              LogicalJoin(condition=[AND(=($0, $16), =($9, $17))], joinType=[inner])\n"
        + "                LogicalJoin(condition=[true], joinType=[inner])\n"
        + "                  LogicalFilter(condition=[AND(=(CAST($5):INTEGER, 41), "
        + "LIKE($4, '%NICKEL'))])\n"
        + "                    LogicalTableScan(table=[[TPCH, PART]])\n"
        + "                  LogicalTableScan(table=[[TPCH, SUPPLIER]])\n"
        + "                LogicalTableScan(table=[[TPCH, PARTSUPP]])\n"
        + "              LogicalTableScan(table=[[TPCH, NATION]])\n"
        + "            LogicalFilter(condition=[=(CAST($1):VARCHAR, 'EUROPE')])\n"
        + "              LogicalTableScan(table=[[TPCH, REGION]])\n"
        + "          LogicalAggregate(group=[{}], EXPR$0=[MIN($0)])\n"
        + "            LogicalProject(PS_SUPPLYCOST=[$3])\n"
        + "              LogicalFilter(condition=[=($cor0.P_PARTKEY, $0)])\n"
        + "                LogicalJoin(condition=[=($14, $16)], joinType=[inner])\n"
        + "                  LogicalJoin(condition=[=($8, $12)], joinType=[inner])\n"
        + "                    LogicalJoin(condition=[=($5, $1)], joinType=[inner])\n"
        + "                      LogicalTableScan(table=[[TPCH, PARTSUPP]])\n"
        + "                      LogicalTableScan(table=[[TPCH, SUPPLIER]])\n"
        + "                    LogicalTableScan(table=[[TPCH, NATION]])\n"
        + "                  LogicalFilter(condition=[=(CAST($1):VARCHAR, 'EUROPE')])\n"
        + "                    LogicalTableScan(table=[[TPCH, REGION]])";
    assertThat(rel.explain(), containsStringLinux(expected));
  }

  @Test void testQuery03() {
    checkQuery(3);
  }

  @Test void testQuery04() {
    checkQuery(4);
  }

  @Test void testQuery05() {
    checkQuery(5);
  }

  @Test void testQuery06() {
    checkQuery(6);
  }

  @Test void testQuery07() {
    checkQuery(7);
  }

  @Test void testQuery08() {
    checkQuery(8);
  }

  @Test void testQuery09() {
    checkQuery(9);
  }

  @Test void testQuery10() {
    checkQuery(10);
  }

  @Test void testQuery11() {
    checkQuery(11);
  }

  @Test void testQuery12() {
    checkQuery(12);
  }

  @Test void testQuery13() {
    checkQuery(13);
  }

  @Test void testQuery14() {
    checkQuery(14);
  }

  @Test void testQuery15() {
    checkQuery(15);
  }

  @Test void testQuery16() {
    checkQuery(16);
  }

  @Test void testQuery17() {
    checkQuery(17);
  }

  @Test void testQuery18() {
    checkQuery(18);
  }

  // a bit slow
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @Disabled("Too slow, more than 5 min")
  @Test void testQuery19() {
    checkQuery(19);
  }

  @Test void testQuery20() {
    checkQuery(20);
  }

  @Test void testQuery21() {
    checkQuery(21);
  }

  @Test void testQuery22() {
    checkQuery(22);
  }

  private void checkQuery(int i) {
    query(i).runs();
  }

  /** Runs with query #i.
   *
   * @param i Ordinal of query, per the benchmark, 1-based */
  private CalciteAssert.AssertQuery query(int i) {
    return with()
        .query(QUERIES.get(i - 1).replace("tpch.", "tpch_01."));
  }
}
