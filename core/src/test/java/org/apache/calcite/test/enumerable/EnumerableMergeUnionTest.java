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
package org.apache.calcite.test.enumerable;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.schemata.hr.HrSchemaBig;

import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

/**
 * Unit test for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableMergeUnion}.
 */
class EnumerableMergeUnionTest {

  @Test void mergeUnionAllOrderByEmpid() {
    tester(false,
        new HrSchemaBig(),
        "select * from (select empid, name from emps where name like 'G%' union all select empid, name from emps where name like '%l') order by empid")
        .explainContains("EnumerableMergeUnion(all=[true])\n"
            + "  EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['G%'], expr#6=[LIKE($t2, $t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "  EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['%l'], expr#6=[LIKE($t2, $t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "empid=1; name=Bill",
            "empid=6; name=Guy",
            "empid=10; name=Gabriel",
            "empid=10; name=Gabriel",
            "empid=12; name=Paul",
            "empid=29; name=Anibal",
            "empid=40; name=Emmanuel",
            "empid=45; name=Pascal");
  }

  @Test void mergeUnionOrderByEmpid() {
    tester(false,
        new HrSchemaBig(),
        "select * from (select empid, name from emps where name like 'G%' union select empid, name from emps where name like '%l') order by empid")
        .explainContains("EnumerableMergeUnion(all=[false])\n"
            + "  EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['G%'], expr#6=[LIKE($t2, $t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "  EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['%l'], expr#6=[LIKE($t2, $t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "empid=1; name=Bill",
            "empid=6; name=Guy",
            "empid=10; name=Gabriel",
            "empid=12; name=Paul",
            "empid=29; name=Anibal",
            "empid=40; name=Emmanuel",
            "empid=45; name=Pascal");
  }

  @Test void mergeUnionAllOrderByName() {
    tester(false,
        new HrSchemaBig(),
        "select * from (select empid, name from emps where name like 'G%' union all select empid, name from emps where name like '%l') order by name")
        .explainContains("EnumerableMergeUnion(all=[true])\n"
            + "  EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['G%'], expr#6=[LIKE($t2, $t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "  EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['%l'], expr#6=[LIKE($t2, $t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "empid=29; name=Anibal",
            "empid=1; name=Bill",
            "empid=40; name=Emmanuel",
            "empid=10; name=Gabriel",
            "empid=10; name=Gabriel",
            "empid=6; name=Guy",
            "empid=45; name=Pascal",
            "empid=12; name=Paul");
  }

  @Test void mergeUnionOrderByName() {
    tester(false,
        new HrSchemaBig(),
        "select * from (select empid, name from emps where name like 'G%' union select empid, name from emps where name like '%l') order by name")
        .explainContains("EnumerableMergeUnion(all=[false])\n"
            + "  EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['G%'], expr#6=[LIKE($t2, $t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "  EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['%l'], expr#6=[LIKE($t2, $t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "empid=29; name=Anibal",
            "empid=1; name=Bill",
            "empid=40; name=Emmanuel",
            "empid=10; name=Gabriel",
            "empid=6; name=Guy",
            "empid=45; name=Pascal",
            "empid=12; name=Paul");
  }

  @Test void mergeUnionSingleColumnOrderByName() {
    tester(false,
        new HrSchemaBig(),
        "select * from (select name from emps where name like 'G%' union select name from emps where name like '%l') order by name")
        .explainContains("EnumerableMergeUnion(all=[false])\n"
            + "  EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['G%'], expr#6=[LIKE($t2, $t5)], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "  EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['%l'], expr#6=[LIKE($t2, $t5)], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "name=Anibal",
            "name=Bill",
            "name=Emmanuel",
            "name=Gabriel",
            "name=Guy",
            "name=Pascal",
            "name=Paul");
  }

  @Test void mergeUnionOrderByNameWithLimit() {
    tester(false,
        new HrSchemaBig(),
        "select * from (select empid, name from emps where name like 'G%' union select empid, name from emps where name like '%l') order by name limit 3")
        .explainContains("EnumerableLimit(fetch=[3])\n"
            + "  EnumerableMergeUnion(all=[false])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], name=[$t2])\n"
            + "      EnumerableLimitSort(sort0=[$2], dir0=[ASC], fetch=[3])\n"
            + "        EnumerableCalc(expr#0..4=[{inputs}], expr#5=['G%'], expr#6=[LIKE($t2, $t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
            + "          EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], name=[$t2])\n"
            + "      EnumerableLimitSort(sort0=[$2], dir0=[ASC], fetch=[3])\n"
            + "        EnumerableCalc(expr#0..4=[{inputs}], expr#5=['%l'], expr#6=[LIKE($t2, $t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
            + "          EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "empid=29; name=Anibal",
            "empid=1; name=Bill",
            "empid=40; name=Emmanuel");
  }

  @Test void mergeUnionOrderByNameWithOffset() {
    tester(false,
        new HrSchemaBig(),
        "select * from (select empid, name from emps where name like 'G%' union select empid, name from emps where name like '%l') order by name offset 2")
        .explainContains("EnumerableLimit(offset=[2])\n"
            + "  EnumerableMergeUnion(all=[false])\n"
            + "    EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "      EnumerableCalc(expr#0..4=[{inputs}], expr#5=['G%'], expr#6=[LIKE($t2, $t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "        EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "      EnumerableCalc(expr#0..4=[{inputs}], expr#5=['%l'], expr#6=[LIKE($t2, $t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "        EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "empid=40; name=Emmanuel",
            "empid=10; name=Gabriel",
            "empid=6; name=Guy",
            "empid=45; name=Pascal",
            "empid=12; name=Paul");
  }

  @Test void mergeUnionOrderByNameWithLimitAndOffset() {
    tester(false,
        new HrSchemaBig(),
        "select * from (select empid, name from emps where name like 'G%' union select empid, name from emps where name like '%l') order by name limit 3 offset 2")
        .explainContains("EnumerableLimit(offset=[2], fetch=[3])\n"
            + "  EnumerableMergeUnion(all=[false])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], name=[$t2])\n"
            + "      EnumerableLimitSort(sort0=[$2], dir0=[ASC], fetch=[5])\n"
            + "        EnumerableCalc(expr#0..4=[{inputs}], expr#5=['G%'], expr#6=[LIKE($t2, $t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
            + "          EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], name=[$t2])\n"
            + "      EnumerableLimitSort(sort0=[$2], dir0=[ASC], fetch=[5])\n"
            + "        EnumerableCalc(expr#0..4=[{inputs}], expr#5=['%l'], expr#6=[LIKE($t2, $t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
            + "          EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "empid=40; name=Emmanuel",
            "empid=10; name=Gabriel",
            "empid=6; name=Guy");
  }

  @Test void mergeUnionAllOrderByCommissionAscNullsFirstAndNameDesc() {
    tester(false,
        new HrSchemaBig(),
        "select * from (select commission, name from emps where name like 'R%' union all select commission, name from emps where name like '%y%') order by commission asc nulls first, name desc")
        .explainContains("EnumerableMergeUnion(all=[true])\n"
            + "  EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC-nulls-first], dir1=[DESC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['R%'], expr#6=[LIKE($t2, $t5)], commission=[$t4], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "  EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC-nulls-first], dir1=[DESC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['%y%'], expr#6=[LIKE($t2, $t5)], commission=[$t4], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "commission=null; name=Taylor",
            "commission=null; name=Riyad",
            "commission=null; name=Riyad",
            "commission=null; name=Ralf",
            "commission=250; name=Seohyun",
            "commission=250; name=Hyuna",
            "commission=250; name=Andy",
            "commission=500; name=Kylie",
            "commission=500; name=Guy");
  }

  @Test void mergeUnionOrderByCommissionAscNullsFirstAndNameDesc() {
    tester(false,
        new HrSchemaBig(),
        "select * from (select commission, name from emps where name like 'R%' union select commission, name from emps where name like '%y%') order by commission asc nulls first, name desc")
        .explainContains("EnumerableMergeUnion(all=[false])\n"
            + "  EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC-nulls-first], dir1=[DESC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['R%'], expr#6=[LIKE($t2, $t5)], commission=[$t4], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "  EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC-nulls-first], dir1=[DESC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['%y%'], expr#6=[LIKE($t2, $t5)], commission=[$t4], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "commission=null; name=Taylor",
            "commission=null; name=Riyad",
            "commission=null; name=Ralf",
            "commission=250; name=Seohyun",
            "commission=250; name=Hyuna",
            "commission=250; name=Andy",
            "commission=500; name=Kylie",
            "commission=500; name=Guy");
  }

  @Test void mergeUnionAllOrderByCommissionAscNullsLastAndNameDesc() {
    tester(false,
        new HrSchemaBig(),
        "select * from (select commission, name from emps where name like 'R%' union all select commission, name from emps where name like '%y%') order by commission asc nulls last, name desc")
        .explainContains("EnumerableMergeUnion(all=[true])\n"
            + "  EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['R%'], expr#6=[LIKE($t2, $t5)], commission=[$t4], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "  EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['%y%'], expr#6=[LIKE($t2, $t5)], commission=[$t4], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "commission=250; name=Seohyun",
            "commission=250; name=Hyuna",
            "commission=250; name=Andy",
            "commission=500; name=Kylie",
            "commission=500; name=Guy",
            "commission=null; name=Taylor",
            "commission=null; name=Riyad",
            "commission=null; name=Riyad",
            "commission=null; name=Ralf");
  }

  @Test void mergeUnionOrderByCommissionAscNullsLastAndNameDesc() {
    tester(false,
        new HrSchemaBig(),
        "select * from (select commission, name from emps where name like 'R%' union select commission, name from emps where name like '%y%') order by commission asc nulls last, name desc")
        .explainContains("EnumerableMergeUnion(all=[false])\n"
            + "  EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['R%'], expr#6=[LIKE($t2, $t5)], commission=[$t4], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "  EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=['%y%'], expr#6=[LIKE($t2, $t5)], commission=[$t4], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "commission=250; name=Seohyun",
            "commission=250; name=Hyuna",
            "commission=250; name=Andy",
            "commission=500; name=Kylie",
            "commission=500; name=Guy",
            "commission=null; name=Taylor",
            "commission=null; name=Riyad",
            "commission=null; name=Ralf");
  }

  private CalciteAssert.AssertQuery tester(boolean forceDecorrelate,
      Object schema, String sqlQuery) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, forceDecorrelate)
        .withSchema("s", new ReflectiveSchema(schema))
        .query(sqlQuery)
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // Force UNION to be implemented via EnumerableMergeUnion
          planner.removeRule(EnumerableRules.ENUMERABLE_UNION_RULE);
          // Allow EnumerableLimitSort optimization
          planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
        });
  }
}
