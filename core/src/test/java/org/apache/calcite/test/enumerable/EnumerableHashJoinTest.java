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

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.JdbcTest;

import org.junit.Test;

/**
 * Unit test for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableHashJoin}.
 */
public class EnumerableHashJoinTest {

  @Test public void innerJoin() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e join depts "
                + "d on e.deptno=d.deptno")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], "
            + "name=[$t2], dept=[$t4])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales",
            "empid=150; name=Sebastian; dept=Sales");
  }

  @Test public void innerJoinWithPredicate() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e join depts d"
                + " on e.deptno=d.deptno and e.empid<150 and e.empid>d.deptno")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], expr#5=[>($t0,"
            + " $t3)], empid=[$t0], name=[$t2], dept=[$t4], $condition=[$t5])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[150], "
            + "expr#6=[<($t0, $t5)], proj#0..2=[{exprs}], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales");
  }

  @Test public void leftOuterJoin() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e  left outer "
                + "join depts d on e.deptno=d.deptno")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], "
            + "name=[$t2], dept=[$t4])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[left])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales",
            "empid=150; name=Sebastian; dept=Sales",
            "empid=200; name=Eric; dept=null");
  }

  @Test public void rightOuterJoin() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e  right outer "
                + "join depts d on e.deptno=d.deptno")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], "
            + "name=[$t2], dept=[$t4])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[right])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales",
            "empid=150; name=Sebastian; dept=Sales",
            "empid=null; name=null; dept=Marketing",
            "empid=null; name=null; dept=HR");
  }

  @Test public void leftOuterJoinWithPredicate() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e left outer "
                + "join depts d on e.deptno=d.deptno and e.empid<150 and e"
                + ".empid>d.deptno")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], "
            + "name=[$t2], dept=[$t4])\n"
            + "  EnumerableHashJoin(condition=[AND(=($1, $3), <($0, 150), >"
            + "($0, $3))], joinType=[left])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales",
            "empid=150; name=Sebastian; dept=null",
            "empid=200; name=Eric; dept=null");
  }

  @Test public void rightOuterJoinWithPredicate() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e right outer "
                + "join depts d on e.deptno=d.deptno and e.empid<150")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], "
            + "name=[$t2], dept=[$t4])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[right])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[150], "
            + "expr#6=[<($t0, $t5)], proj#0..2=[{exprs}], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales",
            "empid=null; name=null; dept=Marketing",
            "empid=null; name=null; dept=HR");
  }


  @Test public void semiJoin() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "SELECT d.deptno, d.name FROM depts d WHERE d.deptno in (SELECT e.deptno FROM emps e)")
        .explainContains("EnumerableHashJoin(condition=[=($0, $3)], "
            + "joinType=[semi])\n"
            + "  EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "    EnumerableTableScan(table=[[s, depts]])\n"
            + "  EnumerableTableScan(table=[[s, emps]])")
        .returnsUnordered(
            "deptno=10; name=Sales");
  }

  @Test public void semiJoinWithPredicate() {
    tester(false, new JdbcTest.HrSchema())
        .query("?")
        .withRel(
            // Retrieve employees with the top salary in their department. Equivalent SQL:
            //   SELECT e.name, e.salary FROM emps e
            //   WHERE  EXISTS (
            //     SELECT 1 FROM emps e2
            //     WHERE e.deptno = e2.deptno AND e2.salary > e.salary)
            builder -> builder
                .scan("s", "emps").as("e")
                .scan("s", "emps").as("e2")
                .semiJoin(
                    builder.and(
                        builder.equals(
                            builder.field(2, "e", "deptno"),
                            builder.field(2, "e2", "deptno")),
                        builder.call(
                            SqlStdOperatorTable.GREATER_THAN,
                            builder.field(2, "e2", "salary"),
                            builder.field(2, "e", "salary"))))
                .project(
                    builder.field("name"),
                    builder.field("salary"))
                .build())
        .returnsUnordered(
            "name=Bill; salary=10000.0",
            "name=Sebastian; salary=7000.0");
  }

  private CalciteAssert.AssertThat tester(boolean forceDecorrelate,
      Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, forceDecorrelate)
        .withSchema("s", new ReflectiveSchema(schema));
  }
}

// End EnumerableHashJoinTest.java
