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
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.JdbcTest;

import org.junit.Test;

/**
 * Unit test for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableCorrelate}.
 */
public class EnumerableCorrelateTest {
  @Test public void simpleCorrelateDecorrelated() {
    tester(true, new JdbcTest.HrSchema())
        .query(
            "select empid, name from emps e where exists (select 1 from depts d where d.deptno=e.deptno)")
        .explainContains(
            "EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], name=[$t2])\n"
            + "  EnumerableSemiJoin(condition=[=($1, $5)], joinType=[inner])\n"
            + "    EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[true], deptno0=[$t0], $f0=[$t5])\n"
            + "      EnumerableJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "        EnumerableAggregate(group=[{0}])\n"
            + "          EnumerableCalc(expr#0..4=[{inputs}], deptno=[$t1])\n"
            + "            EnumerableTableScan(table=[[s, emps]])\n"
            + "        EnumerableTableScan(table=[[s, depts]])")
        .returnsUnordered(
            "empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");

  }

  @Test public void simpleCorrelate() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select empid, name from emps e where exists (select 1 from depts d where d.deptno=e.deptno)")
        .explainContains(
            "EnumerableCalc(expr#0..5=[{inputs}], expr#6=[IS NOT NULL($t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "  EnumerableCorrelate(correlation=[$cor0], joinType=[LEFT], requiredColumns=[{1}])\n"
            + "    EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableAggregate(group=[{}], agg#0=[MIN($0)])\n"
            + "      EnumerableCalc(expr#0..3=[{inputs}], expr#4=[true], expr#5=[$cor0], expr#6=[$t5.deptno], expr#7=[=($t0, $t6)], $f0=[$t4], $condition=[$t7])\n"
            + "        EnumerableTableScan(table=[[s, depts]])")
        .returnsUnordered(
            "empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");

  }

  private CalciteAssert.AssertThat tester(boolean forceDecorrelate,
      Object schema) {
    return CalciteAssert.that()
        .with("lex", "JAVA")
        .with("forceDecorrelate", Boolean.toString(forceDecorrelate))
        .withSchema("s", new ReflectiveSchema(schema));
  }
}

// End EnumerableCorrelateTest.java
