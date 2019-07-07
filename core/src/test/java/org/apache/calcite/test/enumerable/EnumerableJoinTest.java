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
 * Unit tests for the different Enumerable Join implementations.
 */
public class EnumerableJoinTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2968">[CALCITE-2968]
   * New AntiJoin relational expression</a>. */
  @Test public void equiAntiJoin() {
    tester(false, new JdbcTest.HrSchema())
        .query("?")
        .withRel(
            // Retrieve departments without employees. Equivalent SQL:
            //   SELECT d.deptno, d.name FROM depts d
            //   WHERE NOT EXISTS (SELECT 1 FROM emps e WHERE e.deptno = d.deptno)
            builder -> builder
                .scan("s", "depts").as("d")
                .scan("s", "emps").as("e")
                .antiJoin(
                    builder.equals(
                        builder.field(2, "d", "deptno"),
                        builder.field(2, "e", "deptno")))
                .project(
                    builder.field("deptno"),
                    builder.field("name"))
                .build())
        .returnsUnordered(
            "deptno=30; name=Marketing",
            "deptno=40; name=HR");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2968">[CALCITE-2968]
   * New AntiJoin relational expression</a>. */
  @Test public void nonEquiAntiJoin() {
    tester(false, new JdbcTest.HrSchema())
        .query("?")
        .withRel(
            // Retrieve employees with the top salary in their department. Equivalent SQL:
            //   SELECT e.name, e.salary FROM emps e
            //   WHERE NOT EXISTS (
            //     SELECT 1 FROM emps e2
            //     WHERE e.deptno = e2.deptno AND e2.salary > e.salary)
            builder -> builder
                .scan("s", "emps").as("e")
                .scan("s", "emps").as("e2")
                .antiJoin(
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
            "name=Theodore; salary=11500.0",
            "name=Eric; salary=8000.0");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2968">[CALCITE-2968]
   * New AntiJoin relational expression</a>. */
  @Test public void equiAntiJoinWithNullValues() {
    final Integer salesDeptNo = 10;
    tester(false, new JdbcTest.HrSchema())
        .query("?")
        .withRel(
            // Retrieve employees from any department other than Sales (deptno 10) whose
            // commission is different from any Sales employee commission. Since there
            // is a Sales employee with null commission, the goal is to validate that antiJoin
            // behaves as a NOT EXISTS (and returns results), and not as a NOT IN (which would
            // not return any result due to its null handling). Equivalent SQL:
            //   SELECT empOther.empid, empOther.name FROM emps empOther
            //   WHERE empOther.deptno <> 10 AND NOT EXISTS
            //     (SELECT 1 FROM emps empSales
            //      WHERE empSales.deptno = 10 AND empSales.commission = empOther.commission)
            builder -> builder
                .scan("s", "emps").as("empOther")
                .filter(
                    builder.notEquals(
                        builder.field("empOther", "deptno"),
                        builder.literal(salesDeptNo)))
                .scan("s", "emps").as("empSales")
                .filter(
                    builder.equals(
                        builder.field("empSales", "deptno"),
                        builder.literal(salesDeptNo)))
                .antiJoin(
                    builder.equals(
                        builder.field(2, "empOther", "commission"),
                        builder.field(2, "empSales", "commission")))
                .project(
                    builder.field("empid"),
                    builder.field("name"))
                .build())
        .returnsUnordered("empid=200; name=Eric");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3170">[CALCITE-3170]
   * ANTI join on conditions push down generates wrong plan</a>. */
  @Test public void testCanNotPushAntiJoinConditionsToLeft() {
    tester(false, new JdbcTest.HrSchema())
        .query("?").withRel(
            // build a rel equivalent to sql:
            // select * from emps
            // where emps.deptno
            // not in (select depts.deptno from depts where emps.name = 'ddd')

            // Use `equals` instead of `is not distinct from` only for testing.
            builder -> builder
                .scan("s", "emps")
                .scan("s", "depts")
                .antiJoin(
                    builder.equals(
                        builder.field(2, 0, "deptno"),
                        builder.field(2, 1, "deptno")),
                    builder.equals(builder.field(2, 0, "name"),
                        builder.literal("ddd")))
                .project(builder.field(0))
                .build()
    ).returnsUnordered(
        "empid=100",
        "empid=110",
        "empid=150",
        "empid=200");
  }

  private CalciteAssert.AssertThat tester(boolean forceDecorrelate,
      Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, forceDecorrelate)
        .withSchema("s", new ReflectiveSchema(schema));
  }
}

// End EnumerableJoinTest.java
