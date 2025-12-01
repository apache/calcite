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
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.schemata.hr.HrSchema;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link org.apache.calcite.adapter.enumerable.EnumerableCombine}.
 */
class EnumerableCombineTest {

  /**
   * Test that executes two simple queries combined.
   * Query 1: Select employee names from department 10
   * Query 2: Select department names
   *
   * <p>The Combine operator returns results in a structured format where each
   * query's results are grouped: {@code QUERY_0={...}; QUERY_1={...}}
   */
  @Test void testCombineTwoQueries() {
    tester(new HrSchema())
        .withRel(
            builder -> {
              // Query 1: SELECT name FROM emps WHERE deptno = 10
              builder.scan("s", "emps")
                  .filter(
                      builder.equals(
                          builder.field("deptno"),
                          builder.literal(10)))
                  .project(builder.field("name"));

              // Query 2: SELECT name FROM depts
              builder.scan("s", "depts")
                  .project(builder.field("name"));

              // Combine both queries
              return builder.combine(2).build();
            })
        .returnsOrdered(
            "QUERY=[{name=Bill}, {name=Sebastian}, {name=Theodore}]",
            "QUERY=[{name=Sales}, {name=Marketing}, {name=HR}]");
  }

  /**
   * Test that executes two queries with aggregates.
   * Query 1: Count of employees
   * Query 2: Average salary of employees
   */
  @Test void testCombineWithAggregates() {
    tester(new HrSchema())
        .withRel(
            builder -> {
              // Query 1: SELECT deptno, COUNT(*) AS emp_count FROM emps GROUP BY deptno
              builder.scan("s", "emps")
                  .aggregate(builder.groupKey("deptno"),
                      builder.count().as("emp_count"));

              // Query 2: SELECT AVG(salary) AS avg_salary FROM emps
              builder.scan("s", "emps")
                  .aggregate(builder.groupKey(),
                      builder.avg(builder.field("salary")).as("avg_salary"));

              // Combine both queries
              return builder.combine(2).build();
            })
        .returnsUnordered(
            "QUERY=[{deptno=20, emp_count=1}, {deptno=10, emp_count=3}]",
            "QUERY=[{avg_salary=9125.0}]");
  }

  /**
   * Test that executes two queries with multiple columns each.
   * Query 1: Select empid and name from employees in department 10
   * Query 2: Select deptno and name from departments
   */
  @Test void testCombineMultipleColumns() {
    tester(new HrSchema())
        .withRel(
            builder -> {
              // Query 1: SELECT empid, name FROM emps WHERE deptno = 10
              builder.scan("s", "emps")
                  .filter(
                      builder.equals(
                          builder.field("deptno"),
                          builder.literal(10)))
                  .project(
                      builder.field("empid"),
                      builder.field("name"));

              // Query 2: SELECT deptno, name FROM depts
              builder.scan("s", "depts")
                  .project(
                      builder.field("deptno"),
                      builder.field("name"));

              // Combine both queries
              return builder.combine(2).build();
            })
        .returnsUnordered(
            "QUERY=[{empid=100, name=Bill}, {empid=150, name=Sebastian}, {empid=110, name=Theodore}]",
            "QUERY=[{deptno=10, name=Sales}, {deptno=30, name=Marketing}, {deptno=40, name=HR}]");
  }

  /**
   * Test that executes two queries returning different numbers of columns.
   * Query 1: Select name (1 column)
   * Query 2: Select empid, name, deptno (3 columns)
   */
  @Test void testCombineDifferentColumnCounts() {
    tester(new HrSchema())
        .withRel(
            builder -> {
              // Query 1: SELECT name FROM depts (1 column)
              builder.scan("s", "depts")
                  .project(builder.field("name"));

              // Query 2: SELECT empid, name, deptno FROM emps WHERE deptno = 10 (3 columns)
              builder.scan("s", "emps")
                  .filter(
                      builder.equals(
                          builder.field("deptno"),
                          builder.literal(10)))
                  .project(
                      builder.field("empid"),
                      builder.field("name"),
                      builder.field("deptno"));

              // Combine both queries
              return builder.combine(2).build();
            })
        .returnsUnordered(
            "QUERY=[{name=Sales}, {name=Marketing}, {name=HR}]",
            "QUERY=[{empid=100, name=Bill, deptno=10}, {empid=150, name=Sebastian, deptno=10}, "
                + "{empid=110, name=Theodore, deptno=10}]");
  }

  private CalciteAssert.AssertThat tester(Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .withSchema("s", new ReflectiveSchema(schema));
  }
}
