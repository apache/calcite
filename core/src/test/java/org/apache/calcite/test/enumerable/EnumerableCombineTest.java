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
   * <p>The Combine operator returns results in a wide format where each query
   * is a column (QUERY_0, QUERY_1, etc.) and each row contains a struct (map)
   * for each query. Queries with fewer rows have null values for additional rows.
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
            "QUERY_0={name=Bill}; QUERY_1={name=Sales}",
            "QUERY_0={name=Sebastian}; QUERY_1={name=Marketing}",
            "QUERY_0={name=Theodore}; QUERY_1={name=HR}");
  }

  /**
   * Test that executes two queries with different row counts.
   * Query 1: Select all employee names (4 rows)
   * Query 2: Select all department names (3 rows)
   *
   * <p>Since Query 2 returns fewer rows, QUERY1 is null for the extra rows.
   */
  @Test void testCombineDifferentRowCounts() {
    tester(new HrSchema())
        .withRel(
            builder -> {
              // Query 1: SELECT name FROM emps (4 rows)
              builder.scan("s", "emps")
                  .project(builder.field("name"));

              // Query 2: SELECT name FROM depts (3 rows)
              builder.scan("s", "depts")
                  .project(builder.field("name"));

              // Combine both queries
              return builder.combine(2).build();
            })
        .returnsUnordered(
            "QUERY_0={name=Bill}; QUERY_1={name=Sales}",
            "QUERY_0={name=Eric}; QUERY_1={name=Marketing}",
            "QUERY_0={name=Sebastian}; QUERY_1={name=HR}",
            "QUERY_0={name=Theodore}; QUERY_1=null");
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
            "QUERY_0={empid=100, name=Bill}; QUERY_1={deptno=10, name=Sales}",
            "QUERY_0={empid=150, name=Sebastian}; QUERY_1={deptno=30, name=Marketing}",
            "QUERY_0={empid=110, name=Theodore}; QUERY_1={deptno=40, name=HR}");
  }

  /**
   * Test that executes two queries returning different numbers of rows.
   * Query 1: Select name from depts (3 rows)
   * Query 2: Select empid, name, deptno from emps where deptno = 10 (3 rows)
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
            "QUERY_0={name=Sales}; QUERY_1={empid=100, name=Bill, deptno=10}",
            "QUERY_0={name=Marketing}; QUERY_1={empid=150, name=Sebastian, deptno=10}",
            "QUERY_0={name=HR}; QUERY_1={empid=110, name=Theodore, deptno=10}");
  }

  private CalciteAssert.AssertThat tester(Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .withSchema("s", new ReflectiveSchema(schema));
  }
}
