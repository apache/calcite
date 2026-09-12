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
package org.apache.calcite.test;

import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.test.CalciteAssert.Config;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@code *} as a grouping element in GROUPING SETS, ROLLUP and CUBE,
 * yielding non-aggregated (detail) rows.
 *
 * @see org.apache.calcite.sql.validate.SqlConformance#isGroupingSetsStarAllowed()
 */
class GroupingSetsStarTest {
  /** {@code GROUPING SETS ((deptno), (*))} returns department totals (ename
   * NULL) plus one detail row per employee. */
  @Test void testGroupingSetsStar() {
    CalciteAssert.that()
        .with(Config.SCOTT)
        .with(SqlConformanceEnum.LENIENT)
        .query("select deptno, ename, sum(sal) as sumsal\n"
            + "from emp\n"
            + "group by grouping sets ((deptno), (*))\n"
            + "order by deptno, ename nulls first")
        .returnsUnordered(
            "DEPTNO=10; ENAME=null; SUMSAL=8750.00",
            "DEPTNO=10; ENAME=CLARK; SUMSAL=2450.00",
            "DEPTNO=10; ENAME=KING; SUMSAL=5000.00",
            "DEPTNO=10; ENAME=MILLER; SUMSAL=1300.00",
            "DEPTNO=20; ENAME=null; SUMSAL=10875.00",
            "DEPTNO=20; ENAME=ADAMS; SUMSAL=1100.00",
            "DEPTNO=20; ENAME=FORD; SUMSAL=3000.00",
            "DEPTNO=20; ENAME=JONES; SUMSAL=2975.00",
            "DEPTNO=20; ENAME=SCOTT; SUMSAL=3000.00",
            "DEPTNO=20; ENAME=SMITH; SUMSAL=800.00",
            "DEPTNO=30; ENAME=null; SUMSAL=9400.00",
            "DEPTNO=30; ENAME=ALLEN; SUMSAL=1600.00",
            "DEPTNO=30; ENAME=BLAKE; SUMSAL=2850.00",
            "DEPTNO=30; ENAME=JAMES; SUMSAL=950.00",
            "DEPTNO=30; ENAME=MARTIN; SUMSAL=1250.00",
            "DEPTNO=30; ENAME=TURNER; SUMSAL=1500.00",
            "DEPTNO=30; ENAME=WARD; SUMSAL=1250.00");
  }

  /** {@code ROLLUP (deptno, *)} expands (standard semantics) to detail rows,
   * department totals and a grand total. */
  @Test void testRollupStar() {
    CalciteAssert.that()
        .with(Config.SCOTT)
        .with(SqlConformanceEnum.LENIENT)
        .query("select deptno, ename, sum(sal) as sumsal\n"
            + "from emp\n"
            + "group by rollup (deptno, *)\n"
            + "order by deptno nulls last, ename nulls first")
        .returnsUnordered(
            "DEPTNO=10; ENAME=null; SUMSAL=8750.00",
            "DEPTNO=10; ENAME=CLARK; SUMSAL=2450.00",
            "DEPTNO=10; ENAME=KING; SUMSAL=5000.00",
            "DEPTNO=10; ENAME=MILLER; SUMSAL=1300.00",
            "DEPTNO=20; ENAME=null; SUMSAL=10875.00",
            "DEPTNO=20; ENAME=ADAMS; SUMSAL=1100.00",
            "DEPTNO=20; ENAME=FORD; SUMSAL=3000.00",
            "DEPTNO=20; ENAME=JONES; SUMSAL=2975.00",
            "DEPTNO=20; ENAME=SCOTT; SUMSAL=3000.00",
            "DEPTNO=20; ENAME=SMITH; SUMSAL=800.00",
            "DEPTNO=30; ENAME=null; SUMSAL=9400.00",
            "DEPTNO=30; ENAME=ALLEN; SUMSAL=1600.00",
            "DEPTNO=30; ENAME=BLAKE; SUMSAL=2850.00",
            "DEPTNO=30; ENAME=JAMES; SUMSAL=950.00",
            "DEPTNO=30; ENAME=MARTIN; SUMSAL=1250.00",
            "DEPTNO=30; ENAME=TURNER; SUMSAL=1500.00",
            "DEPTNO=30; ENAME=WARD; SUMSAL=1250.00",
            "DEPTNO=null; ENAME=null; SUMSAL=29025.00");
  }

  /** {@code GROUPING} distinguishes the detail set (ename grouped, g=0) from
   * the department set (ename not grouped, g=1). */
  @Test void testGroupingSetsStarWithGrouping() {
    CalciteAssert.that()
        .with(Config.SCOTT)
        .with(SqlConformanceEnum.LENIENT)
        .query("select g, count(*) as cnt\n"
            + "from (\n"
            + "  select grouping(ename) as g\n"
            + "  from emp\n"
            + "  group by grouping sets ((deptno), (*)))\n"
            + "group by g\n"
            + "order by g")
        .returnsUnordered(
            "G=0; CNT=14", // detail rows: ename grouped
            "G=1; CNT=3");  // department totals: ename not grouped
  }
}
