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

/** Tests for {@link org.apache.calcite.adapter.enumerable.EnumerableAggregate}. */
public class EnumerableAggregateTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7828">[CALCITE-7828]
   * Aggregate whose grouping sets include () returns no rows on empty
   * input</a>.
   *
   * <p>ROLLUP(deptno) is GROUPING SETS ((deptno), ()), and the () set yields
   * one row even when the input is empty, as a query with no GROUP BY does
   * (CALCITE-1059). */
  @Test void rollupOnEmptyInput() {
    tester()
        .query("select deptno, count(*) as c\n"
            + "from emps where false group by rollup(deptno)")
        .explainContains("EnumerableAggregate(group=[{0}], groups=[[{0}, {}]], c=[COUNT()])\n"
            + "  EnumerableValues(tuples=[[]])")
        .returnsUnordered("deptno=null; c=0");
  }

  /** As {@link #rollupOnEmptyInput()}, but the input is empty at run time
   * (not at plan time), so the plan still contains a filter. */
  @Test void rollupOnEmptyRuntimeInput() {
    tester()
        .query("select deptno, count(*) as c\n"
            + "from emps where deptno > 100 group by rollup(deptno)")
        .returnsUnordered("deptno=null; c=0");
  }

  /** As {@link #rollupOnEmptyInput()}, with CUBE and more than one grouping
   * column. CUBE(deptno, commission) is GROUPING SETS ((deptno, commission),
   * (deptno), (commission), ()). */
  @Test void cubeOnEmptyInput() {
    tester()
        .query("select deptno, commission, count(*) as c\n"
            + "from emps where false group by cube(deptno, commission)")
        .returnsUnordered("deptno=null; commission=null; c=0");
  }

  /** As {@link #rollupOnEmptyInput()}, with GROUPING SETS. */
  @Test void groupingSetsWithEmptyGroupOnEmptyInput() {
    tester()
        .query("select deptno, count(*) as c\n"
            + "from emps where false group by grouping sets ((deptno), ())")
        .explainContains("EnumerableAggregate(group=[{0}], groups=[[{0}, {}]], c=[COUNT()])\n"
            + "  EnumerableValues(tuples=[[]])")
        .returnsUnordered("deptno=null; c=0");
  }

  /** A grouping set other than () yields no row when the input is empty. */
  @Test void groupingSetsWithoutEmptyGroupOnEmptyInput() {
    tester()
        .query("select deptno, count(*) as c\n"
            + "from emps where false group by grouping sets ((deptno), (commission))")
        .returnsCount(0);
  }

  /** Sanity check: on non-empty input, ROLLUP produces a row per group,
   * plus the grand-total row. */
  @Test void rollupOnNonEmptyInput() {
    tester()
        .query("select deptno, count(*) as c\n"
            + "from emps group by rollup(deptno)")
        .returnsUnordered(
            "deptno=10; c=3",
            "deptno=20; c=1",
            "deptno=null; c=4");
  }

  /** As {@link #rollupOnEmptyInput()}, with aggregate functions that return
   * NULL on empty input (SUM), and GROUPING, which reads the indicator of
   * the synthetic empty-group key. */
  @Test void rollupOnEmptyInputSumGrouping() {
    tester()
        .query("select deptno, grouping(deptno) as g, sum(salary) as s\n"
            + "from emps where false group by rollup(deptno)")
        .returnsUnordered("deptno=null; g=1; s=null");
  }

  private CalciteAssert.AssertThat tester() {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .withSchema("s", new ReflectiveSchema(new HrSchema()));
  }
}
