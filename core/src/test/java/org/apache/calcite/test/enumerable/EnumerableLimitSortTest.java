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
import org.apache.calcite.util.TestUtil;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableLimitSort}. */
public class EnumerableLimitSortTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5730">[CALCITE-5730]
   * First nulls can be dropped by EnumerableLimitSort with offset</a>. */
  @Test void nullsFirstWithLimitAndOffset() {
    tester("select commission from emps order by commission nulls first limit 1 offset 1 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4])\n"
            + "  EnumerableLimitSort(sort0=[$4], dir0=[ASC-nulls-first], offset=[1], fetch=[1])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered("commission=null");
  }

  @Test void nullsLastWithLimitAndOffset() {
    tester("select commission from emps order by commission desc nulls last limit 8 offset 10 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4])\n"
            + "  EnumerableLimitSort(sort0=[$4], dir0=[DESC-nulls-last], offset=[10], fetch=[8])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=1000",
            "commission=1000",
            "commission=500",
            "commission=500",
            "commission=500",
            "commission=500",
            "commission=500",
            "commission=500");
  }

  @Test void nullsFirstWithLimit() {
    tester("select commission from emps order by commission nulls first limit 13 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4])\n"
            + "  EnumerableLimitSort(sort0=[$4], dir0=[ASC-nulls-first], fetch=[13])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=250");
  }

  @Test void nullsLastWithLimit() {
    tester("select commission from emps order by commission nulls last limit 5 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4])\n"
            + "  EnumerableLimitSort(sort0=[$4], dir0=[ASC], fetch=[5])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=250",
            "commission=250",
            "commission=250",
            "commission=250",
            "commission=250");
  }

  @Test void multiOrderByColumnsWithLimitAndOffset() {
    tester("select commission, salary, empid from emps"
        + " order by commission nulls first, salary asc, empid desc limit 4 offset 6 ")
        .explainContains(
            "EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4], salary=[$t3], empid=[$t0])\n"
            + "  EnumerableLimitSort(sort0=[$4], sort1=[$3], sort2=[$0], dir0=[ASC-nulls-first], dir1=[ASC], dir2=[DESC], offset=[6], fetch=[4])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=null; salary=7000.0; empid=23",
                    "commission=null; salary=7000.0; empid=19",
                    "commission=null; salary=7000.0; empid=15",
                    "commission=null; salary=7000.0; empid=11");
  }

  @Test void multiOrderByColumnsWithLimit() {
    tester("select commission, deptno from emps"
        + " order by commission desc nulls first, deptno asc limit 13 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4], deptno=[$t1])\n"
            + "  EnumerableLimitSort(sort0=[$4], sort1=[$1], dir0=[DESC], dir1=[ASC], fetch=[13])\n"
            + "    EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "commission=null; deptno=8",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=60",
            "commission=null; deptno=80",
            "commission=1000; deptno=10");
  }

  @Test void multiOrderByColumnsNullsLastWithLimitAndOffset() {
    tester("select commission, salary from emps"
        + " order by commission desc nulls last, salary limit 6 offset 12 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4], salary=[$t3])\n"
            + "  EnumerableLimitSort(sort0=[$4], sort1=[$3], dir0=[DESC-nulls-last], dir1=[ASC], offset=[12], fetch=[6])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=500; salary=8000.0",
            "commission=500; salary=8000.0",
            "commission=500; salary=8000.0",
            "commission=500; salary=8000.0",
            "commission=500; salary=8000.0",
            "commission=500; salary=8000.0");
  }

  @Test void multiOrderByColumnsNullsLastWithLimit() {
    tester("select commission, empid from emps"
        + " order by commission nulls last, empid desc limit 4 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4], empid=[$t0])\n"
            + "  EnumerableLimitSort(sort0=[$4], sort1=[$0], dir0=[ASC], dir1=[DESC], fetch=[4])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=250; empid=48",
            "commission=250; empid=44",
            "commission=250; empid=40",
            "commission=250; empid=36");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7624">[CALCITE-7624]
   * Support BigDecimal for FETCH and OFFSET in Enumerable</a>. */
  @Test void limitWithoutOffsetUsesBigDecimalDefaultOffset() {
    tester("select empid from emps order by empid limit 2")
        .explainContains("EnumerableLimitSort(sort0=[$0], dir0=[ASC], "
            + "fetch=[2])")
        .returnsOrdered(
            "empid=1",
            "empid=2");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7624">[CALCITE-7624]
   * Support BigDecimal for FETCH and OFFSET in Enumerable</a>. */
  @Test void offsetWithoutLimitUsesBigDecimalDefaultFetch() {
    tester("select empid from emps where empid <= 4 order by empid offset 2")
        .explainContains("EnumerableLimitSort(sort0=[$0], dir0=[ASC], "
            + "offset=[2])")
        .returnsOrdered(
            "empid=3",
            "empid=4");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7624">[CALCITE-7624]
   * Support BigDecimal for FETCH and OFFSET in Enumerable</a>. */
  @Test void fractionalOffsetAndFetchUseIndependentRowCounts() {
    tester("select empid from emps where empid <= 4 order by empid "
        + "offset 1.5 fetch next 1.5 rows only")
        .explainContains("EnumerableLimitSort(sort0=[$0], dir0=[ASC], "
            + "offset=[1.5:DECIMAL(2, 1)], fetch=[1.5:DECIMAL(2, 1)])")
        .returnsOrdered(
            "empid=3",
            "empid=4");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7624">[CALCITE-7624]
   * Support BigDecimal for FETCH and OFFSET in Enumerable</a>. */
  @Test void dynamicParametersWithNegativeBigDecimalFetch() throws Exception {
    final String sql = "select empid from emps where empid <= 2 order by empid "
        + "offset ? fetch next ? rows only";
    tester(sql)
        .explainContains("EnumerableLimitSort(sort0=[$0], dir0=[ASC], "
            + "offset=[?0], fetch=[?1])");
    failsWithNegativeParameter(sql, p -> {
      p.setBigDecimal(1, BigDecimal.ZERO);
      p.setBigDecimal(2, BigDecimal.valueOf(-1));
    }, "FETCH must not be negative");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7624">[CALCITE-7624]
   * Support BigDecimal for FETCH and OFFSET in Enumerable</a>. */
  @Test void dynamicParametersWithNegativeBigDecimalOffset() throws Exception {
    final String sql = "select empid from emps where empid <= 3 order by empid "
        + "offset ? fetch next ? rows only";
    tester(sql)
        .explainContains("EnumerableLimitSort(sort0=[$0], dir0=[ASC], "
            + "offset=[?0], fetch=[?1])");
    failsWithNegativeParameter(sql, p -> {
      p.setBigDecimal(1, BigDecimal.valueOf(-1));
      p.setBigDecimal(2, BigDecimal.valueOf(2));
    }, "OFFSET must not be negative");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7624">[CALCITE-7624]
   * Support BigDecimal for FETCH and OFFSET in Enumerable</a>. */
  @Test void dynamicParametersWithNegativeBigDecimalLimit() throws Exception {
    final String sql = "select empid from emps where empid <= 2 order by empid "
        + "limit ? offset ?";
    tester(sql)
        .explainContains("EnumerableLimitSort(sort0=[$0], dir0=[ASC], "
            + "offset=[?1], fetch=[?0])");
    failsWithNegativeParameter(sql, p -> {
      p.setBigDecimal(1, BigDecimal.valueOf(-1));
      p.setBigDecimal(2, BigDecimal.ZERO);
    }, "FETCH must not be negative");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7624">[CALCITE-7624]
   * Support BigDecimal for FETCH and OFFSET in Enumerable</a>. */
  @Test void dynamicParametersWithBigDecimal() {
    tester("select commission from emps order by commission nulls last "
        + "offset ? fetch next ? rows only")
        .explainContains("EnumerableLimitSort(sort0=[$0], dir0=[ASC], "
            + "offset=[?0], fetch=[?1])\n"
            + "  EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .consumesPreparedStatement(p -> {
          p.setBigDecimal(1, BigDecimal.valueOf(1));
          p.setBigDecimal(2, BigDecimal.valueOf(2));
        })
        .returnsOrdered(
            "commission=250",
            "commission=250");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7624">[CALCITE-7624]
   * Support BigDecimal for FETCH and OFFSET in Enumerable</a>. */
  @Test void dynamicParametersWithBigDecimalAboveIntegerMax() {
    tester("select commission from emps order by commission nulls last "
        + "offset ? fetch next ? rows only")
        .explainContains("EnumerableLimitSort(sort0=[$0], dir0=[ASC], "
            + "offset=[?0], fetch=[?1])")
        .consumesPreparedStatement(p -> {
          p.setBigDecimal(1,
              BigDecimal.valueOf(Integer.MAX_VALUE).add(BigDecimal.ONE));
          p.setBigDecimal(2, BigDecimal.ONE);
        })
        .returnsOrdered();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7624">[CALCITE-7624]
   * Support BigDecimal for FETCH and OFFSET in Enumerable</a>. */
  @Test void offsetLiteralAboveIntegerMax() {
    tester("select commission from emps order by commission nulls last "
        + "offset 2147483648 fetch next 1 rows only")
        .explainContains("EnumerableLimitSort(sort0=[$4], dir0=[ASC], "
            + "offset=[2147483648:BIGINT], fetch=[1])")
        .returnsOrdered();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7624">[CALCITE-7624]
   * Support BigDecimal for FETCH and OFFSET in Enumerable</a>. */
  @Test void dynamicParametersWithBigDecimalAboveLongMax() {
    tester("select empid from emps where empid <= 2 order by empid "
        + "offset ? fetch next ? rows only")
        .explainContains("EnumerableLimitSort(sort0=[$0], dir0=[ASC], "
            + "offset=[?0], fetch=[?1])")
        .consumesPreparedStatement(p -> {
          p.setBigDecimal(1, BigDecimal.ZERO);
          p.setBigDecimal(2,
              BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE));
        })
        .returnsOrdered(
            "empid=1",
            "empid=2");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7624">[CALCITE-7624]
   * Support BigDecimal for FETCH and OFFSET in Enumerable</a>. */
  @Test void fetchLiteralAboveLongMax() {
    tester("select empid from emps where empid <= 2 order by empid "
        + "offset 0 fetch next 9223372036854775808 rows only")
        .explainContains("EnumerableLimitSort(sort0=[$0], dir0=[ASC], "
            + "offset=[0], fetch=[9223372036854775808:DECIMAL(19, 0)])")
        .returnsOrdered(
            "empid=1",
            "empid=2");
  }

  private CalciteAssert.AssertQuery tester(String sqlQuery) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, false)
        .withSchema("s", new ReflectiveSchema(new HrSchemaBig()))
        .query(sqlQuery)
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.removeRule(EnumerableRules.ENUMERABLE_SORT_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
        });
  }

  private void failsWithNegativeParameter(String sql,
      CalciteAssert.PreparedStatementConsumer consumer, String message)
      throws Exception {
    CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, false)
        .withSchema("s", new ReflectiveSchema(new HrSchemaBig()))
        .doWithConnection(connection -> {
          try (Hook.Closeable ignored =
                   Hook.PLANNER.addThread((Consumer<RelOptPlanner>) planner -> {
                     planner.removeRule(EnumerableRules.ENUMERABLE_SORT_RULE);
                     planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
                   });
               PreparedStatement p = connection.prepareStatement(sql)) {
            consumer.accept(p);
            final SQLException e = assertThrows(SQLException.class, p::executeQuery);
            assertThat(e.getMessage(), containsString(message));
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }
}
