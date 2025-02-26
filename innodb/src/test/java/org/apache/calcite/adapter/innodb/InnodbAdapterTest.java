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
package org.apache.calcite.adapter.innodb;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Sources;

import com.alibaba.innodb.java.reader.util.Utils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Tests for the {@code org.apache.calcite.adapter.innodb} package.
 *
 * <p>Will read InnoDB data file {@code emp.ibd} and {@code dept.ibd}.
 */
public class InnodbAdapterTest {

  private static final ImmutableMap<String, String> INNODB_MODEL =
      ImmutableMap.of("model",
          Sources.of(
                  requireNonNull(
                      InnodbAdapterTest.class.getResource("/model.json"),
                      "url"))
              .file().getAbsolutePath());

  @Test void testSelectCount() {
    sql("SELECT * FROM \"EMP\"")
        .returnsCount(14);
  }

  @Test void testSelectAll() {
    sql("SELECT * FROM \"EMP\"")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbTableScan(table=[[test, EMP]])\n")
        .returns(all());
  }

  @Test void testSelectAll2() {
    sql("SELECT * FROM \"DEPT\"")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbTableScan(table=[[test, DEPT]])\n")
        .returns("DEPTNO=10; DNAME=ACCOUNTING; LOC=NEW YORK\n"
            + "DEPTNO=20; DNAME=RESEARCH; LOC=DALLAS\n"
            + "DEPTNO=30; DNAME=SALES; LOC=CHICAGO\n"
            + "DEPTNO=40; DNAME=OPERATIONS; LOC=BOSTON\n");
  }

  @Test void testSelectAllProjectSomeFields() {
    sql("SELECT EMPNO,ENAME FROM \"EMP\"")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7369; ENAME=SMITH\n"
            + "EMPNO=7499; ENAME=ALLEN\n"
            + "EMPNO=7521; ENAME=WARD\n"
            + "EMPNO=7566; ENAME=JONES\n"
            + "EMPNO=7654; ENAME=MARTIN\n"
            + "EMPNO=7698; ENAME=BLAKE\n"
            + "EMPNO=7782; ENAME=CLARK\n"
            + "EMPNO=7788; ENAME=SCOTT\n"
            + "EMPNO=7839; ENAME=KING\n"
            + "EMPNO=7844; ENAME=TURNER\n"
            + "EMPNO=7876; ENAME=ADAMS\n"
            + "EMPNO=7900; ENAME=JAMES\n"
            + "EMPNO=7902; ENAME=FORD\n"
            + "EMPNO=7934; ENAME=MILLER\n");
  }

  @Test void testSelectAllOrderByAsc() {
    sql("SELECT * FROM \"EMP\" ORDER BY EMPNO ASC")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbSort(sort0=[$0], dir0=[ASC])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(all());
  }

  @Test void testSelectAllOrderByDesc() {
    sql("SELECT * FROM \"EMP\" ORDER BY EMPNO DESC")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbSort(sort0=[$0], dir0=[DESC])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(allReversed());
  }

  @Test void testSelectAllProjectSomeFieldsOrderByDesc() {
    sql("SELECT EMPNO,ENAME FROM \"EMP\" ORDER BY EMPNO DESC")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1])\n"
            + "    InnodbSort(sort0=[$0], dir0=[DESC])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7934; ENAME=MILLER\n"
            + "EMPNO=7902; ENAME=FORD\n"
            + "EMPNO=7900; ENAME=JAMES\n"
            + "EMPNO=7876; ENAME=ADAMS\n"
            + "EMPNO=7844; ENAME=TURNER\n"
            + "EMPNO=7839; ENAME=KING\n"
            + "EMPNO=7788; ENAME=SCOTT\n"
            + "EMPNO=7782; ENAME=CLARK\n"
            + "EMPNO=7698; ENAME=BLAKE\n"
            + "EMPNO=7654; ENAME=MARTIN\n"
            + "EMPNO=7566; ENAME=JONES\n"
            + "EMPNO=7521; ENAME=WARD\n"
            + "EMPNO=7499; ENAME=ALLEN\n"
            + "EMPNO=7369; ENAME=SMITH\n");
  }

  @Test void testSelectByPrimaryKey() {
    for (Integer empno : EMPNO_MAP.keySet()) {
      sql("SELECT * FROM \"EMP\" WHERE EMPNO = " + empno)
          .explainContains("PLAN=InnodbToEnumerableConverter\n"
              + "  InnodbFilter(condition=[(PK_POINT_QUERY, index=PRIMARY_KEY, EMPNO="
              + empno + ")])\n"
              + "    InnodbTableScan(table=[[test, EMP]])\n")
          .returns(some(empno));
    }
  }

  @Test void testSelectByPrimaryKeyNothing() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO = 0")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_POINT_QUERY, index=PRIMARY_KEY, EMPNO=0)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns("");
  }

  @Test void testSelectByPrimaryKeyProjectAllFields() {
    sql("SELECT ENAME,EMPNO,JOB,AGE,MGR,HIREDATE,SAL,COMM,DEPTNO,EMAIL,"
        + "CREATE_DATETIME,CREATE_TIME,UPSERT_TIME FROM \"EMP\" WHERE EMPNO = 7499")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(ENAME=[$1], EMPNO=[$0], JOB=[$2], AGE=[$3], MGR=[$4], "
            + "HIREDATE=[$5], SAL=[$6], COMM=[$7], DEPTNO=[$8], EMAIL=[$9], "
            + "CREATE_DATETIME=[$10], CREATE_TIME=[$11], UPSERT_TIME=[$12])\n"
            + "    InnodbFilter(condition=[(PK_POINT_QUERY, index=PRIMARY_KEY, EMPNO=7499)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("ENAME=ALLEN; EMPNO=7499; JOB=SALESMAN; AGE=24; MGR=7698; HIREDATE=1981-02-20; "
            + "SAL=1600.00; COMM=300.00; DEPTNO=30; EMAIL=allen@calcite; "
            + "CREATE_DATETIME=2018-04-09 09:00:00; CREATE_TIME=09:00:00; "
            + "UPSERT_TIME=" + expectedLocalTime("2018-04-09 09:00:00") + "\n");
  }

  @Test void testSelectByPrimaryKeyProjectSomeFields() {
    sql("SELECT EMPNO,AGE,HIREDATE FROM \"EMP\" WHERE EMPNO = 7902")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], AGE=[$3], HIREDATE=[$5])\n"
            + "    InnodbFilter(condition=[(PK_POINT_QUERY, index=PRIMARY_KEY, EMPNO=7902)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7902; AGE=28; HIREDATE=1981-12-03\n");
  }

  @Test void testSelectByPrimaryKeyRangeQueryGt() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO > 7600")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, EMPNO>7600)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(someEmpnoGt(7600));
  }

  @Test void testSelectByPrimaryKeyRangeQueryGt2() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO > 7654")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, EMPNO>7654)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(someEmpnoGt(7654));
  }

  @Test void testSelectByPrimaryKeyRangeQueryGtNothing() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO > 10000")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, EMPNO>10000)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns("");
  }

  @Test void testSelectByPrimaryKeyRangeQueryGte() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO >= 7600")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, EMPNO>=7600)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(someEmpnoGte(7600));
  }

  @Test void testSelectByPrimaryKeyRangeQueryGte2() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO >= 7654")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, EMPNO>=7654)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(someEmpnoGte(7654));
  }

  @Test void testSelectByPrimaryKeyRangeQueryGte3() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO >= 10000")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, EMPNO>=10000)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns("");
  }

  @Test void testSelectByPrimaryKeyRangeQueryLt() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO < 7800")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, EMPNO<7800)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(someEmpnoLt(7800));
  }

  @Test void testSelectByPrimaryKeyRangeQueryLt2() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO < 7839")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, EMPNO<7839)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(someEmpnoLt(7839));
  }

  @Test void testSelectByPrimaryKeyRangeQueryLtNothing() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO < 5000")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, EMPNO<5000)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns("");
  }

  @Test void testSelectByPrimaryKeyRangeQueryLte() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO <= 7800")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, EMPNO<=7800)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(someEmpnoLte(7800));
  }

  @Test void testSelectByPrimaryKeyRangeQueryLte2() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO <= 7839")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, EMPNO<=7839)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(someEmpnoLte(7839));
  }

  @Test void testSelectByPrimaryKeyRangeQueryLteNothing() {
    sql("SELECT * FROM \"EMP\" WHERE EMPNO <= 5000")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, EMPNO<=5000)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns("");
  }

  @Test void testSelectByPrimaryKeyRangeQueryGtLtProjectSomeFields() {
    sql("SELECT EMPNO,ENAME FROM \"EMP\" WHERE EMPNO > 7600 AND EMPNO < 7900")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1])\n"
            + "    InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, "
            + "EMPNO>7600, EMPNO<7900)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7654; ENAME=MARTIN\n"
            + "EMPNO=7698; ENAME=BLAKE\n"
            + "EMPNO=7782; ENAME=CLARK\n"
            + "EMPNO=7788; ENAME=SCOTT\n"
            + "EMPNO=7839; ENAME=KING\n"
            + "EMPNO=7844; ENAME=TURNER\n"
            + "EMPNO=7876; ENAME=ADAMS\n");
  }

  @Test void testSelectByPrimaryKeyRangeQueryGtLteProjectSomeFields() {
    sql("SELECT EMPNO,ENAME FROM \"EMP\" WHERE EMPNO > 7600 AND EMPNO <= 7900")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1])\n"
            + "    InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, "
            + "EMPNO>7600, EMPNO<=7900)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7654; ENAME=MARTIN\n"
            + "EMPNO=7698; ENAME=BLAKE\n"
            + "EMPNO=7782; ENAME=CLARK\n"
            + "EMPNO=7788; ENAME=SCOTT\n"
            + "EMPNO=7839; ENAME=KING\n"
            + "EMPNO=7844; ENAME=TURNER\n"
            + "EMPNO=7876; ENAME=ADAMS\n"
            + "EMPNO=7900; ENAME=JAMES\n");
  }

  @Test void testSelectByPrimaryKeyRangeQueryGteLtProjectSomeFields() {
    sql("SELECT EMPNO,ENAME FROM \"EMP\" WHERE EMPNO >= 7369 AND EMPNO < 7900")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1])\n"
            + "    InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, "
            + "EMPNO>=7369, EMPNO<7900)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7369; ENAME=SMITH\n"
            + "EMPNO=7499; ENAME=ALLEN\n"
            + "EMPNO=7521; ENAME=WARD\n"
            + "EMPNO=7566; ENAME=JONES\n"
            + "EMPNO=7654; ENAME=MARTIN\n"
            + "EMPNO=7698; ENAME=BLAKE\n"
            + "EMPNO=7782; ENAME=CLARK\n"
            + "EMPNO=7788; ENAME=SCOTT\n"
            + "EMPNO=7839; ENAME=KING\n"
            + "EMPNO=7844; ENAME=TURNER\n"
            + "EMPNO=7876; ENAME=ADAMS\n");
  }

  @Test void testSelectByPrimaryKeyRangeQueryGteLteProjectSomeFields() {
    sql("SELECT EMPNO,ENAME FROM \"EMP\" WHERE EMPNO >= 7788 AND EMPNO <= 7900")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1])\n"
            + "    InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, "
            + "EMPNO>=7788, EMPNO<=7900)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7788; ENAME=SCOTT\n"
            + "EMPNO=7839; ENAME=KING\n"
            + "EMPNO=7844; ENAME=TURNER\n"
            + "EMPNO=7876; ENAME=ADAMS\n"
            + "EMPNO=7900; ENAME=JAMES\n");
  }

  @Test void testSelectByPrimaryKeyRangeQueryGtLtNothing() {
    sql("SELECT EMPNO,ENAME FROM \"EMP\" WHERE EMPNO > 7370 AND EMPNO < 7400")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1])\n"
            + "    InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, "
            + "EMPNO>7370, EMPNO<7400)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("");
  }

  @Test void testSelectByPrimaryKeyRangeQueryGteLteEqualsProjectSomeFields() {
    for (Integer empno : EMPNO_MAP.keySet()) {
      sql("SELECT EMPNO FROM \"EMP\" WHERE EMPNO >= " + empno
          + " AND EMPNO <= " + empno)
          .explainContains("PLAN=InnodbToEnumerableConverter\n"
              + "  InnodbProject(EMPNO=[$0])\n"
              + "    InnodbFilter(condition=[(PK_POINT_QUERY, index=PRIMARY_KEY, EMPNO="
              + empno + ")])\n"
              + "      InnodbTableScan(table=[[test, EMP]])\n")
          .returns("EMPNO=" + empno + "\n");
    }
  }

  @Test void testSelectByPrimaryKeyRangeQueryGtProjectSomeFieldsOrderByAsc() {
    sql("SELECT EMPNO,ENAME FROM \"EMP\" WHERE EMPNO > 7600 ORDER BY EMPNO ASC")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1])\n"
            + "    InnodbSort(sort0=[$0], dir0=[ASC])\n"
            + "      InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, "
            + "EMPNO>7600)])\n"
            + "        InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7654; ENAME=MARTIN\n"
            + "EMPNO=7698; ENAME=BLAKE\n"
            + "EMPNO=7782; ENAME=CLARK\n"
            + "EMPNO=7788; ENAME=SCOTT\n"
            + "EMPNO=7839; ENAME=KING\n"
            + "EMPNO=7844; ENAME=TURNER\n"
            + "EMPNO=7876; ENAME=ADAMS\n"
            + "EMPNO=7900; ENAME=JAMES\n"
            + "EMPNO=7902; ENAME=FORD\n"
            + "EMPNO=7934; ENAME=MILLER\n");
  }

  @Test void testSelectByPrimaryKeyRangeQueryGtProjectSomeFieldsOrderByDesc() {
    sql("SELECT EMPNO,ENAME FROM \"EMP\" WHERE EMPNO > 7600 ORDER BY EMPNO DESC")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1])\n"
            + "    InnodbSort(sort0=[$0], dir0=[DESC])\n"
            + "      InnodbFilter(condition=[(PK_RANGE_QUERY, index=PRIMARY_KEY, "
            + "EMPNO>7600)])\n"
            + "        InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7934; ENAME=MILLER\n"
            + "EMPNO=7902; ENAME=FORD\n"
            + "EMPNO=7900; ENAME=JAMES\n"
            + "EMPNO=7876; ENAME=ADAMS\n"
            + "EMPNO=7844; ENAME=TURNER\n"
            + "EMPNO=7839; ENAME=KING\n"
            + "EMPNO=7788; ENAME=SCOTT\n"
            + "EMPNO=7782; ENAME=CLARK\n"
            + "EMPNO=7698; ENAME=BLAKE\n"
            + "EMPNO=7654; ENAME=MARTIN\n");
  }

  @Test void testSelectBySkVarchar() {
    sql("SELECT * FROM \"EMP\" WHERE ENAME = 'JONES'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(SK_POINT_QUERY, index=ENAME_KEY, ENAME=JONES)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(some(7566));
  }

  @Test void testSelectBySkVarcharNotExists() {
    sql("SELECT * FROM \"EMP\" WHERE ENAME = 'NOT_EXISTS'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(SK_POINT_QUERY, index=ENAME_KEY, "
            + "ENAME=NOT_EXISTS)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns("");
  }

  @Test void testSelectBySkVarcharInTransformToPointQuery() {
    sql("SELECT * FROM \"EMP\" WHERE ENAME IN ('FORD')")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(SK_POINT_QUERY, index=ENAME_KEY, ENAME=FORD)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(some(7902));
  }

  @Test void testSelectBySkVarcharCaseInsensitive() {
    sql("SELECT * FROM \"EMP\" WHERE ENAME = 'miller'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(SK_POINT_QUERY, index=ENAME_KEY, ENAME=miller)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(some(7934));
  }

  @Test void testSelectBySkVarcharProjectSomeFields() {
    sql("SELECT ENAME,UPSERT_TIME FROM \"EMP\" WHERE ENAME = 'BLAKE'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(ENAME=[$1], UPSERT_TIME=[$12])\n"
            + "    InnodbFilter(condition=[(SK_POINT_QUERY, index=ENAME_KEY, ENAME=BLAKE)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("ENAME=BLAKE; UPSERT_TIME=" + expectedLocalTime("2018-06-01 14:45:00") + "\n");
  }

  @Test void testSelectBySkVarcharRangeQueryCoveringIndexOrderByDesc() {
    sql("SELECT ENAME FROM \"EMP\" WHERE ENAME >= 'CLARK' AND ENAME < 'SMITHY' ORDER BY ENAME DESC")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(ENAME=[$1])\n"
            + "    InnodbSort(sort0=[$1], dir0=[DESC])\n"
            + "      InnodbFilter(condition=[(SK_RANGE_QUERY, index=ENAME_KEY, "
            + "ENAME>=CLARK, ENAME<SMITHY)])\n"
            + "        InnodbTableScan(table=[[test, EMP]])\n")
        .returns("ENAME=SMITH\n"
            + "ENAME=SCOTT\n"
            + "ENAME=MILLER\n"
            + "ENAME=MARTIN\n"
            + "ENAME=KING\n"
            + "ENAME=JONES\n"
            + "ENAME=JAMES\n"
            + "ENAME=FORD\n"
            + "ENAME=CLARK\n");
  }

  @Test void testSelectBySkVarcharRangeQueryGtProjectSomeFieldsOrderByNonSkAsc() {
    sql("SELECT EMPNO,ENAME,AGE FROM \"EMP\" WHERE ENAME > 'MILLER' ORDER BY AGE ASC")
        .explainContains("PLAN=EnumerableSort(sort0=[$2], dir0=[ASC])\n"
            + "  InnodbToEnumerableConverter\n"
            + "    InnodbProject(EMPNO=[$0], ENAME=[$1], AGE=[$3])\n"
            + "      InnodbFilter(condition=[(SK_RANGE_QUERY, index=ENAME_KEY, "
            + "ENAME>MILLER)])\n"
            + "        InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7369; ENAME=SMITH; AGE=30\n"
            + "EMPNO=7521; ENAME=WARD; AGE=41\n"
            + "EMPNO=7788; ENAME=SCOTT; AGE=45\n"
            + "EMPNO=7844; ENAME=TURNER; AGE=54\n");
  }

  @Test void testSelectBySkVarcharRangeQueryGtProjectSomeFieldsOrderByNonSkDesc() {
    sql("SELECT EMPNO,ENAME,SAL FROM \"EMP\" WHERE ENAME > 'MILLER' ORDER BY SAL DESC")
        .explainContains("PLAN=EnumerableSort(sort0=[$2], dir0=[DESC])\n"
            + "  InnodbToEnumerableConverter\n"
            + "    InnodbProject(EMPNO=[$0], ENAME=[$1], SAL=[$6])\n"
            + "      InnodbFilter(condition=[(SK_RANGE_QUERY, index=ENAME_KEY, "
            + "ENAME>MILLER)])\n"
            + "        InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7788; ENAME=SCOTT; SAL=3000.00\n"
            + "EMPNO=7844; ENAME=TURNER; SAL=1500.00\n"
            + "EMPNO=7521; ENAME=WARD; SAL=1250.00\n"
            + "EMPNO=7369; ENAME=SMITH; SAL=800.00\n");
  }

  @Test void testSelectBySkDateProjectSomeFields() {
    sql("SELECT EMPNO,ENAME,HIREDATE FROM \"EMP\" WHERE HIREDATE = '1980-12-17'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1], HIREDATE=[$5])\n"
            + "    InnodbFilter(condition=[(SK_POINT_QUERY, index=HIREDATE_KEY, "
            + "HIREDATE=1980-12-17)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7369; ENAME=SMITH; HIREDATE=1980-12-17\n");
  }

  @Test void testSelectBySkDateRangeQueryGtProjectSomeFields() {
    sql("SELECT DEPTNO,ENAME,HIREDATE FROM \"EMP\" WHERE HIREDATE > '1970-01-01'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(DEPTNO=[$8], ENAME=[$1], HIREDATE=[$5])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=HIREDATE_KEY, "
            + "HIREDATE>1970-01-01)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("DEPTNO=20; ENAME=SMITH; HIREDATE=1980-12-17\n"
            + "DEPTNO=30; ENAME=BLAKE; HIREDATE=1981-01-05\n"
            + "DEPTNO=20; ENAME=JONES; HIREDATE=1981-02-04\n"
            + "DEPTNO=30; ENAME=ALLEN; HIREDATE=1981-02-20\n"
            + "DEPTNO=30; ENAME=WARD; HIREDATE=1981-02-22\n"
            + "DEPTNO=10; ENAME=CLARK; HIREDATE=1981-06-09\n"
            + "DEPTNO=30; ENAME=TURNER; HIREDATE=1981-09-08\n"
            + "DEPTNO=30; ENAME=MARTIN; HIREDATE=1981-09-28\n"
            + "DEPTNO=10; ENAME=KING; HIREDATE=1981-11-17\n"
            + "DEPTNO=30; ENAME=JAMES; HIREDATE=1981-12-03\n"
            + "DEPTNO=20; ENAME=FORD; HIREDATE=1981-12-03\n"
            + "DEPTNO=10; ENAME=MILLER; HIREDATE=1982-01-23\n"
            + "DEPTNO=20; ENAME=SCOTT; HIREDATE=1987-04-19\n"
            + "DEPTNO=20; ENAME=ADAMS; HIREDATE=1987-05-23\n")
        .returnsCount(14);
  }

  @Test void testSelectBySkDateRangeQueryLtProjectSomeFieldsOrderByDesc() {
    sql("SELECT DEPTNO,ENAME,HIREDATE FROM \"EMP\" WHERE HIREDATE < '2020-01-01' "
        + "ORDER BY HIREDATE DESC")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(DEPTNO=[$8], ENAME=[$1], HIREDATE=[$5])\n"
            + "    InnodbSort(sort0=[$5], dir0=[DESC])\n"
            + "      InnodbFilter(condition=[(SK_RANGE_QUERY, index=HIREDATE_KEY, "
            + "HIREDATE<2020-01-01)])\n"
            + "        InnodbTableScan(table=[[test, EMP]])\n")
        .returns("DEPTNO=20; ENAME=ADAMS; HIREDATE=1987-05-23\n"
            + "DEPTNO=20; ENAME=SCOTT; HIREDATE=1987-04-19\n"
            + "DEPTNO=10; ENAME=MILLER; HIREDATE=1982-01-23\n"
            + "DEPTNO=20; ENAME=FORD; HIREDATE=1981-12-03\n"
            + "DEPTNO=30; ENAME=JAMES; HIREDATE=1981-12-03\n"
            + "DEPTNO=10; ENAME=KING; HIREDATE=1981-11-17\n"
            + "DEPTNO=30; ENAME=MARTIN; HIREDATE=1981-09-28\n"
            + "DEPTNO=30; ENAME=TURNER; HIREDATE=1981-09-08\n"
            + "DEPTNO=10; ENAME=CLARK; HIREDATE=1981-06-09\n"
            + "DEPTNO=30; ENAME=WARD; HIREDATE=1981-02-22\n"
            + "DEPTNO=30; ENAME=ALLEN; HIREDATE=1981-02-20\n"
            + "DEPTNO=20; ENAME=JONES; HIREDATE=1981-02-04\n"
            + "DEPTNO=30; ENAME=BLAKE; HIREDATE=1981-01-05\n"
            + "DEPTNO=20; ENAME=SMITH; HIREDATE=1980-12-17\n")
        .returnsCount(14);
  }

  @Test void testSelectBySkDateRangeQueryGtLteProjectSomeFields() {
    sql("SELECT DEPTNO,ENAME,HIREDATE FROM \"EMP\" WHERE HIREDATE < '1981-12-03' "
        + "AND HIREDATE >= '1981-06-09'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(DEPTNO=[$8], ENAME=[$1], HIREDATE=[$5])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=HIREDATE_KEY, "
            + "HIREDATE>=1981-06-09, HIREDATE<1981-12-03)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("DEPTNO=10; ENAME=CLARK; HIREDATE=1981-06-09\n"
            + "DEPTNO=30; ENAME=TURNER; HIREDATE=1981-09-08\n"
            + "DEPTNO=30; ENAME=MARTIN; HIREDATE=1981-09-28\n"
            + "DEPTNO=10; ENAME=KING; HIREDATE=1981-11-17\n");
  }

  @Test void testSelectBySkDateRangeQueryNothing() {
    sql("SELECT DEPTNO,ENAME,HIREDATE FROM \"EMP\" WHERE HIREDATE < '1981-12-03' "
        + "AND HIREDATE > '1981-12-01'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(DEPTNO=[$8], ENAME=[$1], HIREDATE=[$5])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=HIREDATE_KEY, "
            + "HIREDATE>1981-12-01, "
            + "HIREDATE<1981-12-03)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("");
  }

  @Test void testSelectBySkTime() {
    sql("SELECT * FROM \"EMP\" WHERE CREATE_TIME = '12:12:56'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(SK_POINT_QUERY, index=CREATE_TIME_KEY, "
            + "CREATE_TIME=12:12:56)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(some(7654));
  }

  @Test void testSelectBySkTimeRangeQueryGtLteProjectSomeFields() {
    sql("SELECT EMPNO,ENAME,CREATE_TIME FROM \"EMP\" WHERE CREATE_TIME > '12:00:00' "
        + "AND CREATE_TIME <= '18:00:00'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1], CREATE_TIME=[$11])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=CREATE_TIME_KEY, "
            + "CREATE_TIME>12:00:00, CREATE_TIME<=18:00:00)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7788; ENAME=SCOTT; CREATE_TIME=12:12:12\n"
            + "EMPNO=7654; ENAME=MARTIN; CREATE_TIME=12:12:56\n"
            + "EMPNO=7900; ENAME=JAMES; CREATE_TIME=12:19:00\n"
            + "EMPNO=7698; ENAME=BLAKE; CREATE_TIME=14:45:00\n");
  }

  @Test void testSelectBySkTimeRangeQueryNothing() {
    sql("SELECT EMPNO,ENAME,UPSERT_TIME FROM \"EMP\" WHERE CREATE_TIME > '23:50:00' "
        + "AND CREATE_TIME < '23:59:00'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1], UPSERT_TIME=[$12])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=CREATE_TIME_KEY, "
            + "CREATE_TIME>23:50:00, "
            + "CREATE_TIME<23:59:00)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("");
  }

  @Test void testSelectBySkTimestamp() {
    sql("SELECT * FROM \"EMP\" WHERE UPSERT_TIME = '"
        + expectedLocalTime("2018-09-02 12:12:56") + "'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(SK_POINT_QUERY, index=UPSERT_TIME_KEY, UPSERT_TIME="
            + expectedLocalTime("2018-09-02 12:12:56") + ")])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(some(7654));
  }

  @Test void testSelectBySkTimestampRangeQueryGteProjectSomeFields() {
    sql("SELECT EMPNO,ENAME,UPSERT_TIME FROM \"EMP\" WHERE UPSERT_TIME >= '2000-01-01 00:00:00'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1], UPSERT_TIME=[$12])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=UPSERT_TIME_KEY, "
            + "UPSERT_TIME>=2000-01-01 00:00:00)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7566; ENAME=JONES; UPSERT_TIME="
            + expectedLocalTime("2015-03-09 22:16:30") + "\n"
            + "EMPNO=7934; ENAME=MILLER; UPSERT_TIME="
            + expectedLocalTime("2016-09-02 23:15:01") + "\n"
            + "EMPNO=7844; ENAME=TURNER; UPSERT_TIME="
            + expectedLocalTime("2017-08-17 22:01:37") + "\n"
            + "EMPNO=7876; ENAME=ADAMS; UPSERT_TIME="
            + expectedLocalTime("2017-08-18 23:11:06") + "\n"
            + "EMPNO=7499; ENAME=ALLEN; UPSERT_TIME="
            + expectedLocalTime("2018-04-09 09:00:00") + "\n"
            + "EMPNO=7698; ENAME=BLAKE; UPSERT_TIME="
            + expectedLocalTime("2018-06-01 14:45:00") + "\n"
            + "EMPNO=7654; ENAME=MARTIN; UPSERT_TIME="
            + expectedLocalTime("2018-09-02 12:12:56") + "\n"
            + "EMPNO=7902; ENAME=FORD; UPSERT_TIME="
            + expectedLocalTime("2019-05-29 00:00:00") + "\n"
            + "EMPNO=7839; ENAME=KING; UPSERT_TIME="
            + expectedLocalTime("2019-06-08 15:15:15") + "\n"
            + "EMPNO=7788; ENAME=SCOTT; UPSERT_TIME="
            + expectedLocalTime("2019-07-28 12:12:12") + "\n"
            + "EMPNO=7782; ENAME=CLARK; UPSERT_TIME="
            + expectedLocalTime("2019-09-30 02:14:56") + "\n"
            + "EMPNO=7521; ENAME=WARD; UPSERT_TIME="
            + expectedLocalTime("2019-11-16 10:26:40") + "\n"
            + "EMPNO=7369; ENAME=SMITH; UPSERT_TIME="
            + expectedLocalTime("2020-01-01 18:35:40") + "\n"
            + "EMPNO=7900; ENAME=JAMES; UPSERT_TIME="
            + expectedLocalTime("2020-01-02 12:19:00") + "\n");
  }

  @Test void testSelectBySkTimestampRangeQueryLteProjectSomeFields() {
    sql("SELECT EMPNO,ENAME,UPSERT_TIME FROM \"EMP\" WHERE UPSERT_TIME <= '2018-09-04 12:12:56'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1], UPSERT_TIME=[$12])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=UPSERT_TIME_KEY, "
            + "UPSERT_TIME<=2018-09-04 12:12:56)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7566; ENAME=JONES; UPSERT_TIME="
            + expectedLocalTime("2015-03-09 22:16:30") + "\n"
            + "EMPNO=7934; ENAME=MILLER; UPSERT_TIME="
            + expectedLocalTime("2016-09-02 23:15:01") + "\n"
            + "EMPNO=7844; ENAME=TURNER; UPSERT_TIME="
            + expectedLocalTime("2017-08-17 22:01:37") + "\n"
            + "EMPNO=7876; ENAME=ADAMS; UPSERT_TIME="
            + expectedLocalTime("2017-08-18 23:11:06") + "\n"
            + "EMPNO=7499; ENAME=ALLEN; UPSERT_TIME="
            + expectedLocalTime("2018-04-09 09:00:00") + "\n"
            + "EMPNO=7698; ENAME=BLAKE; UPSERT_TIME="
            + expectedLocalTime("2018-06-01 14:45:00") + "\n"
            + "EMPNO=7654; ENAME=MARTIN; UPSERT_TIME="
            + expectedLocalTime("2018-09-02 12:12:56") + "\n");
  }

  @Test void testSelectBySkTimestampRangeQueryGtLteProjectSomeFields() {
    sql("SELECT EMPNO,ENAME,UPSERT_TIME FROM \"EMP\" WHERE UPSERT_TIME > '"
        + expectedLocalTime("2017-08-18 23:11:06")
        + "' AND UPSERT_TIME <= '" + expectedLocalTime("2018-09-02 12:12:56") + "'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1], UPSERT_TIME=[$12])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=UPSERT_TIME_KEY, UPSERT_TIME>"
            + expectedLocalTime("2017-08-18 23:11:06") + ", UPSERT_TIME<="
            + expectedLocalTime("2018-09-02 12:12:56") + ")])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7499; ENAME=ALLEN; UPSERT_TIME="
            + expectedLocalTime("2018-04-09 09:00:00") + "\n"
            + "EMPNO=7698; ENAME=BLAKE; UPSERT_TIME="
            + expectedLocalTime("2018-06-01 14:45:00") + "\n"
            + "EMPNO=7654; ENAME=MARTIN; UPSERT_TIME="
            + expectedLocalTime("2018-09-02 12:12:56") + "\n");
  }

  @Test void testSelectBySkTimestampRangeQueryNothing() {
    sql("SELECT EMPNO,ENAME,UPSERT_TIME FROM \"EMP\" WHERE UPSERT_TIME > '2020-08-18 13:11:06' "
        + "AND UPSERT_TIME <= '2020-08-18 23:11:06'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1], UPSERT_TIME=[$12])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=UPSERT_TIME_KEY, "
            + "UPSERT_TIME>2020-08-18 13:11:06, UPSERT_TIME<=2020-08-18 23:11:06)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("");
  }

  @Test void testSelectBySkSmallint() {
    sql("SELECT * FROM \"EMP\" WHERE AGE = 30")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(SK_POINT_QUERY, index=AGE_KEY, AGE=30)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(some(7369));
  }

  @Test void testSelectBySkSmallint2() {
    sql("SELECT * FROM \"EMP\" WHERE AGE = 32")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(SK_POINT_QUERY, index=AGE_KEY, AGE=32)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(some(7782, 7934));
  }

  @Test void testSelectBySmallintRangeQueryLtProjectSomeFields() {
    sql("SELECT EMPNO,ENAME,AGE FROM \"EMP\" WHERE AGE < 30")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1], AGE=[$3])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=AGE_KEY, AGE<30)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7839; ENAME=KING; AGE=22\n"
            + "EMPNO=7499; ENAME=ALLEN; AGE=24\n"
            + "EMPNO=7654; ENAME=MARTIN; AGE=27\n"
            + "EMPNO=7566; ENAME=JONES; AGE=28\n"
            + "EMPNO=7902; ENAME=FORD; AGE=28\n");
  }

  @Test void testSelectBySkSmallintRangeQueryGtProjectSomeFields() {
    sql("SELECT EMPNO,ENAME,AGE FROM \"EMP\" WHERE AGE > 30")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1], AGE=[$3])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=AGE_KEY, AGE>30)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7782; ENAME=CLARK; AGE=32\n"
            + "EMPNO=7934; ENAME=MILLER; AGE=32\n"
            + "EMPNO=7876; ENAME=ADAMS; AGE=35\n"
            + "EMPNO=7698; ENAME=BLAKE; AGE=38\n"
            + "EMPNO=7900; ENAME=JAMES; AGE=40\n"
            + "EMPNO=7521; ENAME=WARD; AGE=41\n"
            + "EMPNO=7788; ENAME=SCOTT; AGE=45\n"
            + "EMPNO=7844; ENAME=TURNER; AGE=54\n");
  }

  @Test void testSelectBySkSmallintRangeQueryGtLtProjectSomeFields() {
    sql("SELECT EMPNO,ENAME,AGE FROM \"EMP\" WHERE AGE > 30 AND AGE < 35")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1], AGE=[$3])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=AGE_KEY, AGE>30, AGE<35)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7782; ENAME=CLARK; AGE=32\n"
            + "EMPNO=7934; ENAME=MILLER; AGE=32\n");
  }

  @Test void testSelectBySkSmallintNothing() {
    sql("SELECT * FROM \"EMP\" WHERE AGE = 100")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(SK_POINT_QUERY, index=AGE_KEY, AGE=100)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns("");
  }

  @Test void testSelectBySkSmallintRangeQueryNoting() {
    sql("SELECT * FROM \"EMP\" WHERE AGE > 80")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(SK_RANGE_QUERY, index=AGE_KEY, AGE>80)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns("");
  }

  @Test void testSelectBySkSmallintRangeQueryLtProjectSomeFieldsOrderByDesc() {
    sql("SELECT EMPNO,ENAME,AGE FROM \"EMP\" WHERE AGE < 32 ORDER BY AGE DESC")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1], AGE=[$3])\n"
            + "    InnodbSort(sort0=[$3], dir0=[DESC])\n"
            + "      InnodbFilter(condition=[(SK_RANGE_QUERY, index=AGE_KEY, AGE<32)])\n"
            + "        InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7369; ENAME=SMITH; AGE=30\n"
            + "EMPNO=7902; ENAME=FORD; AGE=28\n"
            + "EMPNO=7566; ENAME=JONES; AGE=28\n"
            + "EMPNO=7654; ENAME=MARTIN; AGE=27\n"
            + "EMPNO=7499; ENAME=ALLEN; AGE=24\n"
            + "EMPNO=7839; ENAME=KING; AGE=22\n");
  }

  @Test void testSelectBySkLimitedLengthVarcharConditionNotPushDown() {
    sql("SELECT * FROM \"EMP\" WHERE EMAIL = 'king@calcite'")
        .explainContains("InnodbToEnumerableConverter\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n");
  }

  @Test void testSelectBySkLimitedLengthVarcharConditionPushDown() {
    sql("SELECT EMPNO,ENAME,EMAIL FROM \"EMP\" WHERE EMAIL > 'kkk'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1], EMAIL=[$9])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=EMAIL_KEY, EMAIL>kkk)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7654; ENAME=MARTIN; EMAIL=martin@calcite\n"
            + "EMPNO=7788; ENAME=SCOTT; EMAIL=scott@calcite\n"
            + "EMPNO=7369; ENAME=SMITH; EMAIL=smith@calcite\n"
            + "EMPNO=7844; ENAME=TURNER; EMAIL=turner@calcite\n"
            + "EMPNO=7521; ENAME=WARD; EMAIL=ward@calcite\n");
  }

  @Test void testSelectByMultipleSkDateTimeRangeQuery() {
    sql("SELECT EMPNO,CREATE_DATETIME,JOB FROM \"EMP\" WHERE "
        + "CREATE_DATETIME >= '2018-09-02 12:12:56'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], CREATE_DATETIME=[$10], JOB=[$2])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=CREATE_DATETIME_JOB_KEY, "
            + "CREATE_DATETIME>=2018-09-02 12:12:56)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7654; CREATE_DATETIME=2018-09-02 12:12:56; JOB=SALESMAN\n"
            + "EMPNO=7902; CREATE_DATETIME=2019-05-29 00:00:00; JOB=ANALYST\n"
            + "EMPNO=7839; CREATE_DATETIME=2019-06-08 15:15:15; JOB=PRESIDENT\n"
            + "EMPNO=7788; CREATE_DATETIME=2019-07-28 12:12:12; JOB=ANALYST\n"
            + "EMPNO=7782; CREATE_DATETIME=2019-09-30 02:14:56; JOB=MANAGER\n"
            + "EMPNO=7521; CREATE_DATETIME=2019-11-16 10:26:40; JOB=SALESMAN\n"
            + "EMPNO=7369; CREATE_DATETIME=2020-01-01 18:35:40; JOB=CLERK\n"
            + "EMPNO=7900; CREATE_DATETIME=2020-01-02 12:19:00; JOB=CLERK\n");
  }

  @Test void testSelectByMultipleSkDateTimeVarcharPointQueryCoveringIndex() {
    sql("SELECT EMPNO,CREATE_DATETIME,JOB FROM \"EMP\" WHERE "
        + "CREATE_DATETIME = '2018-09-02 12:12:56' AND JOB = 'SALESMAN'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], CREATE_DATETIME=[$10], JOB=[$2])\n"
            + "    InnodbFilter(condition=[(SK_POINT_QUERY, index=CREATE_DATETIME_JOB_KEY, "
            + "CREATE_DATETIME=2018-09-02 12:12:56,JOB=SALESMAN)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7654; CREATE_DATETIME=2018-09-02 12:12:56; JOB=SALESMAN\n");
  }

  @Test void testSelectByMultipleSkTinyIntVarcharPointQueryCoveringIndex() {
    sql("SELECT EMPNO,DEPTNO,JOB FROM \"EMP\" WHERE DEPTNO = 20 AND JOB = 'ANALYST'")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], DEPTNO=[$8], JOB=[$2])\n"
            + "    InnodbFilter(condition=[(SK_POINT_QUERY, index=DEPTNO_JOB_KEY, "
            + "DEPTNO=20,JOB=ANALYST)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7788; DEPTNO=20; JOB=ANALYST\n"
            + "EMPNO=7902; DEPTNO=20; JOB=ANALYST\n");
  }

  @Test void testSelectByMultipleSkRangeQueryPushDownPartialCondition() {
    sql("SELECT EMPNO,DEPTNO,ENAME FROM \"EMP\" WHERE DEPTNO = 20")
        .explainContains("InnodbFilter(condition=[(SK_RANGE_QUERY, index=DEPTNO_JOB_KEY, "
            + "DEPTNO>=20, DEPTNO<=20)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7788; DEPTNO=20; ENAME=SCOTT\n"
            + "EMPNO=7902; DEPTNO=20; ENAME=FORD\n"
            + "EMPNO=7369; DEPTNO=20; ENAME=SMITH\n"
            + "EMPNO=7876; DEPTNO=20; ENAME=ADAMS\n"
            + "EMPNO=7566; DEPTNO=20; ENAME=JONES\n");
  }

  @Test void testSelectByMultipleSkRangeQueryPushDownPartialCondition2() {
    sql("SELECT EMPNO,DEPTNO,JOB FROM \"EMP\" WHERE JOB = 'SALESMAN' AND DEPTNO > 20")
        .explainContains("InnodbToEnumerableConverter\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=DEPTNO_JOB_KEY, "
            + "DEPTNO>20)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7499; DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7521; DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7654; DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7844; DEPTNO=30; JOB=SALESMAN\n");
  }

  @Test void testSelectByMultipleSkRangeQueryPushDownPartialCondition3() {
    sql("SELECT EMPNO,DEPTNO,JOB FROM \"EMP\" WHERE JOB >= 'SALE' AND DEPTNO >= 20")
        .explainContains("PLAN=EnumerableCalc(expr#0..12=[{inputs}], "
            + "expr#13=[CAST($t2):CHAR(4) NOT NULL], "
            + "expr#14=['SALE'], expr#15=[>=($t13, $t14)], "
            + "EMPNO=[$t0], DEPTNO=[$t8], JOB=[$t2], $condition=[$t15])\n"
            + "  InnodbToEnumerableConverter\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=DEPTNO_JOB_KEY, "
            + "DEPTNO>=20)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7499; DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7521; DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7654; DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7844; DEPTNO=30; JOB=SALESMAN\n");
  }

  @Test void testSelectByMultipleSkBreakLeftPrefixRuleConditionNotPushDown() {
    sql("SELECT * FROM \"EMP\" WHERE JOB = 'CLERK'")
        .explainContains("InnodbToEnumerableConverter\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n");
  }

  @Test void testSelectByMultipleSkTinyIntDecimalDecimalPointQueryProjectAllFields() {
    sql("SELECT * FROM \"EMP\" WHERE DEPTNO = 30 AND SAL = 1250 AND COMM = 500.00")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbFilter(condition=[(SK_POINT_QUERY, index=DEPTNO_SAL_COMM_KEY, "
            + "DEPTNO=30,SAL=1250.00,COMM=500.00)])\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n")
        .returns(some(7521));
  }

  @Test void testSelectByMultipleSkTinyIntDecimalDecimalPointQueryProjectSomeFields() {
    sql("SELECT EMPNO,ENAME FROM \"EMP\" WHERE DEPTNO = 30 AND SAL = 1250 AND COMM = 500.00")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], ENAME=[$1])\n"
            + "    InnodbFilter(condition=[(SK_POINT_QUERY, index=DEPTNO_SAL_COMM_KEY, "
            + "DEPTNO=30,SAL=1250.00,COMM=500.00)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])")
        .returns("EMPNO=7521; ENAME=WARD\n");
  }

  @Test void testSelectByMultipleSkWithSameLeftPrefixChooseOneIndex() {
    sql("SELECT EMPNO,DEPTNO,ENAME FROM \"EMP\" WHERE DEPTNO = 20 AND SAL > 30")
        .explainContains("InnodbToEnumerableConverter\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=DEPTNO_JOB_KEY, "
            + "DEPTNO>=20, DEPTNO<=20)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("EMPNO=7788; DEPTNO=20; ENAME=SCOTT\n"
            + "EMPNO=7902; DEPTNO=20; ENAME=FORD\n"
            + "EMPNO=7369; DEPTNO=20; ENAME=SMITH\n"
            + "EMPNO=7876; DEPTNO=20; ENAME=ADAMS\n"
            + "EMPNO=7566; DEPTNO=20; ENAME=JONES\n");
  }

  @Test void testSelectByMultipleSkForceIndexAsPrimaryKey() {
    sql("SELECT EMPNO,DEPTNO,ENAME FROM \"EMP\"/*+ index(PRIMARY_KEY) */ WHERE "
        + "DEPTNO = 10 AND SAL > 500")
        .explainContains("InnodbToEnumerableConverter\n"
            + "    InnodbTableScan(table=[[test, EMP]], forceIndex=[PRIMARY_KEY])\n")
        .returns("EMPNO=7782; DEPTNO=10; ENAME=CLARK\n"
            + "EMPNO=7839; DEPTNO=10; ENAME=KING\n"
            + "EMPNO=7934; DEPTNO=10; ENAME=MILLER\n");
  }

  @Test void testSelectByMultipleSkWithSameLeftPrefixForceIndex() {
    sql("SELECT EMPNO,DEPTNO,ENAME FROM \"EMP\"/*+ index(DEPTNO_SAL_COMM_KEY) */ WHERE "
        + "DEPTNO = 20 AND SAL > 30")
        .explainContains("InnodbToEnumerableConverter\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=DEPTNO_SAL_COMM_KEY, "
            + "DEPTNO>=20, DEPTNO<=20)])\n"
            + "      InnodbTableScan(table=[[test, EMP]], forceIndex=[DEPTNO_SAL_COMM_KEY])\n")
        .returns("EMPNO=7369; DEPTNO=20; ENAME=SMITH\n"
            + "EMPNO=7876; DEPTNO=20; ENAME=ADAMS\n"
            + "EMPNO=7566; DEPTNO=20; ENAME=JONES\n"
            + "EMPNO=7788; DEPTNO=20; ENAME=SCOTT\n"
            + "EMPNO=7902; DEPTNO=20; ENAME=FORD\n");
  }

  @Test void testSelectByMultipleSkWithSameLeftPrefixForceIndexCoveringIndex() {
    sql("SELECT EMPNO,DEPTNO,MGR FROM \"EMP\"/*+ index(DEPTNO_MGR_KEY) */ WHERE DEPTNO > 0")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], DEPTNO=[$8], MGR=[$4])\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=DEPTNO_MGR_KEY, DEPTNO>0)])\n"
            + "      InnodbTableScan(table=[[test, EMP]], forceIndex=[DEPTNO_MGR_KEY])\n")
        .returns("EMPNO=7839; DEPTNO=10; MGR=null\n"
            + "EMPNO=7934; DEPTNO=10; MGR=7782\n"
            + "EMPNO=7782; DEPTNO=10; MGR=7839\n"
            + "EMPNO=7788; DEPTNO=20; MGR=7566\n"
            + "EMPNO=7902; DEPTNO=20; MGR=7566\n"
            + "EMPNO=7876; DEPTNO=20; MGR=7788\n"
            + "EMPNO=7566; DEPTNO=20; MGR=7839\n"
            + "EMPNO=7369; DEPTNO=20; MGR=7902\n"
            + "EMPNO=7499; DEPTNO=30; MGR=7698\n"
            + "EMPNO=7521; DEPTNO=30; MGR=7698\n"
            + "EMPNO=7654; DEPTNO=30; MGR=7698\n"
            + "EMPNO=7844; DEPTNO=30; MGR=7698\n"
            + "EMPNO=7900; DEPTNO=30; MGR=7698\n"
            + "EMPNO=7698; DEPTNO=30; MGR=7839\n");
  }

  @Test void testGroupByFilterPushDown() {
    sql("SELECT DEPTNO,SUM(SAL) AS TOTAL_SAL FROM EMP WHERE AGE > 30 GROUP BY DEPTNO")
        .explainContains("PLAN=EnumerableAggregate(group=[{8}], TOTAL_SAL=[$SUM0($6)])\n"
            + "  InnodbToEnumerableConverter\n"
            + "    InnodbFilter(condition=[(SK_RANGE_QUERY, index=AGE_KEY, AGE>30)])\n"
            + "      InnodbTableScan(table=[[test, EMP]])\n")
        .returns("DEPTNO=20; TOTAL_SAL=4100.00\n"
            + "DEPTNO=10; TOTAL_SAL=3750.00\n"
            + "DEPTNO=30; TOTAL_SAL=6550.00\n");
  }

  @Test void testJoinProjectAndFilterPushDown() {
    sql("SELECT EMPNO,EMP.DEPTNO,JOB,DNAME FROM \"EMP\" JOIN \"DEPT\" "
        + "ON EMP.DEPTNO = DEPT.DEPTNO AND EMP.DEPTNO = 20")
        .explainContains("EnumerableHashJoin(condition=[=($2, $3)], joinType=[inner])\n"
            + "    InnodbToEnumerableConverter\n"
            + "      InnodbProject(EMPNO=[$0], JOB=[$2], DEPTNO=[$8])\n"
            + "        InnodbFilter(condition=[(SK_RANGE_QUERY, index=DEPTNO_JOB_KEY, "
            + "DEPTNO>=20, DEPTNO<=20)])\n"
            + "          InnodbTableScan(table=[[test, EMP]])\n"
            + "    InnodbToEnumerableConverter\n"
            + "      InnodbProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "        InnodbTableScan(table=[[test, DEPT]])\n")
        .returns("EMPNO=7788; DEPTNO=20; JOB=ANALYST; DNAME=RESEARCH\n"
            + "EMPNO=7902; DEPTNO=20; JOB=ANALYST; DNAME=RESEARCH\n"
            + "EMPNO=7369; DEPTNO=20; JOB=CLERK; DNAME=RESEARCH\n"
            + "EMPNO=7876; DEPTNO=20; JOB=CLERK; DNAME=RESEARCH\n"
            + "EMPNO=7566; DEPTNO=20; JOB=MANAGER; DNAME=RESEARCH\n");
  }

  @Test void testJoinProjectAndFilterPushDown2() {
    sql("SELECT EMPNO,EMP.DEPTNO,JOB,DNAME FROM \"EMP\" JOIN \"DEPT\" "
        + "ON EMP.DEPTNO = DEPT.DEPTNO AND EMP.EMPNO = 7900")
        .explainContains("EnumerableHashJoin(condition=[=($2, $3)], joinType=[inner])\n"
            + "    InnodbToEnumerableConverter\n"
            + "      InnodbProject(EMPNO=[$0], JOB=[$2], DEPTNO=[$8])\n"
            + "        InnodbFilter(condition=[(PK_POINT_QUERY, index=PRIMARY_KEY, "
            + "EMPNO=7900)])\n"
            + "          InnodbTableScan(table=[[test, EMP]])\n"
            + "    InnodbToEnumerableConverter\n"
            + "      InnodbProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "        InnodbTableScan(table=[[test, DEPT]])\n")
        .returns("EMPNO=7900; DEPTNO=30; JOB=CLERK; DNAME=SALES\n");
  }

  @Test void testSelectFilterNoIndex() {
    sql("SELECT * FROM \"EMP\" WHERE MGR = 7839")
        .explainContains("InnodbToEnumerableConverter\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n");
  }

  @Test void testSelectForceIndexCoveringIndex() {
    sql("SELECT EMPNO,MGR FROM \"EMP\"/*+ index(DEPTNO_MGR_KEY) */ WHERE MGR = 7839")
        .explainContains("  InnodbToEnumerableConverter\n"
            + "    InnodbProject(EMPNO=[$0], MGR=[$4])\n"
            + "      InnodbTableScan(table=[[test, EMP]], forceIndex=[DEPTNO_MGR_KEY])\n");
  }

  @Test void testSelectForceIndexIncorrectIndexName() {
    sql("SELECT * FROM \"EMP\"/*+ index(NOT_EXISTS) */ WHERE MGR = 7839")
        .explainContains("InnodbToEnumerableConverter\n"
            + "    InnodbTableScan(table=[[test, EMP]])\n");
  }

  @Test void testSelectByMultipleSkForceIndexOrderByDesc() {
    sql("SELECT EMPNO,DEPTNO,JOB FROM \"EMP\"/*+ index(DEPTNO_JOB_KEY) */ WHERE DEPTNO > 10 "
        + "ORDER BY DEPTNO DESC,JOB DESC")
        .explainContains("PLAN=InnodbToEnumerableConverter\n"
            + "  InnodbProject(EMPNO=[$0], DEPTNO=[$8], JOB=[$2])\n"
            + "    InnodbSort(sort0=[$8], sort1=[$2], dir0=[DESC], dir1=[DESC])\n"
            + "      InnodbFilter(condition=[(SK_RANGE_QUERY, index=DEPTNO_JOB_KEY, "
            + "DEPTNO>10)])\n"
            + "        InnodbTableScan(table=[[test, EMP]], forceIndex=[DEPTNO_JOB_KEY])\n")
        .returns("EMPNO=7844; DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7654; DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7521; DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7499; DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7698; DEPTNO=30; JOB=MANAGER\n"
            + "EMPNO=7900; DEPTNO=30; JOB=CLERK\n"
            + "EMPNO=7566; DEPTNO=20; JOB=MANAGER\n"
            + "EMPNO=7876; DEPTNO=20; JOB=CLERK\n"
            + "EMPNO=7369; DEPTNO=20; JOB=CLERK\n"
            + "EMPNO=7902; DEPTNO=20; JOB=ANALYST\n"
            + "EMPNO=7788; DEPTNO=20; JOB=ANALYST\n");
  }

  @Test void testSelectNotExistTable() {
    sql("SELECT * FROM \"NOT_EXIST\"")
        .failsAtValidation("Object 'NOT_EXIST' not found");
  }

  static final List<Pair<Integer, String>> ROWS =
      Lists.newArrayList(
          Pair.of(7369, "EMPNO=7369; ENAME=SMITH; JOB=CLERK; AGE=30; MGR=7902; "
              + "HIREDATE=1980-12-17; SAL=800.00; COMM=null; DEPTNO=20; EMAIL=smith@calcite; "
              + "CREATE_DATETIME=2020-01-01 18:35:40; CREATE_TIME=18:35:40; UPSERT_TIME="
              + expectedLocalTime("2020-01-01 18:35:40")),
          Pair.of(7499, "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; AGE=24; MGR=7698; "
              + "HIREDATE=1981-02-20; SAL=1600.00; COMM=300.00; DEPTNO=30; EMAIL=allen@calcite; "
              + "CREATE_DATETIME=2018-04-09 09:00:00; CREATE_TIME=09:00:00; UPSERT_TIME="
              + expectedLocalTime("2018-04-09 09:00:00")),
          Pair.of(7521, "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; AGE=41; MGR=7698; "
              + "HIREDATE=1981-02-22; SAL=1250.00; COMM=500.00; DEPTNO=30; EMAIL=ward@calcite; "
              + "CREATE_DATETIME=2019-11-16 10:26:40; CREATE_TIME=10:26:40; UPSERT_TIME="
              + expectedLocalTime("2019-11-16 10:26:40")),
          Pair.of(7566, "EMPNO=7566; ENAME=JONES; JOB=MANAGER; AGE=28; MGR=7839; "
              + "HIREDATE=1981-02-04; SAL=2975.00; COMM=null; DEPTNO=20; EMAIL=jones@calcite; "
              + "CREATE_DATETIME=2015-03-09 22:16:30; CREATE_TIME=22:16:30; UPSERT_TIME="
              + expectedLocalTime("2015-03-09 22:16:30")),
          Pair.of(7654, "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; AGE=27; MGR=7698; "
              + "HIREDATE=1981-09-28; SAL=1250.00; COMM=1400.00; DEPTNO=30; EMAIL=martin@calcite; "
              + "CREATE_DATETIME=2018-09-02 12:12:56; CREATE_TIME=12:12:56; UPSERT_TIME="
              + expectedLocalTime("2018-09-02 12:12:56")),
          Pair.of(7698, "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; AGE=38; MGR=7839; "
              + "HIREDATE=1981-01-05; SAL=2850.00; COMM=null; DEPTNO=30; EMAIL=blake@calcite; "
              + "CREATE_DATETIME=2018-06-01 14:45:00; CREATE_TIME=14:45:00; UPSERT_TIME="
              + expectedLocalTime("2018-06-01 14:45:00")),
          Pair.of(7782, "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; AGE=32; MGR=7839; "
              + "HIREDATE=1981-06-09; SAL=2450.00; COMM=null; DEPTNO=10; EMAIL=null; "
              + "CREATE_DATETIME=2019-09-30 02:14:56; CREATE_TIME=02:14:56; UPSERT_TIME="
              + expectedLocalTime("2019-09-30 02:14:56")),
          Pair.of(7788, "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; AGE=45; MGR=7566; "
              + "HIREDATE=1987-04-19; SAL=3000.00; COMM=null; DEPTNO=20; EMAIL=scott@calcite; "
              + "CREATE_DATETIME=2019-07-28 12:12:12; CREATE_TIME=12:12:12; UPSERT_TIME="
              + expectedLocalTime("2019-07-28 12:12:12")),
          Pair.of(7839, "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; AGE=22; MGR=null; "
              + "HIREDATE=1981-11-17; SAL=5000.00; COMM=null; DEPTNO=10; EMAIL=king@calcite; "
              + "CREATE_DATETIME=2019-06-08 15:15:15; CREATE_TIME=null; UPSERT_TIME="
              + expectedLocalTime("2019-06-08 15:15:15")),
          Pair.of(7844, "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; AGE=54; MGR=7698; "
              + "HIREDATE=1981-09-08; SAL=1500.00; COMM=0.00; DEPTNO=30; EMAIL=turner@calcite; "
              + "CREATE_DATETIME=2017-08-17 22:01:37; CREATE_TIME=22:01:37; UPSERT_TIME="
              + expectedLocalTime("2017-08-17 22:01:37")),
          Pair.of(7876, "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; AGE=35; MGR=7788; "
              + "HIREDATE=1987-05-23; SAL=1100.00; COMM=null; DEPTNO=20; EMAIL=adams@calcite; "
              + "CREATE_DATETIME=null; CREATE_TIME=23:11:06; UPSERT_TIME="
              + expectedLocalTime("2017-08-18 23:11:06")),
          Pair.of(7900, "EMPNO=7900; ENAME=JAMES; JOB=CLERK; AGE=40; MGR=7698; "
              + "HIREDATE=1981-12-03; SAL=950.00; COMM=null; DEPTNO=30; EMAIL=james@calcite; "
              + "CREATE_DATETIME=2020-01-02 12:19:00; CREATE_TIME=12:19:00; UPSERT_TIME="
              + expectedLocalTime("2020-01-02 12:19:00")),
          Pair.of(7902, "EMPNO=7902; ENAME=FORD; JOB=ANALYST; AGE=28; MGR=7566; "
              + "HIREDATE=1981-12-03; SAL=3000.00; COMM=null; DEPTNO=20; EMAIL=ford@calcite; "
              + "CREATE_DATETIME=2019-05-29 00:00:00; CREATE_TIME=null; UPSERT_TIME="
              + expectedLocalTime("2019-05-29 00:00:00")),
          Pair.of(7934, "EMPNO=7934; ENAME=MILLER; JOB=CLERK; AGE=32; MGR=7782; "
              + "HIREDATE=1982-01-23; SAL=1300.00; COMM=null; DEPTNO=10; EMAIL=null; "
              + "CREATE_DATETIME=2016-09-02 23:15:01; CREATE_TIME=23:15:01; UPSERT_TIME="
              + expectedLocalTime("2016-09-02 23:15:01")));

  static final List<Pair<Integer, String>> REVERSED_ROWS = ROWS.stream()
      .sorted(Comparator.reverseOrder()).collect(toList());

  static final Map<Integer, String> EMPNO_MAP = ROWS.stream()
      .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

  /**
   * Whether to run this test.
   */
  private boolean enabled() {
    return CalciteSystemProperty.TEST_INNODB.value();
  }

  private CalciteAssert.AssertQuery sql(String sql) {
    return CalciteAssert.that()
        .with(INNODB_MODEL)
        .enable(enabled())
        .query(sql);
  }

  Hook.@Nullable Closeable closeable;

  @BeforeEach
  public void before() {
    this.closeable =
        Hook.SQL2REL_CONVERTER_CONFIG_BUILDER.addThread(
            InnodbAdapterTest::assignHints);
  }

  @AfterEach
  public void after() {
    if (this.closeable != null) {
      this.closeable.close();
      this.closeable = null;
    }
  }

  static void assignHints(Holder<SqlToRelConverter.Config> configHolder) {
    HintStrategyTable strategies = HintStrategyTable.builder()
        .hintStrategy("index", HintPredicates.TABLE_SCAN)
        .build();
    configHolder.accept(config -> config.withHintStrategyTable(strategies));
  }

  private static String expectedLocalTime(String dateTime) {
    ZoneRules rules = ZoneId.systemDefault().getRules();
    LocalDateTime ldt = Utils.parseDateTimeText(dateTime);
    Instant instant = ldt.toInstant(ZoneOffset.of("+00:00"));
    ZoneOffset standardOffset = rules.getOffset(instant);
    OffsetDateTime odt = instant.atOffset(standardOffset);
    return odt.toLocalDateTime().format(Utils.TIME_FORMAT_TIMESTAMP[0]);
  }

  private static String all() {
    return String.join("\n", Pair.right(ROWS)) + "\n";
  }

  private static String allReversed() {
    return String.join("\n", Pair.right(REVERSED_ROWS)) + "\n";
  }

  private static String someEmpnoGt(int empno) {
    return some(ROWS.stream().map(Pair::getKey).filter(i -> i > empno)
        .collect(toList()));
  }

  private static String someEmpnoGte(int empno) {
    return some(ROWS.stream().map(Pair::getKey).filter(i -> i >= empno)
        .collect(toList()));
  }

  private static String someEmpnoLt(int empno) {
    return some(ROWS.stream().map(Pair::getKey).filter(i -> i < empno)
        .collect(toList()));
  }

  private static String someEmpnoLte(int empno) {
    return some(ROWS.stream().map(Pair::getKey).filter(i -> i <= empno)
        .collect(toList()));
  }

  private static String some(int... empnos) {
    return some(Arrays.stream(empnos).boxed().collect(toList()));
  }

  private static String some(List<Integer> empnos) {
    if (empnos == null) {
      return "";
    }
    List<String> result = empnos.stream()
        .map(empno -> EMPNO_MAP.get(empno)).collect(toList());
    return join(result);
  }

  private static String join(List<String> empList) {
    if (empList.isEmpty()) {
      return "";
    }
    return String.join("\n", empList) + "\n";
  }
}
