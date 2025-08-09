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
package org.apache.calcite.rel;

import org.apache.calcite.test.RelSuggesterFixture;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RelCommonExpressionBasicSuggester}.
 * The tests ensure that the suggester can correctly identify common expressions in the query plan
 * that have a certain structure.
 */
public class RelCommonExpressionBasicSuggesterTest {

  private static RelSuggesterFixture fixture() {
    return RelSuggesterFixture.of(RelCommonExpressionBasicSuggesterTest.class,
        new RelCommonExpressionBasicSuggester());
  }

  private static RelSuggesterFixture sql(String sql) {
    return fixture().withSql(sql);
  }

  @Test void testSuggestReturnsCommonFilterTableScan() {
    String sql = "SELECT '2025' FROM emp WHERE empno = 1\n"
        + "UNION\n"
        + "SELECT '2024' FROM emp WHERE empno = 1";
    sql(sql).checkSuggestions();
  }

  @Test void testSuggestReturnsTwoCommonFilterTableScans() {
    String sql = "SELECT '2025' FROM emp WHERE empno = 1\n"
        + "UNION\n"
        + "SELECT '2024' FROM emp WHERE empno = 1\n"
        + "UNION\n"
        + "SELECT '2023' FROM emp WHERE empno = 2\n"
        + "UNION\n"
        + "SELECT '2022' FROM emp WHERE empno = 2";
    sql(sql).checkSuggestions();
  }

  @Test void testSuggestReturnsCommonProjectFilterTableScan() {
    String sql = "SELECT ename FROM emp WHERE empno = 1\n"
        + "UNION\n"
        + "SELECT ename FROM emp WHERE empno = 1";
    sql(sql).checkSuggestions();
  }

  @Test void testSuggestReturnsCommonAggregateProjectTableScan() {
    String sql = "SELECT COUNT(*), 'A' FROM emp GROUP BY ename\n"
        + "UNION\n"
        + "SELECT COUNT(*), 'B' FROM emp GROUP BY ename";
    sql(sql).checkSuggestions();
  }

  @Test void testSuggestReturnsCommonAggregateProjectFilterTableScan() {
    String sql = "SELECT COUNT(*), 'A' FROM emp WHERE empno > 50 GROUP BY ename\n"
        + "UNION\n"
        + "SELECT COUNT(*), 'B' FROM emp WHERE empno > 50 GROUP BY ename";
    sql(sql).checkSuggestions();
  }

  @Test void testSuggestReturnsCommonProjectJoinTableScan() {
    String sql = "WITH cx AS (\n"
        + "SELECT e.empno, d.dname FROM emp e\n"
        + "INNER JOIN dept d ON e.deptno = d.deptno)\n"
        + "SELECT * FROM cx WHERE cx.empno > 50\n"
        + "INTERSECT\n"
        + "SELECT * FROM cx WHERE cx.empno < 100";
    sql(sql).checkSuggestions();
  }

  @Test void testSuggestReturnsCommonProjectFilterJoinTableScan() {
    String sql = "WITH cx AS (\n"
        + "SELECT e.empno, d.dname FROM emp e\n"
        + "INNER JOIN dept d ON e.deptno = d.deptno\n"
        + "WHERE d.dname = 'Sales')\n"
        + "SELECT * FROM cx WHERE cx.empno > 50\n"
        + "INTERSECT\n"
        + "SELECT * FROM cx WHERE cx.empno < 100";
    sql(sql).checkSuggestions();
  }

  @Test void testSuggestReturnsCommonAggregateProjectFilterJoinTableScan() {
    String sql = "WITH cx AS (\n"
        + "SELECT ename, COUNT(*) as cnt FROM emp e\n"
        + "INNER JOIN dept d ON e.deptno = d.deptno\n"
        + "WHERE d.dname = 'Sales' GROUP BY ename)\n"
        + "SELECT * FROM cx WHERE cnt > 50\n"
        + "INTERSECT\n"
        + "SELECT * FROM cx WHERE cnt < 100";
    sql(sql).checkSuggestions();
  }

  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().checkActualAndReferenceFiles();
  }

}
