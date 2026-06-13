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
package org.apache.calcite.sql.parser;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.test.DiffTestCase;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.SortedSet;
import java.util.TreeSet;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests SQL Parser.
 */
public class CoreSqlParserTest extends SqlParserTest {

  /**
   * Tests that reserved keywords are not added to the parser unintentionally.
   * (Most keywords are non-reserved. The set of reserved words generally
   * only changes with a new version of the SQL standard.)
   *
   * <p>If the new keyword added is intended to be a reserved keyword, update
   * the {@link SqlParserTest#RESERVED_KEYWORDS} list. If not, add the keyword to the
   * non-reserved keyword list in the parser.
   */
  @Test void testNoUnintendedNewReservedKeywords() {
    assumeTrue(isNotSubclass(), "don't run this test for sub-classes");
    final SqlAbstractParserImpl.Metadata metadata =
        fixture().parser().getMetadata();

    final SortedSet<String> reservedKeywords = new TreeSet<>();
    final SortedSet<String> keywords92 = keywords("92");
    for (String s : metadata.getTokens()) {
      if (metadata.isKeyword(s) && metadata.isReservedWord(s)) {
        reservedKeywords.add(s);
      }
      // Check that the parser's list of SQL:92
      // reserved words is consistent with keywords("92").
      assertThat(s, metadata.isSql92ReservedWord(s),
          is(keywords92.contains(s)));
    }

    final String reason = "The parser has at least one new reserved keyword. "
        + "Are you sure it should be reserved? Difference:\n"
        + DiffTestCase.diffLines(ImmutableList.copyOf(getReservedKeywords()),
        ImmutableList.copyOf(reservedKeywords));
    assertThat(reason, reservedKeywords, is(getReservedKeywords()));
  }

  private boolean isNotSubclass() {
    return this.getClass().equals(CoreSqlParserTest.class);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7603">[CALCITE-7603]
   * Support ROW constructors that name fields</a>. */
  @Test void testRowWithFieldNames() {
    // All fields named
    sql("select row(1 as a, 'hello' as b) from emp")
        .ok("SELECT (ROW(1 AS `A`, 'hello' AS `B`))\nFROM `EMP`");
    // Mixed: some fields named, some not
    sql("select row(1 as a, 2) from emp")
        .ok("SELECT (ROW(1 AS `A`, 2))\nFROM `EMP`");
    // No field names (existing behavior unchanged)
    sql("select row(1, 2) from emp")
        .ok("SELECT (ROW(1, 2))\nFROM `EMP`");
    // Expression with AS
    sql("select row(empno + 1 as eno, ename as en) from emp")
        .ok("SELECT (ROW((`EMPNO` + 1) AS `ENO`, `ENAME` AS `EN`))\nFROM `EMP`");
    // Round-trip: the canonical form can be re-parsed
    final SqlParserFixture f = fixture().withConfig(c -> c.withQuoting(Quoting.BACK_TICK));
    f.sql("SELECT (ROW(1 AS `A`, 2))\nFROM `EMP`").same();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7364">[CALCITE-7364]
   * Support the syntax ROW(T.* EXCLUDE cols) for creating nested ROW values</a>. */
  @Test void testRowStarExclude() {
    // Use backticks to ensure that sql(q).same() in general
    final SqlParserFixture f = fixture().withConfig(c -> c.withQuoting(Quoting.BACK_TICK));
    final String empExcludeEmpno = "SELECT (ROW(`EMP`.* EXCLUDE (`EMP`.`EMPNO`)))\n"
        + "FROM `EMP`";

    // Simple star with one excluded column
    final String starExcludeEmpno = "SELECT (ROW(* EXCLUDE (`EMPNO`)))\n"
        + "FROM `EMP`";
    sql("select row(* exclude(empno)) from emp").ok(starExcludeEmpno);
    f.sql(starExcludeEmpno).same();

    // Table-qualified star with excluded column
    sql("select row(emp.* exclude(emp.empno)) from emp").ok(empExcludeEmpno);
    f.sql(empExcludeEmpno).same();

    // EXCEPT is normalized to EXCLUDE on unparse
    sql("select row(emp.* except(emp.empno)) from emp").ok(empExcludeEmpno);

    // Multiple excluded columns
    final String starExcludeEmpnoMgr = "SELECT (ROW(* EXCLUDE (`EMPNO`, `MGR`)))\n"
        + "FROM `EMP`";
    sql("select row(* exclude(empno, mgr)) from emp").ok(starExcludeEmpnoMgr);
    f.sql(starExcludeEmpnoMgr).same();

    // Mixed: table-qualified star with exclude, plus plain star
    final String empExcludeEmpnoDeptStar =
        "SELECT (ROW(`EMP`.* EXCLUDE (`EMP`.`EMPNO`), `DEPT`.*))\n"
        + "FROM `EMP`\n"
        + "INNER JOIN `DEPT` ON (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)";
    sql("select row(emp.* exclude(emp.empno), dept.*)"
            + " from emp join dept on emp.deptno = dept.deptno")
        .ok(empExcludeEmpnoDeptStar);
    f.sql(empExcludeEmpnoDeptStar).same();

    // Nested ROW with EXCLUDE
    final String nestedStarExcludeEmpno = "SELECT (ROW((ROW(* EXCLUDE (`EMPNO`)))))\n"
        + "FROM `EMP`";
    sql("select row(row(* exclude(empno))) from emp").ok(nestedStarExcludeEmpno);
    f.sql(nestedStarExcludeEmpno).same();
  }
}
