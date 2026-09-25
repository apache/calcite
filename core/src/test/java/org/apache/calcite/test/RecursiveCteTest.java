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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCycleClause;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import static java.util.Objects.requireNonNull;

/** Examples of recursive CTE parsing, validation and execution, including the
 * SQL-standard CYCLE clause. */
@Timeout(30)
class RecursiveCteTest {
  private static final String WITH_NUMBERS = "WITH RECURSIVE t(n) AS (\n"
      + "  VALUES (1)\n"
      + "  UNION ALL\n"
      + "  SELECT n + 1 FROM t WHERE n < 3\n"
      + ")\n";

  private static final String NUMBERS = WITH_NUMBERS + "SELECT * FROM t";

  /** The self-reference is an identifier in the parse tree, not a Java object
   * reference back to the WITH item. Name resolution happens during validation.
   *
   * <pre>
   * SqlWith
   *   withList: SqlNodeList
   *     SqlWithItem(name=T, columnList=[N], recursive=true)
   *       query: UNION ALL
   *         VALUES (1)
   *         SqlSelect(n + 1, from=T, where=n &lt; 3)
   *   body: SqlSelect(*, from=T)
   * </pre>
   */
  @Test void testRecursiveCteParseTree() throws SqlParseException {
    final SqlWith with = (SqlWith) SqlParser.create(NUMBERS).parseQuery();
    assertThat(with.getKind(), is(SqlKind.WITH));
    assertThat(with.withList, hasSize(1));
    final SqlWithItem item = (SqlWithItem) with.withList.get(0);
    assertThat(item.name.getSimple(), is("T"));
    assertThat(item.recursive.booleanValue(), is(true));
    final SqlNodeList columnList = requireNonNull(item.columnList);
    assertThat(columnList, hasSize(1));
    assertThat(((SqlIdentifier) columnList.get(0)).getSimple(), is("N"));
    // CYCLE is the optional fifth operand of a WITH item.
    assertThat(item.getOperandList(), hasSize(5));
    final SqlCall union = (SqlCall) item.query;
    assertThat(union.getOperator(), is(SqlStdOperatorTable.UNION_ALL));
    assertThat(union.operand(0).getKind(), is(SqlKind.VALUES));
    final SqlSelect step = union.operand(1);
    assertThat(((SqlIdentifier) requireNonNull(step.getFrom())).getSimple(), is("T"));
    assertThat(requireNonNull(step.getWhere()).getKind(), is(SqlKind.LESS_THAN));
    final SqlSelect body = (SqlSelect) with.body;
    assertThat(((SqlIdentifier) requireNonNull(body.getFrom())).getSimple(), is("T"));
  }

  /** RECURSIVE belongs to WITH syntactically, but is copied to each WITH item. */
  @Test void testRecursiveKeywordAppliesToEveryWithItem() throws SqlParseException {
    final String sql = "WITH RECURSIVE a(n) AS (VALUES (1)),\n"
        + "b(n) AS (SELECT n FROM a UNION ALL SELECT n + 1 FROM b WHERE n < 3)\n"
        + "SELECT * FROM b";
    final SqlWith with = (SqlWith) SqlParser.create(sql).parseQuery();
    assertThat(with.withList, hasSize(2));
    assertThat(((SqlWithItem) with.withList.get(0)).recursive.booleanValue(), is(true));
    assertThat(((SqlWithItem) with.withList.get(1)).recursive.booleanValue(), is(true));
  }

  /** CYCLE follows AS (query) and belongs to that WITH item. */
  @ParameterizedTest
  @ValueSource(strings = {
      "CYCLE n SET is_cycle TO 'Y' DEFAULT 'N' USING cycle_path",
      "CYCLE n, depth SET is_cycle TO 'Y' DEFAULT 'N' USING cycle_path"
  })
  void testStandardCycleClause(String cycleClause) throws SqlParseException {
    final String sql = "WITH RECURSIVE t(n, depth) AS (\n"
        + "  VALUES (1, 0)\n"
        + "  UNION ALL\n"
        + "  SELECT MOD(n + 1, 2), depth + 1 FROM t WHERE depth < 3\n"
        + ")\n"
        + cycleClause + "\n"
        + "SELECT * FROM t";
    final SqlWith with = (SqlWith) SqlParser.create(sql).parseQuery();
    final SqlCycleClause cycle =
        requireNonNull(((SqlWithItem) with.withList.get(0)).cycleClause);
    assertThat(cycle.getKind(), is(SqlKind.CYCLE));
    assertThat(cycle.getOperandList(), hasSize(5));
    assertThat(cycle.markColumn.getSimple(), is("IS_CYCLE"));
    assertThat(cycle.pathColumn.getSimple(), is("CYCLE_PATH"));
    assertThat(cycle.getParserPosition().getLineNum(), is(6));
    assertThat(with.toString(), containsString("CYCLE"));
    final String unparsed = with.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    final SqlWith roundTrip = (SqlWith) SqlParser.create(unparsed).parseQuery();
    assertThat(requireNonNull(((SqlWithItem) roundTrip.withList.get(0)).cycleClause)
        .columns, hasSize(cycle.columns.size()));
  }

  @Test void testCycleExecution() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1)\n"
            + "UNION ALL SELECT MOD(n + 1, 3) FROM t)\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p\n"
            + "SELECT n, c, CARDINALITY(p) AS depth FROM t")
        .returnsOrdered("N=1; C=N; DEPTH=1", "N=2; C=N; DEPTH=2",
            "N=0; C=N; DEPTH=3", "N=1; C=Y; DEPTH=4");
  }

  /** Two paths reach D independently; returning to A closes each path. */
  @Test void testCycleDirectedGraph() {
    CalciteAssert.that()
        .query("WITH RECURSIVE edges(src, dst) AS (\n"
            + "VALUES ('A', 'B'), ('A', 'C'), ('B', 'D'), ('C', 'D'), ('D', 'A')),\n"
            + "walk(node) AS (VALUES ('A')\n"
            + "UNION ALL SELECT e.dst FROM walk AS w JOIN edges AS e ON e.src = w.node)\n"
            + "CYCLE node SET is_cycle TO 'Y' DEFAULT 'N' USING cycle_path\n"
            + "SELECT node, is_cycle, CARDINALITY(cycle_path) AS depth FROM walk")
        .returnsUnordered("NODE=A; IS_CYCLE=N; DEPTH=1",
            "NODE=B; IS_CYCLE=N; DEPTH=2", "NODE=C; IS_CYCLE=N; DEPTH=2",
            "NODE=D; IS_CYCLE=N; DEPTH=3", "NODE=D; IS_CYCLE=N; DEPTH=3",
            "NODE=A; IS_CYCLE=Y; DEPTH=4", "NODE=A; IS_CYCLE=Y; DEPTH=4");
  }

  @ParameterizedTest
  @ValueSource(strings = {"UNION ALL", "UNION DISTINCT"})
  void testCycleCompositeKey(String union) {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n, k, depth) AS (VALUES (0, 0, 0)\n"
            + union + " SELECT MOD(n + 1, 2), MOD(k + 1, 3), depth + 1 FROM t\n"
            + "WHERE depth < 10) CYCLE n, k SET c TO TRUE DEFAULT FALSE USING p\n"
            + "SELECT n, k, depth, c, CARDINALITY(p) AS len FROM t")
        .returnsOrdered("N=0; K=0; DEPTH=0; C=false; LEN=1",
            "N=1; K=1; DEPTH=1; C=false; LEN=2",
            "N=0; K=2; DEPTH=2; C=false; LEN=3",
            "N=1; K=0; DEPTH=3; C=false; LEN=4",
            "N=0; K=1; DEPTH=4; C=false; LEN=5",
            "N=1; K=2; DEPTH=5; C=false; LEN=6",
            "N=0; K=0; DEPTH=6; C=true; LEN=7");
  }

  @Test void testCyclePath() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT * FROM t)\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p\n"
            + "SELECT * FROM t")
        .returnsOrdered("N=1; C=N; P=[{1}]", "N=1; C=Y; P=[{1}, {1}]");
  }

  @Test void testCycleUnionDistinctSeedDuplicates() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1), (1) UNION SELECT n FROM t)\n"
            + "CYCLE n SET c TO TRUE DEFAULT FALSE USING p SELECT * FROM t")
        .returnsOrdered("N=1; C=false; P=[{1}]", "N=1; C=true; P=[{1}, {1}]");
  }

  @Test void testCycleInferredColumnNames() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t AS (SELECT 1 AS n\n"
            + "UNION ALL SELECT * FROM t) CYCLE n SET c TO 1 DEFAULT 0 USING p\n"
            + "SELECT n, c FROM t")
        .returnsOrdered("N=1; C=0", "N=1; C=1");
  }

  @Test void testCycleColumnAliases() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1)\n"
            + "UNION ALL SELECT w.c FROM t AS w(c))\n"
            + "CYCLE n SET c TO TRUE DEFAULT FALSE USING p SELECT n, c FROM t")
        .returnsOrdered("N=1; C=false", "N=1; C=true");
  }

  @Test void testCycleColumnAliasCollisions() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n, k, m) AS (VALUES (1, 2, 3)\n"
            + "UNION ALL SELECT w.c, w.c0, w.p FROM t AS w(c, c0, p))\n"
            + "CYCLE n SET c TO TRUE DEFAULT FALSE USING p\n"
            + "SELECT n, k, m, c, CARDINALITY(p) AS depth FROM t")
        .returnsOrdered("N=1; K=2; M=3; C=false; DEPTH=1",
            "N=1; K=2; M=3; C=true; DEPTH=2");
  }

  @Test void testCycleWithConjunction() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1), (2), (3)\n"
            + "UNION ALL SELECT n FROM t WHERE n > 1 AND n < 3)\n"
            + "CYCLE n SET c TO TRUE DEFAULT FALSE USING p SELECT n, c FROM t")
        .returnsUnordered("N=1; C=false", "N=2; C=false", "N=3; C=false",
            "N=2; C=true");
  }

  @Test void testCycleNaturalJoin() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1)\n"
            + "UNION ALL SELECT n FROM t NATURAL JOIN (VALUES (1, TRUE)) AS v(n, c))\n"
            + "CYCLE n SET c TO TRUE DEFAULT FALSE USING p SELECT n, c FROM t")
        .returnsOrdered("N=1; C=false", "N=1; C=true");
  }

  @Test void testCycleNullKey() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n, depth) AS (VALUES (CAST(NULL AS INTEGER), 0)\n"
            + "UNION ALL SELECT n, depth + 1 FROM t WHERE depth < 3)\n"
            + "CYCLE n SET c TO TRUE DEFAULT FALSE USING p SELECT n, depth, c FROM t")
        .returnsOrdered("N=null; DEPTH=0; C=false", "N=null; DEPTH=1; C=false",
            "N=null; DEPTH=2; C=false", "N=null; DEPTH=3; C=false");
  }

  @Test void testCycleEmptySeed() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (SELECT n FROM (VALUES (1)) AS v(n) WHERE FALSE\n"
            + "UNION ALL SELECT n FROM t)\n"
            + "CYCLE n SET c TO TRUE DEFAULT FALSE USING p SELECT * FROM t")
        .returnsCount(0);
  }

  @Test void testCycleRowType() {
    SqlValidatorTestCase.FIXTURE.withSql(WITH_NUMBERS
        + "CYCLE n SET c TO TRUE DEFAULT FALSE USING p SELECT * FROM t")
        .type("RecordType(INTEGER NOT NULL N, BOOLEAN NOT NULL C, "
            + "RecordType(INTEGER NOT NULL EXPR$0) NOT NULL ARRAY NOT NULL P) NOT NULL");
  }

  @ParameterizedTest
  @ValueSource(strings = {"SELECT a.n FROM t a JOIN t b ON a.n = b.n",
      "SELECT a.n FROM (VALUES (1)) v(n) LEFT JOIN t a ON a.n = v.n",
      "SELECT a.n FROM t a WHERE EXISTS (SELECT 1 FROM t b WHERE b.n = a.n)"})
  void testCycleInvalidRecursiveReference(String step) {
    SqlValidatorTestCase.FIXTURE.withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
        + "UNION ALL " + step + ") ^CYCLE n SET c TO TRUE DEFAULT FALSE USING p^\n"
        + "SELECT * FROM t")
        .fails("CYCLE requires a UNION \\[ALL\\] with a recursive SELECT containing"
            + " exactly one direct reference to its WITH item");
  }

  @ParameterizedTest
  @ValueSource(strings =
      {"SELECT DISTINCT n FROM t", "SELECT MAX(n) FROM t", "SELECT SUM(n) OVER () FROM t"})
  void testCycleInvalidRecursiveSelect(String step) {
    SqlValidatorTestCase.FIXTURE.withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
        + "UNION ALL " + step + ") ^CYCLE n SET c TO TRUE DEFAULT FALSE USING p^\n"
        + "SELECT * FROM t")
        .fails("CYCLE is not supported with aggregation, DISTINCT, or window functions"
            + " in the recursive SELECT");
  }

  @Test void testCycleQuotedNames() {
    CalciteAssert.that()
        .query("WITH RECURSIVE \"Walk\"(\"Node\") AS (VALUES (1)\n"
            + "UNION ALL SELECT w.\"Node\" FROM \"Walk\" AS w)\n"
            + "CYCLE \"Node\" SET \"Cycle\" TO 'yes' DEFAULT 'no' USING \"Path\"\n"
            + "SELECT \"Node\", \"Cycle\" FROM \"Walk\"")
        .returnsOrdered("Node=1; Cycle=no ", "Node=1; Cycle=yes");
  }

  @Test void testCycleNoCycle() {
    CalciteAssert.that()
        .query(WITH_NUMBERS + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p\n"
            + "SELECT n, c, CARDINALITY(p) AS depth FROM t")
        .returnsOrdered("N=1; C=N; DEPTH=1", "N=2; C=N; DEPTH=2",
            "N=3; C=N; DEPTH=3");
  }

  @Test void testCycleFollowingWithItem() {
    CalciteAssert.that()
        .query(WITH_NUMBERS + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p,\n"
            + "u AS (SELECT n, c, CARDINALITY(p) AS depth FROM t) SELECT * FROM u")
        .returnsOrdered("N=1; C=N; DEPTH=1", "N=2; C=N; DEPTH=2",
            "N=3; C=N; DEPTH=3");
  }

  @Test void testCycleNegativeMark() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n FROM t)\n"
            + "CYCLE n SET c TO -1 DEFAULT 0 USING p SELECT n, c FROM t")
        .returnsOrdered("N=1; C=0", "N=1; C=-1");
  }

  @Test void testCycleWithLocalWith() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (WITH seed(n) AS (VALUES (1))\n"
            + "SELECT n FROM seed UNION ALL SELECT n FROM t)\n"
            + "CYCLE n SET c TO TRUE DEFAULT FALSE USING p SELECT n, c FROM t")
        .returnsOrdered("N=1; C=false", "N=1; C=true");
  }

  @Test void testCyclePlan() {
    final RelNode rel = SqlToRelFixture.DEFAULT.withSql(WITH_NUMBERS
        + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT * FROM t").toRel();
    final String plan = RelOptUtil.toString(rel);
    assertThat(plan, containsString("LogicalRepeatUnion(all=[true])"));
    assertThat(plan, containsString("$CYCLE_PATH_CONTAINS"));
    assertThat(plan, containsString("$CYCLE_PATH_APPEND"));
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "CYCLE ^missing^ SET c TO TRUE DEFAULT FALSE USING p|CYCLE column 'MISSING' is not a column of WITH item 'T'",
      "CYCLE n, ^n^ SET c TO TRUE DEFAULT FALSE USING p|Duplicate name 'N' in column list",
      "CYCLE n SET ^n^ TO TRUE DEFAULT FALSE USING p|CYCLE generated column 'N' conflicts with another column",
      "CYCLE n SET c TO TRUE DEFAULT FALSE USING ^n^|CYCLE generated column 'N' conflicts with another column",
      "CYCLE n SET c TO TRUE DEFAULT FALSE USING ^c^|CYCLE generated column 'C' conflicts with another column"
  })
  void testCycleInvalidColumns(String clause, String error) {
    SqlValidatorTestCase.FIXTURE.withSql(WITH_NUMBERS + clause + " SELECT * FROM t")
        .fails(error);
  }

  @ParameterizedTest
  @ValueSource(strings = {"TO NULL DEFAULT 'N'", "TO 'Y' DEFAULT NULL",
      "TO TRUE DEFAULT TRUE", "TO 1 DEFAULT 1.0", "TO TRUE DEFAULT 1",
      "TO n DEFAULT 0", "TO 'Y' DEFAULT 'Y '", "TO 'Y' DEFAULT 'Y'"})
  void testCycleInvalidMark(String values) {
    SqlValidatorTestCase.FIXTURE.withSql(WITH_NUMBERS
        + "^CYCLE n SET c " + values + " USING p^ SELECT * FROM t")
        .fails("CYCLE mark and default must be non-null literals of compatible types"
            + " with distinct values");
  }

  @Test void testCycleRequiresRecursive() {
    SqlValidatorTestCase.FIXTURE.withSql("WITH t(n) AS (VALUES (1))\n"
        + "^CYCLE n SET c TO TRUE DEFAULT FALSE USING p^ SELECT * FROM t")
        .fails("CYCLE requires WITH RECURSIVE");
  }

  @Test void testCycleRequiresRecursiveReference() {
    SqlValidatorTestCase.FIXTURE.withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
        + "UNION ALL SELECT 2) ^CYCLE n SET c TO TRUE DEFAULT FALSE USING p^\n"
        + "SELECT * FROM t")
        .fails("CYCLE requires a UNION \\[ALL\\] with a recursive SELECT containing"
            + " exactly one direct reference to its WITH item");
  }

  @Test void testRecursiveReferenceRequiresRecursiveKeyword() {
    SqlValidatorTestCase.FIXTURE
        .withSql("WITH t(n) AS (VALUES (1)\n"
            + "UNION ALL SELECT n + 1 FROM ^t^ WHERE n < 3)\n"
            + "SELECT * FROM t")
        .fails("Object 'T' not found");
  }

  @Test void testRecursiveReferenceNotVisibleInSeed() {
    SqlValidatorTestCase.FIXTURE
        .withSql("WITH RECURSIVE t(n) AS (SELECT n FROM ^t^\n"
            + "UNION ALL SELECT n + 1 FROM t WHERE n < 3)\n"
            + "SELECT * FROM t")
        .fails("Object 'T' not found");
  }

  @Test void testRecursiveCteRowType() {
    SqlValidatorTestCase.FIXTURE.withSql(NUMBERS)
        .type("RecordType(INTEGER NOT NULL N) NOT NULL");
  }

  /** Both branches write the working table; the recursive branch reads it. */
  @Test void testRecursiveCtePlan() {
    final RelNode rel = SqlToRelFixture.DEFAULT.withSql(NUMBERS).toRel();
    final String expected = "LogicalProject(N=[$0])\n"
        + "  LogicalRepeatUnion(all=[true])\n"
        + "    LogicalTableSpool(readType=[LAZY], writeType=[LAZY], table=[[T]])\n"
        + "      LogicalValues(tuples=[[{ 1 }]])\n"
        + "    LogicalTableSpool(readType=[LAZY], writeType=[LAZY], table=[[T]])\n"
        + "      LogicalProject(EXPR$0=[+($0, 1)])\n"
        + "        LogicalFilter(condition=[<($0, 3)])\n"
        + "          LogicalTableScan(table=[[T]])\n";
    assertThat(RelOptUtil.toString(rel), isLinux(expected));
  }

  @Test void testRecursiveCteExecution() {
    CalciteAssert.that()
        .query(NUMBERS + " ORDER BY n")
        .returnsOrdered("N=1", "N=2", "N=3");
  }

  /** UNION removes duplicate whole rows. It does not implement CYCLE n:
   * n repeats, but the changing depth keeps each row distinct. The explicit
   * depth bound makes this example terminate without cycle detection. */
  @Test void testUnionDistinctDoesNotDetectCyclesByKey() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n, depth) AS (\n"
            + "  VALUES (1, 0)\n"
            + "  UNION\n"
            + "  SELECT MOD(n + 1, 2), depth + 1 FROM t WHERE depth < 3\n"
            + ")\n"
            + "SELECT * FROM t ORDER BY depth")
        .returnsOrdered("N=1; DEPTH=0", "N=0; DEPTH=1",
            "N=1; DEPTH=2", "N=0; DEPTH=3");
  }
}
