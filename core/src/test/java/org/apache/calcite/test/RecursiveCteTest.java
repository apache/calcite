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
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSearchClause;
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

import java.util.regex.Pattern;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import static java.util.Objects.requireNonNull;

/** Examples of recursive CTE parsing, validation and execution, including the
 * SQL-standard SEARCH and CYCLE clauses. */
@Timeout(30)
class RecursiveCteTest {
  private static final String WITH_NUMBERS = "WITH RECURSIVE t(n) AS (\n"
      + "  VALUES (1)\n"
      + "  UNION ALL\n"
      + "  SELECT n + 1 FROM t WHERE n < 3\n"
      + ")\n";

  private static final String NUMBERS = WITH_NUMBERS + "SELECT * FROM t";

  private static final String WITH_TREE = "WITH RECURSIVE edges(src, dst) AS (\n"
      + "  VALUES (0, 2), (0, 1), (1, 4), (2, 3)),\n"
      + "walk(node) AS (VALUES (0)\n"
      + "  UNION ALL\n"
      + "  SELECT e.dst FROM walk AS w JOIN edges AS e ON e.src = w.node)\n";

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
    // CYCLE and SEARCH are the optional fifth and sixth operands of a WITH item.
    assertThat(item.getOperandList(), hasSize(6));
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

  /** SEARCH follows AS (query); parsing and unparsing preserve its mode and keys. */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "DEPTH|n", "BREADTH|n", "DEPTH|n, depth", "BREADTH|n, depth"
  })
  void testSearchClause(String mode, String keys) throws SqlParseException {
    final String sql = "WITH RECURSIVE t(n, depth) AS (VALUES (1, 0)\n"
        + "UNION ALL SELECT n + 1, depth + 1 FROM t WHERE n < 3)\n"
        + "SEARCH " + mode + " FIRST BY " + keys + " SET seq\n"
        + "SELECT n FROM t ORDER BY seq";
    final SqlNode parsed = SqlParser.create(sql).parseQuery();
    final SqlWith with = (SqlWith) ((SqlOrderBy) parsed).query;
    final SqlSearchClause search =
        requireNonNull(((SqlWithItem) with.withList.get(0)).searchClause);
    assertThat(search.getKind(), is(SqlKind.SEARCH_CLAUSE));
    assertThat(search.breadthFirst.booleanValue(), is(mode.equals("BREADTH")));
    assertThat(search.orderColumn.getSimple(), is("SEQ"));
    assertThat(search.columns, hasSize(keys.equals("n") ? 1 : 2));
    assertThat(search.getParserPosition().getLineNum(), is(3));
    final String unparsed = parsed.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    final String quotedKeys = keys.equals("n") ? "\"N\"" : "\"N\", \"DEPTH\"";
    assertThat(Pattern.compile("\\s+").matcher(unparsed).replaceAll(" "),
        containsString("SEARCH " + mode + " FIRST BY " + quotedKeys + " SET \"SEQ\""));
    final SqlNode roundTrip = SqlParser.create(unparsed).parseQuery();
    assertThat(roundTrip.toSqlString(CalciteSqlDialect.DEFAULT).getSql(), is(unparsed));
  }

  /** SEARCH creates a sort key; ORDER BY requests the corresponding row order. */
  @Test void testSearchDepthFirst() {
    CalciteAssert.that()
        .query(WITH_TREE + "SEARCH DEPTH FIRST BY node SET seq\n"
            + "SELECT node FROM walk ORDER BY seq")
        .returnsOrdered("NODE=0", "NODE=1", "NODE=4", "NODE=2", "NODE=3");
  }

  /** Within a level, BY keys take precedence over the parents' order. */
  @Test void testSearchBreadthFirst() {
    CalciteAssert.that()
        .query(WITH_TREE + "SEARCH BREADTH FIRST BY node SET seq\n"
            + "SELECT node FROM walk ORDER BY seq")
        .returnsOrdered("NODE=0", "NODE=1", "NODE=2", "NODE=3", "NODE=4");
  }

  @ParameterizedTest
  @ValueSource(strings = {"DEPTH", "BREADTH"})
  void testSearchCompositeKey(String mode) {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n, k, depth) AS (VALUES (0, 0, 0)\n"
            + "UNION ALL SELECT v.n, v.k, t.depth + 1\n"
            + "FROM t CROSS JOIN (VALUES (1, 2), (2, 1), (1, 1)) AS v(n, k)\n"
            + "WHERE t.depth < 1)\n"
            + "SEARCH " + mode + " FIRST BY k, n SET seq\n"
            + "SELECT n, k FROM t ORDER BY seq")
        .returnsOrdered("N=0; K=0", "N=1; K=1", "N=2; K=1", "N=1; K=2");
  }

  @ParameterizedTest
  @ValueSource(strings = {"DEPTH", "BREADTH"})
  void testSearchFollowingWithItem(String mode) {
    CalciteAssert.that()
        .query(WITH_NUMBERS + "SEARCH " + mode + " FIRST BY n SET seq,\n"
            + "u AS (SELECT * FROM t) SELECT n FROM u ORDER BY seq")
        .returnsOrdered("N=1", "N=2", "N=3");
  }

  @ParameterizedTest
  @ValueSource(strings = {"DEPTH", "BREADTH"})
  void testSearchQuotedNames(String mode) {
    CalciteAssert.that()
        .query("WITH RECURSIVE \"Walk\"(\"Node\") AS (VALUES (1)\n"
            + "UNION ALL SELECT \"Node\" + 1 FROM \"Walk\" WHERE \"Node\" < 3)\n"
            + "SEARCH " + mode + " FIRST BY \"Node\" SET \"Sequence\"\n"
            + "SELECT \"Node\" FROM \"Walk\" ORDER BY \"Sequence\"")
        .returnsOrdered("Node=1", "Node=2", "Node=3");
  }

  @ParameterizedTest
  @ValueSource(strings = {"DEPTH", "BREADTH"})
  void testSearchEmptySeed(String mode) {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (SELECT n FROM (VALUES (1)) AS v(n) WHERE FALSE\n"
            + "UNION ALL SELECT n + 1 FROM t WHERE n < 3)\n"
            + "SEARCH " + mode + " FIRST BY n SET seq\n"
            + "SELECT * FROM t ORDER BY seq")
        .returnsCount(0);
  }

  /** SEARCH precedes CYCLE, which stops expansion of the repeated node. */
  @ParameterizedTest
  @ValueSource(strings = {"DEPTH", "BREADTH"})
  void testSearchWithCycle(String mode) {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n, depth) AS (VALUES (1, 0)\n"
            + "UNION ALL SELECT MOD(n + 1, 3), depth + 1 FROM t WHERE depth < 5)\n"
            + "SEARCH " + mode + " FIRST BY n SET seq\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p\n"
            + "SELECT n, c, CARDINALITY(p) AS path_length FROM t ORDER BY seq")
        .returnsOrdered("N=1; C=N; PATH_LENGTH=1", "N=2; C=N; PATH_LENGTH=2",
            "N=0; C=N; PATH_LENGTH=3", "N=1; C=Y; PATH_LENGTH=4");
  }

  @ParameterizedTest
  @CsvSource({"DEPTH, UNION ALL", "BREADTH, UNION ALL",
      "DEPTH, UNION", "BREADTH, UNION"})
  void testSearchMultipleRoots(String mode, String union) {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (2), (1)\n"
            + union + " SELECT n + 2 FROM t WHERE n < 3)\n"
            + "SEARCH " + mode + " FIRST BY n SET seq\n"
            + "SELECT n FROM t ORDER BY seq")
        .returnsOrdered(mode.equals("DEPTH")
            ? new String[]{"N=1", "N=3", "N=2", "N=4"}
            : new String[]{"N=1", "N=2", "N=3", "N=4"});
  }

  @ParameterizedTest
  @ValueSource(strings = {"DEPTH", "BREADTH"})
  void testSearchNullKey(String mode) {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n, depth) AS (VALUES (0, 0)\n"
            + "UNION ALL SELECT v.n, t.depth + 1\n"
            + "FROM t CROSS JOIN (VALUES (CAST(NULL AS INTEGER)), (2), (1)) AS v(n)\n"
            + "WHERE t.depth < 1)\n"
            + "SEARCH " + mode + " FIRST BY n SET seq\n"
            + "SELECT n FROM t ORDER BY seq")
        .returnsOrdered("N=0", "N=1", "N=2", "N=null");
  }

  @ParameterizedTest
  @CsvSource({"DEPTH, false", "BREADTH, false", "DEPTH, true", "BREADTH, true"})
  void testSearchMultipleSortKeys(String mode, boolean limit) {
    CalciteAssert.that()
        .query(WITH_TREE + "SEARCH " + mode + " FIRST BY node SET seq\n"
            + "SELECT node FROM walk ORDER BY seq DESC, node"
            + (limit ? " FETCH NEXT 5 ROWS ONLY" : ""))
        .returnsOrdered(mode.equals("DEPTH")
            ? new String[]{"NODE=3", "NODE=2", "NODE=4", "NODE=1", "NODE=0"}
            : new String[]{"NODE=4", "NODE=3", "NODE=2", "NODE=1", "NODE=0"});
  }

  @ParameterizedTest
  @CsvSource({"false, *", "true, *", "false, t.*", "true, t.*"})
  void testSearchStarInRecursiveTerm(boolean identifierExpansion, String projection) {
    SqlValidatorTestCase.FIXTURE
        .withValidatorIdentifierExpansion(identifierExpansion)
        .withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
            + "UNION ALL SELECT " + projection + " FROM t WHERE n < 0)\n"
            + "SEARCH DEPTH FIRST BY n SET seq SELECT * FROM t")
        .type("RecordType(INTEGER NOT NULL N, "
            + "RecordType(INTEGER NOT NULL N) NOT NULL ARRAY NOT NULL SEQ) NOT NULL");
  }

  @Test void testSearchBreadthFirstRowType() {
    SqlValidatorTestCase.FIXTURE.withSql(WITH_NUMBERS
        + "SEARCH BREADTH FIRST BY n SET seq SELECT * FROM t")
        .type("RecordType(INTEGER NOT NULL N, "
            + "RecordType(INTEGER NOT NULL $DEPTH, INTEGER NOT NULL N) NOT NULL SEQ) NOT NULL");
  }

  @ParameterizedTest
  @ValueSource(strings = {"DEPTH", "BREADTH"})
  void testSearchColumnAliases(String mode) {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1)\n"
            + "UNION ALL SELECT w.seq + 1 FROM t AS w(seq) WHERE w.seq < 3)\n"
            + "SEARCH " + mode + " FIRST BY n SET seq SELECT n FROM t ORDER BY seq")
        .returnsOrdered("N=1", "N=2", "N=3");
  }

  @ParameterizedTest
  @ValueSource(strings = {"DEPTH", "BREADTH"})
  void testSearchInferredColumnNames(String mode) {
    CalciteAssert.that()
        .query("WITH RECURSIVE t AS (SELECT 1 AS n\n"
            + "UNION ALL SELECT n + 1 FROM t WHERE n < 3)\n"
            + "SEARCH " + mode + " FIRST BY n SET seq SELECT n FROM t ORDER BY seq")
        .returnsOrdered("N=1", "N=2", "N=3");
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "SEARCH DEPTH FIRST BY ^missing^ SET seq|SEARCH column 'MISSING' is not a column of WITH item 'T'",
      "SEARCH BREADTH FIRST BY n, ^n^ SET seq|Duplicate name 'N' in column list",
      "SEARCH DEPTH FIRST BY n SET ^n^|SEARCH generated column 'N' conflicts with another column",
      "SEARCH DEPTH FIRST BY n SET seq CYCLE n SET ^seq^ USING p|CYCLE generated column 'SEQ' conflicts with another column",
      "SEARCH BREADTH FIRST BY n SET seq CYCLE n SET c USING ^seq^|CYCLE generated column 'SEQ' conflicts with another column"
  })
  void testSearchInvalidColumns(String clause, String error) {
    SqlValidatorTestCase.FIXTURE.withSql(WITH_NUMBERS + clause + " SELECT * FROM t")
        .fails(error);
  }

  @Test void testSearchRequiresRecursive() {
    SqlValidatorTestCase.FIXTURE.withSql("WITH t(n) AS (VALUES (1))\n"
        + "^SEARCH DEPTH FIRST BY n SET seq^ SELECT * FROM t")
        .fails("SEARCH requires WITH RECURSIVE");
  }

  @ParameterizedTest
  @ValueSource(strings = {"SELECT 2", "SELECT a.n FROM t a JOIN t b ON a.n = b.n",
      "SELECT a.n FROM (VALUES (1)) v(n) LEFT JOIN t a ON a.n = v.n",
      "SELECT a.n FROM t a WHERE EXISTS (SELECT 1 FROM t b WHERE b.n = a.n)"})
  void testSearchInvalidRecursiveReference(String step) {
    SqlValidatorTestCase.FIXTURE.withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
        + "UNION ALL " + step + ") ^SEARCH DEPTH FIRST BY n SET seq^ SELECT * FROM t")
        .fails("SEARCH requires a UNION \\[ALL\\] with a recursive SELECT containing"
            + " exactly one direct reference to its WITH item");
  }

  @ParameterizedTest
  @ValueSource(strings =
      {"SELECT DISTINCT n FROM t", "SELECT MAX(n) FROM t", "SELECT SUM(n) OVER () FROM t"})
  void testSearchInvalidRecursiveSelect(String step) {
    SqlValidatorTestCase.FIXTURE.withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
        + "UNION ALL " + step + ") ^SEARCH BREADTH FIRST BY n SET seq^ SELECT * FROM t")
        .fails("SEARCH is not supported with aggregation, DISTINCT, or window functions"
            + " in the recursive SELECT");
  }

  @Test void testSearchOrderNotVisibleInRecursiveTerm() {
    SqlValidatorTestCase.FIXTURE.withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
        + "UNION ALL SELECT n FROM t WHERE ^seq^ IS NOT NULL)\n"
        + "SEARCH DEPTH FIRST BY n SET seq SELECT * FROM t")
        .fails("Column 'SEQ' not found in any table");
  }

  /** CYCLE follows AS (query) and belongs to that WITH item. */
  @ParameterizedTest
  @ValueSource(strings = {
      "CYCLE n SET is_cycle TO 'Y' DEFAULT 'N' USING cycle_path",
      "CYCLE n, depth SET is_cycle TO 'Y' DEFAULT 'N' USING cycle_path",
      "CYCLE n, depth SET is_cycle USING cycle_path"
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

  @Test void testCycleDefaultMarks() throws SqlParseException {
    final String sql = "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n FROM t)\n"
        + "CYCLE n SET c USING p SELECT * FROM t";
    final SqlWith with = (SqlWith) SqlParser.create(sql).parseQuery();
    final SqlCycleClause cycle =
        requireNonNull(((SqlWithItem) with.withList.get(0)).cycleClause);
    assertThat(((SqlLiteral) cycle.markValue).booleanValue(), is(true));
    assertThat(((SqlLiteral) cycle.defaultValue).booleanValue(), is(false));
    assertThat(with.toSqlString(CalciteSqlDialect.DEFAULT).getSql(),
        containsString("TO TRUE DEFAULT FALSE"));
    CalciteAssert.that().query(sql)
        .returnsOrdered("N=1; C=false; P=[{1}]", "N=1; C=true; P=[{1}, {1}]");
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
            + "WHERE depth < 10) CYCLE n, k SET c TO 'Y' DEFAULT 'N' USING p\n"
            + "SELECT n, k, depth, c, CARDINALITY(p) AS len FROM t")
        .returnsOrdered("N=0; K=0; DEPTH=0; C=N; LEN=1",
            "N=1; K=1; DEPTH=1; C=N; LEN=2",
            "N=0; K=2; DEPTH=2; C=N; LEN=3",
            "N=1; K=0; DEPTH=3; C=N; LEN=4",
            "N=0; K=1; DEPTH=4; C=N; LEN=5",
            "N=1; K=2; DEPTH=5; C=N; LEN=6",
            "N=0; K=0; DEPTH=6; C=Y; LEN=7");
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
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT * FROM t")
        .returnsOrdered("N=1; C=N; P=[{1}]", "N=1; C=Y; P=[{1}, {1}]");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "INTERVAL '1' DAY",
      "INTERVAL '123-4' YEAR(3) TO MONTH",
      "INTERVAL '123 04:05:06.789' DAY(3) TO SECOND(3)"
  })
  void testCycleIntervalKey(String key) {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(k, d) AS (VALUES (" + key + ", 0)\n"
            + "UNION ALL SELECT k, d + 1 FROM t WHERE d < 2)\n"
            + "CYCLE k SET c TO 'Y' DEFAULT 'N' USING p\n"
            + "SELECT d, c, k = " + key + " AS key_matches, CARDINALITY(p) AS depth FROM t")
        .returnsOrdered("D=0; C=N; KEY_MATCHES=true; DEPTH=1",
            "D=1; C=Y; KEY_MATCHES=true; DEPTH=2");
  }

  @Test void testCycleInferredColumnNames() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t AS (SELECT 1 AS n\n"
            + "UNION ALL SELECT * FROM t) CYCLE n SET c TO 'Y' DEFAULT 'N' USING p\n"
            + "SELECT n, c FROM t")
        .returnsOrdered("N=1; C=N", "N=1; C=Y");
  }

  @Test void testCycleColumnAliases() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1)\n"
            + "UNION ALL SELECT w.c FROM t AS w(c))\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT n, c FROM t")
        .returnsOrdered("N=1; C=N", "N=1; C=Y");
  }

  @Test void testCycleColumnAliasCollisions() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n, k, m) AS (VALUES (1, 2, 3)\n"
            + "UNION ALL SELECT w.c, w.c0, w.p FROM t AS w(c, c0, p))\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p\n"
            + "SELECT n, k, m, c, CARDINALITY(p) AS depth FROM t")
        .returnsOrdered("N=1; K=2; M=3; C=N; DEPTH=1",
            "N=1; K=2; M=3; C=Y; DEPTH=2");
  }

  @Test void testCycleWithConjunction() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1), (2), (3)\n"
            + "UNION ALL SELECT n FROM t WHERE n > 1 AND n < 3)\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT n, c FROM t")
        .returnsUnordered("N=1; C=N", "N=2; C=N", "N=3; C=N",
            "N=2; C=Y");
  }

  @Test void testCycleNaturalJoin() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1)\n"
            + "UNION ALL SELECT n FROM t NATURAL JOIN (VALUES (1, TRUE)) AS v(n, c))\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT n, c FROM t")
        .returnsOrdered("N=1; C=N", "N=1; C=Y");
  }

  @Test void testCycleNullKey() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n, depth) AS (VALUES (CAST(NULL AS INTEGER), 0)\n"
            + "UNION ALL SELECT n, depth + 1 FROM t WHERE depth < 3)\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT n, depth, c FROM t")
        .returnsOrdered("N=null; DEPTH=0; C=N", "N=null; DEPTH=1; C=N",
            "N=null; DEPTH=2; C=N", "N=null; DEPTH=3; C=N");
  }

  @Test void testCycleEmptySeed() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (SELECT n FROM (VALUES (1)) AS v(n) WHERE FALSE\n"
            + "UNION ALL SELECT n FROM t)\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT * FROM t")
        .returnsCount(0);
  }

  @ParameterizedTest
  @CsvSource({"false, *", "true, *", "false, t.*", "true, t.*"})
  void testCycleStarInRecursiveTerm(boolean identifierExpansion, String projection) {
    SqlValidatorTestCase.FIXTURE
        .withValidatorIdentifierExpansion(identifierExpansion)
        .withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
            + "UNION ALL SELECT " + projection + " FROM t)\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT * FROM t")
        .type("RecordType(INTEGER NOT NULL N, CHAR(1) NOT NULL C, "
            + "RecordType(INTEGER NOT NULL N) NOT NULL ARRAY NOT NULL P) NOT NULL");
  }

  @Test void testCycleRowType() {
    SqlValidatorTestCase.FIXTURE.withSql(WITH_NUMBERS
        + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT * FROM t")
        .type("RecordType(INTEGER NOT NULL N, CHAR(1) NOT NULL C, "
            + "RecordType(INTEGER NOT NULL N) NOT NULL ARRAY NOT NULL P) NOT NULL");
  }

  @Test void testCyclePathFieldNames() {
    SqlValidatorTestCase.FIXTURE
        .withSql("WITH RECURSIVE t(\"nodeId\", k) AS (VALUES (1, 2)\n"
            + "UNION ALL SELECT * FROM t)\n"
            + "CYCLE k, \"nodeId\" SET c USING p SELECT * FROM t")
        .type("RecordType(INTEGER NOT NULL nodeId, INTEGER NOT NULL K, "
            + "BOOLEAN NOT NULL C, RecordType(INTEGER NOT NULL K, "
            + "INTEGER NOT NULL nodeId) NOT NULL ARRAY NOT NULL P) NOT NULL");
  }

  @ParameterizedTest
  @ValueSource(strings = {"SELECT a.n FROM t a JOIN t b ON a.n = b.n",
      "SELECT a.n FROM (VALUES (1)) v(n) LEFT JOIN t a ON a.n = v.n",
      "SELECT a.n FROM t a WHERE EXISTS (SELECT 1 FROM t b WHERE b.n = a.n)"})
  void testCycleInvalidRecursiveReference(String step) {
    SqlValidatorTestCase.FIXTURE.withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
        + "UNION ALL " + step + ") ^CYCLE n SET c TO 'Y' DEFAULT 'N' USING p^\n"
        + "SELECT * FROM t")
        .fails("CYCLE requires a UNION \\[ALL\\] with a recursive SELECT containing"
            + " exactly one direct reference to its WITH item");
  }

  @ParameterizedTest
  @ValueSource(strings =
      {"SELECT DISTINCT n FROM t", "SELECT MAX(n) FROM t", "SELECT SUM(n) OVER () FROM t"})
  void testCycleInvalidRecursiveSelect(String step) {
    SqlValidatorTestCase.FIXTURE.withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
        + "UNION ALL " + step + ") ^CYCLE n SET c TO 'Y' DEFAULT 'N' USING p^\n"
        + "SELECT * FROM t")
        .fails("CYCLE is not supported with aggregation, DISTINCT, or window functions"
            + " in the recursive SELECT");
  }

  @Test void testCycleMarkNotVisibleInRecursiveTerm() {
    SqlValidatorTestCase.FIXTURE
        .withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
            + "UNION ALL SELECT n FROM t WHERE ^c^ = 'N')\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p\n"
            + "SELECT * FROM t")
        .fails("Column 'C' not found in any table");
  }

  @Test void testCyclePathNotVisibleInRecursiveTerm() {
    SqlValidatorTestCase.FIXTURE
        .withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
            + "UNION ALL SELECT n FROM t WHERE CARDINALITY(^p^) < 3)\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p\n"
            + "SELECT * FROM t")
        .fails("Column 'P' not found in any table");
  }

  @Test void testCycleQuotedNames() {
    CalciteAssert.that()
        .query("WITH RECURSIVE \"Walk\"(\"Node\") AS (VALUES (1)\n"
            + "UNION ALL SELECT w.\"Node\" FROM \"Walk\" AS w)\n"
            + "CYCLE \"Node\" SET \"Cycle\" TO 'Y' DEFAULT 'N' USING \"Path\"\n"
            + "SELECT \"Node\", \"Cycle\" FROM \"Walk\"")
        .returnsOrdered("Node=1; Cycle=N", "Node=1; Cycle=Y");
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

  @ParameterizedTest
  @ValueSource(strings = {"Y", " ", "\u00e9"})
  void testCycleSingleCharacterMarks(String mark) {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n FROM t)\n"
            + "CYCLE n SET c TO '" + mark + "' DEFAULT 'N' USING p SELECT n, c FROM t")
        .returnsOrdered("N=1; C=N", "N=1; C=" + mark);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCycleBooleanMarks(boolean mark) {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n FROM t)\n"
            + "CYCLE n SET c TO " + mark + " DEFAULT " + !mark + " USING p SELECT n, c FROM t")
        .returnsOrdered("N=1; C=" + !mark, "N=1; C=" + mark);
  }

  @Test void testCycleWithLocalWith() {
    CalciteAssert.that()
        .query("WITH RECURSIVE t(n) AS (WITH seed(n) AS (VALUES (1))\n"
            + "SELECT n FROM seed UNION ALL SELECT n FROM t)\n"
            + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT n, c FROM t")
        .returnsOrdered("N=1; C=N", "N=1; C=Y");
  }

  @Test void testCyclePlan() {
    final RelNode rel = SqlToRelFixture.DEFAULT.withSql(WITH_NUMBERS
        + "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT * FROM t").toRel();
    final String plan = RelOptUtil.toString(rel);
    assertThat(plan, containsString("LogicalRepeatUnion(all=[true])"));
    assertThat(plan, containsString("$CYCLE_PATH_CONTAINS"));
    assertThat(plan, containsString("$RECURSIVE_PATH_APPEND"));
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "CYCLE ^missing^ SET c TO 'Y' DEFAULT 'N' USING p|CYCLE column 'MISSING' is not a column of WITH item 'T'",
      "CYCLE n, ^n^ SET c TO 'Y' DEFAULT 'N' USING p|Duplicate name 'N' in column list",
      "CYCLE n SET ^n^ TO 'Y' DEFAULT 'N' USING p|CYCLE generated column 'N' conflicts with another column",
      "CYCLE n SET c TO 'Y' DEFAULT 'N' USING ^n^|CYCLE generated column 'N' conflicts with another column",
      "CYCLE n SET c TO 'Y' DEFAULT 'N' USING ^c^|CYCLE generated column 'C' conflicts with another column"
  })
  void testCycleInvalidColumns(String clause, String error) {
    SqlValidatorTestCase.FIXTURE.withSql(WITH_NUMBERS + clause + " SELECT * FROM t")
        .fails(error);
  }

  @ParameterizedTest
  @ValueSource(strings = {"TO NULL DEFAULT 'N'", "TO 'Y' DEFAULT NULL",
      "TO 'Y' DEFAULT 'Y'", "TO ' ' DEFAULT ' '",
      "TO '' DEFAULT 'N'", "TO 'Y' DEFAULT ''",
      "TO 'YY' DEFAULT 'N'", "TO 'Y' DEFAULT 'NN'",
      "TO TRUE DEFAULT TRUE", "TO FALSE DEFAULT FALSE", "TO TRUE DEFAULT UNKNOWN",
      "TO TRUE DEFAULT 'N'", "TO 'Y' DEFAULT FALSE", "TO 1 DEFAULT 0", "TO -1 DEFAULT 0",
      "TO 1.0 DEFAULT 0.0", "TO 1e0 DEFAULT 0e0",
      "TO 'Y' DEFAULT 0", "TO n DEFAULT 'N'",
      "TO X'01' DEFAULT X'0100'", "TO INTERVAL '1' DAY DEFAULT INTERVAL '24' HOUR"})
  void testCycleInvalidMark(String values) {
    SqlValidatorTestCase.FIXTURE.withSql(WITH_NUMBERS
        + "^CYCLE n SET c " + values + " USING p^ SELECT * FROM t")
        .fails("CYCLE mark and default must be distinct, non-null Boolean"
            + " or compatible single-character string literals");
  }

  @Test void testCycleRequiresRecursive() {
    SqlValidatorTestCase.FIXTURE.withSql("WITH t(n) AS (VALUES (1))\n"
        + "^CYCLE n SET c TO 'Y' DEFAULT 'N' USING p^ SELECT * FROM t")
        .fails("CYCLE requires WITH RECURSIVE");
  }

  @Test void testCycleRequiresRecursiveReference() {
    SqlValidatorTestCase.FIXTURE.withSql("WITH RECURSIVE t(n) AS (VALUES (1)\n"
        + "UNION ALL SELECT 2) ^CYCLE n SET c TO 'Y' DEFAULT 'N' USING p^\n"
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
