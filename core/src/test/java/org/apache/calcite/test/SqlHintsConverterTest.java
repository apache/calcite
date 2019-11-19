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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.HintStrategies;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Unit test for {@link org.apache.calcite.rel.hint.RelHint}.
 */
public class SqlHintsConverterTest extends SqlToRelTestBase {
  //~ Static fields/initializers ---------------------------------------------

  private static final String HINT = "properties(k1='v1', k2='v2'), index(ename), no_hash_join";

  private static final RelHint PROPS_HINT = new RelHint(new ArrayList<>(),
      "PROPERTIES", null,
      ImmutableMap.of("K1", "v1", "K2", "v2"));

  private static final RelHint IDX_HINT = new RelHint(new ArrayList<>(), "INDEX",
      ImmutableList.of("ENAME"), null);

  private static final RelHint JOIN_HINT = new RelHint(new ArrayList<>(), "NO_HASH_JOIN",
      null, null);

  private static final HintStrategyTable HINT_STRATEGY_TABLE = createHintStrategies();

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  //~ Tests ------------------------------------------------------------------

  @Test public void testQueryHint() {
    final String sql = withHint("select /*+ %s */ *\n"
        + "from emp e1\n"
        + "inner join dept d1 on e1.deptno = d1.deptno\n"
        + "inner join emp e2 on e1.ename = e2.job");
    final List<String> expectedHints = Arrays.asList(
        "Project:[[PROPERTIES inheritPath:[] options:{K1=v1, K2=v2}], "
            + "[INDEX inheritPath:[] options:[ENAME]], "
            + "[NO_HASH_JOIN inheritPath:[]]]",
        "LogicalJoin:[[NO_HASH_JOIN inheritPath:[0]]]",
        "LogicalJoin:[[NO_HASH_JOIN inheritPath:[0, 0]]]",
        "TableScan:[[PROPERTIES inheritPath:[0, 0, 0] options:{K1=v1, K2=v2}], "
            + "[INDEX inheritPath:[0, 0, 0] options:[ENAME]]]",
        "TableScan:[[PROPERTIES inheritPath:[0, 0, 1] options:{K1=v1, K2=v2}], "
            + "[INDEX inheritPath:[0, 0, 1] options:[ENAME]]]",
        "TableScan:[[PROPERTIES inheritPath:[0, 1, 0] options:{K1=v1, K2=v2}], "
            + "[INDEX inheritPath:[0, 1, 0] options:[ENAME]]]");
    sql(sql).ok(expectedHints);
  }

  @Test public void testNestedQueryHint() {
    final String sql = "select /*+ resource(parallelism='3') */ empno\n"
        + "from (select /*+ resource(mem='20Mb')*/ empno, ename from emp)";
    sql(sql).ok(
        Collections.singletonList("Project:[[RESOURCE inheritPath:{} options:{PARALLELISM=3}]]"));
  }

  @Test public void testTwoLevelNestedQueryHint() {
    final String sql = "select /*+ resource(parallelism='3'), no_hash_join */ empno\n"
        + "from (select /*+ resource(mem='20Mb')*/ empno, ename\n"
        + "from emp left join dept on emp.deptno = dept.deptno)";
    final List<String> expectedHints = Arrays.asList(
        "Project:[[RESOURCE inheritPath:{} options:{PARALLELISM=3}]"
            + ", [NO_HASH_JOIN inheritPath:{}]]",
        "LogicalJoin:[[NO_HASH_JOIN inheritPath:{0}]]");
    sql(sql).ok(expectedHints);
  }

  @Test public void testInvalidQueryHint() {
    final String sql = "select /*+ weird_hint */ empno\n"
        + "from (select /*+ resource(mem='20Mb')*/ empno, ename\n"
        + "from emp left join dept on emp.deptno = dept.deptno)";
    sql(sql).fails("Hint: WEIRD_HINT should be registered in the HintStrategies.");

    final String sql1 = "select /*+ resource(mem='20Mb')*/ empno\n"
        + "from (select /*+ weird_kv_hint(k1='v1') */ empno, ename\n"
        + "from emp left join dept on emp.deptno = dept.deptno)";
    sql(sql1).fails("Hint: WEIRD_KV_HINT should be registered in the HintStrategies.");
  }

  @Test public void testTableHintsInJoin() {
    final String sql = "select\n"
        + "ename, job, sal, dept.name\n"
        + "from emp /*+ index(idx1, idx2) */\n"
        + "join dept /*+ properties(k1='v1', k2='v2') */\n"
        + "on emp.deptno = dept.deptno";
    final List<String> expectedHints = Arrays.asList(
        "TableScan:[[INDEX inheritPath:{} options:[IDX1, IDX2]]]",
        "TableScan:[[PROPERTIES inheritPath:{} options:{K1=v1, K2=v2}]]");
    sql(sql).ok(expectedHints);
  }

  @Test public void testTableHintsInSelect() {
    final String sql = withHint("select * from emp /*+ %s */");
    final List<String> expectedHints = Collections
        .singletonList("TableScan:["
            + "[PROPERTIES inheritPath:{} options:{K1=v1, K2=v2}], "
            + "[INDEX inheritPath:{} options:[ENAME]]]");
    sql(sql).ok(expectedHints);
  }

  @Test public void testTableHintsInInsert() throws Exception {
    final String sql = withHint("insert into dept /*+ %s */ (deptno, name) "
        + "select deptno, name from dept");
    final SqlInsert insert = (SqlInsert) tester.parseQuery(sql);
    assert insert.getTargetTable() instanceof SqlTableRef;
    final SqlTableRef tableRef = (SqlTableRef) insert.getTargetTable();
    List<RelHint> hints = SqlUtil.getRelHint(HINT_STRATEGY_TABLE,
        (SqlNodeList) tableRef.getOperandList().get(1));
    assertHintsEquals(Arrays.asList(PROPS_HINT, IDX_HINT, JOIN_HINT), hints);
  }

  @Test public void testTableHintsInUpdate() throws Exception {
    final String sql = withHint("update emp /*+ %s */ set name = 'test' where deptno = 1");
    final SqlUpdate sqlUpdate = (SqlUpdate) tester.parseQuery(sql);
    assert sqlUpdate.getTargetTable() instanceof SqlTableRef;
    final SqlTableRef tableRef = (SqlTableRef) sqlUpdate.getTargetTable();
    List<RelHint> hints = SqlUtil.getRelHint(HINT_STRATEGY_TABLE,
        (SqlNodeList) tableRef.getOperandList().get(1));
    assertHintsEquals(Arrays.asList(PROPS_HINT, IDX_HINT, JOIN_HINT), hints);
  }

  @Test public void testTableHintsInDelete() throws Exception {
    final String sql = withHint("delete from emp /*+ %s */ where deptno = 1");
    final SqlDelete sqlDelete = (SqlDelete) tester.parseQuery(sql);
    assert sqlDelete.getTargetTable() instanceof SqlTableRef;
    final SqlTableRef tableRef = (SqlTableRef) sqlDelete.getTargetTable();
    List<RelHint> hints = SqlUtil.getRelHint(HINT_STRATEGY_TABLE,
        (SqlNodeList) tableRef.getOperandList().get(1));
    assertHintsEquals(Arrays.asList(PROPS_HINT, IDX_HINT, JOIN_HINT), hints);
  }

  @Test public void testInvalidTableHints() {
    final String sql = "select\n"
        + "ename, job, sal, dept.name\n"
        + "from emp /*+ weird_hint(idx1, idx2) */\n"
        + "join dept /*+ properties(k1='v1', k2='v2') */\n"
        + "on emp.deptno = dept.deptno";
    sql(sql).fails("Hint: WEIRD_HINT should be registered in the HintStrategies.");

    final String sql1 = "select\n"
        + "ename, job, sal, dept.name\n"
        + "from emp /*+ index(idx1, idx2) */\n"
        + "join dept /*+ weird_kv_hint(k1='v1', k2='v2') */\n"
        + "on emp.deptno = dept.deptno";
    sql(sql1).fails("Hint: WEIRD_HINT should be registered in the HintStrategies.");
  }

  @Test public void testJoinHintRequiresSpecificInputs() {
    final String sql = "select /*+ use_hash_join(r, s), use_hash_join(emp, dept) */\n"
        + "ename, job, sal, dept.name\n"
        + "from emp join dept on emp.deptno = dept.deptno";
    final List<String> expectedHints = Arrays.asList(
        "Project:[[USE_HASH_JOIN inheritPath:{} options:[R, S]], "
            + "[USE_HASH_JOIN inheritPath:{} options:[EMP, DEPT]]]",
        "LogicalJoin:[[USE_HASH_JOIN inheritPath:{0} options:[EMP, DEPT]]]");
    // Hint use_hash_join(r, s) expect to be ignored by the join node.
    sql(sql).ok(expectedHints);
  }

  @Test public void testHintsPropagationInHepPlannerRules() {
    final String sql = "select /*+ use_hash_join(r, s), use_hash_join(emp, dept) */\n"
        + "ename, job, sal, dept.name\n"
        + "from emp join dept on emp.deptno = dept.deptno";
    final RelNode rel = tester.convertSqlToRel(sql).rel;
    final RelHint hint = new RelHint(
        Collections.singletonList(0),
        "USE_HASH_JOIN",
        Arrays.asList("EMP", "DEPT"),
        null);
    // Validate Hep planner.
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(MockJoinRule.INSTANCE)
        .build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(rel);
    RelNode newRel = planner.findBestExp();
    new ValidateHintVisitor(hint, Join.class).go(newRel);
  }

  @Test public void testHintsPropagationInVolcanoPlannerRules() {
    final String sql = "select /*+ use_hash_join(r, s), use_hash_join(emp, dept) */\n"
        + "ename, job, sal, dept.name\n"
        + "from emp join dept on emp.deptno = dept.deptno";
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    Tester tester1 = tester.withDecorrelation(true)
        .withClusterFactory(
            relOptCluster -> RelOptCluster.create(planner, relOptCluster.getRexBuilder()));
    final RelNode rel = tester1.convertSqlToRel(sql).rel;
    final RelHint hint = new RelHint(
        Collections.singletonList(0),
        "USE_HASH_JOIN",
        Arrays.asList("EMP", "DEPT"),
        null);
    // Validate Volcano planner.
    RuleSet ruleSet = RuleSets.ofList(
        new MockEnumerableJoinRule(hint), // Rule to validate the hint.
        FilterProjectTransposeRule.INSTANCE,
        FilterMergeRule.INSTANCE,
        ProjectMergeRule.INSTANCE,
        EnumerableRules.ENUMERABLE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_FILTER_RULE,
        EnumerableRules.ENUMERABLE_SORT_RULE,
        EnumerableRules.ENUMERABLE_LIMIT_RULE,
        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    Program program = Programs.of(ruleSet);
    RelTraitSet toTraits = rel
        .getCluster()
        .traitSet()
        .replace(EnumerableConvention.INSTANCE);

    program.run(planner, rel, toTraits,
        Collections.emptyList(), Collections.emptyList());
  }

  //~ Methods ----------------------------------------------------------------

  @Override protected Tester createTester() {
    return super.createTester()
        .withConfig(SqlToRelConverter
            .configBuilder()
            .withHintStrategyTable(HINT_STRATEGY_TABLE)
            .build());
  }

  /** Format the query with hint {@link #HINT}. */
  private static String withHint(String sql) {
    return String.format(Locale.ROOT, sql, HINT);
  }

  /** Sets the SQL statement for a test. */
  public final Sql sql(String sql) {
    return new Sql(sql, tester, expectedEx);
  }

  /**
   * Creates mock hint strategies.
   *
   * @return HintStrategyTable instance
   */
  private static HintStrategyTable createHintStrategies() {
    return HintStrategyTable.builder()
        .addHintStrategy("no_hash_join", HintStrategies.JOIN)
        .addHintStrategy("time_zone", HintStrategies.SET_VAR)
        .addHintStrategy("index", HintStrategies.TABLE_SCAN)
        .addHintStrategy("properties", HintStrategies.TABLE_SCAN)
        .addHintStrategy("resource", HintStrategies.PROJECT)
        .addHintStrategy("use_hash_join",
            HintStrategies.cascade(HintStrategies.JOIN,
                HintStrategies.explicit((hint, rel) -> {
                  if (!(rel instanceof LogicalJoin)) {
                    return false;
                  }
                  LogicalJoin join = (LogicalJoin) rel;
                  final List<String> tableNames = hint.listOptions;
                  final List<String> inputTables = join.getInputs().stream()
                      .filter(input -> input instanceof TableScan)
                      .map(scan -> Util.last(scan.getTable().getQualifiedName()))
                      .collect(Collectors.toList());
                  return equalsStringList(tableNames, inputTables);
                })))
        .build();
  }

  private static boolean equalsStringList(List<String> l, List<String> r) {
    if (l.size() != r.size()) {
      return false;
    }
    for (String s : l) {
      if (!r.contains(s)) {
        return false;
      }
    }
    return true;
  }

  private static void assertHintsEquals(List<RelHint> expected, List<RelHint> actual) {
    assertArrayEquals(expected.toArray(new RelHint[0]), actual.toArray(new RelHint[0]));
  }

  //~ Inner Class ------------------------------------------------------------

  /** A Mock rule to validate the hint. */
  private static class MockJoinRule extends RelOptRule {
    public static final MockJoinRule INSTANCE = new MockJoinRule();

    MockJoinRule() {
      super(operand(LogicalJoin.class, any()), "MockJoinRule");
    }

    public void onMatch(RelOptRuleCall call) {
      LogicalJoin join = call.rel(0);
      assertThat(1, is(join.getHints().size()));
      call.transformTo(
          LogicalJoin.create(join.getLeft(),
              join.getRight(),
              join.getCondition(),
              join.getVariablesSet(),
              join.getJoinType()));
    }
  }

  /** A Mock rule to validate the hint.
   * This rule also converts the rel to EnumerableConvention. */
  private static class MockEnumerableJoinRule extends ConverterRule {
    private final RelHint expectedHint;

    MockEnumerableJoinRule(RelHint hint) {
      super(
          LogicalJoin.class,
          Convention.NONE,
          EnumerableConvention.INSTANCE,
          "MockEnumerableJoinRule");
      this.expectedHint = hint;
    }

    @Override public RelNode convert(RelNode rel) {
      LogicalJoin join = (LogicalJoin) rel;
      assertThat(join.getHints().size(), is(1));
      assertEquals(expectedHint, join.getHints().get(0));
      List<RelNode> newInputs = new ArrayList<>();
      for (RelNode input : join.getInputs()) {
        if (!(input.getConvention() instanceof EnumerableConvention)) {
          input =
              convert(
                  input,
                  input.getTraitSet()
                      .replace(EnumerableConvention.INSTANCE));
        }
        newInputs.add(input);
      }
      final RelOptCluster cluster = join.getCluster();
      final RelNode left = newInputs.get(0);
      final RelNode right = newInputs.get(1);
      final JoinInfo info = join.analyzeCondition();
      return EnumerableHashJoin.create(
          left,
          right,
          info.getEquiCondition(left, right, cluster.getRexBuilder()),
          join.getVariablesSet(),
          join.getJoinType());
    }
  }

  /** A visitor to validate the join node has specific hint. **/
  private static class ValidateHintVisitor extends RelVisitor {
    private RelHint expectedHint;
    private Class clazz;

    /**
     * Creates the validate visitor.
     *
     * @param hint  the hint to validate
     * @param clazz the node type to validate the hint with
     */
    ValidateHintVisitor(RelHint hint, Class clazz) {
      this.expectedHint = hint;
      this.clazz = clazz;
    }

    @Override public void visit(
        RelNode node,
        int ordinal,
        RelNode parent) {
      if (clazz.isInstance(node)) {
        Join join = (Join) node;
        assertThat(join.getHints().size(), is(1));
        assertEquals(expectedHint, join.getHints().get(0));
      }
      super.visit(node, ordinal, parent);
    }
  }

  /** Sql test tool. */
  private static class Sql {
    private String sql;
    private Tester tester;
    private List<String> hintsCollect;
    private ExpectedException expectedEx;

    Sql(String sql, Tester tester, ExpectedException expectedEx) {
      this.sql = sql;
      this.tester = tester;
      this.hintsCollect = new ArrayList<>();
      this.expectedEx = expectedEx;
    }

    Sql tester(Tester tester) {
      this.tester = tester;
      return this;
    }

    Sql ok(List<String> expectedHints) {
      final HintCollector collector = new HintCollector(hintsCollect);
      final RelNode rel = tester.convertSqlToRel(sql).rel;
      rel.accept(collector);
      assertArrayEquals(expectedHints.toArray(new String[0]), hintsCollect.toArray(new String[0]));
      return this;
    }

    Sql fails(String failedMsg) {
      expectedEx.expect(RuntimeException.class);
      expectedEx.expectMessage(failedMsg);
      tester.convertSqlToRel(sql);
      return this;
    }

    /** A shuttle to collect all the hints within the relational expression into a collection. */
    private static class HintCollector extends RelShuttleImpl {
      private final List<String> hintsCollect;

      HintCollector(List<String> hintsCollect) {
        this.hintsCollect = hintsCollect;
      }

      @Override public RelNode visit(TableScan scan) {
        if (scan.getHints().size() > 0) {
          this.hintsCollect.add("TableScan:" + scan.getHints().toString());
        }
        return super.visit(scan);
      }

      @Override public RelNode visit(LogicalJoin join) {
        if (join.getHints().size() > 0) {
          this.hintsCollect.add("LogicalJoin:" + join.getHints().toString());
        }
        return super.visit(join);
      }

      @Override public RelNode visit(LogicalProject project) {
        if (project.getHints().size() > 0) {
          this.hintsCollect.add("Project:" + project.getHints().toString());
        }
        return super.visit(project);
      }
    }
  }
}

// End SqlHintsConverterTest.java
