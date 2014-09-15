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
package org.eigenbase.test;

import java.util.List;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableModificationRel;
import org.eigenbase.rel.metadata.CachingRelMetadataProvider;
import org.eigenbase.rel.metadata.ChainedRelMetadataProvider;
import org.eigenbase.rel.metadata.DefaultRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMetadataProvider;
import org.eigenbase.rel.rules.AddRedundantSemiJoinRule;
import org.eigenbase.rel.rules.AggregateProjectMergeRule;
import org.eigenbase.rel.rules.CoerceInputsRule;
import org.eigenbase.rel.rules.ConvertMultiJoinRule;
import org.eigenbase.rel.rules.ExtractJoinFilterRule;
import org.eigenbase.rel.rules.FilterToCalcRule;
import org.eigenbase.rel.rules.MergeCalcRule;
import org.eigenbase.rel.rules.MergeProjectRule;
import org.eigenbase.rel.rules.ProjectToCalcRule;
import org.eigenbase.rel.rules.PullConstantsThroughAggregatesRule;
import org.eigenbase.rel.rules.PushAggregateThroughUnionRule;
import org.eigenbase.rel.rules.PushFilterPastJoinRule;
import org.eigenbase.rel.rules.PushFilterPastProjectRule;
import org.eigenbase.rel.rules.PushFilterPastSetOpRule;
import org.eigenbase.rel.rules.PushJoinThroughUnionRule;
import org.eigenbase.rel.rules.PushProjectPastFilterRule;
import org.eigenbase.rel.rules.PushProjectPastJoinRule;
import org.eigenbase.rel.rules.PushProjectPastSetOpRule;
import org.eigenbase.rel.rules.PushSemiJoinPastFilterRule;
import org.eigenbase.rel.rules.PushSemiJoinPastJoinRule;
import org.eigenbase.rel.rules.PushSemiJoinPastProjectRule;
import org.eigenbase.rel.rules.ReduceAggregatesRule;
import org.eigenbase.rel.rules.ReduceExpressionsRule;
import org.eigenbase.rel.rules.ReduceValuesRule;
import org.eigenbase.rel.rules.RemoveEmptyRules;
import org.eigenbase.rel.rules.RemoveSemiJoinRule;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.rel.rules.SemiJoinRule;
import org.eigenbase.rel.rules.TableAccessRule;
import org.eigenbase.rel.rules.TransitivePredicatesOnJoinRule;
import org.eigenbase.rel.rules.UnionToDistinctRule;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.hep.HepMatchOrder;
import org.eigenbase.relopt.hep.HepPlanner;
import org.eigenbase.relopt.hep.HepProgram;
import org.eigenbase.relopt.hep.HepProgramBuilder;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.util.Util;

import net.hydromatic.optiq.prepare.Prepare;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for rules in {@code org.eigenbase.rel} and subpackages.
 *
 * <p>As input, the test supplies a SQL statement and a single rule; the SQL is
 * translated into relational algebra and then fed into a
 * {@link org.eigenbase.relopt.hep.HepPlanner}. The planner fires the rule on
 * every
 * pattern match in a depth-first left-to-right preorder traversal of the tree
 * for as long as the rule continues to succeed in applying its transform. (For
 * rules which call transformTo more than once, only the last result is used.)
 * The plan before and after "optimization" is diffed against a .ref file using
 * {@link DiffRepository}.
 *
 * <p>Procedure for adding a new test case:
 *
 * <ol>
 * <li>Add a new public test method for your rule, following the existing
 * examples. You'll have to come up with an SQL statement to which your rule
 * will apply in a meaningful way. See {@link SqlToRelTestBase} class comments
 * for details on the schema.
 * <li>Run the test. It should fail. Inspect the output in
 * RelOptRulesTest.log.xml; verify that the "planBefore" is the correct
 * translation of your SQL, and that it contains the pattern on which your rule
 * is supposed to fire. If all is well, check out RelOptRulesTest.ref.xml and
 * replace it with the new .log.xml.
 * <li>Run the test again. It should fail again, but this time it should contain
 * a "planAfter" entry for your rule. Verify that your rule applied its
 * transformation correctly, and then update the .ref.xml file again.
 * <li>Run the test one last time; this time it should pass.
 * </ol>
 */
public class RelOptRulesTest extends RelOptTestBase {
  //~ Methods ----------------------------------------------------------------

  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(RelOptRulesTest.class);
  }

  @Test public void testUnionToDistinctRule() {
    checkPlanning(
        UnionToDistinctRule.INSTANCE,
        "select * from dept union select * from dept");
  }

  @Test public void testExtractJoinFilterRule() {
    checkPlanning(
        ExtractJoinFilterRule.INSTANCE,
        "select 1 from emp inner join dept on emp.deptno=dept.deptno");
  }

  @Test public void testAddRedundantSemiJoinRule() {
    checkPlanning(
        AddRedundantSemiJoinRule.INSTANCE,
        "select 1 from emp inner join dept on emp.deptno = dept.deptno");
  }

  @Test public void testStrengthenJoinType() {
    // The "Filter(... , right.c IS NOT NULL)" above a left join is pushed into
    // the join, makes it an inner join, and then disappears because c is NOT
    // NULL.
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(MergeProjectRule.INSTANCE)
            .addRuleInstance(PushFilterPastProjectRule.INSTANCE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
            .build();
    checkPlanning(tester.withDecorrelation(true).withTrim(true), preProgram,
        new HepPlanner(program),
        "select * from dept where exists (\n"
        + "  select * from emp\n"
        + "  where emp.deptno = dept.deptno\n"
        + "  and emp.sal > 100)");
  }

  @Test public void testPushFilterThroughOuterJoin() {
    checkPlanning(
        PushFilterPastJoinRule.FILTER_ON_JOIN,
        "select 1 from sales.dept d left outer join sales.emp e"
        + " on d.deptno = e.deptno"
        + " where d.name = 'Charlie'");
  }

  @Test public void testSemiJoinRule() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(PushFilterPastProjectRule.INSTANCE)
            .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(MergeProjectRule.INSTANCE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(SemiJoinRule.INSTANCE)
            .build();
    checkPlanning(tester.withDecorrelation(true).withTrim(true), preProgram,
        new HepPlanner(program),
        "select * from dept where exists (\n"
            + "  select * from emp\n"
            + "  where emp.deptno = dept.deptno\n"
            + "  and emp.sal > 100)");
  }

  protected void semiJoinTrim() {

    final DiffRepository diffRepos = getDiffRepos();
    String sql = diffRepos.expand(null, "${sql}");

    TesterImpl t = (TesterImpl) tester;
    final RelDataTypeFactory typeFactory = t.getTypeFactory();
    final Prepare.CatalogReader catalogReader =
        t.createCatalogReader(typeFactory);
    final SqlValidator validator =
        t.createValidator(
            catalogReader, typeFactory);
    final SqlToRelConverter converter =
        t.createSqlToRelConverter(
            validator,
            catalogReader,
            typeFactory);

    final SqlNode sqlQuery;
    try {
      sqlQuery = t.parseQuery(sql);
    } catch (Exception e) {
      throw Util.newInternal(e);
    }

    final SqlNode validatedQuery = validator.validate(sqlQuery);
    RelNode rel =
        converter.convertQuery(validatedQuery, false, true);
    rel = converter.decorrelate(sqlQuery, rel);

    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(PushFilterPastProjectRule.INSTANCE)
            .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(MergeProjectRule.INSTANCE)
            .addRuleInstance(SemiJoinRule.INSTANCE)
            .build();

    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(rel);
    rel = planner.findBestExp();

    String planBefore = NL + RelOptUtil.toString(rel);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    converter.setTrimUnusedFields(true);
    rel = converter.trimUnusedFields(rel);
    String planAfter = NL + RelOptUtil.toString(rel);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test public void testSemiJoinTrim() {
    semiJoinTrim();
  }

  @Test public void testReduceAverage() {
    checkPlanning(
        ReduceAggregatesRule.INSTANCE,
        "select name, max(name), avg(deptno), min(name)"
        + " from sales.dept group by name");
  }

  @Test public void testPushProjectPastFilter() {
    checkPlanning(
        PushProjectPastFilterRule.INSTANCE,
        "select empno + deptno from emp where sal = 10 * comm "
        + "and upper(ename) = 'FOO'");
  }

  @Test public void testPushProjectPastJoin() {
    checkPlanning(
        PushProjectPastJoinRule.INSTANCE,
        "select e.sal + b.comm from emp e inner join bonus b "
        + "on e.ename = b.ename and e.deptno = 10");
  }

  @Test public void testPushProjectPastSetOp() {
    checkPlanning(
        PushProjectPastSetOpRule.INSTANCE,
        "select sal from "
        + "(select * from emp e1 union all select * from emp e2)");
  }

  @Test public void testPushJoinThroughUnionOnLeft() {
    checkPlanning(
        PushJoinThroughUnionRule.LEFT_UNION,
        "select r1.sal from "
        + "(select * from emp e1 union all select * from emp e2) r1, "
        + "emp r2");
  }

  @Test public void testPushJoinThroughUnionOnRight() {
    checkPlanning(
        PushJoinThroughUnionRule.RIGHT_UNION,
        "select r1.sal from "
        + "emp r1, "
        + "(select * from emp e1 union all select * from emp e2) r2");
  }

  @Ignore("cycles")
  @Test public void testMergeFilterWithJoinCondition() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(TableAccessRule.INSTANCE)
        .addRuleInstance(ExtractJoinFilterRule.INSTANCE)
        .addRuleInstance(FilterToCalcRule.INSTANCE)
        .addRuleInstance(MergeCalcRule.INSTANCE)
            //.addRuleInstance(FennelCalcRule.instance);
            //.addRuleInstance(FennelCartesianJoinRule.instance);
        .addRuleInstance(ProjectToCalcRule.INSTANCE)
        .build();

    checkPlanning(program,
        "select d.name as dname,e.ename as ename"
        + " from emp e inner join dept d"
        + " on e.deptno=d.deptno"
        + " where d.name='Propane'");
  }

  @Ignore("cycles")
  @Test public void testHeterogeneousConversion() throws Exception {
    // This one tests the planner's ability to correctly
    // apply different converters on top of a common
    // sub-expression.  The common sub-expression is the
    // reference to the table sales.emps.  On top of that
    // are two projections, unioned at the top.  For one
    // of the projections, we force a Fennel implementation.
    // For the other, we force a Java implementation.
    // Then, we request conversion from Fennel to Java,
    // and verify that it only applies to one usage of the
    // table, not both (which would be incorrect).
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(TableAccessRule.INSTANCE)
        .addRuleInstance(ProjectToCalcRule.INSTANCE)

            // Control the calc conversion.
        .addMatchLimit(1)

            // Let the converter rule fire to its heart's content.
        .addMatchLimit(HepProgram.MATCH_UNTIL_FIXPOINT)
        .build();

    checkPlanning(program,
        "select upper(ename) from emp union all"
        + " select lower(ename) from emp");
  }

  @Test public void testPushSemiJoinPastJoinRuleLeft() throws Exception {
    // tests the case where the semijoin is pushed to the left
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(PushSemiJoinPastJoinRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
        + "where e1.deptno = d.deptno and e1.empno = e2.empno");
  }

  @Test public void testPushSemiJoinPastJoinRuleRight() throws Exception {
    // tests the case where the semijoin is pushed to the right
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(PushSemiJoinPastJoinRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
  }

  @Test public void testPushSemiJoinPastFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(PushSemiJoinPastFilterRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e.ename from emp e, dept d "
        + "where e.deptno = d.deptno and e.ename = 'foo'");
  }

  @Test public void testConvertMultiJoinRule() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(ConvertMultiJoinRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
  }

  @Test public void testReduceConstants() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    // NOTE jvs 27-May-2006: among other things, this verifies
    // intentionally different treatment for identical coalesce expression
    // in select and where.

    // There is "CAST(2 AS INTEGER)" in the plan because 2 has type "INTEGER NOT
    // NULL" and we need "INTEGER".
    checkPlanning(program,
        "select 1+2, d.deptno+(3+4), (5+6)+d.deptno, cast(null as integer),"
        + " coalesce(2,null), row(7+8)"
        + " from dept d inner join emp e"
        + " on d.deptno = e.deptno + (5-5)"
        + " where d.deptno=(7+8) and d.deptno=coalesce(2,null)");
  }

  @Test public void testReduceConstants2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    checkPlanning(program,
        "select p1 is not distinct from p0 from (values (2, cast(null as integer))) as t(p0, p1)");
  }

  @Test public void testReduceConstantsEliminatesFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    // WHERE NULL is the same as WHERE FALSE, so get empty result
    checkPlanning(program,
        "select * from (values (1,2)) where 1 + 2 > 3 + CAST(NULL AS INTEGER)");
  }

  @Test public void testAlreadyFalseEliminatesFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    checkPlanning(program,
        "select * from (values (1,2)) where false");
  }

  @Test public void testReduceConstantsCalc() throws Exception {
    // This reduction does not work using
    // ReduceExpressionsRule.PROJECT_INSTANCE or FILTER_INSTANCE,
    // only CALC_INSTANCE, because we need to pull the project expression
    //    upper('table')
    // into the condition
    //    upper('table') = 'TABLE'
    // and reduce it to TRUE. Only in the Calc are projects and conditions
    // combined.
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastProjectRule.INSTANCE)
        .addRuleInstance(PushFilterPastSetOpRule.INSTANCE)
        .addRuleInstance(FilterToCalcRule.INSTANCE)
        .addRuleInstance(ProjectToCalcRule.INSTANCE)
        .addRuleInstance(MergeCalcRule.INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.CALC_INSTANCE)

            // the hard part is done... a few more rule calls to clean up
        .addRuleInstance(RemoveEmptyRules.UNION_INSTANCE)
        .addRuleInstance(ProjectToCalcRule.INSTANCE)
        .addRuleInstance(MergeCalcRule.INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.CALC_INSTANCE)
        .build();

    // Result should be same as typing
    //  SELECT * FROM (VALUES ('TABLE        ', 'T')) AS T(U, S)
    checkPlanning(program,
        "select * from (\n"
        + "  select upper(substring(x FROM 1 FOR 2) || substring(x FROM 3)) as u,\n"
        + "      substring(x FROM 1 FOR 1) as s\n"
        + "  from (\n"
        + "    select 'table' as x from (values (true))\n"
        + "    union\n"
        + "    select 'view' from (values (true))\n"
        + "    union\n"
        + "    select 'foreign table' from (values (true))\n"
        + "  )\n"
        + ") where u = 'TABLE'");
  }

  @Test public void testRemoveSemiJoin() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(RemoveSemiJoinRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e.ename from emp e, dept d "
        + "where e.deptno = d.deptno");
  }

  @Test public void testRemoveSemiJoinWithFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(PushSemiJoinPastFilterRule.INSTANCE)
        .addRuleInstance(RemoveSemiJoinRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e.ename from emp e, dept d "
        + "where e.deptno = d.deptno and e.ename = 'foo'");
  }

  @Test public void testRemoveSemiJoinRight() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(PushSemiJoinPastJoinRule.INSTANCE)
        .addRuleInstance(RemoveSemiJoinRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
  }

  @Test public void testRemoveSemiJoinRightWithFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(PushSemiJoinPastJoinRule.INSTANCE)
        .addRuleInstance(PushSemiJoinPastFilterRule.INSTANCE)
        .addRuleInstance(RemoveSemiJoinRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno "
        + "and d.name = 'foo'");
  }

  @Test public void testConvertMultiJoinRuleOuterJoins() throws Exception {
    final Tester tester1 = tester.withCatalogReaderFactory(
        new Function<RelDataTypeFactory, Prepare.CatalogReader>() {
          public Prepare.CatalogReader apply(RelDataTypeFactory typeFactory) {
            return new MockCatalogReader(typeFactory, true) {
              @Override
              public MockCatalogReader init() {
                // CREATE SCHEMA abc;
                // CREATE TABLE a(a INT);
                // ...
                // CREATE TABLE j(j INT);
                MockSchema schema = new MockSchema("SALES");
                registerSchema(schema);
                final RelDataType intType =
                    typeFactory.createSqlType(SqlTypeName.INTEGER);
                for (int i = 0; i < 10; i++) {
                  String t = String.valueOf((char) ('A' + i));
                  MockTable table = new MockTable(this, schema, t);
                  table.addColumn(t, intType);
                  registerTable(table);
                }
                return this;
              }
              // CHECKSTYLE: IGNORE 1
            }.init();
          }
        });
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(RemoveTrivialProjectRule.INSTANCE)
        .addRuleInstance(ConvertMultiJoinRule.INSTANCE)
        .build();
    checkPlanning(tester1, null,
        new HepPlanner(program),
        "select * from "
        + "    (select * from "
        + "        (select * from "
        + "            (select * from A right outer join B on a = b) "
        + "            left outer join "
        + "            (select * from C full outer join D on c = d)"
        + "            on a = c and b = d) "
        + "        right outer join "
        + "        (select * from "
        + "            (select * from E full outer join F on e = f) "
        + "            right outer join "
        + "            (select * from G left outer join H on g = h) "
        + "            on e = g and f = h) "
        + "        on a = e and b = f and c = g and d = h) "
        + "    inner join "
        + "    (select * from I inner join J on i = j) "
        + "    on a = i and h = j");
  }

  @Test public void testPushSemiJoinPastProject() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(PushSemiJoinPastProjectRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e.* from "
        + "(select ename, trim(job), sal * 2, deptno from emp) e, dept d "
        + "where e.deptno = d.deptno");
  }

  @Test public void testReduceValuesUnderFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastProjectRule.INSTANCE)
        .addRuleInstance(ReduceValuesRule.FILTER_INSTANCE)
        .build();

    // Plan should be same as for
    // select a, b from (values (10,'x')) as t(a, b)");
    checkPlanning(program,
        "select a, b from (values (10, 'x'), (20, 'y')) as t(a, b) where a < 15");
  }

  @Test public void testReduceValuesUnderProject() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(MergeProjectRule.INSTANCE)
        .addRuleInstance(ReduceValuesRule.PROJECT_INSTANCE)
        .build();

    // Plan should be same as for
    // select a, b as x from (values (11), (23)) as t(x)");
    checkPlanning(program,
        "select a + b from (values (10, 1), (20, 3)) as t(a, b)");
  }

  @Test public void testReduceValuesUnderProjectFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastProjectRule.INSTANCE)
        .addRuleInstance(MergeProjectRule.INSTANCE)
        .addRuleInstance(ReduceValuesRule.PROJECT_FILTER_INSTANCE)
        .build();

    // Plan should be same as for
    // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
    checkPlanning(program,
        "select a + b as x, b, a from (values (10, 1), (30, 7), (20, 3)) as t(a, b)"
        + " where a - b < 21");
  }

  @Ignore // Optiq does not support INSERT yet
  @Test public void testReduceValuesNull() throws Exception {
    // The NULL literal presents pitfalls for value-reduction. Only
    // an INSERT statement contains un-CASTed NULL values.
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceValuesRule.PROJECT_INSTANCE)
        .build();
    checkPlanning(program,
        "insert into sales.depts(deptno,name) values (NULL, 'null')");
  }

  @Test public void testReduceValuesToEmpty() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastProjectRule.INSTANCE)
        .addRuleInstance(MergeProjectRule.INSTANCE)
        .addRuleInstance(ReduceValuesRule.PROJECT_FILTER_INSTANCE)
        .build();

    // Plan should be same as for
    // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
    checkPlanning(program,
        "select a + b as x, b, a from (values (10, 1), (30, 7)) as t(a, b)"
        + " where a - b < 0");
  }

  @Test public void testEmptyFilterProjectUnion() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastSetOpRule.INSTANCE)
        .addRuleInstance(PushFilterPastProjectRule.INSTANCE)
        .addRuleInstance(MergeProjectRule.INSTANCE)
        .addRuleInstance(ReduceValuesRule.PROJECT_FILTER_INSTANCE)
        .addRuleInstance(RemoveEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(RemoveEmptyRules.UNION_INSTANCE)
        .build();

    // Plan should be same as for
    // select * from (values (30, 3)) as t(x, y)");
    checkPlanning(program,
        "select * from (\n"
        + "select * from (values (10, 1), (30, 3)) as t (x, y)\n"
        + "union all\n"
        + "select * from (values (20, 2))\n"
        + ")\n"
        + "where x + y > 30");
  }

  @Test public void testEmptyJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(RemoveEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(RemoveEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(RemoveEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be empty
    checkPlanning(program,
        "select * from (\n"
        + "select * from emp where false)\n"
        + "join dept using (deptno)");
  }

  @Test public void testEmptyJoinLeft() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(RemoveEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(RemoveEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(RemoveEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be empty
    checkPlanning(program,
        "select * from (\n"
        + "select * from emp where false)\n"
        + "left join dept using (deptno)");
  }

  @Test public void testEmptyJoinRight() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(RemoveEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(RemoveEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(RemoveEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be equivalent to "select * from emp join dept".
    // Cannot optimize away the join because of RIGHT.
    checkPlanning(program,
        "select * from (\n"
        + "select * from emp where false)\n"
        + "right join dept using (deptno)");
  }

  @Test public void testEmptySort() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(RemoveEmptyRules.SORT_INSTANCE)
        .build();

    checkPlanning(program,
        "select * from emp where false order by deptno");
  }

  @Test public void testEmptySortLimitZero() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(RemoveEmptyRules.SORT_FETCH_ZERO_INSTANCE)
        .build();

    checkPlanning(program,
        "select * from emp order by deptno limit 0");
  }

  @Test public void testReduceCasts() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    // The resulting plan should have no cast expressions
    checkPlanning(program,
        "select cast(d.name as varchar(128)), cast(e.empno as integer) "
        + "from dept as d inner join emp as e "
        + "on cast(d.deptno as integer) = cast(e.deptno as integer) "
        + "where cast(e.job as varchar(1)) = 'Manager'");
  }

  @Test public void testReduceCastAndConsts() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    // Make sure constant expressions inside the cast can be reduced
    // in addition to the casts.
    checkPlanning(program,
        "select * from emp "
        + "where cast((empno + (10/2)) as int) = 13");
  }

  @Ignore // Optiq does not support INSERT yet
  @Test public void testReduceCastsNullable() throws Exception {
    HepProgram program = new HepProgramBuilder()

        // Simulate the way INSERT will insert casts to the target types
        .addRuleInstance(
            new CoerceInputsRule(TableModificationRel.class, false))

            // Convert projects to calcs, merge two calcs, and then
            // reduce redundant casts in merged calc.
        .addRuleInstance(ProjectToCalcRule.INSTANCE)
        .addRuleInstance(MergeCalcRule.INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.CALC_INSTANCE)
        .build();
    checkPlanning(program,
        "insert into sales.depts(name) "
        + "select cast(gender as varchar(128)) from sales.emps");
  }

  private void basePushAggThroughUnion() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushProjectPastSetOpRule.INSTANCE)
        .addRuleInstance(MergeProjectRule.INSTANCE)
        .addRuleInstance(PushAggregateThroughUnionRule.INSTANCE)
        .build();
    checkPlanning(program, "${sql}");
  }

  @Test public void testPushSumConstantThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushSumNullConstantThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushSumNullableThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushSumNullableNOGBYThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushCountStarThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushCountNullableThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushMaxNullableThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushMinThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushAvgThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushSumCountStarThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  private void basePullConstantTroughAggregate() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(MergeProjectRule.INSTANCE)
        .addRuleInstance(PullConstantsThroughAggregatesRule.INSTANCE)
        .addRuleInstance(MergeProjectRule.INSTANCE)
        .build();
    checkPlanning(program, "${sql}");
  }

  @Test public void testPullConstantThroughConstLast() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregateSimpleNonNullable() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregatePermuted() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregatePermutedConstFirst() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregatePermutedConstGroupBy()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregateConstGroupBy()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregateAllConst()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregateAllLiterals()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testAggregateProjectMerge() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select x, sum(z), y from (\n"
        + "  select deptno as x, empno as y, sal as z, sal * 2 as zz\n"
        + "  from emp)\n"
        + "group by x, y");
  }

  public void transitiveInference() throws Exception {
    final DiffRepository diffRepos = getDiffRepos();
    String sql = diffRepos.expand(null, "${sql}");

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.DUMB_FILTER_ON_JOIN)
        .addRuleInstance(PushFilterPastJoinRule.JOIN)
        .addRuleInstance(PushFilterPastProjectRule.INSTANCE)
        .addRuleInstance(PushFilterPastSetOpRule.INSTANCE)
        .build();
    HepPlanner planner = new HepPlanner(program);

    RelNode relInitial = tester.convertSqlToRel(sql);

    assertTrue(relInitial != null);

    List<RelMetadataProvider> list = Lists.newArrayList();
    DefaultRelMetadataProvider defaultProvider =
        new DefaultRelMetadataProvider();
    list.add(defaultProvider);
    planner.registerMetadataProviders(list);
    RelMetadataProvider plannerChain = ChainedRelMetadataProvider.of(list);
    relInitial.getCluster().setMetadataProvider(
        new CachingRelMetadataProvider(plannerChain, planner));

    planner.setRoot(relInitial);
    RelNode relAfter = planner.findBestExp();

    String planBefore = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    HepProgram program2 = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(PushFilterPastJoinRule.DUMB_FILTER_ON_JOIN)
        .addRuleInstance(PushFilterPastJoinRule.JOIN)
        .addRuleInstance(PushFilterPastProjectRule.INSTANCE)
        .addRuleInstance(PushFilterPastSetOpRule.INSTANCE)
        .addRuleInstance(TransitivePredicatesOnJoinRule.INSTANCE)
        .build();
    HepPlanner planner2 = new HepPlanner(program2);
    planner.registerMetadataProviders(list);
    planner2.setRoot(relAfter);
    relAfter = planner2.findBestExp();

    String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test public void testTransitiveInferenceJoin() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceProject() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceAggregate() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceUnion() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceJoin3way() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceJoin3wayAgg() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceLeftOuterJoin() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceRightOuterJoin() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceFullOuterJoin() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferencePreventProjectPullup()
      throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferencePullupThruAlias() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceConjunctInPullUp() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceNoPullUpExprs() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceUnion3way() throws Exception {
    transitiveInference();
  }

  @Ignore("not working")
  @Test public void testTransitiveInferenceUnion3wayOr() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceConstantEquiPredicate()
      throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceComplexPredicate() throws Exception {
    transitiveInference();
  }

  @Test public void testPushFilterWithRank() throws Exception {
    HepProgram program = new HepProgramBuilder().addRuleInstance(
        PushFilterPastProjectRule.INSTANCE).build();
    checkPlanning(program, "select e1.ename, r\n"
        + "from (\n"
        + "  select ename, "
        + "  rank() over(partition by  deptno order by sal) as r "
        + "  from emp) e1\n"
        + "where r < 2");
  }

  @Test public void testPushFilterWithRankExpr() throws Exception {
    HepProgram program = new HepProgramBuilder().addRuleInstance(
        PushFilterPastProjectRule.INSTANCE).build();
    checkPlanning(program, "select e1.ename, r\n"
        + "from (\n"
        + "  select ename,\n"
        + "  rank() over(partition by  deptno order by sal) + 1 as r "
        + "  from emp) e1\n"
        + "where r < 2");
  }
}

// End RelOptRulesTest.java
