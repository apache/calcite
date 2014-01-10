/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.test;

import org.eigenbase.rel.TableModificationRel;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.hep.*;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit test for rules in {@link org.eigenbase.rel} and subpackages.
 *
 * <p>As input, the test supplies a SQL statement and a single rule; the SQL is
 * translated into relational algebra and then fed into a {@link
 * org.eigenbase.relopt.hep.HepPlanner}. The planner fires the rule on every
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
        UnionToDistinctRule.instance,
        "select * from dept union select * from dept");
  }

  @Test public void testExtractJoinFilterRule() {
    checkPlanning(
        ExtractJoinFilterRule.instance,
        "select 1 from emp inner join dept on emp.deptno=dept.deptno");
  }

  @Test public void testAddRedundantSemiJoinRule() {
    checkPlanning(
        AddRedundantSemiJoinRule.instance,
        "select 1 from emp inner join dept on emp.deptno = dept.deptno");
  }

  @Test public void testPushFilterThroughOuterJoin() {
    checkPlanning(
        PushFilterPastJoinRule.FILTER_ON_JOIN,
        "select 1 from sales.dept d left outer join sales.emp e"
        + " on d.deptno = e.deptno"
        + " where d.name = 'Charlie'");
  }

  @Test public void testReduceAverage() {
    checkPlanning(
        ReduceAggregatesRule.instance,
        "select name, max(name), avg(deptno), min(name)"
        + " from sales.dept group by name");
  }

  @Test public void testPushProjectPastFilter() {
    checkPlanning(
        PushProjectPastFilterRule.instance,
        "select empno + deptno from emp where sal = 10 * comm "
        + "and upper(ename) = 'FOO'");
  }

  @Test public void testPushProjectPastJoin() {
    checkPlanning(
        PushProjectPastJoinRule.instance,
        "select e.sal + b.comm from emp e inner join bonus b "
        + "on e.ename = b.ename and e.deptno = 10");
  }

  @Test public void testPushProjectPastSetOp() {
    checkPlanning(
        PushProjectPastSetOpRule.instance,
        "select sal from "
        + "(select * from emp e1 union all select * from emp e2)");
  }

  @Test public void testPushJoinThroughUnionOnLeft() {
    checkPlanning(
        PushJoinThroughUnionRule.instanceUnionOnLeft,
        "select r1.sal from "
        + "(select * from emp e1 union all select * from emp e2) r1, "
        + "emp r2");
  }

  @Test public void testPushJoinThroughUnionOnRight() {
    checkPlanning(
        PushJoinThroughUnionRule.instanceUnionOnRight,
        "select r1.sal from "
        + "emp r1, "
        + "(select * from emp e1 union all select * from emp e2) r2");
  }

  @Ignore // have not tried under optiq (it might work)
  @Test public void testMergeFilterWithJoinCondition() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(TableAccessRule.instance)
        .addRuleInstance(ExtractJoinFilterRule.instance)
        .addRuleInstance(FilterToCalcRule.instance)
        .addRuleInstance(MergeCalcRule.instance)
            //.addRuleInstance(FennelCalcRule.instance);
            //.addRuleInstance(FennelCartesianJoinRule.instance);
        .addRuleInstance(ProjectToCalcRule.instance)
        .build();

    checkPlanning(program,
        "select d.name as dname,e.name as ename"
        + " from sales.emps e inner join sales.depts d"
        + " on e.deptno=d.deptno"
        + " where d.name='Propane'");
  }

  @Ignore // have not tried under optiq (it might work)
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
        .addRuleInstance(TableAccessRule.instance)
        .addRuleInstance(ProjectToCalcRule.instance)

            // Control the calc conversion.
        .addMatchLimit(1)

            // Let the converter rule fire to its heart's content.
        .addMatchLimit(HepProgram.MATCH_UNTIL_FIXPOINT)
        .build();

    checkPlanning(program,
        "select upper(name) from sales.emps union all"
        + " select lower(name) from sales.emps");
  }

  @Ignore
  @Test public void testPushSemiJoinPastJoinRule_Left() throws Exception {
    // tests the case where the semijoin is pushed to the left
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastJoinRule.instance)
        .build();
    checkPlanning(program,
        "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
        + "where e1.deptno = d.deptno and e1.empno = e2.empno");
  }

  @Ignore // have not tried under optiq (it might work)
  @Test public void testPushSemiJoinPastJoinRule_Right() throws Exception {
    // tests the case where the semijoin is pushed to the right
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastJoinRule.instance)
        .build();
    checkPlanning(program,
        "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
  }

  @Ignore
  @Test public void testPushSemiJoinPastFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastFilterRule.instance)
        .build();
    checkPlanning(program,
        "select e.name from sales.emps e, sales.depts d "
        + "where e.deptno = d.deptno and e.name = 'foo'");
  }

  @Ignore
  @Test public void testConvertMultiJoinRule() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(ConvertMultiJoinRule.instance)
        .build();
    checkPlanning(program,
        "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
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
        .addRuleInstance(PushFilterPastProjectRule.instance)
        .addRuleInstance(PushFilterPastSetOpRule.instance)
        .addRuleInstance(FilterToCalcRule.instance)
        .addRuleInstance(ProjectToCalcRule.instance)
        .addRuleInstance(MergeCalcRule.instance)
        .addRuleInstance(ReduceExpressionsRule.CALC_INSTANCE)

            // the hard part is done... a few more rule calls to clean up
        .addRuleInstance(RemoveEmptyRule.unionInstance)
        .addRuleInstance(ProjectToCalcRule.instance)
        .addRuleInstance(MergeCalcRule.instance)
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

  @Ignore // have not tried under optiq (it might work)
  @Test public void testRemoveSemiJoin() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(RemoveSemiJoinRule.instance)
        .build();
    checkPlanning(program,
        "select e.name from sales.emps e, sales.depts d "
        + "where e.deptno = d.deptno");
  }

  @Ignore // have not tried under optiq (it might work)
  @Test public void testRemoveSemiJoinWithFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastFilterRule.instance)
        .addRuleInstance(RemoveSemiJoinRule.instance)
        .build();
    checkPlanning(program,
        "select e.name from sales.emps e, sales.depts d "
        + "where e.deptno = d.deptno and e.name = 'foo'");
  }

  @Ignore // have not tried under optiq (it might work)
  @Test public void testRemoveSemiJoinRight() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastJoinRule.instance)
        .addRuleInstance(RemoveSemiJoinRule.instance)
        .build();
    checkPlanning(program,
        "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
  }

  @Ignore // have not tried under optiq (it might work)
  @Test public void testRemoveSemiJoinRightWithFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastFilterRule.instance)
        .addRuleInstance(RemoveSemiJoinRule.instance)
        .build();
    checkPlanning(program,
        "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno "
        + "and d.name = 'foo'");
  }

  @Ignore // have not tried under optiq (it might work)
  @Test public void testConvertMultiJoinRuleOuterJoins() throws Exception {
/*
    stmt.executeUpdate("create schema oj");
    stmt.executeUpdate("set schema 'oj'");
    stmt.executeUpdate(
        "create table A(a int primary key)");
    stmt.executeUpdate(
        "create table B(b int primary key)");
    stmt.executeUpdate(
        "create table C(c int primary key)");
    stmt.executeUpdate(
        "create table D(d int primary key)");
    stmt.executeUpdate(
        "create table E(e int primary key)");
    stmt.executeUpdate(
        "create table F(f int primary key)");
    stmt.executeUpdate(
        "create table G(g int primary key)");
    stmt.executeUpdate(
        "create table H(h int primary key)");
    stmt.executeUpdate(
        "create table I(i int primary key)");
    stmt.executeUpdate(
        "create table J(j int primary key)");
*/

    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(RemoveTrivialProjectRule.instance)
        .addRuleInstance(ConvertMultiJoinRule.instance)
        .build();
    checkPlanning(program,
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

  @Ignore // have not tried under optiq (it might work)
  @Test public void testPushSemiJoinPastProject() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastProjectRule.instance)
        .build();
    checkPlanning(program,
        "select e.* from "
        + "(select name, trim(city), age * 2, deptno from sales.emps) e, "
        + "sales.depts d "
        + "where e.deptno = d.deptno");
  }

  @Test public void testReduceValuesUnderFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastProjectRule.instance)
        .addRuleInstance(ReduceValuesRule.FILTER_INSTANCE)
        .build();

    // Plan should be same as for
    // select a, b from (values (10,'x')) as t(a, b)");
    checkPlanning(program,
        "select a, b from (values (10, 'x'), (20, 'y')) as t(a, b) where a < 15");
  }

  @Test public void testReduceValuesUnderProject() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(MergeProjectRule.instance)
        .addRuleInstance(ReduceValuesRule.PROJECT_INSTANCE)
        .build();

    // Plan should be same as for
    // select a, b as x from (values (11), (23)) as t(x)");
    checkPlanning(program,
        "select a + b from (values (10, 1), (20, 3)) as t(a, b)");
  }

  @Test public void testReduceValuesUnderProjectFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastProjectRule.instance)
        .addRuleInstance(MergeProjectRule.instance)
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
        .addRuleInstance(PushFilterPastProjectRule.instance)
        .addRuleInstance(MergeProjectRule.instance)
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
        .addRuleInstance(PushFilterPastSetOpRule.instance)
        .addRuleInstance(PushFilterPastProjectRule.instance)
        .addRuleInstance(MergeProjectRule.instance)
        .addRuleInstance(ReduceValuesRule.PROJECT_FILTER_INSTANCE)
        .addRuleInstance(RemoveEmptyRule.projectInstance)
        .addRuleInstance(RemoveEmptyRule.unionInstance)
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
        .addRuleInstance(ProjectToCalcRule.instance)
        .addRuleInstance(MergeCalcRule.instance)
        .addRuleInstance(ReduceExpressionsRule.CALC_INSTANCE)
        .build();
    checkPlanning(program,
        "insert into sales.depts(name) "
        + "select cast(gender as varchar(128)) from sales.emps");
  }

  @Ignore // assert failure
  @Test public void testPushAggThroughUnion() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushProjectPastSetOpRule.instance)
        .addRuleInstance(PushAggregateThroughUnionRule.instance)
        .build();
    checkPlanning(program,
        "select ename,sum(empno),count(*) from "
        + "(select * from emp as e1 union all "
        + "select * from emp as e2) "
        + "group by ename");
  }
}

// End RelOptRulesTest.java
