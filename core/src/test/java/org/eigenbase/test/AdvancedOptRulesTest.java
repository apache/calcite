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

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;

import org.junit.*;
import org.junit.Test;

/**
 * Like {@link RelOptRulesTest}, but testing extended rules.
 */
public class AdvancedOptRulesTest extends RelOptRulesTest {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AdvancedOptRulesTest.
   */
  public AdvancedOptRulesTest() {
    super();
  }

  //~ Methods ----------------------------------------------------------------

  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(AdvancedOptRulesTest.class);
  }

  protected void checkAbstract(RelOptPlanner planner, RelNode relBefore)
      throws Exception {
    final DiffRepository diffRepos = getDiffRepos();

    String planBefore = NL + RelOptUtil.toString(relBefore);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    planner.setRoot(relBefore);
    RelNode relAfter = planner.findBestExp();

    String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  private void check(HepProgram program, String sql) throws Exception {
    check(program, sql, null);
  }

  private void check(HepProgram program, String sql, List<RelOptRule> rules)
      throws Exception {
    final DiffRepository diffRepos = getDiffRepos();
    String sql2 = diffRepos.expand("sql", sql);

    String explainQuery = "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR " + sql2;

    checkPlanning(program, sql);
  }

  public void testMergeFilterWithJoinCondition() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(TableAccessRule.instance)
        .addRuleInstance(ExtractJoinFilterRule.instance)
        .addRuleInstance(FilterToCalcRule.instance)
        .addRuleInstance(MergeCalcRule.instance)
            //.addRuleInstance(FennelCalcRule.instance);
            //.addRuleInstance(FennelCartesianJoinRule.instance);
        .addRuleInstance(ProjectToCalcRule.instance)
        .build();

    check(program,
        "select d.name as dname,e.name as ename"
        + " from sales.emps e inner join sales.depts d"
        + " on e.deptno=d.deptno"
        + " where d.name='Propane'");
  }

  public void testHeterogeneousConversion() throws Exception {
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

    check(program,
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
    check(program,
        "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
        + "where e1.deptno = d.deptno and e1.empno = e2.empno");
  }

  public void testPushSemiJoinPastJoinRule_Right() throws Exception {
    // tests the case where the semijoin is pushed to the right
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastJoinRule.instance)
        .build();
    check(program,
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
    check(program,
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
    check(program,
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

    check(program,
        "select 1+2, d.deptno+(3+4), (5+6)+d.deptno, cast(null as integer),"
        + " coalesce(2,null), row(7+8)"
        + " from sales.depts d inner join sales.emps e"
        + " on d.deptno = e.deptno + (5-5)"
        + " where d.deptno=(7+8) and d.deptno=coalesce(2,null)");
  }

  @Test public void testReduceConstantsEliminatesFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    // WHERE NULL is the same as WHERE FALSE, so get empty result
    check(program,
        "select * from (values (1,2)) where 1 + 2 > 3 + CAST(NULL AS INTEGER)");
  }

  @Test public void testAlreadyFalseEliminatesFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    check(program,
        "select * from (values (1,2)) where false");
  }

  public void testReduceConstantsCalc() throws Exception {
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
    check(program,
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

  public void testRemoveSemiJoin() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(RemoveSemiJoinRule.instance)
        .build();
    check(
        program,
        "select e.name from sales.emps e, sales.depts d "
        + "where e.deptno = d.deptno");
  }

  public void testRemoveSemiJoinWithFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastFilterRule.instance)
        .addRuleInstance(RemoveSemiJoinRule.instance)
        .build();
    check(program,
        "select e.name from sales.emps e, sales.depts d "
        + "where e.deptno = d.deptno and e.name = 'foo'");
  }

  public void testRemoveSemiJoinRight() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastJoinRule.instance)
        .addRuleInstance(RemoveSemiJoinRule.instance)
        .build();
    check(program,
        "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
  }

  public void testRemoveSemiJoinRightWithFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastFilterRule.instance)
        .addRuleInstance(RemoveSemiJoinRule.instance)
        .build();
    check(program,
        "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno "
        + "and d.name = 'foo'");
  }

  public void testConvertMultiJoinRuleOuterJoins() throws Exception {
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
    check(program,
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

  public void testPushSemiJoinPastProject() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(AddRedundantSemiJoinRule.instance)
        .addRuleInstance(PushSemiJoinPastProjectRule.instance)
        .build();
    check(program,
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
    check(program,
        "select a, b from (values (10, 'x'), (20, 'y')) as t(a, b) where a < 15");
  }

  public void testReduceValuesUnderProject() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(MergeProjectRule.instance)
        .addRuleInstance(ReduceValuesRule.PROJECT_INSTANCE)
        .build();

    // Plan should be same as for
    // select a, b as x from (values (11), (23)) as t(x)");
    check(program,
        "select a + b from (values (10, 1), (20, 3)) as t(a, b)");
  }

  public void testReduceValuesUnderProjectFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastProjectRule.instance)
        .addRuleInstance(MergeProjectRule.instance)
        .addRuleInstance(ReduceValuesRule.PROJECT_INSTANCE)
        .build();

    // Plan should be same as for
    // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
    check(program,
        "select a + b as x, b, a from (values (10, 1), (30, 7), (20, 3)) as t(a, b)"
        + " where a - b < 21");
  }

  @Test public void testReduceValuesNull() throws Exception {
    // The NULL literal presents pitfalls for value-reduction. Only
    // an INSERT statement contains un-CASTed NULL values.
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceValuesRule.PROJECT_INSTANCE)
        .build();
    check(program,
        "insert into sales.depts(deptno,name) values (NULL, 'null')");
  }

  public void testReduceValuesToEmpty() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushFilterPastProjectRule.instance)
        .addRuleInstance(MergeProjectRule.instance)
        .addRuleInstance(ReduceValuesRule.PROJECT_FILTER_INSTANCE)
        .build();

    // Plan should be same as for
    // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
    check(program,
        "select a + b as x, b, a from (values (10, 1), (30, 7)) as t(a, b)"
        + " where a - b < 0");
  }

  public void testEmptyFilterProjectUnion() throws Exception {
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
    check(program,
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
    check(program,
        "select cast(d.name as varchar(128)), cast(e.empno as integer) "
        + "from sales.depts d inner join sales.emps e "
        + "on cast(d.deptno as integer) = cast(e.deptno as integer) "
        + "where cast(e.gender as char(1)) = 'M'");
  }

  @Test public void testReduceCastAndConsts() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    // Make sure constant expressions inside the cast can be reduced
    // in addition to the casts.
    check(program,
        "select * from sales.emps "
        + "where cast((empno + (10/2)) as int) = 13");
  }

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
    check(program,
        "insert into sales.depts(name) "
        + "select cast(gender as varchar(128)) from sales.emps");
  }

  @Test public void testPushAggThroughUnion() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PushProjectPastSetOpRule.instance)
        .addRuleInstance(PushAggregateThroughUnionRule.instance)
        .build();
    check(program,
        "select name,sum(empno),count(*) from "
        + "(select * from sales.emps e1 union all "
        + "select * from sales.emps e2) "
        + "group by name");
  }
}

// End AdvancedOptRulesTest.java
