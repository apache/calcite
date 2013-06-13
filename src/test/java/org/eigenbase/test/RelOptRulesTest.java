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

import org.eigenbase.rel.rules.*;

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
public class RelOptRulesTest
    extends RelOptTestBase
{
    //~ Methods ----------------------------------------------------------------

    protected DiffRepository getDiffRepos()
    {
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
            PushFilterPastJoinRule.instance,
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
}

// End RelOptRulesTest.java
