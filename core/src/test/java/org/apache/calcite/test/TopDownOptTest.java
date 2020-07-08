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

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit test for top-down optimization.
 *
 * <p>As input, the test supplies a SQL statement and rules; the SQL is
 * translated into relational algebra and then fed into a
 * {@link VolcanoPlanner}. The plan before and after "optimization" is
 * diffed against a .ref file using {@link DiffRepository}.
 *
 * <p>Procedure for adding a new test case:
 *
 * <ol>
 * <li>Add a new public test method for your rule, following the existing
 * examples. You'll have to come up with an SQL statement to which your rule
 * will apply in a meaningful way. See
 * {@link org.apache.calcite.test.catalog.MockCatalogReaderSimple} class
 * for details on the schema.
 *
 * <li>Run the test. It should fail. Inspect the output in
 * {@code build/resources/test/.../TopDownOptTest_actual.xml}.
 *
 * <li>Verify that the "planBefore" is the correct
 * translation of your SQL, and that it contains the pattern on which your rule
 * is supposed to fire. If all is well, replace
 * {@code src/test/resources/.../TopDownOptTest.xml} and
 * with the new {@code build/resources/test/.../TopDownOptTest_actual.xml}.
 *
 * <li>Run the test again. It should fail again, but this time it should contain
 * a "planAfter" entry for your rule. Verify that your rule applied its
 * transformation correctly, and then update the
 * {@code src/test/resources/.../TopDownOptTest.xml} file again.
 *
 * <li>Run the test one last time; this time it should pass.
 * </ol>
 */
class TopDownOptTest extends RelOptTestBase {
  @Test void testValuesTraitRequest() {
    final String sql = "SELECT * from (values (1, 1), (2, 1), (1, 2), (2, 2))\n"
        + "as t(a, b) order by b, a";
    Query.create(sql).check();
  }

  @Test void testValuesTraitRequestNeg() {
    final String sql = "SELECT * from (values (1, 1), (2, 1), (3, 2), (2, 2))\n"
        + "as t(a, b) order by b, a";
    Query.create(sql).check();
  }

  @Test void testSortAgg() {
    final String sql = "select mgr, count(*) from sales.emp\n"
        + "group by mgr order by mgr desc nulls last limit 5";
    Query.create(sql).check();
  }

  @Test void testSortAggPartialKey() {
    final String sql = "select mgr,deptno,comm,count(*) from sales.emp\n"
        + "group by mgr,deptno,comm\n"
        + "order by comm desc nulls last, deptno nulls first";
    Query.create(sql).check();
  }

  @Test void testSortMergeJoin() {
    final String sql = "select * from\n"
        + "sales.emp r join sales.bonus s on r.ename=s.ename and r.job=s.job\n"
        + "order by r.job desc nulls last, r.ename nulls first";
    Query.create(sql).check();
  }

  @Test void testSortMergeJoinRight() {
    final String sql = "select * from\n"
        + "sales.emp r join sales.bonus s on r.ename=s.ename and r.job=s.job\n"
        + "order by s.job desc nulls last, s.ename nulls first";
    Query.create(sql).check();
  }

  @Test void testMergeJoinDeriveLeft1() {
    final String sql = "select * from\n"
        + "(select ename, job, max(sal) from sales.emp group by ename, job) r\n"
        + "join sales.bonus s on r.job=s.job and r.ename=s.ename";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  @Test void testMergeJoinDeriveLeft2() {
    final String sql = "select * from\n"
        + "(select ename, job, mgr, max(sal) from sales.emp group by ename, job, mgr) r\n"
        + "join sales.bonus s on r.job=s.job and r.ename=s.ename";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  @Test void testMergeJoinDeriveRight1() {
    final String sql = "select * from sales.bonus s join\n"
        + "(select ename, job, max(sal) from sales.emp group by ename, job) r\n"
        + "on r.job=s.job and r.ename=s.ename";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  @Test void testMergeJoinDeriveRight2() {
    final String sql = "select * from sales.bonus s join\n"
        + "(select ename, job, mgr, max(sal) from sales.emp group by ename, job, mgr) r\n"
        + "on r.job=s.job and r.ename=s.ename";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  // Order by left field(s): push down sort to left input.
  @Test void testCorrelateInnerJoinDeriveLeft() {
    final String sql = "select * from emp e\n"
        + "join dept d on e.deptno=d.deptno\n"
        + "order by e.ename";
    Query.create(sql)
        .addRule(CoreRules.JOIN_TO_CORRELATE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // Order by contains right field: sort cannot be pushed down.
  @Test void testCorrelateInnerJoinNoDerive() {
    final String sql = "select * from emp e\n"
        + "join dept d on e.deptno=d.deptno\n"
        + "order by e.ename, d.name";
    Query.create(sql)
        .addRule(CoreRules.JOIN_TO_CORRELATE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // Order by left field(s): push down sort to left input.
  @Test void testCorrelateLeftJoinDeriveLeft() {
    final String sql = "select * from emp e\n"
        + "left join dept d on e.deptno=d.deptno\n"
        + "order by e.ename";
    Query.create(sql)
        .addRule(CoreRules.JOIN_TO_CORRELATE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // Order by contains right field: sort cannot be pushed down.
  @Test void testCorrelateLeftJoinNoDerive() {
    final String sql = "select * from emp e\n"
        + "left join dept d on e.deptno=d.deptno\n"
        + "order by e.ename, d.name";
    Query.create(sql)
        .addRule(CoreRules.JOIN_TO_CORRELATE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // Order by left field(s): push down sort to left input.
  @Test void testCorrelateSemiJoinDeriveLeft() {
    final String sql = "select * from dept d\n"
        + "where exists (select 1 from emp e where e.deptno=d.deptno)\n"
        + "order by d.name";
    Query.create(sql)
        .addRule(CoreRules.JOIN_TO_CORRELATE)
        .addRule(CoreRules.JOIN_TO_SEMI_JOIN)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // test if "order by mgr desc nulls last" can be pushed through the projection ("select mgr").
  @Test void testSortProject() {
    final String sql = "select mgr from sales.emp order by mgr desc nulls last";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // test that Sort cannot push through projection because of non-trival call
  // (e.g. RexCall(sal * -1)). In this example, the reason is that "sal * -1"
  // creates opposite ordering if Sort is pushed down.
  @Test void testSortProjectOnRexCall() {
    final String sql = "select ename, sal * -1 as sal, mgr from\n"
        + "sales.emp order by ename desc, sal desc, mgr desc nulls last";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // test that Sort can push through projection when cast is monotonic.
  @Test void testSortProjectWhenCastLeadingToMonotonic() {
    final String sql = "select deptno from sales.emp order by cast(deptno as float) desc";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // test that Sort cannot push through projection when cast is not monotonic.
  @Test void testSortProjectWhenCastLeadingToNonMonotonic() {
    final String sql = "select deptno from sales.emp order by cast(deptno as varchar) desc";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // No sort on left join input.
  @Test void testSortProjectDeriveWhenCastLeadingToMonotonic() {
    final String sql = "select * from\n"
        + "(select ename, cast(job as varchar) as job, max_sal + 1 from\n"
        + "(select ename, job, max(sal) as max_sal from sales.emp group by ename, job) t) r\n"
        + "join sales.bonus s on r.job=s.job and r.ename=s.ename";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  // need sort on left join input.
  @Test void testSortProjectDeriveOnRexCall() {
    final String sql = "select * from\n"
        + "(select ename, sal * -1 as sal, max_job from\n"
        + "(select ename, sal, max(job) as max_job from sales.emp group by ename, sal) t) r\n"
        + "join sales.bonus s on r.sal=s.sal and r.ename=s.ename";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  // need sort on left join input.
  @Test void testSortProjectDeriveWhenCastLeadingToNonMonotonic() {
    final String sql = "select * from\n"
        + "(select ename, cast(job as numeric) as job, max_sal + 1 from\n"
        + "(select ename, job, max(sal) as max_sal from sales.emp group by ename, job) t) r\n"
        + "join sales.bonus s on r.job=s.job and r.ename=s.ename";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  // no Sort need for left join input.
  @Test void testSortProjectDerive3() {
    final String sql = "select * from\n"
        + "(select ename, cast(job as varchar) as job, sal + 1 from\n"
        + "(select ename, job, sal from sales.emp limit 100) t) r\n"
        + "join sales.bonus s on r.job=s.job and r.ename=s.ename";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  // need Sort on left join input.
  @Test void testSortProjectDerive4() {
    final String sql = "select * from\n"
        + "(select ename, cast(job as bigint) as job, sal + 1 from\n"
        + "(select ename, job, sal from sales.emp limit 100) t) r\n"
        + "join sales.bonus s on r.job=s.job and r.ename=s.ename";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  // test if top projection can enforce sort when inner sort cannot produce satisfying ordering.
  @Test void testSortProjectDerive5() {
    final String sql = "select ename, empno*-1, job from\n"
        + "(select * from sales.emp order by ename, empno, job limit 10) order by ename, job";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  @Test void testSortProjectDerive() {
    final String sql = "select * from\n"
        + "(select ename, job, max_sal + 1 from\n"
        + "(select ename, job, max(sal) as max_sal from sales.emp group by ename, job) t) r\n"
        + "join sales.bonus s on r.job=s.job and r.ename=s.ename";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  // need Sort on projection.
  @Test void testSortProjectDerive2() {
    final String sql = "select distinct ename, sal*-2, mgr\n"
        + "from (select ename, mgr, sal from sales.emp order by ename, mgr, sal limit 100) t";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  @Test void testSortProjectDerive6() {
    final String sql = "select comm, deptno, slacker from\n"
        + "(select * from sales.emp order by comm, deptno, slacker limit 10) t\n"
        + "order by comm, slacker";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // test traits push through filter.
  @Test void testSortFilter() {
    final String sql = "select ename, job, mgr, max_sal from\n"
        + "(select ename, job, mgr, max(sal) as max_sal from sales.emp group by ename, job, mgr) as t\n"
        + "where max_sal > 1000\n"
        + "order by mgr desc, ename";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // test traits derivation in filter.
  @Test void testSortFilterDerive() {
    final String sql = "select * from\n"
        + "(select ename, job, max_sal from\n"
        + "(select ename, job, max(sal) as max_sal from sales.emp group by ename, job) t where job > 1000) r\n"
        + "join sales.bonus s on r.job=s.job and r.ename=s.ename";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  // Not push down sort for hash join in full outer join case.
  @Test void testHashJoinFullOuterJoinNotPushDownSort() {
    final String sql = "select * from\n"
        + "sales.emp r full outer join sales.bonus s on r.ename=s.ename and r.job=s.job\n"
        + "order by r.job desc nulls last, r.ename nulls first";
    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .check();
  }

  // Push down sort to left input.
  @Test void testHashJoinLeftOuterJoinPushDownSort() {
    final String sql = "select * from\n"
        + "(select contactno, email from customer.contact_peek) r left outer join\n"
        + "(select acctno, type from customer.account) s\n"
        + "on r.contactno=s.acctno and r.email=s.type\n"
        + "order by r.contactno desc, r.email desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // Push down sort to left input.
  @Test void testHashJoinLeftOuterJoinPushDownSort2() {
    final String sql = "select * from\n"
        + "customer.contact_peek r left outer join\n"
        + "customer.account s\n"
        + "on r.contactno=s.acctno and r.email=s.type\n"
        + "order by r.fname desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // Push down sort to left input.
  @Test void testHashJoinInnerJoinPushDownSort() {
    final String sql = "select * from\n"
        + "(select contactno, email from customer.contact_peek) r inner join\n"
        + "(select acctno, type from customer.account) s\n"
        + "on r.contactno=s.acctno and r.email=s.type\n"
        + "order by r.contactno desc, r.email desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // do not push down sort.
  @Test void testHashJoinRightOuterJoinPushDownSort() {
    final String sql = "select * from\n"
        + "(select contactno, email from customer.contact_peek) r right outer join\n"
        + "(select acctno, type from customer.account) s\n"
        + "on r.contactno=s.acctno and r.email=s.type\n"
        + "order by s.acctno desc, s.type desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // push sort to left input
  @Test void testNestedLoopJoinLeftOuterJoinPushDownSort() {
    final String sql = "select * from\n"
        + " customer.contact_peek r left outer join\n"
        + "customer.account s\n"
        + "on r.contactno>s.acctno and r.email<s.type\n"
        + "order by r.contactno desc, r.email desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // push sort to left input
  @Test void testNestedLoopJoinLeftOuterJoinPushDownSort2() {
    final String sql = "select * from\n"
        + " customer.contact_peek r left outer join\n"
        + "customer.account s\n"
        + "on r.contactno>s.acctno and r.email<s.type\n"
        + "order by r.fname desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // do not push sort to left input cause sort keys are on right input.
  @Test void testNestedLoopJoinLeftOuterJoinSortKeyOnRightInput() {
    final String sql = "select * from\n"
        + " customer.contact_peek r left outer join\n"
        + "customer.account s\n"
        + "on r.contactno>s.acctno and r.email<s.type\n"
        + "order by s.acctno desc, s.type desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // do not push down sort to right input because traits propagation does not work
  // for right/full outer join.
  @Test void testNestedLoopJoinRightOuterJoinSortPushDown() {
    final String sql = "select r.contactno, r.email, s.acctno, s.type from\n"
        + " customer.contact_peek r right outer join\n"
        + "customer.account s\n"
        + "on r.contactno>s.acctno and r.email<s.type\n"
        + "order by s.acctno desc, s.type desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // Collation can be derived from left input so that top Sort is removed.
  @Test void testHashJoinTraitDerivation() {
    final String sql = "select * from\n"
        + "(select ename, job, mgr from sales.emp order by ename desc, job desc, mgr limit 10) r\n"
        + "join sales.bonus s on r.ename=s.ename and r.job=s.job\n"
        + "order by r.ename desc, r.job desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // Collation can be derived from left input so that top Sort is removed.
  @Test void testHashJoinTraitDerivation2() {
    final String sql = "select * from\n"
        + "(select ename, job, mgr from sales.emp order by mgr desc limit 10) r\n"
        + "join sales.bonus s on r.ename=s.ename and r.job=s.job\n"
        + "order by r.mgr desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // Collation derived from left input is not what the top Sort needs.
  @Test void testHashJoinTraitDerivationNegativeCase() {
    final String sql = "select * from\n"
        + "(select ename, job, mgr from sales.emp order by mgr desc limit 10) r\n"
        + "join sales.bonus s on r.ename=s.ename and r.job=s.job\n"
        + "order by r.mgr";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // Collation can be derived from left input so that top Sort is removed.
  @Test void testNestedLoopJoinTraitDerivation() {
    final String sql = "select * from\n"
        + "(select ename, job, mgr from sales.emp order by ename desc, job desc, mgr limit 10) r\n"
        + "join sales.bonus s on r.ename>s.ename and r.job<s.job\n"
        + "order by r.ename desc, r.job desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // Collation can be derived from left input so that top Sort is removed.
  @Test void testNestedLoopJoinTraitDerivation2() {
    final String sql = "select * from\n"
        + "(select ename, job, mgr from sales.emp order by mgr limit 10) r\n"
        + "join sales.bonus s on r.ename>s.ename and r.job<s.job\n"
        + "order by r.mgr";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // Collation derived from left input is not what the top Sort needs.
  @Test void testNestedLoopJoinTraitDerivationNegativeCase() {
    final String sql = "select * from\n"
        + "(select ename, job, mgr from sales.emp order by mgr limit 10) r\n"
        + "join sales.bonus s on r.ename>s.ename and r.job<s.job\n"
        + "order by r.mgr desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .check();
  }

  // test if "order by mgr desc nulls last" can be pushed through the calc ("select mgr").
  @Test void testSortCalc() {
    final String sql = "select mgr from sales.emp order by mgr desc nulls last";
    Query.create(sql)
        .addRule(CoreRules.PROJECT_TO_CALC)
        .addRule(EnumerableRules.ENUMERABLE_CALC_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_PROJECT_RULE)
        .check();
  }

  // test that Sort cannot push through calc because of non-trival call
  // (e.g. RexCall(sal * -1)). In this example, the reason is that "sal * -1"
  // creates opposite ordering if Sort is pushed down.
  @Test void testSortCalcOnRexCall() {
    final String sql = "select ename, sal * -1 as sal, mgr from\n"
        + "sales.emp order by ename desc, sal desc, mgr desc nulls last";
    Query.create(sql)
        .addRule(CoreRules.PROJECT_TO_CALC)
        .addRule(EnumerableRules.ENUMERABLE_CALC_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_PROJECT_RULE)
        .check();
  }

  // test that Sort can push through calc when cast is monotonic.
  @Test void testSortCalcWhenCastLeadingToMonotonic() {
    final String sql = "select cast(deptno as float) from sales.emp order by deptno desc";
    Query.create(sql)
        .addRule(CoreRules.PROJECT_TO_CALC)
        .addRule(EnumerableRules.ENUMERABLE_CALC_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_PROJECT_RULE)
        .check();
  }

  // test that Sort cannot push through calc when cast is not monotonic.
  @Test void testSortCalcWhenCastLeadingToNonMonotonic() {
    final String sql = "select deptno from sales.emp order by cast(deptno as varchar) desc";
    Query.create(sql)
        .addRule(CoreRules.PROJECT_TO_CALC)
        .addRule(EnumerableRules.ENUMERABLE_CALC_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_PROJECT_RULE)
        .check();
  }

  // test traits push through calc with filter.
  @Test void testSortCalcWithFilter() {
    final String sql = "select ename, job, mgr, max_sal from\n"
        + "(select ename, job, mgr, max(sal) as max_sal from sales.emp group by ename, job, mgr) as t\n"
        + "where max_sal > 1000\n"
        + "order by mgr desc, ename";
    Query.create(sql)
        .addRule(CoreRules.PROJECT_TO_CALC)
        .addRule(CoreRules.FILTER_TO_CALC)
        .addRule(EnumerableRules.ENUMERABLE_CALC_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_PROJECT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_FILTER_RULE)
        .check();
  }

  // Do not need Sort for calc.
  @Test void testSortCalcDerive1() {
    final String sql = "select * from\n"
        + "(select ename, job, max_sal + 1 from\n"
        + "(select ename, job, max(sal) as max_sal from sales.emp "
        + "group by ename, job) t) r\n"
        + "join sales.bonus s on r.job=s.job and r.ename=s.ename";
    Query.create(sql)
        .addRule(CoreRules.PROJECT_TO_CALC)
        .addRule(EnumerableRules.ENUMERABLE_CALC_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_PROJECT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  // Need Sort for calc.
  @Test void testSortCalcDerive2() {
    final String sql = "select distinct ename, sal*-2, mgr\n"
        + "from (select ename, mgr, sal from sales.emp order by ename, mgr, sal limit 100) t";
    Query.create(sql)
        .addRule(CoreRules.PROJECT_TO_CALC)
        .addRule(EnumerableRules.ENUMERABLE_CALC_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_PROJECT_RULE)
        .check();
  }

  // Do not need Sort for left join input.
  @Test void testSortCalcDerive3() {
    final String sql = "select * from\n"
        + "(select ename, cast(job as varchar) as job, sal + 1 from\n"
        + "(select ename, job, sal from sales.emp limit 100) t) r\n"
        + "join sales.bonus s on r.job=s.job and r.ename=s.ename";
    Query.create(sql)
        .addRule(CoreRules.PROJECT_TO_CALC)
        .addRule(EnumerableRules.ENUMERABLE_CALC_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_PROJECT_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .check();
  }

  // push sort to left input
  @Test void testBatchNestedLoopJoinLeftOuterJoinPushDownSort() {
    final String sql = "select * from\n"
        + " customer.contact_peek r left outer join\n"
        + "customer.account s\n"
        + "on r.contactno>s.acctno and r.email<s.type\n"
        + "order by r.contactno desc, r.email desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .addRule(EnumerableRules.ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE)
        .check();
  }

  // Collation can be derived from left input so that top Sort is removed.
  @Test void testBatchNestedLoopJoinTraitDerivation() {
    final String sql = "select * from\n"
        + "(select ename, job, mgr from sales.emp order by ename desc, job desc, mgr limit 10) r\n"
        + "join sales.bonus s on r.ename>s.ename and r.job<s.job\n"
        + "order by r.ename desc, r.job desc";

    Query.create(sql)
        .removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
        .removeRule(EnumerableRules.ENUMERABLE_SORT_RULE)
        .addRule(EnumerableRules.ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE)
        .check();
  }
}

/**
 * A helper class that creates Volcano planner with top-down optimization enabled. This class
 * allows easy-to-add and easy-to-remove rules from the planner.
 */
class Query extends RelOptTestBase {
  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(TopDownOptTest.class);
  }

  private String sql;
  private VolcanoPlanner planner;

  private Query(String sql) {
    this.sql = sql;

    planner = new VolcanoPlanner();
    // Always use top-down optimization
    planner.setTopDownOpt(true);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

    RelOptUtil.registerDefaultRules(planner, false, false);

    // Remove to Keep deterministic join order.
    planner.removeRule(CoreRules.JOIN_COMMUTE);
    planner.removeRule(JoinPushThroughJoinRule.LEFT);
    planner.removeRule(JoinPushThroughJoinRule.RIGHT);

    // Always use sorted agg.
    planner.addRule(EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE);
    planner.removeRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);

    // pushing down sort should be handled by top-down optimization.
    planner.removeRule(CoreRules.SORT_PROJECT_TRANSPOSE);

    // Sort will only be pushed down by traits propagation.
    planner.removeRule(CoreRules.SORT_JOIN_TRANSPOSE);
    planner.removeRule(CoreRules.SORT_JOIN_COPY);
  }

  public static Query create(String sql) {
    return new Query(sql);
  }

  public Query addRule(RelOptRule ruleToAdd) {
    planner.addRule(ruleToAdd);
    return this;
  }

  public Query addRules(List<RelOptRule> rulesToAdd) {
    for (RelOptRule ruleToAdd : rulesToAdd) {
      planner.addRule(ruleToAdd);
    }
    return this;
  }

  public Query removeRule(RelOptRule ruleToRemove) {
    planner.removeRule(ruleToRemove);
    return this;
  }

  public Query removeRules(List<RelOptRule> rulesToRemove) {
    for (RelOptRule ruleToRemove : rulesToRemove) {
      planner.removeRule(ruleToRemove);
    }
    return this;
  }

  public void check() {
    SqlToRelTestBase.Tester tester = createTester().withDecorrelation(true)
        .withClusterFactory(cluster -> RelOptCluster.create(planner, cluster.getRexBuilder()));

    new Sql(tester, sql, null, planner,
        ImmutableMap.of(), ImmutableList.of()).check();
  }
}
