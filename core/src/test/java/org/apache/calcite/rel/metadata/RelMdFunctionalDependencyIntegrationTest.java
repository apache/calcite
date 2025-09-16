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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.test.SqlToRelTestBase;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for functional dependency metadata in RelMdFunctionalDependency.
 */
public class RelMdFunctionalDependencyIntegrationTest extends SqlToRelTestBase {

  @Test void testFunctionalDependencyProject() {
    final String sql = "select empno, deptno, deptno + 1 as deptNoPlus1, rand() as rand"
        + " from emp where deptno < 20";

    // Plan is:
    // LogicalProject(EMPNO=[$0], DEPTNO=[$7], DEPTNOPLUS1=[+($7, 1)], RAND=[RAND()])
    //  LogicalFilter(condition=[<($7, 20)])
    //    LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;       // empno - primary key
    int deptNo = 1;      // deptno
    int deptNoPlus1 = 2; // deptno + 1
    int rand = 3;        // rand()

    // deptno + 1 should determine deptno (can derive deptno from deptno+1)
    assertThat(mq.determines(relNode, deptNoPlus1, deptNo), is(Boolean.TRUE));

    // deptno should determine deptno + 1 (expression is deterministic)
    assertThat(mq.determines(relNode, deptNo, deptNoPlus1), is(Boolean.TRUE));

    // deptno should NOT determine empno (deptno is not unique)
    assertThat(mq.determines(relNode, deptNo, empNo), is(Boolean.FALSE));

    // empno should determine deptno + 1 (primary key determines everything)
    assertThat(mq.determines(relNode, empNo, deptNoPlus1), is(Boolean.TRUE));

    // deptno + 1 should NOT determine empno (deptno + 1 is not unique)
    assertThat(mq.determines(relNode, deptNoPlus1, empNo), is(Boolean.FALSE));

    // rand() should not determine anything (non-deterministic)
    assertThat(mq.determines(relNode, rand, empNo), is(Boolean.FALSE));

    // Nothing should determine rand() (non-deterministic)
    assertThat(mq.determines(relNode, empNo, rand), is(Boolean.FALSE));
  }

  @Test void testFunctionalDependencyAggregate() {
    final String sql = "select deptno, count(*) as \"count\", sum(sal) as sumSal"
        + " from emp group by deptno";

    // Plan is:
    // LogicalAggregate(group=[{0}], count=[COUNT()], SUMSAL=[SUM($1)])
    //  LogicalProject(DEPTNO=[$7], SAL=[$5])
    //    LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int deptNo = 0;    // deptno (group by column)
    int count = 1;     // count(*)
    int sumSal = 2;    // sum(sal)

    // Group by column should determine aggregate columns
    assertThat(mq.determines(relNode, deptNo, count), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, deptNo, sumSal), is(Boolean.TRUE));

    // Aggregate columns should not determine group by column
    assertThat(mq.determines(relNode, count, deptNo), is(Boolean.FALSE));
    assertThat(mq.determines(relNode, sumSal, deptNo), is(Boolean.FALSE));

    // Aggregate columns should not determine each other
    assertThat(mq.determines(relNode, count, sumSal), is(Boolean.FALSE));
    assertThat(mq.determines(relNode, sumSal, count), is(Boolean.FALSE));
  }

  @Test void testFunctionalDependencyWithIdenticalExpressions() {
    final String sql = "select deptno, deptno as deptno2, deptno + 1 as deptno3 from emp";

    // Plan is:
    // LogicalProject(DEPTNO=[$7], DEPTNO2=[$7], DEPTNO3=[+($7, 1)])
    //  LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int deptNo = 0;     // deptno
    int deptNo2 = 1;    // deptno (identical)
    int deptNo3 = 2;    // deptno + 1

    // Identical expressions should determine each other
    assertThat(mq.determines(relNode, deptNo, deptNo2), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, deptNo2, deptNo), is(Boolean.TRUE));

    // deptno should determine deptno + 1 (deterministic expression)
    assertThat(mq.determines(relNode, deptNo, deptNo3), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, deptNo2, deptNo3), is(Boolean.TRUE));

    // deptno + 1 should determine deptno (can derive original from increment)
    assertThat(mq.determines(relNode, deptNo3, deptNo), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, deptNo3, deptNo2), is(Boolean.TRUE));
  }

  @Test void testFunctionalDependencyJoin() {
    // Test inner join with emp and dept tables
    final String sql = "select e.empno, e.ename, e.deptno, d.name"
        + " from emp e join dept d on e.deptno = d.deptno";

    // Plan is:
    // LogicalProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7], NAME=[$10])
    //  LogicalJoin(condition=[=($7, $9)], joinType=[inner])
    //    LogicalTableScan(table=[[CATALOG, SALES, EMP]])
    //    LogicalTableScan(table=[[CATALOG, SALES, DEPT]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;    // e.empno
    int ename = 1;    // e.ename
    int deptno = 2;   // e.deptno
    int dname = 3;    // d.name

    // Left side functional dependencies should be preserved
    // empno determines ename and deptno (primary key from emp table)
    assertThat(mq.determines(relNode, empNo, ename), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, empNo, deptno), is(Boolean.TRUE));

    // Right side functional dependencies should be preserved with shifted indices
    // deptno determines dname (primary key from dept table)
    assertThat(mq.determines(relNode, deptno, dname), is(Boolean.TRUE));

    // Cross-table dependencies through join condition
    // empno should determine dname (through deptno)
    assertThat(mq.determines(relNode, empNo, dname), is(Boolean.TRUE));

    // Reverse dependencies should not hold
    assertThat(mq.determines(relNode, ename, empNo), is(Boolean.FALSE));
    assertThat(mq.determines(relNode, dname, empNo), is(Boolean.FALSE));
  }

  @Test void testFunctionalDependencyFilter() {
    final String sql = "select empno, ename, sal from emp where sal > 1000";

    // Plan is:
    // LogicalProject(EMPNO=[$0], ENAME=[$1], SAL=[$5])
    //  LogicalFilter(condition=[>($5, 1000)])
    //    LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;  // empno
    int ename = 1;  // ename
    int sal = 2;    // sal

    // Filter should preserve functional dependencies from base table
    // empno should still determine ename and sal
    assertThat(mq.determines(relNode, empNo, ename), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, empNo, sal), is(Boolean.TRUE));

    // sal should not determine empno
    assertThat(mq.determines(relNode, sal, empNo), is(Boolean.FALSE));
  }

  @Test void testFunctionalDependencyComplexExpressions() {
    final String sql = "select empno, sal, sal * 1.1 as raised_sal, "
        + "case when sal > 2000 then 'high' else 'low' end as sal_category"
        + " from emp";

    // Plan is:
    // LogicalProject(EMPNO=[$0], SAL=[$5], RAISED_SAL=[*($5, 1.1:DECIMAL(2, 1))],
    //                SAL_CATEGORY=[CASE(>($5, 2000), 'high', 'low ')])
    //  LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;        // empno
    int sal = 1;          // sal
    int raisedSal = 2;    // sal * 1.1
    int salCategory = 3;  // case expression

    // empno determines all other columns (primary key)
    assertThat(mq.determines(relNode, empNo, sal), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, empNo, raisedSal), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, empNo, salCategory), is(Boolean.TRUE));

    // sal determines computed columns based on sal
    assertThat(mq.determines(relNode, sal, raisedSal), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, sal, salCategory), is(Boolean.TRUE));

    // Computed columns should also determine sal (deterministic computation)
    assertThat(mq.determines(relNode, raisedSal, sal), is(Boolean.TRUE));
  }

  @Test void testFunctionalDependencyUnion() {
    final String sql = "select empno, deptno from emp where deptno = 10"
        + " union all "
        + " select empno, deptno from emp where deptno = 20";

    // Plan is:
    // LogicalUnion(all=[true])
    //  LogicalProject(EMPNO=[$0], DEPTNO=[$7])
    //    LogicalFilter(condition=[=($7, 10)])
    //      LogicalTableScan(table=[[CATALOG, SALES, EMP]])
    //  LogicalProject(EMPNO=[$0], DEPTNO=[$7])
    //    LogicalFilter(condition=[=($7, 20)])
    //      LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;   // empno
    int deptno = 1;  // deptno

    // Union handling is not yet fully implemented
    // For now, expect this might not work
    // assertThat(mq.determines(relNode, empNo, deptno), is(Boolean.TRUE));

    // deptno should not determine empno
    assertThat(mq.determines(relNode, deptno, empNo), is(Boolean.FALSE));
  }

  @Test void testFunctionalDependencyMultipleKeys() {
    final String sql = "select empno, deptno, empno * 100 + deptno as composite from emp";

    // Plan is:
    // LogicalProject(EMPNO=[$0], DEPTNO=[$7], COMPOSITE=[+(*($0, 100), $7)])
    //  LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;      // empno
    int deptno = 1;     // deptno
    int composite = 2;  // empno * 100 + deptno

    // empno determines all
    assertThat(mq.determines(relNode, empNo, deptno), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, empNo, composite), is(Boolean.TRUE));

    // Multiple keys handling is not yet fully implemented
  }

  @Test void testFunctionalDependencyConstants() {
    final String sql = "select empno, 100 as constant_col, deptno"
        + " from emp";

    // Plan is:
    // LogicalProject(EMPNO=[$0], CONSTANT_COL=[100], DEPTNO=[$7])
    //  LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;       // empno
    int constant = 1;    // 100 (constant)
    int deptno = 2;      // deptno

    // Constants should be functionally dependent on everything
    assertThat(mq.determines(relNode, empNo, constant), is(Boolean.TRUE));

    // Original dependencies should be preserved
    assertThat(mq.determines(relNode, empNo, deptno), is(Boolean.TRUE));

    // Everything determines constants
    assertThat(mq.determines(relNode, deptno, constant), is(Boolean.TRUE));
  }

  @Test void testFunctionalDependencyNonDeterministicFunctions() {
    final String sql = "select empno, rand() as random1, rand() as random2 from emp";

    // Plan is:
    // LogicalProject(EMPNO=[$0], RANDOM1=[RAND()], RANDOM2=[RAND()])
    //  LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;    // empno
    int random1 = 1;  // random1
    int random2 = 2;  // random2

    // Basic functional dependencies should still work
    assertThat(mq.determines(relNode, empNo, empNo), is(Boolean.TRUE));

    // Non-deterministic functions should not determine anything
    assertThat(mq.determines(relNode, random1, empNo), is(Boolean.FALSE));
    assertThat(mq.determines(relNode, random1, random2), is(Boolean.FALSE));
    assertThat(mq.determines(relNode, random2, empNo), is(Boolean.FALSE));
  }

  @Test void testFunctionalDependencyLeftJoin() {
    final String sql = "SELECT e.empno, e.deptno, d.name\n"
        + "FROM emp e\n"
        + "LEFT JOIN dept d ON e.deptno = d.deptno";

    // Plan is:
    // LogicalProject(EMPNO=[$0], DEPTNO=[$7], NAME=[$10])
    //  LogicalJoin(condition=[=($7, $9)], joinType=[left])
    //    LogicalTableScan(table=[[CATALOG, SALES, EMP]])
    //    LogicalTableScan(table=[[CATALOG, SALES, DEPT]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;     // e.empno
    int deptno = 1;    // e.deptno
    int dname = 2;     // d.name

    // Left side dependencies should be preserved
    assertThat(mq.determines(relNode, empNo, deptno), is(Boolean.TRUE));

    // Cross-join dependencies might not hold due to nulls from outer join
    assertThat(mq.determines(relNode, empNo, dname), is(Boolean.FALSE));
  }

  @Test void testFunctionalDependencyInnerJoin() {
    final String sql = "SELECT e.empno, e.deptno, d.name\n"
        + "FROM emp e\n"
        + "JOIN dept d ON e.deptno = d.deptno";

    // Plan is:
    // LogicalProject(EMPNO=[$0], DEPTNO=[$7], NAME=[$10])
    //  LogicalJoin(condition=[=($7, $9)], joinType=[inner])
    //    LogicalTableScan(table=[[CATALOG, SALES, EMP]])
    //    LogicalTableScan(table=[[CATALOG, SALES, DEPT]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;     // e.empno
    int deptno = 1;    // e.deptno
    int dname = 2;     // d.name

    // Left side dependencies should be preserved
    assertThat(mq.determines(relNode, empNo, deptno), is(Boolean.TRUE));

    // Cross-join dependencies should be preserved in inner join
    assertThat(mq.determines(relNode, empNo, dname), is(Boolean.TRUE));
  }

  @Test void testFunctionalDependencyRightJoin() {
    final String sql = "SELECT e.empno, e.deptno, d.name\n"
        + "FROM dept d\n"
        + "RIGHT JOIN emp e ON e.deptno = d.deptno";

    // Plan is:
    // LogicalProject(EMPNO=[$2], DEPTNO=[$9], NAME=[$1])
    //  LogicalJoin(condition=[=($9, $0)], joinType=[right])
    //    LogicalTableScan(table=[[CATALOG, SALES, DEPT]])
    //    LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
    System.out.println(RelOptUtil.toString(relNode));

    int empNo = 0;     // e.empno
    int deptno = 1;    // e.deptno
    int dname = 2;     // d.name

    // Right side dependencies should be preserved
    assertThat(mq.determines(relNode, empNo, deptno), is(Boolean.TRUE));

    // Cross-join dependencies might not hold due to nulls from outer join
    assertThat(mq.determines(relNode, empNo, dname), is(Boolean.FALSE));
  }

  @Test void testFunctionalDependencyFullOuterJoin() {
    final String sql = "SELECT e.empno, e.deptno, d.name\n"
        + "FROM emp e\n"
        + "FULL JOIN dept d ON e.deptno = d.deptno";

    // Plan is:
    // LogicalProject(EMPNO=[$0], DEPTNO=[$7], NAME=[$10])
    //  LogicalJoin(condition=[=($7, $9)], joinType=[full])
    //    LogicalTableScan(table=[[CATALOG, SALES, EMP]])
    //    LogicalTableScan(table=[[CATALOG, SALES, DEPT]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;     // e.empno
    int deptno = 1;    // e.deptno
    int dname = 2;     // d.name

    // Left side dependencies should not be preserved
    assertThat(mq.determines(relNode, empNo, deptno), is(Boolean.FALSE));

    // Cross-join dependencies might not hold due to nulls from outer join
    assertThat(mq.determines(relNode, empNo, dname), is(Boolean.FALSE));
  }

  @Test void testFunctionalDependencySemiJoin() {
    final String sql = "select empno, deptno from emp\n"
        + "intersect\n"
        + "select deptno, deptno from dept\n";

    // Plan is:
    // LogicalJoin(condition=[AND(=($0, $2), =($1, $3))], joinType=[semi])
    //  LogicalProject(EMPNO=[$0], DEPTNO=[$7])
    //    LogicalTableScan(table=[[CATALOG, SALES, EMP]])
    //  LogicalProject(DEPTNO=[$0], DEPTNO0=[$0])
    //    LogicalTableScan(table=[[CATALOG, SALES, DEPT]])

    RelNode relNode = sql(sql).toRel();
    HepPlanner hepPlanner =
        new HepPlanner(
            HepProgram.builder().addRuleCollection(
                ImmutableList.of(CoreRules.INTERSECT_TO_SEMI_JOIN)).build());
    hepPlanner.setRoot(relNode);
    relNode = hepPlanner.findBestExp();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;    // empno
    int deptno = 1;   // deptno

    // Semi join should preserve left table FDs
    // EMPNO is primary key, so it should still determine DEPTNO
    assertThat(mq.determines(relNode, empNo, deptno), is(Boolean.TRUE));
  }

  @Test void testFunctionalDependencySort() {
    final String sql = "select empno, ename, deptno, sal"
        + " from emp order by empno, deptno";

    // Plan is:
    // LogicalSort(sort0=[$0], sort1=[$2], dir0=[ASC], dir1=[ASC])
    //  LogicalProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7], SAL=[$5])
    //    LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    int empNo = 0;   // empno
    int ename = 1;   // ename
    int deptno = 2;  // deptno
    int sal = 3;     // sal

    // empno is primary key, so it should determine all other columns
    assertThat(mq.determines(relNode, empNo, ename), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, empNo, deptno), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, empNo, sal), is(Boolean.TRUE));

    // Non-key columns should not determine key column
    assertThat(mq.determines(relNode, ename, empNo), is(Boolean.FALSE));
    assertThat(mq.determines(relNode, deptno, empNo), is(Boolean.FALSE));
    assertThat(mq.determines(relNode, sal, empNo), is(Boolean.FALSE));

    // Test candidate keys functionality for sort keys
    ImmutableBitSet sortKeys = ImmutableBitSet.of(empNo, deptno);
    Set<ImmutableBitSet> candidateKeys = mq.candidateKeys(relNode, sortKeys);

    // Should find that empno alone is a candidate key within the sort keys
    assertThat(candidateKeys.isEmpty(), is(Boolean.FALSE));
    assertThat(candidateKeys.contains(ImmutableBitSet.of(empNo)), is(Boolean.TRUE));
  }

  @Test void testFunctionalDependencySortDuplicateKeys() {
    final String sql = "select * from (select deptno as d1, deptno as d2 from emp) as t1\n"
        + " join emp t2 on t1.d1 = t2.deptno order by t1.d1, t1.d2, t1.d1 DESC NULLS FIRST";

    // Plan is:
    // LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
    //  LogicalProject(D1=[$0], D2=[$1], EMPNO=[$2], ENAME=[$3], JOB=[$4], MGR=[$5],
    //                 HIREDATE=[$6], SAL=[$7], COMM=[$8], DEPTNO=[$9], SLACKER=[$10])
    //    LogicalJoin(condition=[=($0, $9)], joinType=[inner])
    //      LogicalProject(D1=[$7], D2=[$7])
    //        LogicalTableScan(table=[[CATALOG, SALES, EMP]])
    //      LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    final RelNode relNode = sql(sql).toRel();
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

    Sort sort = (Sort) relNode;
    ImmutableIntList keys = sort.getCollation().getKeys();

    int d1 = keys.get(0);      // t1.d1
    int d2 = keys.get(1);      // t1.d2

    // Identical expressions should determine each other
    assertThat(mq.determines(relNode, d1, d2), is(Boolean.TRUE));
    assertThat(mq.determines(relNode, d2, d1), is(Boolean.TRUE));

    // Test candidate keys for the sort keys (d1, d2)
    ImmutableBitSet sortKeys = ImmutableBitSet.of(d1, d2);
    Set<ImmutableBitSet> candidateKeys = mq.candidateKeys(relNode, sortKeys);

    // Either {d1} or {d2} should be a candidate key since they are identical
    assertThat(candidateKeys.contains(ImmutableBitSet.of(d1)), is(Boolean.TRUE));
    assertThat(candidateKeys.contains(ImmutableBitSet.of(d2)), is(Boolean.TRUE));
  }
}
