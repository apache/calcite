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
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Combine;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CombineSimpleEquivalenceRule;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

import java.util.function.Function;

/**
 * Unit tests for {@link Combine} RelNode demonstrating various
 * shared component patterns including joins, filters, aggregations, and projections.
 */
class CombineRelOptRulesTest extends RelOptTestBase {

  @Override RelOptFixture fixture() {
    return super.fixture()
        .withDiffRepos(DiffRepository.lookup(CombineRelOptRulesTest.class));
  }

  @Test void testSharedJoin() {
    // Two queries sharing the same EMP-DEPT join
    // Query 1: SELECT E.EMPNO, D.DNAME FROM EMP E JOIN DEPT D ON E.DEPTNO = D.DEPTNO
    // Query 2: SELECT E.ENAME, D.LOC FROM EMP E JOIN DEPT D ON E.DEPTNO = D.DEPTNO
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1
      b.scan("EMP")
          .scan("DEPT")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.EQUALS,
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .project(b.field("EMPNO"), b.field("DNAME"));

      // Query 2
      b.scan("EMP")
          .scan("DEPT")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.EQUALS,
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .project(b.field("ENAME"), b.field("LOC"));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSimpleEquivalenceRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testSharedComplexJoin() {
    // Multiple queries sharing a 3-way join: EMP -> DEPT -> SALGRADE
    // Query 1: Count employees per department grade
    // Query 2: Average salary per department grade
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: SELECT D.DNAME, S.GRADE, COUNT(*) ...
      b.scan("EMP")
          .scan("DEPT")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.EQUALS,
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .scan("SALGRADE")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                      b.field(2, 0, "SAL"),
                      b.field(2, 1, "LOSAL")),
                  b.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                      b.field(2, 0, "SAL"),
                      b.field(2, 1, "HISAL"))))
          .aggregate(
              b.groupKey("DNAME", "GRADE"),
              b.count(false, "EMP_COUNT"));

      // Query 2: SELECT D.DNAME, S.GRADE, AVG(SAL) ...
      b.scan("EMP")
          .scan("DEPT")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.EQUALS,
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .scan("SALGRADE")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                      b.field(2, 0, "SAL"),
                      b.field(2, 1, "LOSAL")),
                  b.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                      b.field(2, 0, "SAL"),
                      b.field(2, 1, "HISAL"))))
          .aggregate(
              b.groupKey("DNAME", "GRADE"),
              b.avg(false, "AVG_SAL", b.field("SAL")));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSimpleEquivalenceRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  // ========== Shared Filter Tests ==========

  @Test void testSharedFilter() {
    // Two queries sharing the same filter condition
    // Query 1: SELECT EMPNO, SAL FROM EMP WHERE SAL > 2000 AND DEPTNO = 10
    // Query 2: SELECT ENAME, JOB FROM EMP WHERE SAL > 2000 AND DEPTNO = 10
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN,
                      b.field("SAL"),
                      b.literal(2000)),
                  b.call(SqlStdOperatorTable.EQUALS,
                      b.field("DEPTNO"),
                      b.literal(10))))
          .project(b.field("EMPNO"), b.field("SAL"));

      // Query 2
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN,
                      b.field("SAL"),
                      b.literal(2000)),
                  b.call(SqlStdOperatorTable.EQUALS,
                      b.field("DEPTNO"),
                      b.literal(10))))
          .project(b.field("ENAME"), b.field("JOB"));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSimpleEquivalenceRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testSharedFilterWithDifferentProjections() {
    // Three queries sharing same filter but different projections
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Shared filter: EMP WHERE SAL BETWEEN 1000 AND 3000
      // Query 1: Count
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                      b.field("SAL"),
                      b.literal(1000)),
                  b.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                      b.field("SAL"),
                      b.literal(3000))))
          .aggregate(b.groupKey(), b.count(false, "CNT"));

      // Query 2: Average salary
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                      b.field("SAL"),
                      b.literal(1000)),
                  b.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                      b.field("SAL"),
                      b.literal(3000))))
          .aggregate(b.groupKey(), b.avg(false, "AVG_SAL", b.field("SAL")));

      // Query 3: List of names
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                      b.field("SAL"),
                      b.literal(1000)),
                  b.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                      b.field("SAL"),
                      b.literal(3000))))
          .project(b.field("ENAME"), b.field("SAL"));

      return b.combine(3).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSimpleEquivalenceRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testSharedAggregationBase() {
    // Two queries that could share aggregation computation
    // Query 1: SELECT DEPTNO, SUM(SAL), COUNT(*) FROM EMP GROUP BY DEPTNO
    // Query 2: SELECT DEPTNO, SUM(SAL) FROM EMP GROUP BY DEPTNO WHERE SUM(SAL) > 10000
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: Basic aggregation
      b.scan("EMP")
          .aggregate(
              b.groupKey("DEPTNO"),
              b.sum(false, "TOTAL_SAL", b.field("SAL")));

      // Query 2: Same aggregation with HAVING clause
      b.scan("EMP")
          .aggregate(
              b.groupKey("DEPTNO"),
              b.sum(false, "TOTAL_SAL", b.field("SAL")))
          .filter(
              b.call(SqlStdOperatorTable.GREATER_THAN,
                  b.field("TOTAL_SAL"),
                  b.literal(10000)));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSimpleEquivalenceRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testSharedJoinThenAggregation() {
    // Queries sharing join followed by different aggregations
    // Query 1: Total salary by department
    // Query 2: Employee count by location
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: SELECT D.DNAME, SUM(E.SAL) FROM EMP E JOIN DEPT D ... GROUP BY D.DNAME
      b.scan("EMP")
          .scan("DEPT")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.EQUALS,
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .aggregate(
              b.groupKey("DNAME"),
              b.sum(false, "TOTAL_SAL", b.field("SAL")));

      // Query 2: SELECT D.LOC, COUNT(*) FROM EMP E JOIN DEPT D ... GROUP BY D.LOC
      b.scan("EMP")
          .scan("DEPT")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.EQUALS,
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .aggregate(
              b.groupKey("LOC"),
              b.count(false, "EMP_CNT"));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSimpleEquivalenceRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testSharedFilterJoinAggregate() {
    // Complex pattern: Filter -> Join -> different aggregations
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: High earners by department with count
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.GREATER_THAN,
                  b.field("SAL"),
                  b.literal(2000)))
          .scan("DEPT")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.EQUALS,
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .aggregate(
              b.groupKey("DNAME"),
              b.count(false, "HIGH_EARNER_CNT"));

      // Query 2: High earners by department with average
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.GREATER_THAN,
                  b.field("SAL"),
                  b.literal(2000)))
          .scan("DEPT")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.EQUALS,
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .aggregate(
              b.groupKey("DNAME"),
              b.avg(false, "AVG_HIGH_SAL", b.field("SAL")));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSimpleEquivalenceRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  // ========== Tests WITHOUT Shared Expressions (No Trivial Equivalence) ==========

  @Test void testNoSharedExpressionsDifferentTables() {
    // Two queries on completely different tables - no sharing possible
    // Query 1: SELECT * FROM EMP
    // Query 2: SELECT * FROM SALGRADE
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: EMP table
      b.scan("EMP")
          .project(b.field("EMPNO"), b.field("ENAME"), b.field("SAL"));

      // Query 2: SALGRADE table (completely unrelated)
      b.scan("SALGRADE")
          .project(b.field("GRADE"), b.field("LOSAL"), b.field("HISAL"));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSimpleEquivalenceRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testNoSharedExpressionsDifferentFilters() {
    // Two queries on same table but with non-overlapping filters - no sharing
    // Query 1: SELECT EMPNO FROM EMP WHERE DEPTNO = 10
    // Query 2: SELECT ENAME FROM EMP WHERE JOB = 'CLERK'
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: Filter on DEPTNO
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.EQUALS,
              b.field("DEPTNO"),
              b.literal(10)))
          .project(b.field("EMPNO"));

      // Query 2: Filter on JOB (different filter, not shareable)
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.EQUALS,
              b.field("JOB"),
              b.literal("CLERK")))
          .project(b.field("ENAME"));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSimpleEquivalenceRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testNoSharedExpressionsDifferentJoins() {
    // Two queries with different join conditions - no sharing
    // Query 1: EMP JOIN DEPT ON DEPTNO
    // Query 2: EMP JOIN SALGRADE ON SAL between LOSAL and HISAL
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: EMP-DEPT join
      b.scan("EMP")
          .scan("DEPT")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.EQUALS,
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .project(b.field("ENAME"), b.field("DNAME"));

      // Query 2: EMP-SALGRADE join (completely different join)
      b.scan("EMP")
          .scan("SALGRADE")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                      b.field(2, 0, "SAL"),
                      b.field(2, 1, "LOSAL")),
                  b.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                      b.field(2, 0, "SAL"),
                      b.field(2, 1, "HISAL"))))
          .project(b.field("ENAME"), b.field("GRADE"));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSimpleEquivalenceRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testNoSharedExpressionsDifferentGroupByKeys() {
    // Two queries with different GROUP BY keys - no sharing
    // Query 1: GROUP BY DEPTNO
    // Query 2: GROUP BY JOB
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: Group by DEPTNO
      b.scan("EMP")
          .aggregate(
              b.groupKey("DEPTNO"),
              b.count(false, "CNT"));

      // Query 2: Group by JOB (different key, not shareable)
      b.scan("EMP")
          .aggregate(
              b.groupKey("JOB"),
              b.count(false, "CNT"));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSimpleEquivalenceRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testNoSharedExpressionsDifferentJoinTypes() {
    // Two queries with same tables but different join types - no sharing
    // Query 1: EMP INNER JOIN DEPT
    // Query 2: EMP FULL OUTER JOIN DEPT
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: Inner join
      b.scan("EMP")
          .scan("DEPT")
          .join(JoinRelType.INNER,
              b.call(SqlStdOperatorTable.EQUALS,
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .project(b.field("EMPNO"), b.field("DNAME"));

      // Query 2: Full outer join (different join type)
      b.scan("EMP")
          .scan("DEPT")
          .join(JoinRelType.FULL,
              b.call(SqlStdOperatorTable.EQUALS,
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .project(b.field("EMPNO"), b.field("DNAME"));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSimpleEquivalenceRule.Config.DEFAULT.toRule());
        })
        .check();
  }
}
