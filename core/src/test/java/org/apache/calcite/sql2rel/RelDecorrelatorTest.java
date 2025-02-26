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
package org.apache.calcite.sql2rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.MatcherAssert.assertThat;

import static java.util.Objects.requireNonNull;

/**
 * Tests for {@link RelDecorrelator}.
 */
public class RelDecorrelatorTest {
  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
        .traitDefs((List<RelTraitDef>) null);
  }

  @Test void testGroupKeyNotInFrontWhenDecorrelate() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    RelNode before = builder.scan("EMP")
        .variable(v::set)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0),
                builder.call(
                    SqlStdOperatorTable.PLUS,
                    builder.literal(10),
                    builder.field(v.get(), "DEPTNO"))))
        .correlate(JoinRelType.LEFT, v.get().id, builder.field(2, 0, "DEPTNO"))
        .aggregate(builder.groupKey("ENAME"), builder.max(builder.field("EMPNO")))
        .build();

    final String planBefore = ""
        + "LogicalAggregate(group=[{1}], agg#0=[MAX($0)])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalFilter(condition=[=($0, +(10, $cor0.DEPTNO))])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(before, hasTree(planBefore));

    RelNode after = RelDecorrelator.decorrelateQuery(before, builder);

    final String planAfter = ""
        + "LogicalAggregate(group=[{0}], agg#0=[MAX($1)])\n"
        + "  LogicalProject(ENAME=[$1], EMPNO=[$0])\n"
        + "    LogicalJoin(condition=[=($8, $9)], joinType=[left])\n"
        + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], "
        + "SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+(10, $7)])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6468">[CALCITE-6468] RelDecorrelator
   * throws AssertionError if correlated variable is used as Aggregate group key</a>.
   */
  @Test void testCorrVarOnAggregateKey() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = "WITH agg_sal AS"
        + " (SELECT deptno, sum(sal) AS total FROM emp GROUP BY deptno)\n"
        + " SELECT 1 FROM agg_sal s1"
        + " WHERE s1.total > (SELECT avg(total) FROM agg_sal s2 WHERE s1.deptno = s2.deptno)";
    final RelNode originalRel;
    try {
      final SqlNode parse = planner.parse(sql);
      final SqlNode validate = planner.validate(parse);
      originalRel = planner.rel(validate).rel;
    } catch (Exception e) {
      throw TestUtil.rethrow(e);
    }

    final HepProgram hepProgram = HepProgram.builder()
        .addRuleCollection(
            ImmutableList.of(
                // SubQuery program rules
                CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
                CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE,
                // plus FilterAggregateTransposeRule
                CoreRules.FILTER_AGGREGATE_TRANSPOSE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(EXPR$0=[1])\n"
        + "  LogicalProject(DEPTNO=[$0], TOTAL=[$1])\n"
        + "    LogicalFilter(condition=[>($1, $2)])\n"
        + "      LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
        + "        LogicalAggregate(group=[{0}], TOTAL=[SUM($1)])\n"
        + "          LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
        + "            LogicalTableScan(table=[[scott, EMP]])\n"
        + "        LogicalAggregate(group=[{}], EXPR$0=[AVG($0)])\n"
        + "          LogicalProject(TOTAL=[$1])\n"
        + "            LogicalAggregate(group=[{0}], TOTAL=[SUM($1)])\n"
        + "              LogicalFilter(condition=[=($cor0.DEPTNO, $0)])\n"
        + "                LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
        + "                  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(before, hasTree(planBefore));

    // Check decorrelation does not fail here
    final RelNode after = RelDecorrelator.decorrelateQuery(before, builder);

    // Verify plan
    final String planAfter = ""
        + "LogicalProject(EXPR$0=[1])\n"
        + "  LogicalJoin(condition=[AND(=($0, $2), >($1, $3))], joinType=[inner])\n"
        + "    LogicalAggregate(group=[{0}], TOTAL=[SUM($1)])\n"
        + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalAggregate(group=[{0}], EXPR$0=[AVG($1)])\n"
        + "      LogicalProject(DEPTNO=[$0], TOTAL=[$1])\n"
        + "        LogicalAggregate(group=[{0}], TOTAL=[SUM($1)])\n"
        + "          LogicalProject(DEPTNO=[$0], SAL=[$1])\n"
        + "            LogicalFilter(condition=[IS NOT NULL($0)])\n"
        + "              LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
        + "                LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6674">[CALCITE-6674] Make
   * RelDecorrelator rules configurable</a>.
   */
  @Test void testDecorrelatorCustomizeRules() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = "select ROW("
        + "(select deptno\n"
        + "from dept\n"
        + "where dept.deptno = emp.deptno), emp.ename)\n"
        + "from emp";
    final RelNode parsedRel;
    try {
      final SqlNode parse = planner.parse(sql);
      final SqlNode validate = planner.validate(parse);
      parsedRel = planner.rel(validate).rel;
    } catch (Exception e) {
      throw TestUtil.rethrow(e);
    }

    // Convert SubQuery into Correlate
    final HepProgram hepProgram = HepProgram.builder()
        .addRuleCollection(ImmutableList.of(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode original =
        program.run(cluster.getPlanner(), parsedRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planOriginal = ""
        + "LogicalProject(EXPR$0=[ROW($8, $1)])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalAggregate(group=[{}], agg#0=[SINGLE_VALUE($0)])\n"
        + "      LogicalProject(DEPTNO=[$0])\n"
        + "        LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "          LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(original, hasTree(planOriginal));

    // Default decorrelate
    final RelNode decorrelatedDefault = RelDecorrelator.decorrelateQuery(original, builder);
    final String planDecorrelatedDefault = ""
        + "LogicalProject(EXPR$0=[ROW($8, $1)])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPTNO8=[$8])\n"
        + "    LogicalJoin(condition=[=($8, $7)], joinType=[left])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(decorrelatedDefault, hasTree(planDecorrelatedDefault));

    // Decorrelate using explicitly the same rules as the default ones: same result
    final RuleSet defaultRules =
        RuleSets.ofList(RelDecorrelator.RemoveSingleAggregateRule.DEFAULT.toRule(),
            RelDecorrelator.RemoveCorrelationForScalarProjectRule.DEFAULT.toRule(),
            RelDecorrelator.RemoveCorrelationForScalarAggregateRule.DEFAULT.toRule());
    final RelNode decorrelatedDefault2 =
        RelDecorrelator.decorrelateQuery(original, builder, defaultRules);
    assertThat(decorrelatedDefault2, hasTree(planDecorrelatedDefault));

    // Decorrelate using just the relevant rule for this query: same result
    final RuleSet relevantRule =
        RuleSets.ofList(RelDecorrelator.RemoveCorrelationForScalarProjectRule.DEFAULT.toRule());
    final RelNode decorrelatedRelevantRule =
        RelDecorrelator.decorrelateQuery(original, builder, relevantRule);
    assertThat(decorrelatedRelevantRule, hasTree(planDecorrelatedDefault));

    // Decorrelate without any pre-rules (just the "main" decorrelate program): decorrelated
    // but aggregate is kept
    final RuleSet noRules = RuleSets.ofList(Collections.emptyList());
    final RelNode decorrelatedNoRules =
        RelDecorrelator.decorrelateQuery(original, builder, noRules);
    final String planDecorrelatedNoRules = ""
        + "LogicalProject(EXPR$0=[ROW($9, $1)])\n"
        + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalAggregate(group=[{0}], agg#0=[SINGLE_VALUE($1)])\n"
        + "      LogicalProject(DEPTNO1=[$0], DEPTNO=[$0])\n"
        + "        LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(decorrelatedNoRules, hasTree(planDecorrelatedNoRules));
  }
}
