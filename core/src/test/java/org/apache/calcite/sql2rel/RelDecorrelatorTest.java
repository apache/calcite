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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.test.SqlTestFactory;
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
import java.util.Properties;

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

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7058">[CALCITE-7058]
   * Decorrelator may produce different column names</a>. */
  @Test void testDecorrelatorNames() throws SqlParseException {
    String sql = "SELECT SUM(r.empno * r.deptno) FROM \"scott\".EMP r\n"
        + "WHERE\n"
        + "0.5 * (SELECT SUM(r1.deptno) FROM \"scott\".EMP r1) =\n"
        + "(SELECT SUM(r2.deptno) FROM \"scott\".EMP r2 WHERE r2.empno = r.empno)";
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT);
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(new Properties());
    SqlTestFactory factory = SqlTestFactory.INSTANCE
        .withCatalogReader((typeFactory, caseSensitive) ->
            new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                ImmutableList.of("SCOTT"),
                typeFactory,
                config))
        .withSqlToRelConfig(c -> c.withExpand(true));
    SqlParser parser = factory.createParser(sql);
    SqlNode parsed = parser.parseQuery();
    final SqlToRelConverter sqlToRelConverter = factory.createSqlToRelConverter();
    assert sqlToRelConverter.validator != null;
    final SqlNode validated = sqlToRelConverter.validator.validate(parsed);
    RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);
    // Original plan:
    // LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])
    //   LogicalProject($f0=[*($0, $7)])
    //     LogicalFilter(condition=[=(*(0.5:DECIMAL(2, 1), $9), CAST($10):DECIMAL(12, 1))])
    //       LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])
    //         LogicalJoin(condition=[true], joinType=[left])
    //           LogicalTableScan(table=[[CATALOG, SALES, EMP]])
    //           LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])
    //             LogicalProject(DEPTNO=[$7])
    //               LogicalTableScan(table=[[CATALOG, SALES, EMP]])
    //         LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])
    //           LogicalProject(DEPTNO=[$7])
    //             LogicalFilter(condition=[=($0, $cor0.EMPNO)])
    //               LogicalTableScan(table=[[CATALOG, SALES, EMP]])

    RelNode correlate = root.rel.getInput(0).getInput(0).getInput(0);
    // In the absence of the fix for [CALCITE-7058] the following statement
    // will fail with an assertion failure:
    // Decorrelation produced a relation with a different type;
    // before:
    // RecordType(SMALLINT EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE,
    // DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, TINYINT DEPTNO, TINYINT EXPR$0, TINYINT EXPR$00)
    // after:
    // RecordType(SMALLINT EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE,
    // DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, TINYINT DEPTNO, TINYINT EXPR$0, TINYINT EXPR$09)
    final RelBuilder relBuilder =
        RelFactories.LOGICAL_BUILDER.create(correlate.getCluster(), null);
    RelDecorrelator.decorrelateQuery(correlate, relBuilder);
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7024">[CALCITE-7024]
   * Decorrelator does not always produce a query with the same type signature</a>. */
  @Test void testTypeEquivalence() {
    final String sql = "SELECT dname FROM \"scott\".DEPT WHERE 2000 > "
        + "(SELECT EMP.sal FROM \"scott\".EMP where\n"
        + "DEPT.deptno = EMP.deptno ORDER BY year(hiredate), EMP.sal limit 1)";
    try {
      SchemaPlus rootSchema = Frameworks.createRootSchema(true);
      CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT);
      CalciteConnectionConfig config = new CalciteConnectionConfigImpl(new Properties());
      SqlTestFactory factory = SqlTestFactory.INSTANCE
          .withCatalogReader((typeFactory, caseSensitive) ->
              new CalciteCatalogReader(
                  CalciteSchema.from(rootSchema),
                  ImmutableList.of("SCOTT"),
                  typeFactory,
                  config))
          .withSqlToRelConfig(c -> c.withExpand(true));
      SqlParser parser = factory.createParser(sql);
      SqlNode parsed = parser.parseQuery();
      final SqlToRelConverter sqlToRelConverter = factory.createSqlToRelConverter();
      final SqlNode validated = sqlToRelConverter.validator.validate(parsed);
      RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);

      // The plan starts has this shape:
      // LogicalProject(DNAME=[$1])
      //  LogicalFilter(condition=[>(2000.00, CAST($3):DECIMAL(12, 2))])
      //    LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])
      // we invoke decorrelate on the LogicalCorrelate node directly
      RelNode filter = ((Project) root.rel).getInput();
      RelNode correlate = ((Filter) filter).getInput();

      final RelBuilder relBuilder =
          RelFactories.LOGICAL_BUILDER.create(filter.getCluster(), null);
      RelDecorrelator.decorrelateQuery(correlate, relBuilder);
    } catch (Exception e) {
      throw TestUtil.rethrow(e);
    }
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

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7057">[CALCITE-7057]
   * NPE when decorrelating query containing nested correlated subqueries</a>. */
  @Test void test7057() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = ""
        + "select\n"
        + " (select ename || ' from dept '\n"
        + " || (select dname from dept where deptno = emp.deptno and emp.empno = empnos.empno)\n"
        + "       from emp\n"
        + "    ) as ename_from_dept\n"
        + "from (values (7369), (7499)) as empnos(empno) order by 1";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());

    // before fix:
    //
    // [01]LogicalSort(sort0=[$0], dir0=[ASC])
    // [02] LogicalProject(ENAME_FROM_DEPT=[$1])
    // [03]   LogicalCorrelate(correlation=[$cor2], joinType=[left], requiredColumns=[{0}])
    // [04]     LogicalValues(tuples=[[{ 7369 }, { 7499 }]])
    // [05]     LogicalAggregate(group=[{}], agg#0=[SINGLE_VALUE($0)])
    // [06]       LogicalProject(EXPR$0=[||(||($1, ' from dept '), $8)])
    // [07]         LogicalJoin(condition=[true], joinType=[left], variablesSet=[[$cor0, $cor2]])
    // [08]           LogicalTableScan(table=[[scott, EMP]])
    // [09]           LogicalAggregate(group=[{}], agg#0=[SINGLE_VALUE($0)])
    // [10]             LogicalProject(DNAME=[$1])
    // [11]               LogicalFilter(condition=[AND(=($0, $cor0.DEPTNO),
    //                                  =(CAST($cor0.EMPNO):INTEGER NOT NULL, $cor2.EMPNO))])
    // [12]                 LogicalTableScan(table=[[scott, DEPT]])
    //
    // diff is [07]LogicalJoin(condition=[true], joinType=[left], variablesSet=[[$cor0, $cor2]])
    //             LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0, 7}])
    //
    final String planBefore = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(ENAME_FROM_DEPT=[$1])\n"
        + "    LogicalCorrelate(correlation=[$cor2], joinType=[left], requiredColumns=[{0}])\n"
        + "      LogicalValues(tuples=[[{ 7369 }, { 7499 }]])\n"
        + "      LogicalAggregate(group=[{}], agg#0=[SINGLE_VALUE($0)])\n"
        + "        LogicalProject(EXPR$0=[||(||($1, ' from dept '), $8)])\n"
        + "          LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0, 7}])\n"
        + "            LogicalTableScan(table=[[scott, EMP]])\n"
        + "            LogicalAggregate(group=[{}], agg#0=[SINGLE_VALUE($0)])\n"
        + "              LogicalProject(DNAME=[$1])\n"
        + "                LogicalFilter(condition=[AND(=($0, $cor0.DEPTNO), =(CAST($cor0.EMPNO):INTEGER NOT NULL, $cor2.EMPNO))])\n"
        + "                  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    final String planAfter = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(ENAME_FROM_DEPT=[$2])\n"
        + "    LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $1)], joinType=[left])\n"
        + "      LogicalValues(tuples=[[{ 7369 }, { 7499 }]])\n"
        + "      LogicalAggregate(group=[{0}], agg#0=[SINGLE_VALUE($1)])\n"
        + "        LogicalProject(EMPNO1=[$12], EXPR$0=[||(||($1, ' from dept '), $13)])\n"
        + "          LogicalJoin(condition=[AND(=($7, $10), =($9, $11))], joinType=[left])\n"
        + "            LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPTNO0=[$7], EMPNO0=[CAST($0):INTEGER NOT NULL])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n"
        + "            LogicalAggregate(group=[{0, 1, 2}], agg#0=[SINGLE_VALUE($3)])\n"
        + "              LogicalProject(DEPTNO0=[$3], EMPNO0=[$4], EMPNO=[$5], DNAME=[$1])\n"
        + "                LogicalJoin(condition=[=($0, $3)], joinType=[inner])\n"
        + "                  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "                  LogicalJoin(condition=[=($1, $2)], joinType=[inner])\n"
        + "                    LogicalAggregate(group=[{0, 1}])\n"
        + "                      LogicalProject(DEPTNO=[$7], EMPNO0=[CAST($0):INTEGER NOT NULL])\n"
        + "                        LogicalTableScan(table=[[scott, EMP]])\n"
        + "                    LogicalValues(tuples=[[{ 7369 }, { 7499 }]])\n";
    assertThat(after, hasTree(planAfter));
  }

  @Test void testDecorrelateCountBug() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = "SELECT deptno, "
        + "(SELECT CASE WHEN SUM(sal) > 10 then 'VIP' else 'Regular' END expr "
        + " FROM emp e WHERE d.deptno = e.deptno) a "
        + "FROM dept d";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(DEPTNO=[$0], A=[$3])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalProject(EXPR=[CASE(>($0, 10.00), 'VIP    ', 'Regular')])\n"
        + "      LogicalAggregate(group=[{}], agg#0=[SUM($0)])\n"
        + "        LogicalProject(SAL=[$5])\n"
        + "          LogicalFilter(condition=[=($cor0.DEPTNO, $7)])\n"
        + "            LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));

    // Verify plan
    final String planAfter = ""
        + "LogicalProject(DEPTNO=[$0], A=[$3])\n"
        + "  LogicalJoin(condition=[=($0, $4)], joinType=[left])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalProject(EXPR=[CASE(>($2, 10.00), 'VIP    ', 'Regular')], DEPTNO=[$0])\n"
        + "      LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $1)], joinType=[left])\n"
        + "        LogicalProject(DEPTNO=[$0])\n"
        + "          LogicalTableScan(table=[[scott, DEPT]])\n"
        + "        LogicalAggregate(group=[{0}], agg#0=[SUM($1)])\n"
        + "          LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
        + "            LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7303">[CALCITE-7303]
   * Subqueries cannot be decorrelated if filter condition have multi CorrelationId</a>. */
  @Test void testCorrelationEquivalent() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = "SELECT deptno\n"
        + "FROM emp e\n"
        + "WHERE EXISTS (\n"
        + "  SELECT *\n"
        + "  FROM dept d\n"
        + "  WHERE EXISTS(\n"
        + "    SELECT *\n"
        + "    FROM bonus b\n"
        + "    WHERE b.ename = e.ename\n"
        + "    AND b.job = d.dname\n"
        + "    AND d.deptno = e.deptno))";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(DEPTNO=[$7])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1, 7}])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalAggregate(group=[{0}])\n"
        + "        LogicalProject(i=[true])\n"
        + "          LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
        + "            LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0, 1}])\n"
        + "              LogicalTableScan(table=[[scott, DEPT]])\n"
        + "              LogicalAggregate(group=[{0}])\n"
        + "                LogicalProject(i=[true])\n"
        + "                  LogicalFilter(condition=[AND(=($0, $cor0.ENAME), =(CAST($1):VARCHAR(14), $cor1.DNAME), =($cor1.DEPTNO, $cor0.DEPTNO))])\n"
        + "                    LogicalTableScan(table=[[scott, BONUS]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    // Verify plan
    final String planAfter = ""
        + "LogicalProject(DEPTNO=[$7])\n"
        + "  LogicalJoin(condition=[AND(=($1, $8), =($7, $9))], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalProject(ENAME0=[$0], DEPTNO0=[$1], $f2=[true])\n"
        + "      LogicalAggregate(group=[{0, 1}])\n"
        + "        LogicalProject(ENAME0=[$3], DEPTNO0=[$4])\n"
        + "          LogicalJoin(condition=[AND(=($0, $5), =($1, $6))], joinType=[inner])\n"
        + "            LogicalTableScan(table=[[scott, DEPT]])\n"
        + "            LogicalProject(ENAME0=[$0], DEPTNO=[$1], DEPTNO0=[$2], DNAME=[$3], $f4=[true])\n"
        + "              LogicalAggregate(group=[{0, 1, 2, 3}])\n"
        + "                LogicalProject(ENAME0=[$4], DEPTNO=[$5], DEPTNO0=[$6], DNAME=[$7])\n"
        + "                  LogicalJoin(condition=[AND(=($0, $4), =(CAST($1):VARCHAR(14), $7))], joinType=[inner])\n"
        + "                    LogicalTableScan(table=[[scott, BONUS]])\n"
        + "                    LogicalJoin(condition=[=($2, $1)], joinType=[inner])\n"
        + "                      LogicalProject(ENAME=[$1], DEPTNO=[$7])\n"
        + "                        LogicalTableScan(table=[[scott, EMP]])\n"
        + "                      LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
        + "                        LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7394">[CALCITE-7394]
   * Nested sub-query with multiple levels of correlation returns incorrect results</a>. */
  @Test void testNestedSubQueryWithMultiLevelCorrelation() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = ""
        + "select d.dname,\n"
        + "  (select count(*)\n"
        + "   from emp e\n"
        + "   where e.deptno = d.deptno\n"
        + "   and exists (\n"
        + "     select 1\n"
        + "     from (values (1000), (2000), (3000)) as v(sal)\n"
        + "     where e.sal > v.sal\n"
        + "     and d.deptno * 100 < v.sal\n"
        + "   )\n"
        + "  ) as c\n"
        + "from dept d\n"
        + "order by d.dname";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(DNAME=[$1], C=[$3])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "      LogicalAggregate(group=[{}], EXPR$0=[COUNT()])\n"
        + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "          LogicalFilter(condition=[=($7, $cor0.DEPTNO)])\n"
        + "            LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{5}])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n"
        + "              LogicalAggregate(group=[{0}])\n"
        + "                LogicalProject(i=[true])\n"
        + "                  LogicalFilter(condition=[AND(>(CAST($cor1.SAL):DECIMAL(12, 2), CAST($0):DECIMAL(12, 2) NOT NULL), <(*($cor0.DEPTNO, 100), $0))])\n"
        + "                    LogicalValues(tuples=[[{ 1000 }, { 2000 }, { 3000 }]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    // before fix:
    //
    // LogicalSort(sort0=[$0], dir0=[ASC])
    //  LogicalProject(DNAME=[$1], C=[$7])
    //    LogicalJoin(condition=[AND(=($0, $5), =($4, $6))], joinType=[left])
    //      LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2], DEPTNO0=[$0], $f4=[*($0, 100)])
    //        LogicalTableScan(table=[[scott, DEPT]])
    //      LogicalProject(DEPTNO8=[$0], $f4=[$1], EXPR$0=[CASE(IS NOT NULL($5), $5, 0)])
    //        LogicalJoin(condition=[AND(IS NOT DISTINCT FROM($0, $3),
    //                                   IS NOT DISTINCT FROM($1, $4))], joinType=[left])
    //          LogicalJoin(condition=[true], joinType=[inner])       // <---- error part
    //            LogicalProject(DEPTNO=[$0], $f4=[*($0, 100)])
    //              LogicalTableScan(table=[[scott, DEPT]])
    //            LogicalAggregate(group=[{0}])                       // <---- error part
    //              LogicalProject(SAL0=[CAST($5):DECIMAL(12, 2)])    // <---- error part
    //                LogicalTableScan(table=[[scott, EMP]])          // <---- error part
    //          LogicalAggregate(group=[{0, 1}], EXPR$0=[COUNT()])
    //            LogicalProject(DEPTNO8=[$7], $f4=[$9])
    //              LogicalFilter(condition=[IS NOT NULL($7)])
    //                LogicalProject(..., DEPTNO=[$7], i=[$11], $f4=[$9])
    //                  LogicalJoin(condition=[=($8, $10)], joinType=[inner])
    //                    LogicalProject(..., SAL0=[CAST($5):DECIMAL(12, 2)])
    //                      LogicalTableScan(table=[[scott, EMP]])
    //                    LogicalProject($f4=[$0], SAL0=[$1], $f2=[true])
    //                      LogicalAggregate(group=[{0, 1}])
    //                        LogicalProject($f4=[$1], SAL0=[$2])
    //                          LogicalJoin(condition=[AND(>($2, CAST($0):DECIMAL(12, 2) NOT NULL),
    //                                                    <($1, $0))], joinType=[inner])
    //                            LogicalValues(tuples=[[{ 1000 }, { 2000 }, { 3000 }]])
    //                            LogicalJoin(condition=[true], joinType=[inner])
    //                              LogicalAggregate(group=[{0}])
    //                                LogicalProject($f4=[*($0, 100)])
    //                                  LogicalTableScan(table=[[scott, DEPT]])
    //                              LogicalAggregate(group=[{0}])
    //                                LogicalProject(SAL0=[CAST($5):DECIMAL(12, 2)])
    //                                  LogicalTableScan(table=[[scott, EMP]])
    final String planAfter = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(DNAME=[$1], C=[$7])\n"
        + "    LogicalJoin(condition=[AND(=($0, $5), =($4, $6))], joinType=[left])\n"
        + "      LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2], DEPTNO0=[$0], $f4=[*($0, 100)])\n"
        + "        LogicalTableScan(table=[[scott, DEPT]])\n"
        + "      LogicalProject(DEPTNO8=[$0], $f4=[$1], EXPR$0=[CASE(IS NOT NULL($4), $4, 0)])\n"
        + "        LogicalJoin(condition=[AND(IS NOT DISTINCT FROM($0, $2), IS NOT DISTINCT FROM($1, $3))], joinType=[left])\n"
        + "          LogicalProject(DEPTNO=[$0], $f4=[*($0, 100)])\n"
        + "            LogicalTableScan(table=[[scott, DEPT]])\n"
        + "          LogicalAggregate(group=[{0, 1}], EXPR$0=[COUNT()])\n"
        + "            LogicalProject(DEPTNO8=[$7], $f4=[$9])\n"
        + "              LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "                LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], i=[$11], $f4=[$9])\n"
        + "                  LogicalJoin(condition=[=($8, $10)], joinType=[inner])\n"
        + "                    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], SAL0=[CAST($5):DECIMAL(12, 2)])\n"
        + "                      LogicalTableScan(table=[[scott, EMP]])\n"
        + "                    LogicalProject($f4=[$0], SAL0=[$1], $f2=[true])\n"
        + "                      LogicalAggregate(group=[{0, 1}])\n"
        + "                        LogicalProject($f4=[$1], SAL0=[$2])\n"
        + "                          LogicalJoin(condition=[AND(>($2, CAST($0):DECIMAL(12, 2) NOT NULL), <($1, $0))], joinType=[inner])\n"
        + "                            LogicalValues(tuples=[[{ 1000 }, { 2000 }, { 3000 }]])\n"
        + "                            LogicalJoin(condition=[true], joinType=[inner])\n"
        + "                              LogicalAggregate(group=[{0}])\n"
        + "                                LogicalProject($f4=[*($0, 100)])\n"
        + "                                  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "                              LogicalAggregate(group=[{0}])\n"
        + "                                LogicalProject(SAL0=[CAST($5):DECIMAL(12, 2)])\n"
        + "                                  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7297">[CALCITE-7297]
   * The result is incorrect when the GROUP BY key in a subquery is a RexFieldAccess</a>. */
  @Test void testSkipsRedundantValueGenerator() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = ""
        + "SELECT *,\n"
        + "    (SELECT COUNT(*)\n"
        + "    FROM \n"
        + "        (\n"
        + "        SELECT empno, ename, job\n"
        + "        FROM emp\n"
        + "        WHERE emp.deptno = dept.deptno) AS sub\n"
        + "    GROUP BY deptno) AS num_dept_groups\n"
        + "FROM dept";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalCorrelate(correlation=[$cor1], joinType=[left], requiredColumns=[{0}])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalAggregate(group=[{}], agg#0=[SINGLE_VALUE($0)])\n"
        + "    LogicalProject(EXPR$0=[$1])\n"
        + "      LogicalAggregate(group=[{0}], EXPR$0=[COUNT()])\n"
        + "        LogicalProject($f0=[$cor1.DEPTNO])\n"
        + "          LogicalFilter(condition=[=($7, $cor1.DEPTNO)])\n"
        + "            LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    // Verify plan
    final String planAfter = ""
        + "LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2], $f1=[$4])\n"
        + "  LogicalJoin(condition=[=($0, $3)], joinType=[left])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalAggregate(group=[{0}], agg#0=[SINGLE_VALUE($1)])\n"
        + "      LogicalProject(DEPTNO=[$1], EXPR$0=[$2])\n"
        + "        LogicalAggregate(group=[{0, 1}], EXPR$0=[COUNT()])\n"
        + "          LogicalProject($f0=[$7], DEPTNO=[$7])\n"
        + "            LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(after, hasTree(planAfter));
  }

  @Test void testCorrelationInSetOp0() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = "SELECT ename,\n"
        + "    (SELECT sum(c)\n"
        + "    FROM\n"
        + "        (SELECT deptno AS c\n"
        + "        FROM dept\n"
        + "        WHERE dept.deptno = emp.deptno\n"
        + "        UNION ALL\n"
        + "        SELECT 2 AS c\n"
        + "        FROM bonus) AS union_subquery\n"
        + "    ) AS correlated_sum\n"
        + "FROM emp\n"
        + "ORDER BY ename";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(ENAME=[$1], CORRELATED_SUM=[$8])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])\n"
        + "        LogicalUnion(all=[true])\n"
        + "          LogicalProject(C=[CAST($0):INTEGER NOT NULL])\n"
        + "            LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "              LogicalTableScan(table=[[scott, DEPT]])\n"
        + "          LogicalProject(C=[2])\n"
        + "            LogicalTableScan(table=[[scott, BONUS]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    // Verify plan
    final String planAfter = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(ENAME=[$1], CORRELATED_SUM=[$9])\n"
        + "    LogicalJoin(condition=[IS NOT DISTINCT FROM($7, $8)], joinType=[left])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalAggregate(group=[{0}], EXPR$0=[SUM($1)])\n"
        + "        LogicalProject(DEPTNO=[$0], C=[$1])\n"
        + "          LogicalUnion(all=[true])\n"
        + "            LogicalProject(DEPTNO=[$0], C=[$1])\n"
        + "              LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $2)], joinType=[inner])\n"
        + "                LogicalAggregate(group=[{0}])\n"
        + "                  LogicalProject(DEPTNO=[$7])\n"
        + "                    LogicalTableScan(table=[[scott, EMP]])\n"
        + "                LogicalProject(C=[CAST($0):INTEGER NOT NULL], DEPTNO=[$0])\n"
        + "                  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "            LogicalProject(DEPTNO=[$0], C=[$1])\n"
        + "              LogicalJoin(condition=[true], joinType=[inner])\n"
        + "                LogicalAggregate(group=[{0}])\n"
        + "                  LogicalProject(DEPTNO=[$7])\n"
        + "                    LogicalTableScan(table=[[scott, EMP]])\n"
        + "                LogicalProject(C=[2])\n"
        + "                  LogicalTableScan(table=[[scott, BONUS]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7272">[CALCITE-7272]
   * Subqueries cannot be decorrelated if have set op</a>. */
  @Test void testCorrelationInSetOp1() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = "SELECT ename,\n"
        + "    (SELECT sum(c)\n"
        + "    FROM\n"
        + "        (SELECT deptno AS c\n"
        + "        FROM dept\n"
        + "        WHERE dept.deptno = emp.deptno\n"
        + "        UNION ALL\n"
        + "        SELECT 2 AS c\n"
        + "        FROM bonus\n"
        + "        WHERE bonus.job = emp.job) AS union_subquery\n"
        + "    ) AS correlated_sum\n"
        + "FROM emp\n"
        + "ORDER BY ename";

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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(ENAME=[$1], CORRELATED_SUM=[$8])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{2, 7}])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])\n"
        + "        LogicalUnion(all=[true])\n"
        + "          LogicalProject(C=[CAST($0):INTEGER NOT NULL])\n"
        + "            LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "              LogicalTableScan(table=[[scott, DEPT]])\n"
        + "          LogicalProject(C=[2])\n"
        + "            LogicalFilter(condition=[=($1, $cor0.JOB)])\n"
        + "              LogicalTableScan(table=[[scott, BONUS]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    // Verify plan
    final String planAfter = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(ENAME=[$1], CORRELATED_SUM=[$10])\n"
        + "    LogicalJoin(condition=[AND(IS NOT DISTINCT FROM($2, $8), IS NOT DISTINCT FROM($7, $9))], joinType=[left])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalAggregate(group=[{0, 1}], EXPR$0=[SUM($2)])\n"
        + "        LogicalProject(JOB=[$0], DEPTNO=[$1], C=[$2])\n"
        + "          LogicalUnion(all=[true])\n"
        + "            LogicalProject(JOB=[$0], DEPTNO=[$1], C=[$2])\n"
        + "              LogicalJoin(condition=[IS NOT DISTINCT FROM($1, $3)], joinType=[inner])\n"
        + "                LogicalAggregate(group=[{0, 1}])\n"
        + "                  LogicalProject(JOB=[$2], DEPTNO=[$7])\n"
        + "                    LogicalTableScan(table=[[scott, EMP]])\n"
        + "                LogicalProject(C=[CAST($0):INTEGER NOT NULL], DEPTNO=[$0])\n"
        + "                  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "            LogicalProject(JOB=[$0], DEPTNO=[$1], C=[$2])\n"
        + "              LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $3)], joinType=[inner])\n"
        + "                LogicalAggregate(group=[{0, 1}])\n"
        + "                  LogicalProject(JOB=[$2], DEPTNO=[$7])\n"
        + "                    LogicalTableScan(table=[[scott, EMP]])\n"
        + "                LogicalProject(C=[2], JOB=[$1])\n"
        + "                  LogicalFilter(condition=[IS NOT NULL($1)])\n"
        + "                    LogicalTableScan(table=[[scott, BONUS]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7272">[CALCITE-7272]
   * Subqueries cannot be decorrelated if have set op</a>. */
  @Test void testCorrelationInSetOp2() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = "SELECT d.dname\n"
        + "FROM dept d\n"
        + "WHERE EXISTS (\n"
        + "  SELECT 1\n"
        + "  FROM emp e\n"
        + "  WHERE e.deptno = d.deptno\n"
        + "  AND (\n"
        + "    SELECT SUM(x)\n"
        + "    FROM (\n"
        + "        SELECT COUNT(*) as x\n"
        + "        FROM bonus b\n"
        + "        WHERE b.ename = e.ename\n"
        + "        UNION ALL\n"
        + "        SELECT COUNT(*) as x\n"
        + "        FROM emp e2\n"
        + "        WHERE e2.deptno = d.deptno\n"
        + "    ) t\n"
        + "  ) > 5)";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(DNAME=[$1])\n"
        + "  LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "      LogicalAggregate(group=[{0}])\n"
        + "        LogicalProject(i=[true])\n"
        + "          LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "            LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($8, 5))])\n"
        + "              LogicalCorrelate(correlation=[$cor1], joinType=[left], requiredColumns=[{1}])\n"
        + "                LogicalTableScan(table=[[scott, EMP]])\n"
        + "                LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])\n"
        + "                  LogicalUnion(all=[true])\n"
        + "                    LogicalAggregate(group=[{}], X=[COUNT()])\n"
        + "                      LogicalFilter(condition=[=($0, $cor1.ENAME)])\n"
        + "                        LogicalTableScan(table=[[scott, BONUS]])\n"
        + "                    LogicalAggregate(group=[{}], X=[COUNT()])\n"
        + "                      LogicalFilter(condition=[=($7, $cor0.DEPTNO)])\n"
        + "                        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    // Verify plan
    final String planAfter = ""
        + "LogicalProject(DNAME=[$1])\n"
        + "  LogicalJoin(condition=[=($0, $3)], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalProject(DEPTNO0=[$0], $f1=[true])\n"
        + "      LogicalAggregate(group=[{0}])\n"
        + "        LogicalProject(DEPTNO0=[$8])\n"
        + "          LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPTNO0=[CAST($8):TINYINT], ENAME0=[$9], EXPR$0=[CAST($10):BIGINT])\n"
        + "            LogicalJoin(condition=[AND(IS NOT DISTINCT FROM($1, $9), =($7, $8))], joinType=[inner])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n"
        + "              LogicalFilter(condition=[>($2, 5)])\n"
        + "                LogicalAggregate(group=[{0, 1}], EXPR$0=[SUM($2)])\n"
        + "                  LogicalProject(DEPTNO=[$0], ENAME=[$1], X=[$2])\n"
        + "                    LogicalUnion(all=[true])\n"
        + "                      LogicalProject(DEPTNO=[$0], ENAME=[$1], X=[$3])\n"
        + "                        LogicalJoin(condition=[IS NOT DISTINCT FROM($1, $2)], joinType=[inner])\n"
        + "                          LogicalJoin(condition=[true], joinType=[inner])\n"
        + "                            LogicalProject(DEPTNO=[$0])\n"
        + "                              LogicalTableScan(table=[[scott, DEPT]])\n"
        + "                            LogicalProject(ENAME=[$1])\n"
        + "                              LogicalTableScan(table=[[scott, EMP]])\n"
        + "                          LogicalProject(ENAME=[$0], X=[CASE(IS NOT NULL($2), $2, 0)])\n"
        + "                            LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $1)], joinType=[left])\n"
        + "                              LogicalProject(ENAME=[$1])\n"
        + "                                LogicalTableScan(table=[[scott, EMP]])\n"
        + "                              LogicalAggregate(group=[{0}], X=[COUNT()])\n"
        + "                                LogicalProject(ENAME=[$0])\n"
        + "                                  LogicalFilter(condition=[IS NOT NULL($0)])\n"
        + "                                    LogicalTableScan(table=[[scott, BONUS]])\n"
        + "                      LogicalProject(DEPTNO=[$0], ENAME=[$1], X=[$3])\n"
        + "                        LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $2)], joinType=[inner])\n"
        + "                          LogicalJoin(condition=[true], joinType=[inner])\n"
        + "                            LogicalProject(DEPTNO=[$0])\n"
        + "                              LogicalTableScan(table=[[scott, DEPT]])\n"
        + "                            LogicalProject(ENAME=[$1])\n"
        + "                              LogicalTableScan(table=[[scott, EMP]])\n"
        + "                          LogicalProject(DEPTNO=[$0], X=[CASE(IS NOT NULL($2), $2, 0)])\n"
        + "                            LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $1)], joinType=[left])\n"
        + "                              LogicalProject(DEPTNO=[$0])\n"
        + "                                LogicalTableScan(table=[[scott, DEPT]])\n"
        + "                              LogicalAggregate(group=[{0}], X=[COUNT()])\n"
        + "                                LogicalProject(DEPTNO=[$7])\n"
        + "                                  LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "                                    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7272">[CALCITE-7272]
   * Subqueries cannot be decorrelated if have set op</a>. */
  @Test void testCorrelationInSetOp3() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = "SELECT d.dname\n"
        + "FROM dept d\n"
        + "WHERE EXISTS (\n"
        + "  SELECT 1\n"
        + "  FROM emp e\n"
        + "  WHERE (\n"
        + "    SELECT SUM(x)\n"
        + "    FROM (\n"
        + "        SELECT COUNT(*) as x\n"
        + "        FROM bonus b\n"
        + "        WHERE b.ename = e.ename\n"
        + "        UNION ALL\n"
        + "        SELECT COUNT(*) as x\n"
        + "        FROM emp e2\n"
        + "        WHERE e2.deptno = d.deptno\n"
        + "    ) t\n"
        + "  ) > 5)";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(DNAME=[$1])\n"
        + "  LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
        + "    LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0}])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "      LogicalAggregate(group=[{0}])\n"
        + "        LogicalProject(i=[true])\n"
        + "          LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "            LogicalFilter(condition=[>($8, 5)])\n"
        + "              LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{1}])\n"
        + "                LogicalTableScan(table=[[scott, EMP]])\n"
        + "                LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])\n"
        + "                  LogicalUnion(all=[true])\n"
        + "                    LogicalAggregate(group=[{}], X=[COUNT()])\n"
        + "                      LogicalFilter(condition=[=($0, $cor0.ENAME)])\n"
        + "                        LogicalTableScan(table=[[scott, BONUS]])\n"
        + "                    LogicalAggregate(group=[{}], X=[COUNT()])\n"
        + "                      LogicalFilter(condition=[=($7, $cor1.DEPTNO)])\n"
        + "                        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    // Verify plan
    final String planAfter = ""
        + "LogicalProject(DNAME=[$1])\n"
        + "  LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $3)], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalProject(DEPTNO0=[$0], $f1=[true])\n"
        + "      LogicalAggregate(group=[{0}])\n"
        + "        LogicalProject(DEPTNO0=[$9])\n"
        + "          LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], ENAME0=[$8], DEPTNO0=[CAST($9):TINYINT], EXPR$0=[CAST($10):BIGINT])\n"
        + "            LogicalJoin(condition=[IS NOT DISTINCT FROM($1, $8)], joinType=[inner])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n"
        + "              LogicalFilter(condition=[>($2, 5)])\n"
        + "                LogicalAggregate(group=[{0, 1}], EXPR$0=[SUM($2)])\n"
        + "                  LogicalProject(ENAME=[$0], DEPTNO=[$1], X=[$2])\n"
        + "                    LogicalUnion(all=[true])\n"
        + "                      LogicalProject(ENAME=[$0], DEPTNO=[$1], X=[$3])\n"
        + "                        LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $2)], joinType=[inner])\n"
        + "                          LogicalJoin(condition=[true], joinType=[inner])\n"
        + "                            LogicalProject(ENAME=[$1])\n"
        + "                              LogicalTableScan(table=[[scott, EMP]])\n"
        + "                            LogicalProject(DEPTNO=[$0])\n"
        + "                              LogicalTableScan(table=[[scott, DEPT]])\n"
        + "                          LogicalProject(ENAME=[$0], X=[CASE(IS NOT NULL($2), $2, 0)])\n"
        + "                            LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $1)], joinType=[left])\n"
        + "                              LogicalProject(ENAME=[$1])\n"
        + "                                LogicalTableScan(table=[[scott, EMP]])\n"
        + "                              LogicalAggregate(group=[{0}], X=[COUNT()])\n"
        + "                                LogicalProject(ENAME=[$0])\n"
        + "                                  LogicalFilter(condition=[IS NOT NULL($0)])\n"
        + "                                    LogicalTableScan(table=[[scott, BONUS]])\n"
        + "                      LogicalProject(ENAME=[$0], DEPTNO=[$1], X=[$3])\n"
        + "                        LogicalJoin(condition=[IS NOT DISTINCT FROM($1, $2)], joinType=[inner])\n"
        + "                          LogicalJoin(condition=[true], joinType=[inner])\n"
        + "                            LogicalProject(ENAME=[$1])\n"
        + "                              LogicalTableScan(table=[[scott, EMP]])\n"
        + "                            LogicalProject(DEPTNO=[$0])\n"
        + "                              LogicalTableScan(table=[[scott, DEPT]])\n"
        + "                          LogicalProject(DEPTNO=[$0], X=[CASE(IS NOT NULL($2), $2, 0)])\n"
        + "                            LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $1)], joinType=[left])\n"
        + "                              LogicalProject(DEPTNO=[$0])\n"
        + "                                LogicalTableScan(table=[[scott, DEPT]])\n"
        + "                              LogicalAggregate(group=[{0}], X=[COUNT()])\n"
        + "                                LogicalProject(DEPTNO=[$7])\n"
        + "                                  LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "                                    LogicalTableScan(table=[[scott, EMP]])\n";
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

  @Test void testDecorrelateCorrelatedOrderByLimitToRowNumber() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = ""
        + "SELECT dname FROM  dept WHERE 2000 > (\n"
        + "SELECT emp.sal FROM  emp where dept.deptno = emp.deptno\n"
        + "ORDER BY year(hiredate), emp.sal limit 1)";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(DNAME=[$1])\n"
        + "  LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
        + "    LogicalFilter(condition=[>(2000.00, CAST($3):DECIMAL(12, 2))])\n"
        + "      LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
        + "        LogicalTableScan(table=[[scott, DEPT]])\n"
        + "        LogicalProject(SAL=[$0])\n"
        + "          LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC], fetch=[1])\n"
        + "            LogicalProject(SAL=[$5], EXPR$1=[EXTRACT(FLAG(YEAR), $4)])\n"
        + "              LogicalFilter(condition=[=($cor0.DEPTNO, $7)])\n"
        + "                LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    // Verify plan
    final String planAfter = ""
        + "LogicalProject(DNAME=[$1])\n"
        + "  LogicalJoin(condition=[=($0, $4)], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalFilter(condition=[>(2000.00, CAST($0):DECIMAL(12, 2))])\n"
        + "      LogicalProject(SAL=[$0], DEPTNO=[$2])\n"
        + "        LogicalFilter(condition=[<=($3, 1)])\n"
        + "          LogicalProject(SAL=[$5], EXPR$1=[EXTRACT(FLAG(YEAR), $4)], DEPTNO=[$7], rn=[ROW_NUMBER() OVER (PARTITION BY $7 ORDER BY EXTRACT(FLAG(YEAR), $4) NULLS LAST, $5 NULLS LAST)])\n"
        + "            LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(after, hasTree(planAfter));
  }

  @Test void testDecorrelateCorrelatedOrderByLimitToRowNumber2() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = ""
        + "SELECT *\n"
        + "FROM dept d\n"
        + "WHERE d.deptno IN (\n"
        + "  SELECT e.deptno\n"
        + "  FROM emp e\n"
        + "  WHERE d.deptno = e.deptno\n"
        + "  LIMIT 10\n"
        + "  OFFSET 2\n"
        + ")\n"
        + "LIMIT 2\n"
        + "OFFSET 1";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalSort(offset=[1], fetch=[2])\n"
        + "  LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
        + "    LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
        + "      LogicalFilter(condition=[=($0, $3)])\n"
        + "        LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "          LogicalTableScan(table=[[scott, DEPT]])\n"
        + "          LogicalAggregate(group=[{0}])\n"
        + "            LogicalSort(offset=[2], fetch=[10])\n"
        + "              LogicalProject(DEPTNO=[$7])\n"
        + "                LogicalFilter(condition=[=($cor0.DEPTNO, $7)])\n"
        + "                  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    // Verify plan
    final String planAfter = ""
        + "LogicalSort(offset=[1], fetch=[2])\n"
        + "  LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
        + "    LogicalJoin(condition=[=($0, $4)], joinType=[inner])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "      LogicalFilter(condition=[=($1, $0)])\n"
        + "        LogicalAggregate(group=[{0, 1}])\n"
        + "          LogicalProject(DEPTNO=[$0], DEPTNO1=[$1])\n"
        + "            LogicalFilter(condition=[AND(>($2, 2), <=($2, +(2, 10)))])\n"
        + "              LogicalProject(DEPTNO=[$7], DEPTNO1=[$7], rn=[ROW_NUMBER() OVER (PARTITION BY $7)])\n"
        + "                LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "                  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(after, hasTree(planAfter));
  }

  @Test void testDecorrelateCorrelatedOrderByLimitToRowNumber3() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = ""
        + "SELECT deptno FROM dept WHERE 1000.00 >\n"
        + "(SELECT sal FROM emp WHERE dept.deptno = emp.deptno\n"
        + "order by emp.sal limit 1 offset 10)";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(DEPTNO=[$0])\n"
        + "  LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
        + "    LogicalFilter(condition=[>(1000.00, $3)])\n"
        + "      LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
        + "        LogicalTableScan(table=[[scott, DEPT]])\n"
        + "        LogicalSort(sort0=[$0], dir0=[ASC], offset=[10], fetch=[1])\n"
        + "          LogicalProject(SAL=[$5])\n"
        + "            LogicalFilter(condition=[=($cor0.DEPTNO, $7)])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    // Verify plan
    final String planAfter = ""
        + "LogicalProject(DEPTNO=[$0])\n"
        + "  LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2], SAL=[$3], DEPTNO0=[$4], rn=[CAST($5):BIGINT])\n"
        + "    LogicalJoin(condition=[=($0, $4)], joinType=[inner])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "      LogicalFilter(condition=[>(1000.00, $0)])\n"
        + "        LogicalFilter(condition=[AND(>($2, 10), <=($2, +(10, 1)))])\n"
        + "          LogicalProject(SAL=[$5], DEPTNO=[$7], rn=[ROW_NUMBER() OVER (PARTITION BY $7 ORDER BY $5 NULLS LAST)])\n"
        + "            LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7257">[CALCITE-7257]
   * Subqueries cannot be decorrelated if join condition contains RexFieldAccess</a>. */
  @Test void testJoinConditionContainsRexFieldAccess() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = ""
        + "SELECT E1.* \n"
        + "FROM\n"
        + "  EMP E1\n"
        + "WHERE\n"
        + "  E1.EMPNO = (\n"
        + "    SELECT D1.DEPTNO FROM DEPT D1\n"
        + "    WHERE E1.ENAME IN (SELECT B1.ENAME FROM BONUS B1))";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "    LogicalFilter(condition=[=($0, CAST($8):SMALLINT)])\n"
        + "      LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{1}])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + "        LogicalAggregate(group=[{}], agg#0=[SINGLE_VALUE($0)])\n"
        + "          LogicalProject(DEPTNO=[$0])\n"
        + "            LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
        + "              LogicalJoin(condition=[=($cor0.ENAME, $3)], joinType=[inner])\n"
        + "                LogicalTableScan(table=[[scott, DEPT]])\n"
        + "                LogicalProject(ENAME=[$0])\n"
        + "                  LogicalTableScan(table=[[scott, BONUS]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    final String planAfter = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], ENAME0=[$8], $f1=[CAST($9):TINYINT])\n"
        + "    LogicalJoin(condition=[AND(=($1, $8), =($0, CAST($9):SMALLINT))], joinType=[inner])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalAggregate(group=[{0}], agg#0=[SINGLE_VALUE($1)])\n"
        + "        LogicalProject(ENAME=[$3], DEPTNO=[$0])\n"
        + "          LogicalJoin(condition=[=($3, $4)], joinType=[inner])\n"
        + "            LogicalJoin(condition=[true], joinType=[inner])\n"
        + "              LogicalTableScan(table=[[scott, DEPT]])\n"
        + "              LogicalProject(ENAME=[$1])\n"
        + "                LogicalTableScan(table=[[scott, EMP]])\n"
        + "            LogicalProject(ENAME=[$0])\n"
        + "              LogicalTableScan(table=[[scott, BONUS]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7379">[CALCITE-7379]
   * LHS correlated variables are shadowed by nullable RHS outputs in LEFT JOIN</a>. */
  @Test void testDecorrelateLeftJoinCorVarShadowing() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = ""
        + "WITH\n"
        + "  t1(a, b, c) AS (VALUES (2, 2, 2), (3, 3, 3), (4, 4, 4)),\n"
        + "  t2(a, b, c) AS (VALUES (1, 1, 1), (3, 3, 3), (4, 4, 4)),\n"
        + "  t3(a, b, c) AS (VALUES (1, 1, 1), (2, 2, 2), (4, 4, 4))\n"
        + "SELECT * FROM t1 WHERE EXISTS (\n"
        + "SELECT * FROM t2\n"
        + "LEFT JOIN\n"
        + "(SELECT * FROM t3 WHERE t3.a = t1.a) foo\n"
        + "ON t2.a = foo.a)";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(A=[$0], B=[$1], C=[$2])\n"
        + "  LogicalProject(EXPR$0=[$0], EXPR$1=[$1], EXPR$2=[$2])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "      LogicalValues(tuples=[[{ 2, 2, 2 }, { 3, 3, 3 }, { 4, 4, 4 }]])\n"
        + "      LogicalAggregate(group=[{0}])\n"
        + "        LogicalProject(i=[true])\n"
        + "          LogicalJoin(condition=[=($0, $3)], joinType=[left])\n"
        + "            LogicalValues(tuples=[[{ 1, 1, 1 }, { 3, 3, 3 }, { 4, 4, 4 }]])\n"
        + "            LogicalProject(A=[$0], B=[$1], C=[$2])\n"
        + "              LogicalFilter(condition=[=($0, $cor0.A)])\n"
        + "                LogicalValues(tuples=[[{ 1, 1, 1 }, { 2, 2, 2 }, { 4, 4, 4 }]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    // The plan before fix:
    //
    // LogicalProject(A=[$0], B=[$1], C=[$2])
    //  LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $3)], joinType=[inner])
    //    LogicalValues(tuples=[[{ 2, 2, 2 }, { 3, 3, 3 }, { 4, 4, 4 }]])
    //    LogicalProject(EXPR$00=[$0], $f1=[true])
    //      LogicalAggregate(group=[{0}])
    //        LogicalProject(EXPR$00=[$6])
    //          LogicalJoin(condition=[=($0, $3)], joinType=[left])
    //            LogicalValues(tuples=[[{ 1, 1, 1 }, { 3, 3, 3 }, { 4, 4, 4 }]])
    //            LogicalProject(A=[$0], B=[$1], C=[$2], EXPR$0=[$0])
    //              LogicalValues(tuples=[[{ 1, 1, 1 }, { 2, 2, 2 }, { 4, 4, 4 }]])
    final String planAfter = ""
        + "LogicalProject(A=[$0], B=[$1], C=[$2])\n"
        + "  LogicalJoin(condition=[=($0, $3)], joinType=[inner])\n"
        + "    LogicalValues(tuples=[[{ 2, 2, 2 }, { 3, 3, 3 }, { 4, 4, 4 }]])\n"
        + "    LogicalProject(EXPR$00=[$0], $f1=[true])\n"
        + "      LogicalAggregate(group=[{0}])\n"
        + "        LogicalProject(EXPR$00=[$3])\n"
        + "          LogicalJoin(condition=[AND(=($0, $4), IS NOT DISTINCT FROM($3, $7))], joinType=[left])\n"
        + "            LogicalJoin(condition=[true], joinType=[inner])\n"
        + "              LogicalValues(tuples=[[{ 1, 1, 1 }, { 3, 3, 3 }, { 4, 4, 4 }]])\n"
        + "              LogicalProject(EXPR$0=[$0])\n"
        + "                LogicalValues(tuples=[[{ 2, 2, 2 }, { 3, 3, 3 }, { 4, 4, 4 }]])\n"
        + "            LogicalProject(A=[$0], B=[$1], C=[$2], EXPR$0=[$0])\n"
        + "              LogicalValues(tuples=[[{ 1, 1, 1 }, { 2, 2, 2 }, { 4, 4, 4 }]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7379">[CALCITE-7379]
   * LHS correlated variables are shadowed by nullable RHS outputs in LEFT JOIN</a>. */
  @Test void testDecorrelateFullJoinCorVarShadowing() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = ""
        + "SELECT\n"
        + "    d.dname,\n"
        + "    (SELECT COUNT(sub.empno)\n"
        + "        FROM (\n"
        + "            SELECT * FROM emp e2 WHERE e2.deptno = d.deptno\n"
        + "        ) sub\n"
        + "        FULL JOIN emp e\n"
        + "        ON sub.mgr = e.mgr\n"
        + "    ) as matched_subordinate_count\n"
        + "FROM dept d";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(DNAME=[$1], MATCHED_SUBORDINATE_COUNT=[$3])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalAggregate(group=[{}], EXPR$0=[COUNT($0)])\n"
        + "      LogicalProject(EMPNO=[$0])\n"
        + "        LogicalJoin(condition=[=($3, $11)], joinType=[full])\n"
        + "          LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "            LogicalFilter(condition=[=($7, $cor0.DEPTNO)])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n"
        + "          LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    final String planAfter = ""
        + "LogicalProject(DNAME=[$1], MATCHED_SUBORDINATE_COUNT=[$4])\n"
        + "  LogicalJoin(condition=[=($0, $3)], joinType=[left])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalProject(_cor_$cor0_0=[$0], EXPR$0=[CASE(IS NOT NULL($2), $2, 0)])\n"
        + "      LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $1)], joinType=[left])\n"
        + "        LogicalProject(DEPTNO=[$0])\n"
        + "          LogicalTableScan(table=[[scott, DEPT]])\n"
        + "        LogicalAggregate(group=[{0}], EXPR$0=[COUNT($1)])\n"
        + "          LogicalProject(_cor_$cor0_0=[COALESCE($8, $17)], EMPNO=[$0])\n"
        + "            LogicalJoin(condition=[AND(=($3, $12), IS NOT DISTINCT FROM($8, $17))], joinType=[full])\n"
        + "              LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPTNO8=[$7])\n"
        + "                LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "                  LogicalTableScan(table=[[scott, EMP]])\n"
        + "              LogicalJoin(condition=[true], joinType=[inner])\n"
        + "                LogicalTableScan(table=[[scott, EMP]])\n"
        + "                LogicalProject(DEPTNO=[$0])\n"
        + "                  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-5390">[CALCITE-5390]
   * RelDecorrelator throws NullPointerException</a>. */
  @Test void testCorrelationLexicalScoping() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = ""
        + "select deptno,\n"
        + "  (select min(1) from emp where empno > d.deptno) as i0,\n"
        + "  (select min(0) from emp where deptno = d.deptno and "
        + "ename = 'SMITH' and d.deptno > 0) as i1\n"
        + "from dept as d";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(DEPTNO=[$0], I0=[$3], I1=[$4])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "      LogicalAggregate(group=[{}], EXPR$0=[MIN($0)])\n"
        + "        LogicalProject($f0=[1])\n"
        + "          LogicalFilter(condition=[>($0, CAST($cor0.DEPTNO):SMALLINT NOT NULL)])\n"
        + "            LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalAggregate(group=[{}], EXPR$0=[MIN($0)])\n"
        + "      LogicalProject($f0=[0])\n"
        + "        LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), =($1, 'SMITH'), >(CAST($cor0.DEPTNO):INTEGER NOT NULL, 0))])\n"
        + "          LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    final String planAfter = ""
        + "LogicalProject(DEPTNO=[$0], I0=[$3], I1=[$8])\n"
        + "  LogicalJoin(condition=[AND(=($0, $6), =($5, $7))], joinType=[left])\n"
        + "    LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2], EXPR$0=[$5], DEPTNO0=[$0], $f5=[>(CAST($0):INTEGER NOT NULL, 0)])\n"
        + "      LogicalJoin(condition=[=($3, $4)], joinType=[left])\n"
        + "        LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2], DEPTNO0=[CAST($0):SMALLINT NOT NULL])\n"
        + "          LogicalTableScan(table=[[scott, DEPT]])\n"
        + "        LogicalAggregate(group=[{0}], EXPR$0=[MIN($1)])\n"
        + "          LogicalProject(DEPTNO0=[$8], $f0=[1])\n"
        + "            LogicalJoin(condition=[>($0, $8)], joinType=[inner])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n"
        + "              LogicalAggregate(group=[{0}])\n"
        + "                LogicalProject(DEPTNO0=[CAST($0):SMALLINT NOT NULL])\n"
        + "                  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalAggregate(group=[{0, 1}], EXPR$0=[MIN($2)])\n"
        + "      LogicalProject(DEPTNO0=[$8], $f5=[$9], $f0=[0])\n"
        + "        LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
        + "          LogicalFilter(condition=[=($1, 'SMITH')])\n"
        + "            LogicalTableScan(table=[[scott, EMP]])\n"
        + "          LogicalFilter(condition=[$1])\n"
        + "            LogicalProject(DEPTNO=[$0], $f5=[>(CAST($0):INTEGER NOT NULL, 0)])\n"
        + "              LogicalJoin(condition=[=($3, $4)], joinType=[left])\n"
        + "                LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2], DEPTNO0=[CAST($0):SMALLINT NOT NULL])\n"
        + "                  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "                LogicalAggregate(group=[{0}], EXPR$0=[MIN($1)])\n"
        + "                  LogicalProject(DEPTNO0=[$8], $f0=[1])\n"
        + "                    LogicalJoin(condition=[>($0, $8)], joinType=[inner])\n"
        + "                      LogicalTableScan(table=[[scott, EMP]])\n"
        + "                      LogicalAggregate(group=[{0}])\n"
        + "                        LogicalProject(DEPTNO0=[CAST($0):SMALLINT NOT NULL])\n"
        + "                          LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7320">[CALCITE-7320]
   * AggregateProjectMergeRule throws AssertionError when Project maps multiple grouping keys
   * to the same field</a>. */
  @Test void test7320() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = ""
        + "SELECT deptno,\n"
        + "       (SELECT SUM(cnt)\n"
        + "        FROM (\n"
        + "          SELECT COUNT(*) AS cnt\n"
        + "          FROM emp\n"
        + "          WHERE emp.deptno = dept.deptno\n"
        + "          GROUP BY GROUPING SETS ((deptno), ())\n"
        + "))\n"
        + "FROM dept";
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
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(DEPTNO=[$0], EXPR$1=[$3])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])\n"
        + "      LogicalProject(CNT=[$1])\n"
        + "        LogicalAggregate(group=[{0}], groups=[[{0}, {}]], CNT=[COUNT()])\n"
        + "          LogicalProject(DEPTNO=[$7])\n"
        + "            LogicalFilter(condition=[=($7, $cor0.DEPTNO)])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder, RuleSets.ofList(Collections.emptyList()),
            RuleSets.ofList(Collections.emptyList()));
    final String planAfter = ""
        + "LogicalProject(DEPTNO=[$0], EXPR$1=[$4])\n"
        + "  LogicalJoin(condition=[=($0, $3)], joinType=[left])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalAggregate(group=[{0}], EXPR$0=[SUM($1)])\n"
        + "      LogicalProject(DEPTNO1=[$0], CNT=[CASE(IS NOT NULL($3), $3, 0)])\n"
        + "        LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $2)], joinType=[left])\n"
        + "          LogicalProject(DEPTNO=[$0])\n"
        + "            LogicalTableScan(table=[[scott, DEPT]])\n"
        + "          LogicalAggregate(group=[{0, 1}], groups=[[{0, 1}, {1}]], CNT=[COUNT()])\n"
        + "            LogicalProject(DEPTNO=[$7], DEPTNO1=[$7])\n"
        + "              LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "                LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(after, hasTree(planAfter));
  }
}
