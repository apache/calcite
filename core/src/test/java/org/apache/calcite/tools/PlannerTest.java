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
package org.apache.calcite.tools;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcRel;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.test.SqlTests;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.Util;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matcher;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.plan.RelOptRule.operand;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link Planner}.
 */
public class PlannerTest {
  private void checkParseAndConvert(String query,
      String queryFromParseTree, String expectedRelExpr) throws Exception {
    Planner planner = getPlanner(null);
    SqlNode parse = planner.parse(query);
    assertThat(Util.toLinux(parse.toString()), equalTo(queryFromParseTree));

    SqlNode validate = planner.validate(parse);
    RelNode rel = planner.rel(validate).project();
    assertThat(toString(rel), equalTo(expectedRelExpr));
  }

  @Test public void testParseAndConvert() throws Exception {
    checkParseAndConvert(
        "select * from \"emps\" where \"name\" like '%e%'",

        "SELECT *\n"
            + "FROM `emps`\n"
            + "WHERE `name` LIKE '%e%'",

        "LogicalProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "  LogicalFilter(condition=[LIKE($2, '%e%')])\n"
        + "    EnumerableTableScan(table=[[hr, emps]])\n");
  }

  @Test(expected = SqlParseException.class)
  public void testParseIdentiferMaxLengthWithDefault() throws Exception {
    Planner planner = getPlanner(null, SqlParser.configBuilder().build());
    planner.parse("select name as "
        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa from \"emps\"");
  }

  @Test
  public void testParseIdentiferMaxLengthWithIncreased() throws Exception {
    Planner planner = getPlanner(null,
        SqlParser.configBuilder().setIdentifierMaxLength(512).build());
    planner.parse("select name as "
        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa from \"emps\"");
  }

  /** Unit test that parses, validates and converts the query using
   * order by and offset. */
  @Test public void testParseAndConvertWithOrderByAndOffset() throws Exception {
    checkParseAndConvert(
        "select * from \"emps\" "
            + "order by \"emps\".\"deptno\" offset 10",

        "SELECT *\n"
            + "FROM `emps`\n"
            + "ORDER BY `emps`.`deptno`\n"
            + "OFFSET 10 ROWS",

        "LogicalSort(sort0=[$1], dir0=[ASC], offset=[10])\n"
        + "  LogicalProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "    EnumerableTableScan(table=[[hr, emps]])\n");
  }

  private String toString(RelNode rel) {
    return Util.toLinux(
        RelOptUtil.dumpPlan("", rel, SqlExplainFormat.TEXT,
            SqlExplainLevel.DIGEST_ATTRIBUTES));
  }

  @Test public void testParseFails() throws SqlParseException {
    Planner planner = getPlanner(null);
    try {
      SqlNode parse =
          planner.parse("select * * from \"emps\"");
      fail("expected error, got " + parse);
    } catch (SqlParseException e) {
      assertThat(e.getMessage(),
          containsString("Encountered \"*\" at line 1, column 10."));
    }
  }

  @Test public void testValidateFails() throws SqlParseException {
    Planner planner = getPlanner(null);
    SqlNode parse =
        planner.parse("select * from \"emps\" where \"Xname\" like '%e%'");
    assertThat(Util.toLinux(parse.toString()),
        equalTo("SELECT *\n"
            + "FROM `emps`\n"
            + "WHERE `Xname` LIKE '%e%'"));

    try {
      SqlNode validate = planner.validate(parse);
      fail("expected error, got " + validate);
    } catch (ValidationException e) {
      assertThat(Throwables.getStackTraceAsString(e),
          containsString("Column 'Xname' not found in any table"));
      // ok
    }
  }

  @Test public void testValidateUserDefinedAggregate() throws Exception {
    final SqlStdOperatorTable stdOpTab = SqlStdOperatorTable.instance();
    SqlOperatorTable opTab =
        ChainedSqlOperatorTable.of(stdOpTab,
            new ListSqlOperatorTable(
                ImmutableList.of(new MyCountAggFunction())));
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR))
        .operatorTable(opTab)
        .build();
    final Planner planner = Frameworks.getPlanner(config);
    SqlNode parse =
        planner.parse("select \"deptno\", my_count(\"empid\") from \"emps\"\n"
            + "group by \"deptno\"");
    assertThat(Util.toLinux(parse.toString()),
        equalTo("SELECT `deptno`, `MY_COUNT`(`empid`)\n"
            + "FROM `emps`\n"
            + "GROUP BY `deptno`"));

    // MY_COUNT is recognized as an aggregate function, and therefore it is OK
    // that its argument empid is not in the GROUP BY clause.
    SqlNode validate = planner.validate(parse);
    assertThat(validate, notNullValue());

    // The presence of an aggregate function in the SELECT clause causes it
    // to become an aggregate query. Non-aggregate expressions become illegal.
    planner.close();
    planner.reset();
    parse = planner.parse("select \"deptno\", count(1) from \"emps\"");
    try {
      validate = planner.validate(parse);
      fail("expected exception, got " + validate);
    } catch (ValidationException e) {
      assertThat(e.getCause().getCause().getMessage(),
          containsString("Expression 'deptno' is not being grouped"));
    }
  }

  private Planner getPlanner(List<RelTraitDef> traitDefs, Program... programs) {
    return getPlanner(traitDefs, SqlParser.Config.DEFAULT, programs);
  }

  private Planner getPlanner(List<RelTraitDef> traitDefs,
                             SqlParser.Config parserConfig,
                             Program... programs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR))
        .traitDefs(traitDefs)
        .programs(programs)
        .build();
    return Frameworks.getPlanner(config);
  }

  /** Tests that planner throws an error if you pass to
   * {@link Planner#rel(org.apache.calcite.sql.SqlNode)}
   * a {@link org.apache.calcite.sql.SqlNode} that has been parsed but not
   * validated. */
  @Test public void testConvertWithoutValidateFails() throws Exception {
    Planner planner = getPlanner(null);
    SqlNode parse = planner.parse("select * from \"emps\"");
    try {
      RelRoot rel = planner.rel(parse);
      fail("expected error, got " + rel);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          containsString(
              "cannot move from STATE_3_PARSED to STATE_4_VALIDATED"));
    }
  }

  /** Helper method for testing {@link RelMetadataQuery#getPulledUpPredicates}
   * metadata. */
  private void checkMetadataPredicates(String sql,
      String expectedPredicates) throws Exception {
    Planner planner = getPlanner(null);
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode rel = planner.rel(validate).project();
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(rel);
    final String buf = predicates.pulledUpPredicates.toString();
    assertThat(buf, equalTo(expectedPredicates));
  }

  /** Tests predicates that can be pulled-up from a UNION. */
  @Test public void testMetadataUnionPredicates() throws Exception {
    checkMetadataPredicates(
        "select * from \"emps\" where \"deptno\" < 10\n"
            + "union all\n"
            + "select * from \"emps\" where \"empid\" > 2",
        "[OR(<($1, 10), >($0, 2))]");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-443">[CALCITE-443]
   * getPredicates from a union is not correct</a>. */
  @Test public void testMetadataUnionPredicates2() throws Exception {
    checkMetadataPredicates(
        "select * from \"emps\" where \"deptno\" < 10\n"
            + "union all\n"
            + "select * from \"emps\"",
        "[]");
  }

  @Test public void testMetadataUnionPredicates3() throws Exception {
    checkMetadataPredicates(
        "select * from \"emps\" where \"deptno\" < 10\n"
            + "union all\n"
            + "select * from \"emps\" where \"deptno\" < 10 and \"empid\" > 1",
        "[<($1, 10)]");
  }

  @Test public void testMetadataUnionPredicates4() throws Exception {
    checkMetadataPredicates(
        "select * from \"emps\" where \"deptno\" < 10\n"
            + "union all\n"
            + "select * from \"emps\" where \"deptno\" < 10 or \"empid\" > 1",
        "[OR(<($1, 10), >($0, 1))]");
  }

  @Test public void testMetadataUnionPredicates5() throws Exception {
    final String sql = "select * from \"emps\" where \"deptno\" < 10\n"
        + "union all\n"
        + "select * from \"emps\" where \"deptno\" < 10 and false";
    checkMetadataPredicates(sql, "[<($1, 10)]");
  }

  /** Tests predicates that can be pulled-up from an Aggregate with
   * {@code GROUP BY ()}. This form of Aggregate can convert an empty relation
   * to a single-row relation, so it is not valid to pull up the predicate
   * {@code false}. */
  @Test public void testMetadataAggregatePredicates() throws Exception {
    checkMetadataPredicates("select count(*) from \"emps\" where false",
        "[]");
  }

  /** Tests predicates that can be pulled-up from an Aggregate with a non-empty
   * group key. The {@code false} predicate effectively means that the relation
   * is empty, because no row can satisfy {@code false}. */
  @Test public void testMetadataAggregatePredicates2() throws Exception {
    final String sql = "select \"deptno\", count(\"deptno\")\n"
        + "from \"emps\" where false\n"
        + "group by \"deptno\"";
    checkMetadataPredicates(sql, "[false]");
  }

  @Test public void testMetadataAggregatePredicates3() throws Exception {
    final String sql = "select \"deptno\", count(\"deptno\")\n"
        + "from \"emps\" where \"deptno\" > 10\n"
        + "group by \"deptno\"";
    checkMetadataPredicates(sql, "[>($0, 10)]");
  }

  /** Unit test that parses, validates, converts and plans. */
  @Test public void testPlan() throws Exception {
    Program program =
        Programs.ofRules(
            FilterMergeRule.INSTANCE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE);
    Planner planner = getPlanner(null, program);
    SqlNode parse = planner.parse("select * from \"emps\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).project();
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform),
        equalTo(
            "EnumerableProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
            + "  EnumerableTableScan(table=[[hr, emps]])\n"));
  }

  /** Unit test that parses, validates, converts and plans. */
  @Test public void trimEmptyUnion2() throws Exception {
    checkUnionPruning("values(1) union all select * from (values(2)) where false",
        "EnumerableValues(type=[RecordType(INTEGER EXPR$0)], tuples=[[{ 1 }]])\n");

    checkUnionPruning("select * from (values(2)) where false union all values(1)",
        "EnumerableValues(type=[RecordType(INTEGER EXPR$0)], tuples=[[{ 1 }]])\n");
  }

  @Test public void trimEmptyUnion31() throws Exception {
    emptyUnions31();
  }

  @Test public void trimEmptyUnion31withUnionMerge() throws Exception {
    emptyUnions31(UnionMergeRule.INSTANCE);
  }

  private void emptyUnions31(UnionMergeRule... extraRules)
      throws SqlParseException, ValidationException, RelConversionException {
    String plan = "EnumerableValues(type=[RecordType(INTEGER EXPR$0)], tuples=[[{ 1 }]])\n";
    checkUnionPruning("values(1)"
            + " union all select * from (values(2)) where false"
            + " union all select * from (values(3)) where false",
        plan, extraRules);

    checkUnionPruning("select * from (values(2)) where false"
            + " union all values(1)"
            + " union all select * from (values(3)) where false",
        plan, extraRules);

    checkUnionPruning("select * from (values(2)) where false"
            + " union all select * from (values(3)) where false"
            + " union all values(1)",
        plan, extraRules);
  }

  @Ignore("[CALCITE-2773] java.lang.AssertionError: rel"
      + " [rel#69:EnumerableUnion.ENUMERABLE.[](input#0=RelSubset#78,input#1=RelSubset#71,all=true)]"
      + " has lower cost {4.0 rows, 4.0 cpu, 0.0 io} than best cost {5.0 rows, 5.0 cpu, 0.0 io}"
      + " of subset [rel#67:Subset#6.ENUMERABLE.[]]")
  @Test public void trimEmptyUnion32() throws Exception {
    emptyUnions32();
  }

  @Test public void trimEmptyUnion32withUnionMerge() throws Exception {
    emptyUnions32(UnionMergeRule.INSTANCE);
  }

  private void emptyUnions32(UnionMergeRule... extraRules)
      throws SqlParseException, ValidationException, RelConversionException {
    String plan = "EnumerableUnion(all=[true])\n"
        + "  EnumerableValues(type=[RecordType(INTEGER EXPR$0)], tuples=[[{ 1 }]])\n"
        + "  EnumerableValues(type=[RecordType(INTEGER EXPR$0)], tuples=[[{ 2 }]])\n";

    checkUnionPruning("values(1)"
            + " union all values(2)"
            + " union all select * from (values(3)) where false",
        plan, extraRules);

    checkUnionPruning("values(1)"
            + " union all select * from (values(3)) where false"
            + " union all values(2)",
        plan, extraRules);

    checkUnionPruning("select * from (values(2)) where false"
            + " union all values(1)"
            + " union all values(2)",
        plan, extraRules);
  }

  private void checkUnionPruning(String sql, String plan, RelOptRule... extraRules)
      throws SqlParseException, ValidationException, RelConversionException {
    ImmutableList.Builder<RelOptRule> rules = ImmutableList.<RelOptRule>builder().add(
        PruneEmptyRules.UNION_INSTANCE,
        ValuesReduceRule.PROJECT_FILTER_INSTANCE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_FILTER_RULE,
        EnumerableRules.ENUMERABLE_VALUES_RULE,
        EnumerableRules.ENUMERABLE_UNION_RULE);
    rules.add(extraRules);
    Program program = Programs.ofRules(rules.build());
    Planner planner = getPlanner(null, program);
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).project();
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat("Empty values should be removed from " + sql,
        toString(transform), equalTo(plan));
  }

  @Ignore("[CALCITE-2773] java.lang.AssertionError: rel"
      + " [rel#17:EnumerableUnion.ENUMERABLE.[](input#0=RelSubset#26,input#1=RelSubset#19,all=true)]"
      + " has lower cost {4.0 rows, 4.0 cpu, 0.0 io}"
      + " than best cost {5.0 rows, 5.0 cpu, 0.0 io} of subset [rel#15:Subset#5.ENUMERABLE.[]]")
  @Test public void trimEmptyUnion32viaRelBuidler() throws Exception {
    RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());

    // This somehow blows up (see trimEmptyUnion32, the second case)
    // (values(1) union all select * from (values(3)) where false)
    // union all values(2)

    // Non-trivial filter is important for the test to fail
    RelNode relNode = relBuilder
        .values(new String[]{"x"}, "1")
        .values(new String[]{"x"}, "3")
        .filter(relBuilder.equals(relBuilder.field("x"), relBuilder.literal("30")))
        .union(true)
        .values(new String[]{"x"}, "2")
        .union(true)
        .build();

    RelOptPlanner planner = relNode.getCluster().getPlanner();
    RuleSet ruleSet =
        RuleSets.ofList(
            PruneEmptyRules.UNION_INSTANCE,
            ValuesReduceRule.FILTER_INSTANCE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_VALUES_RULE,
            EnumerableRules.ENUMERABLE_UNION_RULE);
    Program program = Programs.of(ruleSet);

    RelTraitSet toTraits = relNode.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);

    RelNode output = program.run(planner, relNode, toTraits,
        ImmutableList.of(), ImmutableList.of());

    // Expected outcomes are:
    // 1) relation is optimized to simple VALUES
    // 2) the number of rule invocations is reasonable
    // 3) planner does not throw OutOfMemoryError
    assertThat("empty union should be pruned out of " + toString(relNode),
        Util.toLinux(toString(output)),
        equalTo("EnumerableUnion(all=[true])\n"
            + "  EnumerableValues(type=[RecordType(INTEGER EXPR$0)], tuples=[[{ 1 }]])\n"
            + "  EnumerableValues(type=[RecordType(INTEGER EXPR$0)], tuples=[[{ 2 }]])\n"));
  }

  /** Unit test that parses, validates, converts and
   * plans for query using order by */
  @Test public void testSortPlan() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            SortRemoveRule.INSTANCE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE);
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    SqlNode parse = planner.parse(
        "select * from \"emps\" "
            + "order by \"emps\".\"deptno\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).project();
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform),
        equalTo("EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "  EnumerableProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
            + "    EnumerableTableScan(table=[[hr, emps]])\n"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2554">[CALCITE-2554]
   * Enrich EnumerableHashJoin operator with order preserving information</a>.
   *
   * Since left input to the join is sorted, and this join preserves order, there shouldn't be
   * any sort operator above the join.
   */
  @Test public void testRedundantSortOnJoinPlan() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            SortRemoveRule.INSTANCE,
            SortJoinTransposeRule.INSTANCE,
            SortProjectTransposeRule.INSTANCE,
            EnumerableRules.ENUMERABLE_LIMIT_RULE,
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE);
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    SqlNode parse = planner.parse(
        "select e.\"deptno\" from \"emps\" e "
            + "left outer join \"depts\" d "
            + " on e.\"deptno\" = d.\"deptno\" "
            + "order by e.\"deptno\" "
            + "limit 10");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).rel;
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE).simplify();
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform),
        equalTo("EnumerableProject(deptno=[$1])\n"
        + "  EnumerableLimit(fetch=[10])\n"
        + "    EnumerableHashJoin(condition=[=($1, $5)], joinType=[left])\n"
        + "      EnumerableLimit(fetch=[10])\n"
        + "        EnumerableSort(sort0=[$1], dir0=[ASC])\n"
        + "          EnumerableTableScan(table=[[hr, emps]])\n"
        + "      EnumerableProject(deptno=[$0], name=[$1], employees=[$2], x=[$3.x], y=[$3.y])\n"
        + "        EnumerableTableScan(table=[[hr, depts]])\n"));
  }

  /** Unit test that parses, validates, converts and
   * plans for query using two duplicate order by.
   * The duplicate order by should be removed by SortRemoveRule. */
  @Test public void testDuplicateSortPlan() throws Exception {
    runDuplicateSortCheck(
        "select empid from ( "
        + "select * "
        + "from emps "
        + "order by emps.deptno) "
        + "order by deptno",
        "EnumerableProject(empid=[$0], deptno=[$1])\n"
        + "  EnumerableSort(sort0=[$1], dir0=[ASC])\n"
        + "    EnumerableProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "      EnumerableTableScan(table=[[hr, emps]])\n");
  }

  /** Unit test that parses, validates, converts and
   * plans for query using two duplicate order by.
   * The duplicate order by should be removed by SortRemoveRule*/
  @Test public void testDuplicateSortPlanWithExpr() throws Exception {
    runDuplicateSortCheck("select empid+deptno from ( "
        + "select empid, deptno "
        + "from emps "
        + "order by emps.deptno) "
        + "order by deptno",
        "EnumerableProject(EXPR$0=[+($0, $1)], deptno=[$1])\n"
        + "  EnumerableSort(sort0=[$1], dir0=[ASC])\n"
        + "    EnumerableProject(empid=[$0], deptno=[$1])\n"
        + "      EnumerableTableScan(table=[[hr, emps]])\n");
  }

  @Test public void testTwoSortDontRemove() throws Exception {
    runDuplicateSortCheck("select empid+deptno from ( "
        + "select empid, deptno "
        + "from emps "
        + "order by empid) "
        + "order by deptno",
        "EnumerableSort(sort0=[$1], dir0=[ASC])\n"
        + "  EnumerableProject(EXPR$0=[+($0, $1)], deptno=[$1])\n"
        + "    EnumerableSort(sort0=[$0], dir0=[ASC])\n"
        + "      EnumerableProject(empid=[$0], deptno=[$1])\n"
        + "        EnumerableTableScan(table=[[hr, emps]])\n");
  }

  /** Tests that outer order by is not removed since window function
   * might reorder the rows in-between */
  @Ignore("Node [rel#22:Subset#3.ENUMERABLE.[2]] could not be implemented; planner state:\n"
      + "\n"
      + "Root: rel#22:Subset#3.ENUMERABLE.[2]")
  @Test public void testDuplicateSortPlanWithOver() throws Exception {
    runDuplicateSortCheck("select emp_cnt, empid+deptno from ( "
        + "select empid, deptno, count(*) over (partition by deptno) emp_cnt from ( "
        + "  select empid, deptno "
        + "    from emps "
        + "   order by emps.deptno) "
        + ")"
        + "order by deptno",
        "EnumerableProject(EXPR$0=[$0])\n"
        + "  EnumerableSort(sort0=[$1], dir0=[ASC])\n"
        + "    EnumerableProject(EXPR$0=[+($0, $1)], deptno=[$1])\n"
        + "      EnumerableProject(empid=[$0], deptno=[$1], $2=[$2])\n"
        + "        EnumerableWindow(window#0=[window(partition {1} order by [] range between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING aggs [COUNT()])])\n"
        + "          EnumerableSort(sort0=[$1], dir0=[ASC])\n"
        + "            EnumerableProject(empid=[$0], deptno=[$1])\n"
        + "              EnumerableTableScan(table=[[hr, emps]])\n");
  }

  @Test public void testDuplicateSortPlanWithRemovedOver() throws Exception {
    runDuplicateSortCheck("select empid+deptno from ( "
        + "select empid, deptno, count(*) over (partition by deptno) emp_cnt from ( "
        + "  select empid, deptno "
        + "    from emps "
        + "   order by emps.deptno) "
        + ")"
        + "order by deptno",
        "EnumerableProject(EXPR$0=[+($0, $1)], deptno=[$1])\n"
        + "  EnumerableSort(sort0=[$1], dir0=[ASC])\n"
        + "    EnumerableProject(empid=[$0], deptno=[$1])\n"
        + "      EnumerableTableScan(table=[[hr, emps]])\n");
  }

  // If proper "SqlParseException, ValidationException, RelConversionException"
  // is used, then checkstyle fails with
  // "Redundant throws: 'ValidationException' listed more then one time"
  // "Redundant throws: 'RelConversionException' listed more then one time"
  private void runDuplicateSortCheck(String sql, String plan) throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            SortRemoveRule.INSTANCE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_WINDOW_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            ProjectToWindowRule.PROJECT);
    Planner planner = getPlanner(null,
        SqlParser.configBuilder().setLex(Lex.JAVA).build(),
        Programs.of(ruleSet));
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).rel;
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    if (traitSet.getTrait(RelCollationTraitDef.INSTANCE) == null) {
      // SortRemoveRule can only work if collation trait is enabled.
      return;
    }
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), equalTo(plan));
  }

  /** Unit test that parses, validates, converts and
   * plans for query using two duplicate order by.*/
  @Test public void testDuplicateSortPlanWORemoveSortRule() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE);
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    SqlNode parse = planner.parse(
        "select \"empid\" from ( "
            + "select * "
            + "from \"emps\" "
            + "order by \"emps\".\"deptno\") "
            + "order by \"deptno\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).rel;
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform),
        equalTo("EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "  EnumerableProject(empid=[$0], deptno=[$1])\n"
            + "    EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "      EnumerableProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
            + "        EnumerableTableScan(table=[[hr, emps]])\n"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3029">[CALCITE-3029]
   * Java-oriented field type is wrongly forced to be NOT NULL after being converted to
   * SQL-oriented</a>. */
  @Test public void testInsertSourceRelTypeWithNullValues() throws Exception {
    Planner planner = getPlanner(null, Programs.standard());
    SqlNode parse = planner.parse(
        "insert into \"emps\" values(1, 1, null, 1, 1)");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).rel;
    RelDataType insertSourceType = convert.getInput(0).getRowType();
    String typeString = SqlTests.getTypeString(insertSourceType);
    assertEquals("RecordType(INTEGER NOT NULL empid, INTEGER NOT NULL deptno, VARCHAR name, "
            + "REAL NOT NULL salary, INTEGER commission) NOT NULL", typeString);
  }


  /** Unit test that parses, validates, converts and plans. Planner is
   * provided with a list of RelTraitDefs to register. */
  @Test public void testPlanWithExplicitTraitDefs() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            FilterMergeRule.INSTANCE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE);
    final List<RelTraitDef> traitDefs = new ArrayList<>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    Planner planner = getPlanner(traitDefs, Programs.of(ruleSet));

    SqlNode parse = planner.parse("select * from \"emps\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).project();
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform),
        equalTo(
            "EnumerableProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
            + "  EnumerableTableScan(table=[[hr, emps]])\n"));
  }

  /** Unit test that calls {@link Planner#transform} twice. */
  @Test public void testPlanTransformTwice() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            FilterMergeRule.INSTANCE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE);
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    SqlNode parse = planner.parse("select * from \"emps\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).project();
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    RelNode transform2 = planner.transform(0, traitSet, transform);
    assertThat(toString(transform2),
        equalTo(
            "EnumerableProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
            + "  EnumerableTableScan(table=[[hr, emps]])\n"));
  }

  /** Unit test that calls {@link Planner#transform} twice with
   *  rule name conflicts */
  @Test public void testPlanTransformWithRuleNameConflicts() throws Exception {
    // Create two dummy rules with identical rules.
    RelOptRule rule1 = new RelOptRule(
        operand(LogicalProject.class,
            operand(LogicalFilter.class, RelOptRule.any())),
        "MYRULE") {
      @Override public boolean matches(RelOptRuleCall call) {
        return false;
      }

      public void onMatch(RelOptRuleCall call) {
      }
    };

    RelOptRule rule2 = new RelOptRule(
        operand(LogicalFilter.class,
            operand(LogicalProject.class, RelOptRule.any())),
        "MYRULE") {

      @Override public boolean matches(RelOptRuleCall call) {
        return false;
      }

      public void onMatch(RelOptRuleCall call) {
      }
    };

    RuleSet ruleSet1 =
        RuleSets.ofList(
            rule1,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE);

    RuleSet ruleSet2 =
        RuleSets.ofList(
            rule2);

    Planner planner = getPlanner(null, Programs.of(ruleSet1),
        Programs.of(ruleSet2));
    SqlNode parse = planner.parse("select * from \"emps\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).rel;
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    RelNode transform2 = planner.transform(1, traitSet, transform);
    assertThat(toString(transform2),
        equalTo(
            "EnumerableProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
                + "  EnumerableTableScan(table=[[hr, emps]])\n"));
  }

  /** Tests that Hive dialect does not generate "AS". */
  @Test public void testHiveDialect() throws SqlParseException {
    Planner planner = getPlanner(null);
    SqlNode parse = planner.parse(
        "select * from (select * from \"emps\") as t\n"
            + "where \"name\" like '%e%'");
    final SqlDialect hiveDialect =
        SqlDialect.DatabaseProduct.HIVE.getDialect();
    assertThat(Util.toLinux(parse.toSqlString(hiveDialect).getSql()),
        equalTo("SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM emps) T\n"
            + "WHERE name LIKE '%e%'"));
  }

  /** Unit test that calls {@link Planner#transform} twice,
   * with different rule sets, with different conventions.
   *
   * <p>{@link org.apache.calcite.adapter.jdbc.JdbcConvention} is different
   * from the typical convention in that it is not a singleton. Switching to
   * a different instance causes problems unless planner state is wiped clean
   * between calls to {@link Planner#transform}. */
  @Test public void testPlanTransformWithDiffRuleSetAndConvention()
      throws Exception {
    Program program0 =
        Programs.ofRules(
            FilterMergeRule.INSTANCE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE);

    JdbcConvention out = new JdbcConvention(null, null, "myjdbc");
    Program program1 = Programs.ofRules(
        new MockJdbcProjectRule(out), new MockJdbcTableRule(out));

    Planner planner = getPlanner(null, program0, program1);
    SqlNode parse = planner.parse("select T1.\"name\" from \"emps\" as T1 ");

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).project();

    RelTraitSet traitSet0 = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);

    RelTraitSet traitSet1 = convert.getTraitSet()
        .replace(out);

    RelNode transform = planner.transform(0, traitSet0, convert);
    RelNode transform2 = planner.transform(1, traitSet1, transform);
    assertThat(toString(transform2),
        equalTo("JdbcProject(name=[$2])\n"
            + "  MockJdbcTableScan(table=[[hr, emps]])\n"));
  }

  /** Unit test that plans a query with a large number of joins. */
  @Test public void testPlanNWayJoin()
      throws Exception {
    // Here the times before and after enabling LoptOptimizeJoinRule.
    //
    // Note the jump between N=6 and N=7; LoptOptimizeJoinRule is disabled if
    // there are fewer than 6 joins (7 relations).
    //
    //       N    Before     After
    //         time (ms) time (ms)
    // ======= ========= =========
    //       5                 382
    //       6                 790
    //       7                  26
    //       9    6,000         39
    //      10    9,000         47
    //      11   19,000         67
    //      12   40,000         63
    //      13 OOM              96
    //      35 OOM           1,716
    //      60 OOM          12,230
    checkJoinNWay(5); // LoptOptimizeJoinRule disabled; takes about .4s
    checkJoinNWay(9); // LoptOptimizeJoinRule enabled; takes about 0.04s
    checkJoinNWay(35); // takes about 2s
    if (CalciteSystemProperty.TEST_SLOW.value()) {
      checkJoinNWay(60); // takes about 15s
    }
  }

  private void checkJoinNWay(int n) throws Exception {
    final StringBuilder buf = new StringBuilder();
    buf.append("select *");
    for (int i = 0; i < n; i++) {
      buf.append(i == 0 ? "\nfrom " : ",\n ")
          .append("\"depts\" as d").append(i);
    }
    for (int i = 1; i < n; i++) {
      buf.append(i == 1 ? "\nwhere" : "\nand").append(" d")
          .append(i).append(".\"deptno\" = d")
          .append(i - 1).append(".\"deptno\"");
    }
    Planner planner = getPlanner(null,
        Programs.heuristicJoinOrder(Programs.RULE_SET, false, 6));
    SqlNode parse = planner.parse(buf.toString());

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).project();
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform),
        containsString(
            "EnumerableHashJoin(condition=[=($0, $5)], joinType=[inner])"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-435">[CALCITE-435]
   * LoptOptimizeJoinRule incorrectly re-orders outer joins</a>.
   *
   * <p>Checks the
   * {@link org.apache.calcite.rel.rules.LoptOptimizeJoinRule} on a query with a
   * left outer join.
   *
   * <p>Specifically, tests that a relation (dependents) in an inner join
   * cannot be pushed into an outer join (emps left join depts).
   */
  @Test public void testHeuristicLeftJoin() throws Exception {
    final String sql = "select * from \"emps\" as e\n"
        + "left join \"depts\" as d on e.\"deptno\" = d.\"deptno\"\n"
        + "join \"dependents\" as p on e.\"empid\" = p.\"empid\"";
    final String expected = ""
        + "EnumerableProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4], deptno0=[$5], name0=[$6], employees=[$7], location=[$8], location9=[$9], empid0=[$10], name1=[$11])\n"
        + "  EnumerableProject(empid=[$2], deptno=[$3], name=[$4], salary=[$5], commission=[$6], deptno0=[$7], name0=[$8], employees=[$9], x=[$10], y=[$11], empid0=[$0], name1=[$1])\n"
        + "    EnumerableHashJoin(condition=[=($0, $2)], joinType=[inner])\n"
        + "      EnumerableTableScan(table=[[hr, dependents]])\n"
        + "      EnumerableHashJoin(condition=[=($1, $5)], joinType=[left])\n"
        + "        EnumerableTableScan(table=[[hr, emps]])\n"
        + "        EnumerableProject(deptno=[$0], name=[$1], employees=[$2], x=[$3.x], y=[$3.y])\n"
        + "          EnumerableTableScan(table=[[hr, depts]])";
    checkHeuristic(sql, expected);
  }

  /** It would probably be OK to transform
   * {@code (emps right join depts) join dependents}
   * to
   * {@code (emps  join dependents) right join depts}
   * but we do not currently allow it.
   */
  @Test public void testHeuristicPushInnerJoin() throws Exception {
    final String sql = "select * from \"emps\" as e\n"
        + "right join \"depts\" as d on e.\"deptno\" = d.\"deptno\"\n"
        + "join \"dependents\" as p on e.\"empid\" = p.\"empid\"";
    final String expected = ""
        + "EnumerableProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4], deptno0=[$5], name0=[$6], employees=[$7], location=[$8], location9=[$9], empid0=[$10], name1=[$11])\n"
        + "  EnumerableProject(empid=[$2], deptno=[$3], name=[$4], salary=[$5], commission=[$6], deptno0=[$7], name0=[$8], employees=[$9], x=[$10], y=[$11], empid0=[$0], name1=[$1])\n"
        + "    EnumerableHashJoin(condition=[=($0, $2)], joinType=[inner])\n"
        + "      EnumerableTableScan(table=[[hr, dependents]])\n"
        + "      EnumerableProject(empid=[$5], deptno=[$6], name=[$7], salary=[$8], commission=[$9], deptno0=[$0], name0=[$1], employees=[$2], x=[$3], y=[$4])\n"
        + "        EnumerableHashJoin(condition=[=($0, $6)], joinType=[left])\n"
        + "          EnumerableProject(deptno=[$0], name=[$1], employees=[$2], x=[$3.x], y=[$3.y])\n"
        + "            EnumerableTableScan(table=[[hr, depts]])\n"
        + "          EnumerableTableScan(table=[[hr, emps]])";
    checkHeuristic(sql, expected);
  }

  /** Tests that a relation (dependents) that is on the null-generating side of
   * an outer join cannot be pushed into an inner join (emps join depts). */
  @Test public void testHeuristicRightJoin() throws Exception {
    final String sql = "select * from \"emps\" as e\n"
        + "join \"depts\" as d on e.\"deptno\" = d.\"deptno\"\n"
        + "right join \"dependents\" as p on e.\"empid\" = p.\"empid\"";
    final String expected = ""
        + "EnumerableProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4], deptno0=[$5], name0=[$6], employees=[$7], location=[$8], location9=[$9], empid0=[$10], name1=[$11])\n"
        + "  EnumerableProject(empid=[$2], deptno=[$3], name=[$4], salary=[$5], commission=[$6], deptno0=[$7], name0=[$8], employees=[$9], x=[$10], y=[$11], empid0=[$0], name1=[$1])\n"
        + "    EnumerableHashJoin(condition=[=($0, $2)], joinType=[left])\n"
        + "      EnumerableTableScan(table=[[hr, dependents]])\n"
        + "      EnumerableHashJoin(condition=[=($1, $5)], joinType=[inner])\n"
        + "        EnumerableTableScan(table=[[hr, emps]])\n"
        + "        EnumerableProject(deptno=[$0], name=[$1], employees=[$2], x=[$3.x], y=[$3.y])\n"
        + "          EnumerableTableScan(table=[[hr, depts]])";
    checkHeuristic(sql, expected);
  }

  private void checkHeuristic(String sql, String expected) throws Exception {
    Planner planner = getPlanner(null,
        Programs.heuristicJoinOrder(Programs.RULE_SET, false, 0));
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).rel;
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), containsString(expected));
  }

  /** Plans a 3-table join query on the FoodMart schema. The ideal plan is not
   * bushy, but nevertheless exercises the bushy-join heuristic optimizer. */
  @Test public void testAlmostBushy() throws Exception {
    final String sql = "select *\n"
        + "from \"sales_fact_1997\" as s\n"
        + "join \"customer\" as c\n"
        + "  on s.\"customer_id\" = c.\"customer_id\"\n"
        + "join \"product\" as p\n"
        + "  on s.\"product_id\" = p.\"product_id\"\n"
        + "where c.\"city\" = 'San Francisco'\n"
        + "and p.\"brand_name\" = 'Washington'";
    final String expected = ""
        + "EnumerableProject(product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], product_class_id=[$37], product_id0=[$38], brand_name=[$39], product_name=[$40], SKU=[$41], SRP=[$42], gross_weight=[$43], net_weight=[$44], recyclable_package=[$45], low_fat=[$46], units_per_case=[$47], cases_per_pallet=[$48], shelf_width=[$49], shelf_height=[$50], shelf_depth=[$51])\n"
        + "  EnumerableProject(product_id0=[$44], time_id=[$45], customer_id0=[$46], promotion_id=[$47], store_id=[$48], store_sales=[$49], store_cost=[$50], unit_sales=[$51], customer_id=[$15], account_num=[$16], lname=[$17], fname=[$18], mi=[$19], address1=[$20], address2=[$21], address3=[$22], address4=[$23], city=[$24], state_province=[$25], postal_code=[$26], country=[$27], customer_region_id=[$28], phone1=[$29], phone2=[$30], birthdate=[$31], marital_status=[$32], yearly_income=[$33], gender=[$34], total_children=[$35], num_children_at_home=[$36], education=[$37], date_accnt_opened=[$38], member_card=[$39], occupation=[$40], houseowner=[$41], num_cars_owned=[$42], fullname=[$43], product_class_id=[$0], product_id=[$1], brand_name=[$2], product_name=[$3], SKU=[$4], SRP=[$5], gross_weight=[$6], net_weight=[$7], recyclable_package=[$8], low_fat=[$9], units_per_case=[$10], cases_per_pallet=[$11], shelf_width=[$12], shelf_height=[$13], shelf_depth=[$14])\n"
        + "    EnumerableHashJoin(condition=[=($1, $44)], joinType=[inner])\n"
        + "      EnumerableFilter(condition=[=($2, 'Washington')])\n"
        + "        EnumerableTableScan(table=[[foodmart2, product]])\n"
        + "      EnumerableHashJoin(condition=[=($0, $31)], joinType=[inner])\n"
        + "        EnumerableFilter(condition=[=($9, 'San Francisco')])\n"
        + "          EnumerableTableScan(table=[[foodmart2, customer]])\n"
        + "        EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])\n";
    checkBushy(sql, expected);
  }

  /** Plans a 4-table join query on the FoodMart schema.
   *
   * <p>The ideal plan is bushy:
   *   customer x (product_class x  product x sales)
   * which would be written
   *   (customer x ((product_class x product) x sales))
   * if you don't assume 'x' is left-associative. */
  @Test public void testBushy() throws Exception {
    final String sql = "select *\n"
        + "from \"sales_fact_1997\" as s\n"
        + "join \"customer\" as c\n"
        + "  on s.\"customer_id\" = c.\"customer_id\"\n"
        + "join \"product\" as p\n"
        + "  on s.\"product_id\" = p.\"product_id\"\n"
        + "join \"product_class\" as pc\n"
        + "  on p.\"product_class_id\" = pc.\"product_class_id\"\n"
        + "where c.\"city\" = 'San Francisco'\n"
        + "and p.\"brand_name\" = 'Washington'";
    final String expected = ""
        + "EnumerableProject(product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], product_class_id=[$37], product_id0=[$38], brand_name=[$39], product_name=[$40], SKU=[$41], SRP=[$42], gross_weight=[$43], net_weight=[$44], recyclable_package=[$45], low_fat=[$46], units_per_case=[$47], cases_per_pallet=[$48], shelf_width=[$49], shelf_height=[$50], shelf_depth=[$51], product_class_id0=[$52], product_subcategory=[$53], product_category=[$54], product_department=[$55], product_family=[$56])\n"
        + "  EnumerableProject(product_id0=[$49], time_id=[$50], customer_id0=[$51], promotion_id=[$52], store_id=[$53], store_sales=[$54], store_cost=[$55], unit_sales=[$56], customer_id=[$0], account_num=[$1], lname=[$2], fname=[$3], mi=[$4], address1=[$5], address2=[$6], address3=[$7], address4=[$8], city=[$9], state_province=[$10], postal_code=[$11], country=[$12], customer_region_id=[$13], phone1=[$14], phone2=[$15], birthdate=[$16], marital_status=[$17], yearly_income=[$18], gender=[$19], total_children=[$20], num_children_at_home=[$21], education=[$22], date_accnt_opened=[$23], member_card=[$24], occupation=[$25], houseowner=[$26], num_cars_owned=[$27], fullname=[$28], product_class_id0=[$34], product_id=[$35], brand_name=[$36], product_name=[$37], SKU=[$38], SRP=[$39], gross_weight=[$40], net_weight=[$41], recyclable_package=[$42], low_fat=[$43], units_per_case=[$44], cases_per_pallet=[$45], shelf_width=[$46], shelf_height=[$47], shelf_depth=[$48], product_class_id=[$29], product_subcategory=[$30], product_category=[$31], product_department=[$32], product_family=[$33])\n"
        + "    EnumerableHashJoin(condition=[=($0, $51)], joinType=[inner])\n"
        + "      EnumerableFilter(condition=[=($9, 'San Francisco')])\n"
        + "        EnumerableTableScan(table=[[foodmart2, customer]])\n"
        + "      EnumerableHashJoin(condition=[=($6, $20)], joinType=[inner])\n"
        + "        EnumerableHashJoin(condition=[=($0, $5)], joinType=[inner])\n"
        + "          EnumerableTableScan(table=[[foodmart2, product_class]])\n"
        + "          EnumerableFilter(condition=[=($2, 'Washington')])\n"
        + "            EnumerableTableScan(table=[[foodmart2, product]])\n"
        + "        EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])\n";
    checkBushy(sql, expected);
  }

  /** Plans a 5-table join query on the FoodMart schema. The ideal plan is
   * bushy: store x (customer x (product_class x product x sales)). */
  @Test public void testBushy5() throws Exception {
    final String sql = "select *\n"
        + "from \"sales_fact_1997\" as s\n"
        + "join \"customer\" as c\n"
        + "  on s.\"customer_id\" = c.\"customer_id\"\n"
        + "join \"product\" as p\n"
        + "  on s.\"product_id\" = p.\"product_id\"\n"
        + "join \"product_class\" as pc\n"
        + "  on p.\"product_class_id\" = pc.\"product_class_id\"\n"
        + "join \"store\" as st\n"
        + "  on s.\"store_id\" = st.\"store_id\"\n"
        + "where c.\"city\" = 'San Francisco'\n";
    final String expected = ""
        + "EnumerableProject(product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], product_class_id=[$37], product_id0=[$38], brand_name=[$39], product_name=[$40], SKU=[$41], SRP=[$42], gross_weight=[$43], net_weight=[$44], recyclable_package=[$45], low_fat=[$46], units_per_case=[$47], cases_per_pallet=[$48], shelf_width=[$49], shelf_height=[$50], shelf_depth=[$51], product_class_id0=[$52], product_subcategory=[$53], product_category=[$54], product_department=[$55], product_family=[$56], store_id0=[$57], store_type=[$58], region_id=[$59], store_name=[$60], store_number=[$61], store_street_address=[$62], store_city=[$63], store_state=[$64], store_postal_code=[$65], store_country=[$66], store_manager=[$67], store_phone=[$68], store_fax=[$69], first_opened_date=[$70], last_remodel_date=[$71], store_sqft=[$72], grocery_sqft=[$73], frozen_sqft=[$74], meat_sqft=[$75], coffee_bar=[$76], video_store=[$77], salad_bar=[$78], prepared_food=[$79], florist=[$80])\n"
        + "  EnumerableProject(product_id0=[$73], time_id=[$74], customer_id0=[$75], promotion_id=[$76], store_id0=[$77], store_sales=[$78], store_cost=[$79], unit_sales=[$80], customer_id=[$24], account_num=[$25], lname=[$26], fname=[$27], mi=[$28], address1=[$29], address2=[$30], address3=[$31], address4=[$32], city=[$33], state_province=[$34], postal_code=[$35], country=[$36], customer_region_id=[$37], phone1=[$38], phone2=[$39], birthdate=[$40], marital_status=[$41], yearly_income=[$42], gender=[$43], total_children=[$44], num_children_at_home=[$45], education=[$46], date_accnt_opened=[$47], member_card=[$48], occupation=[$49], houseowner=[$50], num_cars_owned=[$51], fullname=[$52], product_class_id0=[$58], product_id=[$59], brand_name=[$60], product_name=[$61], SKU=[$62], SRP=[$63], gross_weight=[$64], net_weight=[$65], recyclable_package=[$66], low_fat=[$67], units_per_case=[$68], cases_per_pallet=[$69], shelf_width=[$70], shelf_height=[$71], shelf_depth=[$72], product_class_id=[$53], product_subcategory=[$54], product_category=[$55], product_department=[$56], product_family=[$57], store_id=[$0], store_type=[$1], region_id=[$2], store_name=[$3], store_number=[$4], store_street_address=[$5], store_city=[$6], store_state=[$7], store_postal_code=[$8], store_country=[$9], store_manager=[$10], store_phone=[$11], store_fax=[$12], first_opened_date=[$13], last_remodel_date=[$14], store_sqft=[$15], grocery_sqft=[$16], frozen_sqft=[$17], meat_sqft=[$18], coffee_bar=[$19], video_store=[$20], salad_bar=[$21], prepared_food=[$22], florist=[$23])\n"
        + "    EnumerableHashJoin(condition=[=($0, $77)], joinType=[inner])\n"
        + "      EnumerableTableScan(table=[[foodmart2, store]])\n"
        + "      EnumerableHashJoin(condition=[=($0, $51)], joinType=[inner])\n"
        + "        EnumerableFilter(condition=[=($9, 'San Francisco')])\n"
        + "          EnumerableTableScan(table=[[foodmart2, customer]])\n"
        + "        EnumerableHashJoin(condition=[=($6, $20)], joinType=[inner])\n"
        + "          EnumerableHashJoin(condition=[=($0, $5)], joinType=[inner])\n"
        + "            EnumerableTableScan(table=[[foodmart2, product_class]])\n"
        + "            EnumerableTableScan(table=[[foodmart2, product]])\n"
        + "          EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])\n";
    checkBushy(sql, expected);
  }

  /** Tests the bushy join algorithm where one table does not join to
   * anything. */
  @Test public void testBushyCrossJoin() throws Exception {
    final String sql = "select * from \"sales_fact_1997\" as s\n"
        + "join \"customer\" as c\n"
        + "  on s.\"customer_id\" = c.\"customer_id\"\n"
        + "cross join \"department\"";
    final String expected = ""
        + "EnumerableProject(product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], department_id=[$37], department_description=[$38])\n"
        + "  EnumerableProject(product_id=[$31], time_id=[$32], customer_id0=[$33], promotion_id=[$34], store_id=[$35], store_sales=[$36], store_cost=[$37], unit_sales=[$38], customer_id=[$2], account_num=[$3], lname=[$4], fname=[$5], mi=[$6], address1=[$7], address2=[$8], address3=[$9], address4=[$10], city=[$11], state_province=[$12], postal_code=[$13], country=[$14], customer_region_id=[$15], phone1=[$16], phone2=[$17], birthdate=[$18], marital_status=[$19], yearly_income=[$20], gender=[$21], total_children=[$22], num_children_at_home=[$23], education=[$24], date_accnt_opened=[$25], member_card=[$26], occupation=[$27], houseowner=[$28], num_cars_owned=[$29], fullname=[$30], department_id=[$0], department_description=[$1])\n"
        + "    EnumerableHashJoin(condition=[true], joinType=[inner])\n"
        + "      EnumerableTableScan(table=[[foodmart2, department]])\n"
        + "      EnumerableHashJoin(condition=[=($0, $31)], joinType=[inner])\n"
        + "        EnumerableTableScan(table=[[foodmart2, customer]])\n"
        + "        EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])";
    checkBushy(sql, expected);
  }

  /** Tests the bushy join algorithm against a query where not all tables have a
   * join condition to the others. */
  @Test public void testBushyCrossJoin2() throws Exception {
    final String sql = "select * from \"sales_fact_1997\" as s\n"
        + "join \"customer\" as c\n"
        + "  on s.\"customer_id\" = c.\"customer_id\"\n"
        + "cross join \"department\" as d\n"
        + "join \"employee\" as e\n"
        + "  on d.\"department_id\" = e.\"department_id\"";
    final String expected = ""
        + "EnumerableProject(product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], department_id=[$37], department_description=[$38], employee_id=[$39], full_name=[$40], first_name=[$41], last_name=[$42], position_id=[$43], position_title=[$44], store_id0=[$45], department_id0=[$46], birth_date=[$47], hire_date=[$48], end_date=[$49], salary=[$50], supervisor_id=[$51], education_level=[$52], marital_status0=[$53], gender0=[$54], management_role=[$55])\n"
        + "  EnumerableProject(product_id=[$48], time_id=[$49], customer_id0=[$50], promotion_id=[$51], store_id0=[$52], store_sales=[$53], store_cost=[$54], unit_sales=[$55], customer_id=[$19], account_num=[$20], lname=[$21], fname=[$22], mi=[$23], address1=[$24], address2=[$25], address3=[$26], address4=[$27], city=[$28], state_province=[$29], postal_code=[$30], country=[$31], customer_region_id=[$32], phone1=[$33], phone2=[$34], birthdate=[$35], marital_status0=[$36], yearly_income=[$37], gender0=[$38], total_children=[$39], num_children_at_home=[$40], education=[$41], date_accnt_opened=[$42], member_card=[$43], occupation=[$44], houseowner=[$45], num_cars_owned=[$46], fullname=[$47], department_id=[$0], department_description=[$1], employee_id=[$2], full_name=[$3], first_name=[$4], last_name=[$5], position_id=[$6], position_title=[$7], store_id=[$8], department_id0=[$9], birth_date=[$10], hire_date=[$11], end_date=[$12], salary=[$13], supervisor_id=[$14], education_level=[$15], marital_status=[$16], gender=[$17], management_role=[$18])\n"
        + "    EnumerableHashJoin(condition=[true], joinType=[inner])\n"
        + "      EnumerableHashJoin(condition=[=($0, $9)], joinType=[inner])\n"
        + "        EnumerableTableScan(table=[[foodmart2, department]])\n"
        + "        EnumerableTableScan(table=[[foodmart2, employee]])\n"
        + "      EnumerableHashJoin(condition=[=($0, $31)], joinType=[inner])\n"
        + "        EnumerableTableScan(table=[[foodmart2, customer]])\n"
        + "        EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])\n";
    checkBushy(sql, expected);
  }

  /** Checks that a query returns a particular plan, using a planner with
   * MultiJoinOptimizeBushyRule enabled. */
  private void checkBushy(String sql, String expected) throws Exception {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema,
                CalciteAssert.SchemaSpec.CLONE_FOODMART))
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
        .build();
    Planner planner = Frameworks.getPlanner(config);
    SqlNode parse = planner.parse(sql);

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).project();
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), containsString(expected));
  }

  /**
   * Rule to convert a
   * {@link org.apache.calcite.adapter.enumerable.EnumerableProject} to an
   * {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProject}.
   */
  private class MockJdbcProjectRule extends ConverterRule {
    private MockJdbcProjectRule(JdbcConvention out) {
      super(EnumerableProject.class, EnumerableConvention.INSTANCE, out,
          "MockJdbcProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final EnumerableProject project = (EnumerableProject) rel;

      return new JdbcRules.JdbcProject(
          rel.getCluster(),
          rel.getTraitSet().replace(getOutConvention()),
          convert(project.getInput(),
              project.getInput().getTraitSet().replace(getOutConvention())),
          project.getProjects(),
          project.getRowType());
    }
  }

  /**
   * Rule to convert a
   * {@link org.apache.calcite.adapter.enumerable.EnumerableTableScan} to an
   * {@link MockJdbcTableScan}.
   */
  private class MockJdbcTableRule extends ConverterRule {
    private MockJdbcTableRule(JdbcConvention out) {
      super(EnumerableTableScan.class,
          EnumerableConvention.INSTANCE, out, "MockJdbcTableRule");
    }

    public RelNode convert(RelNode rel) {
      final EnumerableTableScan scan =
          (EnumerableTableScan) rel;
      return new MockJdbcTableScan(scan.getCluster(),
          scan.getTable(),
          (JdbcConvention) getOutConvention());
    }
  }

  /**
   * Relational expression representing a "mock" scan of a table in a
   * JDBC data source.
   */
  private class MockJdbcTableScan extends TableScan
      implements JdbcRel {

    MockJdbcTableScan(RelOptCluster cluster, RelOptTable table,
        JdbcConvention jdbcConvention) {
      super(cluster, cluster.traitSetOf(jdbcConvention), table);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new MockJdbcTableScan(getCluster(), table,
          (JdbcConvention) getConvention());
    }

    @Override public void register(RelOptPlanner planner) {
      final JdbcConvention out = (JdbcConvention) getConvention();
      for (RelOptRule rule : JdbcRules.rules(out)) {
        planner.addRule(rule);
      }
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return null;
    }
  }

  /**
   * Test to determine whether de-correlation correctly removes Correlator.
   */
  @Test public void testOldJoinStyleDeCorrelation() throws Exception {
    assertFalse(
        checkTpchQuery("select\n p.`pPartkey`\n"
            + "from\n"
            + "  `tpch`.`part` p,\n"
            + "  `tpch`.`partsupp` ps1\n"
            + "where\n"
            + "  p.`pPartkey` = ps1.`psPartkey`\n"
            + "  and ps1.`psSupplyCost` = (\n"
            + "    select\n"
            + "      min(ps.`psSupplyCost`)\n"
            + "    from\n"
            + "      `tpch`.`partsupp` ps\n"
            + "    where\n"
            + "      p.`pPartkey` = ps.`psPartkey`\n"
            + "  )\n")
            .contains("Correlat"));
  }

  public String checkTpchQuery(String tpchTestQuery) throws Exception {
    final SchemaPlus schema =
        Frameworks.createRootSchema(true).add("tpch",
            new ReflectiveSchema(new TpchSchema()));

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
        .defaultSchema(schema)
        .programs(Programs.ofRules(Programs.RULE_SET))
        .build();
    String plan;
    try (Planner p = Frameworks.getPlanner(config)) {
      SqlNode n = p.parse(tpchTestQuery);
      n = p.validate(n);
      RelNode r = p.rel(n).project();
      plan = RelOptUtil.toString(r);
    }
    return plan;
  }

  /** User-defined aggregate function. */
  public static class MyCountAggFunction extends SqlAggFunction {
    public MyCountAggFunction() {
      super("MY_COUNT", null, SqlKind.OTHER_FUNCTION, ReturnTypes.BIGINT, null,
          OperandTypes.ANY, SqlFunctionCategory.NUMERIC, false, false,
          Optionality.FORBIDDEN);
    }

    @SuppressWarnings("deprecation")
    public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
      return ImmutableList.of(typeFactory.createSqlType(SqlTypeName.ANY));
    }

    @SuppressWarnings("deprecation")
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
      return typeFactory.createSqlType(SqlTypeName.BIGINT);
    }

    public RelDataType deriveType(SqlValidator validator,
        SqlValidatorScope scope, SqlCall call) {
      // Check for COUNT(*) function.  If it is we don't
      // want to try and derive the "*"
      if (call.isCountStar()) {
        return validator.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
      }
      return super.deriveType(validator, scope, call);
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-569">[CALCITE-569]
   * ArrayIndexOutOfBoundsException when deducing collation</a>. */
  @Test public void testOrderByNonSelectColumn() throws Exception {
    final SchemaPlus schema = Frameworks.createRootSchema(true)
        .add("tpch", new ReflectiveSchema(new TpchSchema()));

    String query = "select t.psPartkey from \n"
        + "(select ps.psPartkey from `tpch`.`partsupp` ps \n"
        + "order by ps.psPartkey, ps.psSupplyCost) t \n"
        + "order by t.psPartkey";

    List<RelTraitDef> traitDefs = new ArrayList<>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    final SqlParser.Config parserConfig =
        SqlParser.configBuilder().setLex(Lex.MYSQL).build();
    FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(schema)
        .traitDefs(traitDefs)
        .programs(Programs.ofRules(Programs.RULE_SET))
        .build();
    String plan;
    try (Planner p = Frameworks.getPlanner(config)) {
      SqlNode n = p.parse(query);
      n = p.validate(n);
      RelNode r = p.rel(n).project();
      plan = RelOptUtil.toString(r);
      plan = Util.toLinux(plan);
    }
    assertThat(plan,
        equalTo("LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(psPartkey=[$0])\n"
        + "    LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
        + "      LogicalProject(psPartkey=[$0], psSupplyCost=[$1])\n"
        + "        EnumerableTableScan(table=[[tpch, partsupp]])\n"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-648">[CALCITE-649]
   * Update ProjectMergeRule description for new naming convention</a>. */
  @Test public void testMergeProjectForceMode() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            new ProjectMergeRule(true,
                RelBuilder.proto(RelFactories.DEFAULT_PROJECT_FACTORY)));
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    planner.close();
  }

  @Test public void testView() throws Exception {
    final String sql = "select * FROM dept";
    final String expected = "LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
        + "  LogicalValues(type=[RecordType(INTEGER DEPTNO, CHAR(11) DNAME)], "
        + "tuples=[[{ 10, 'Sales      ' },"
        + " { 20, 'Marketing  ' },"
        + " { 30, 'Engineering' },"
        + " { 40, 'Empty      ' }]])\n";
    checkView(sql, is(expected));
  }

  @Test public void testViewOnView() throws Exception {
    final String sql = "select * FROM dept30";
    final String expected = "LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
        + "  LogicalFilter(condition=[=($0, 30)])\n"
        + "    LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
        + "      LogicalValues(type=[RecordType(INTEGER DEPTNO, CHAR(11) DNAME)], "
        + "tuples=[[{ 10, 'Sales      ' },"
        + " { 20, 'Marketing  ' },"
        + " { 30, 'Engineering' },"
        + " { 40, 'Empty      ' }]])\n";
    checkView(sql, is(expected));
  }

  private void checkView(String sql, Matcher<String> matcher)
      throws SqlParseException, ValidationException, RelConversionException {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.POST))
        .build();
    final Planner planner = Frameworks.getPlanner(config);
    SqlNode parse = planner.parse(sql);
    final SqlNode validate = planner.validate(parse);
    final RelRoot root = planner.rel(validate);
    assertThat(toString(root.rel), matcher);
  }
}

// End PlannerTest.java
