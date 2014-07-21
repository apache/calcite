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
package net.hydromatic.optiq.tools;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.config.Lex;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.impl.jdbc.*;
import net.hydromatic.optiq.impl.jdbc.JdbcRules.JdbcProjectRel;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.JavaRules;
import net.hydromatic.optiq.rules.java.JavaRules.EnumerableProjectRel;
import net.hydromatic.optiq.test.OptiqAssert;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.ChainedSqlOperatorTable;
import org.eigenbase.sql.util.ListSqlOperatorTable;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

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
    RelNode rel = planner.convert(validate);
    assertThat(toString(rel), equalTo(expectedRelExpr));
  }

  @Test public void testParseAndConvert() throws Exception {
    checkParseAndConvert(
        "select * from \"emps\" where \"name\" like '%e%'",

        "SELECT *\n"
        + "FROM `emps`\n"
        + "WHERE `name` LIKE '%e%'",

        "ProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "  FilterRel(condition=[LIKE($2, '%e%')])\n"
        + "    EnumerableTableAccessRel(table=[[hr, emps]])\n");
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

        "SortRel(sort0=[$1], dir0=[ASC], offset=[10])\n"
        + "  ProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "    EnumerableTableAccessRel(table=[[hr, emps]])\n");
  }

  private String toString(RelNode rel) {
    return Util.toLinux(
        RelOptUtil.dumpPlan("", rel, false,
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
    assertThat(Util.toLinux(parse.toString()), equalTo(
        "SELECT *\n"
        + "FROM `emps`\n"
        + "WHERE `Xname` LIKE '%e%'"));

    try {
      SqlNode validate = planner.validate(parse);
      fail("expected error, got " + validate);
    } catch (ValidationException e) {
      assertThat(Util.getStackTrace(e),
          containsString("Column 'Xname' not found in any table"));
      // ok
    }
  }

  @Test public void testValidateUserDefinedAggregate() throws Exception {
    final SqlStdOperatorTable stdOpTab = SqlStdOperatorTable.instance();
    SqlOperatorTable opTab = new ChainedSqlOperatorTable(
        ImmutableList.of(stdOpTab,
            new ListSqlOperatorTable(
                ImmutableList.<SqlOperator>of(new MyCountAggFunction()))));
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(
            OptiqAssert.addSchema(rootSchema, OptiqAssert.SchemaSpec.HR))
        .operatorTable(opTab)
        .build();
    final Planner planner = Frameworks.getPlanner(config);
    SqlNode parse =
        planner.parse("select \"deptno\", my_count(\"empid\") from \"emps\"\n"
            + "group by \"deptno\"");
    assertThat(Util.toLinux(parse.toString()),
        equalTo(
            "SELECT `deptno`, `MY_COUNT`(`empid`)\n"
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
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .lex(Lex.ORACLE)
        .defaultSchema(
            OptiqAssert.addSchema(rootSchema, OptiqAssert.SchemaSpec.HR))
        .traitDefs(traitDefs)
        .programs(programs)
        .build();
    return Frameworks.getPlanner(config);
  }

  /** Tests that planner throws an error if you pass to
   * {@link Planner#convert(org.eigenbase.sql.SqlNode)}
   * a {@link org.eigenbase.sql.SqlNode} that has been parsed but not
   * validated. */
  @Test public void testConvertWithoutValidateFails() throws Exception {
    Planner planner = getPlanner(null);
    SqlNode parse = planner.parse("select * from \"emps\"");
    try {
      RelNode rel = planner.convert(parse);
      fail("expected error, got " + rel);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString(
          "cannot move from STATE_3_PARSED to STATE_4_VALIDATED"));
    }
  }

  /** Unit test that parses, validates, converts and plans. */
  @Test public void testPlan() throws Exception {
    Program program =
        Programs.ofRules(
            MergeFilterRule.INSTANCE,
            JavaRules.ENUMERABLE_FILTER_RULE,
            JavaRules.ENUMERABLE_PROJECT_RULE);
    Planner planner = getPlanner(null, program);
    SqlNode parse = planner.parse("select * from \"emps\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), equalTo(
        "EnumerableProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "  EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  /** Unit test that parses, validates, converts and
   * plans for query using order by */
  @Test public void testSortPlan() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            RemoveSortRule.INSTANCE,
            JavaRules.ENUMERABLE_PROJECT_RULE,
            JavaRules.ENUMERABLE_SORT_RULE);
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    SqlNode parse = planner.parse(
        "select * from \"emps\" "
            + "order by \"emps\".\"deptno\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = convert.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), equalTo(
        "EnumerableSortRel(sort0=[$1], dir0=[ASC])\n"
        + "  EnumerableProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "    EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  /** Unit test that parses, validates, converts and
   * plans for query using two duplicate order by.
   * The duplicate order by should be removed by RemoveSortRule*/
  @Test public void testDuplicateSortPlan() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            RemoveSortRule.INSTANCE,
            JavaRules.ENUMERABLE_PROJECT_RULE,
            JavaRules.ENUMERABLE_SORT_RULE);
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    SqlNode parse = planner.parse(
        "select \"empid\" from ( "
         + "select * "
         + "from \"emps\" "
         + "order by \"emps\".\"deptno\") "
         + "order by \"deptno\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), equalTo(
        "EnumerableProjectRel(empid=[$0])\n"
        + "  EnumerableProjectRel(empid=[$0], deptno=[$1])\n"
        + "    EnumerableSortRel(sort0=[$1], dir0=[ASC])\n"
        + "      EnumerableProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "        EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  /** Unit test that parses, validates, converts and
   * plans for query using two duplicate order by.*/
  @Test public void testDuplicateSortPlanWORemoveSortRule() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            JavaRules.ENUMERABLE_PROJECT_RULE,
            JavaRules.ENUMERABLE_SORT_RULE);
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    SqlNode parse = planner.parse(
        "select \"empid\" from ( "
            + "select * "
            + "from \"emps\" "
            + "order by \"emps\".\"deptno\") "
            + "order by \"deptno\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), equalTo(
        "EnumerableProjectRel(empid=[$0])\n"
        + "  EnumerableSortRel(sort0=[$1], dir0=[ASC])\n"
        + "    EnumerableProjectRel(empid=[$0], deptno=[$1])\n"
        + "      EnumerableSortRel(sort0=[$1], dir0=[ASC])\n"
        + "        EnumerableProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "          EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  /** Unit test that parses, validates, converts and plans. Planner is
   * provided with a list of RelTraitDefs to register. */
  @Test public void testPlanWithExplicitTraitDefs() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            MergeFilterRule.INSTANCE,
            JavaRules.ENUMERABLE_FILTER_RULE,
            JavaRules.ENUMERABLE_PROJECT_RULE);
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    Planner planner = getPlanner(traitDefs, Programs.of(ruleSet));

    SqlNode parse = planner.parse("select * from \"emps\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), equalTo(
        "EnumerableProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "  EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  /** Unit test that calls {@link Planner#transform} twice. */
  @Test public void testPlanTransformTwice() throws Exception {
    RuleSet ruleSet =
        RuleSets.ofList(
            MergeFilterRule.INSTANCE,
            JavaRules.ENUMERABLE_FILTER_RULE,
            JavaRules.ENUMERABLE_PROJECT_RULE);
    Planner planner = getPlanner(null, Programs.of(ruleSet));
    SqlNode parse = planner.parse("select * from \"emps\"");
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    RelNode transform2 = planner.transform(0, traitSet, transform);
    assertThat(toString(transform2), equalTo(
        "EnumerableProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "  EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  /** Tests that Hive dialect does not generate "AS". */
  @Test public void testHiveDialect() throws SqlParseException {
    Planner planner = getPlanner(null);
    SqlNode parse = planner.parse(
        "select * from (select * from \"emps\") as t\n"
        + "where \"name\" like '%e%'");
    final SqlDialect hiveDialect =
        new SqlDialect(SqlDialect.DatabaseProduct.HIVE, "Hive", null);
    assertThat(Util.toLinux(parse.toSqlString(hiveDialect).getSql()),
        equalTo("SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM emps) T\n"
            + "WHERE name LIKE '%e%'"));
  }

  /** Unit test that calls {@link Planner#transform} twice,
   * with different rule sets, with different conventions.
   *
   * <p>{@link net.hydromatic.optiq.impl.jdbc.JdbcConvention} is different
   * from the typical convention in that it is not a singleton. Switching to
   * a different instance causes problems unless planner state is wiped clean
   * between calls to {@link Planner#transform}. */
  @Test public void testPlanTransformWithDiffRuleSetAndConvention()
    throws Exception {
    Program program0 =
        Programs.ofRules(
            MergeFilterRule.INSTANCE,
            JavaRules.ENUMERABLE_FILTER_RULE,
            JavaRules.ENUMERABLE_PROJECT_RULE);

    JdbcConvention out = new JdbcConvention(null, null, "myjdbc");
    Program program1 = Programs.ofRules(
        new MockJdbcProjectRule(out), new MockJdbcTableRule(out));

    Planner planner = getPlanner(null, program0, program1);
    SqlNode parse = planner.parse("select T1.\"name\" from \"emps\" as T1 ");

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);

    RelTraitSet traitSet0 = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);

    RelTraitSet traitSet1 = planner.getEmptyTraitSet()
        .replace(out);

    RelNode transform = planner.transform(0, traitSet0, convert);
    RelNode transform2 = planner.transform(1, traitSet1, transform);
    assertThat(toString(transform2), equalTo(
        "JdbcProjectRel(name=[$2])\n"
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
    if (OptiqAssert.ENABLE_SLOW) {
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
    Planner planner =
        getPlanner(null, Programs.heuristicJoinOrder(RULE_SET, false));
    SqlNode parse = planner.parse(buf.toString());

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), containsString(
        "EnumerableJoinRel(condition=[=($3, $0)], joinType=[inner])"));
  }

  /** Plans a 4-table join query on the FoodMart schema. The ideal plan is not
   * bushy, but nevertheless exercises the bushy-join heuristic optimizer. */
  @Test public void testAlmostBushy() throws Exception {
    final String sql = "select *\n"
        + "from \"sales_fact_1997\" as s\n"
        + "  join \"customer\" as c using (\"customer_id\")\n"
        + "  join \"product\" as p using (\"product_id\")\n"
        + "where c.\"city\" = 'San Francisco'\n"
        + "and p.\"brand_name\" = 'Washington'";
    final String expected = ""
        + "EnumerableJoinRel(condition=[=($0, $38)], joinType=[inner])\n"
        + "  EnumerableJoinRel(condition=[=($2, $8)], joinType=[inner])\n"
        + "    EnumerableTableAccessRel(table=[[foodmart2, sales_fact_1997]])\n"
        + "    EnumerableFilterRel(condition=[=($9, 'San Francisco')])\n"
        + "      EnumerableTableAccessRel(table=[[foodmart2, customer]])\n"
        + "  EnumerableFilterRel(condition=[=($2, 'Washington')])\n"
        + "    EnumerableTableAccessRel(table=[[foodmart2, product]])\n";
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .lex(Lex.ORACLE)
        .defaultSchema(
            OptiqAssert.addSchema(rootSchema,
                OptiqAssert.SchemaSpec.CLONE_FOODMART))
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(RULE_SET, true))
        .build();
    Planner planner = Frameworks.getPlanner(config);
    SqlNode parse = planner.parse(sql);

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);
    RelTraitSet traitSet = planner.getEmptyTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(toString(transform), containsString(expected));
  }

  /**
   * Rule to convert a {@link EnumerableProjectRel} to an
   * {@link JdbcProjectRel}.
   */
  private class MockJdbcProjectRule extends ConverterRule {
    private MockJdbcProjectRule(JdbcConvention out) {
      super(EnumerableProjectRel.class, EnumerableConvention.INSTANCE, out,
          "MockJdbcProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final EnumerableProjectRel project = (EnumerableProjectRel) rel;

      return new JdbcProjectRel(
          rel.getCluster(),
          rel.getTraitSet().replace(getOutConvention()),
          convert(project.getChild(),
              project.getChild().getTraitSet().replace(getOutConvention())),
          project.getProjects(),
          project.getRowType(),
          ProjectRelBase.Flags.BOXED);
    }
  }

  /**
   * Rule to convert a {@link JavaRules.EnumerableTableAccessRel} to an
   * {@link MockJdbcTableScan}.
   */
  private class MockJdbcTableRule extends ConverterRule {
    private MockJdbcTableRule(JdbcConvention out) {
      super(JavaRules.EnumerableTableAccessRel.class,
          EnumerableConvention.INSTANCE, out, "MockJdbcTableRule");
    }

    public RelNode convert(RelNode rel) {
      final JavaRules.EnumerableTableAccessRel scan =
          (JavaRules.EnumerableTableAccessRel) rel;
      return new MockJdbcTableScan(scan.getCluster(),
          scan.getTable(),
          (JdbcConvention) getOutConvention());
    }
  }

  /**
   * Relational expression representing a "mock" scan of a table in a
   * JDBC data source.
   */
  private class MockJdbcTableScan extends TableAccessRelBase
      implements JdbcRel {

    public MockJdbcTableScan(RelOptCluster cluster, RelOptTable table,
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

  private static final ImmutableSet<RelOptRule> RULE_SET =
      ImmutableSet.of(
          JavaRules.ENUMERABLE_JOIN_RULE,
          JavaRules.ENUMERABLE_PROJECT_RULE,
          JavaRules.ENUMERABLE_FILTER_RULE,
          JavaRules.ENUMERABLE_AGGREGATE_RULE,
          JavaRules.ENUMERABLE_SORT_RULE,
          JavaRules.ENUMERABLE_LIMIT_RULE,
          JavaRules.ENUMERABLE_UNION_RULE,
          JavaRules.ENUMERABLE_INTERSECT_RULE,
          JavaRules.ENUMERABLE_MINUS_RULE,
          JavaRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
          JavaRules.ENUMERABLE_VALUES_RULE,
          JavaRules.ENUMERABLE_WINDOW_RULE,
          JavaRules.ENUMERABLE_ONE_ROW_RULE,
          JavaRules.ENUMERABLE_EMPTY_RULE,
          TableAccessRule.INSTANCE,
          OptiqPrepareImpl.COMMUTE
              ? CommutativeJoinRule.INSTANCE
              : MergeProjectRule.INSTANCE,
          PushFilterPastProjectRule.INSTANCE,
          PushFilterPastJoinRule.FILTER_ON_JOIN,
          RemoveDistinctAggregateRule.INSTANCE,
          ReduceAggregatesRule.INSTANCE,
          SwapJoinRule.INSTANCE,
          PushJoinThroughJoinRule.RIGHT,
          PushJoinThroughJoinRule.LEFT,
          PushSortPastProjectRule.INSTANCE);

  /**
   * Test to determine whether de-correlation correctly removes CorrelatorRel.
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
            .contains("CorrelatorRel"));
  }

  public String checkTpchQuery(String tpchTestQuery) throws Exception {
    final SchemaPlus schema =
        Frameworks.createRootSchema(true).add("tpch",
            new ReflectiveSchema(new TpchSchema()));

    Planner p = Frameworks.getPlanner(Frameworks.newConfigBuilder() //
        .lex(Lex.MYSQL) //
        .defaultSchema(schema) //
        .programs(Programs.ofRules(RULE_SET)) //
        .build());
    SqlNode n = p.parse(tpchTestQuery);
    n = p.validate(n);
    RelNode r = p.convert(n);
    String plan = RelOptUtil.toString(r);
    p.close();
    return plan;
  }

  /** User-defined aggregate function. */
  public static class MyCountAggFunction extends SqlAggFunction {
    public MyCountAggFunction() {
      super("MY_COUNT", SqlKind.OTHER_FUNCTION, ReturnTypes.BIGINT, null,
          OperandTypes.ANY, SqlFunctionCategory.NUMERIC);
    }

    public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
      return ImmutableList.of(typeFactory.createSqlType(SqlTypeName.ANY));
    }

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
}

// End PlannerTest.java
