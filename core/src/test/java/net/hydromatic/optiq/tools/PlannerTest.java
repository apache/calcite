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

import net.hydromatic.linq4j.function.Function1;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.config.Lex;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.impl.jdbc.*;
import net.hydromatic.optiq.impl.jdbc.JdbcRules.JdbcProjectRel;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.JavaRules;
import net.hydromatic.optiq.rules.java.JavaRules.EnumerableProjectRel;
import net.hydromatic.optiq.test.JdbcTest;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.rules.MergeFilterRule;
import org.eigenbase.relopt.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.util.Util;

import org.junit.Test;


import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link Planner}.
 */
public class PlannerTest {
  @Test public void testParseAndConvert() throws Exception {
    Planner planner = getPlanner();
    SqlNode parse =
        planner.parse("select * from \"emps\" where \"name\" like '%e%'");
    assertThat(Util.toLinux(parse.toString()), equalTo(
        "SELECT *\n"
        + "FROM `emps`\n"
        + "WHERE `name` LIKE '%e%'"));

    SqlNode validate = planner.validate(parse);
    RelNode rel = planner.convert(validate);
    assertThat(toString(rel),
        equalTo(
            "ProjectRel(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
            + "  FilterRel(condition=[LIKE($2, '%e%')])\n"
            + "    EnumerableTableAccessRel(table=[[hr, emps]])\n"));
  }

  private String toString(RelNode rel) {
    return Util.toLinux(
        RelOptUtil.dumpPlan("", rel, false,
            SqlExplainLevel.DIGEST_ATTRIBUTES));
  }

  @Test public void testParseFails() throws SqlParseException {
    Planner planner = getPlanner();
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
    Planner planner = getPlanner();
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

  private Planner getPlanner(RuleSet... ruleSets) {
    return Frameworks.getPlanner(
        Lex.ORACLE,
        new Function1<SchemaPlus, Schema>() {
          public Schema apply(SchemaPlus parentSchema) {
            return new ReflectiveSchema(parentSchema, "hr",
                new JdbcTest.HrSchema());
          }
        }, SqlStdOperatorTable.instance(), ruleSets);
  }

  /** Tests that planner throws an error if you pass to
   * {@link Planner#convert(org.eigenbase.sql.SqlNode)}
   * a {@link org.eigenbase.sql.SqlNode} that has been parsed but not
   * validated. */
  @Test public void testConvertWithoutValidateFails() throws Exception {
    Planner planner = getPlanner();
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
    RuleSet ruleSet =
        RuleSets.ofList(
            MergeFilterRule.INSTANCE,
            JavaRules.ENUMERABLE_FILTER_RULE,
            JavaRules.ENUMERABLE_PROJECT_RULE);
    Planner planner = getPlanner(ruleSet);
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
    Planner planner = getPlanner(ruleSet);
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
    Planner planner = getPlanner();
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
   * with different rule sets, with different conventions */
  @Test public void testPlanTransformWithDiffRulesetAndConvention()
    throws Exception {
    RuleSet ruleSet0 =
      RuleSets.ofList(
            MergeFilterRule.INSTANCE,
            JavaRules.ENUMERABLE_FILTER_RULE,
            JavaRules.ENUMERABLE_PROJECT_RULE);

    JdbcConvention out = new JdbcConvention(null, "myjdbc");
    RuleSet ruleSet1 = RuleSets.ofList(new MockJdbcProjectRule(out),
                                       new MockJdbcTableRule(out));

    Planner planner = getPlanner(ruleSet0, ruleSet1);
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
        "JdbcProjectRel(name=[$2])\n  MockJdbcTableScan(table=[[hr, emps]])\n"));

  }


  /**
   * Rule to convert a {@link EnumerableProjectRel} to an
   * {@link JdbcProjectRel}.
   */
  private class MockJdbcProjectRule
          extends ConverterRule {
    private MockJdbcProjectRule(JdbcConvention out) {
        super(EnumerableProjectRel.class, EnumerableConvention.INSTANCE,
              out, "MockJdbcProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final EnumerableProjectRel project = (EnumerableProjectRel) rel;

      return new JdbcProjectRel(
                    rel.getCluster(),
                    rel.getTraitSet().replace(getOutConvention()),
                    convert(
                            project.getChild(),
                            project.getChild().getTraitSet().
                                replace(getOutConvention())),
                    project.getProjects(),
                    project.getRowType(),
                    ProjectRelBase.Flags.BOXED);
    }
  }

    /**
     * Rule to convert a {@link JavaRules.EnumerableTableAccessRel} to an
     * {@link MockJdbcTableScan}.
     */
  private class MockJdbcTableRule
            extends ConverterRule {
    private MockJdbcTableRule(JdbcConvention out) {
      super(JavaRules.EnumerableTableAccessRel.class,
                  EnumerableConvention.INSTANCE, out, "MockJdbcTableRule");
    }

    public RelNode convert(RelNode rel) {
      final JavaRules.EnumerableTableAccessRel tbl =
                (JavaRules.EnumerableTableAccessRel) rel;
      return new MockJdbcTableScan(tbl.getCluster(),
                                   tbl.getTable(),
                                   (JdbcConvention) getOutConvention());
    }
  }

    /**
     * Relational expression representing a "mock" scan of a table in a
     * JDBC data source.
     */
  private class MockJdbcTableScan extends TableAccessRelBase
    implements JdbcRel {

    public MockJdbcTableScan(
              RelOptCluster cluster,
              RelOptTable table,
              JdbcConvention jdbcConvention) {
      super(cluster, cluster.traitSetOf(jdbcConvention), table);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new MockJdbcTableScan(
                    getCluster(), table, (JdbcConvention) getConvention());
    }

    @Override
    public void register(RelOptPlanner planner) {
      final JdbcConvention out = (JdbcConvention) getConvention();
      for (RelOptRule rule : JdbcRules.rules(out)) {
        planner.addRule(rule);
      }
    }

    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      return null;
    }
  }

}

// End PlannerTest.java
