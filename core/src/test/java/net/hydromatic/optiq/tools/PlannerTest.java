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

import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.JavaRules;
import net.hydromatic.optiq.test.JdbcTest;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.rules.MergeFilterRule;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;

import org.eigenbase.util.Util;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link Planner}.
 */
public class PlannerTest {
  @Test public void testParseAndConvert()
      throws SqlParseException, RelConversionException, ValidationException {
    Planner planner = getPlanner();
    SqlNode parse =
        planner.parse("select * from \"emps\" where \"name\" like '%e%'");
    assertThat(parse.toString(), equalTo(
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
    return RelOptUtil.dumpPlan("", rel, false,
        SqlExplainLevel.DIGEST_ATTRIBUTES);
  }

  @Test public void testParseFails()
      throws SqlParseException {
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

  @Test public void testValidateFails()
      throws SqlParseException {
    Planner planner = getPlanner();
    SqlNode parse =
        planner.parse("select * from \"emps\" where \"Xname\" like '%e%'");
    assertThat(parse.toString(), equalTo(
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
        new Function1<MutableSchema, Schema>() {
          public Schema apply(MutableSchema parentSchema) {
            return ReflectiveSchema.create(parentSchema, "hr",
                new JdbcTest.HrSchema());
          }
        }, SqlStdOperatorTable.instance(), ruleSets);
  }

  /** Tests that planner throws an error if you pass to
   * {@link Planner#convert(org.eigenbase.sql.SqlNode)}
   * a {@link org.eigenbase.sql.SqlNode} that has been parsed but not
   * validated. */
  @Test public void testConvertWithoutValidateFails()
      throws SqlParseException, RelConversionException {
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
  @Test public void testPlan()
      throws SqlParseException, RelConversionException, ValidationException {
    RuleSet ruleSet =
        RuleSets.ofList(
            MergeFilterRule.instance,
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
}

// End PlannerTest.java
