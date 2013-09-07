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
package net.hydromatic.optiq.test;

import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import net.hydromatic.optiq.materialize.MaterializationService;
import net.hydromatic.optiq.prepare.Prepare;

import org.eigenbase.relopt.SubstitutionVisitor;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.*;

/**
 * Unit test for the materialized view rewrite mechanism. Each test has a
 * query and one or more materializations (what Oracle calls materialized views)
 * and checks that the materialization is used.
 */
public class MaterializationTest {
  final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
  final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  @Test public void testFilter() {
    try {
      OptiqAssert.assertThat()
          .with(OptiqAssert.Config.REGULAR)
          .withMaterializations(
              JdbcTest.HR_MODEL,
              "m0",
              "select * from \"emps\" where \"deptno\" = 10")
          .query(
              "select \"empid\" + 1 from \"emps\" where \"deptno\" = 10")
          .enableMaterializations(true)
          .explainContains(
              "EnumerableTableAccessRel(table=[[hr, m0]])")
          .sameResultWithMaterializationsDisabled();
    } finally {
      MaterializationService.INSTANCE.clear();
    }
  }

  @Test public void testFilterQueryOnProjectView() {
    try {
      Prepare.TRIM = true;
      OptiqAssert.assertThat()
          .with(OptiqAssert.Config.REGULAR)
          .withMaterializations(
              JdbcTest.HR_MODEL,
              "m0",
              "select \"deptno\", \"empid\" from \"emps\"")
          .query(
              "select \"empid\" + 1 as x from \"emps\" where \"deptno\" = 10")
          .enableMaterializations(true)
          .explainContains(
              "EnumerableTableAccessRel(table=[[hr, m0]])")
          .sameResultWithMaterializationsDisabled();
    } finally {
      MaterializationService.INSTANCE.clear();
      Prepare.TRIM = false;
    }
  }

  /** Checks that a given query can use a materialized view with a given
   * definition. */
  private void checkMaterialize(String materialize, String query) {
    try {
      Prepare.TRIM = true;
      OptiqAssert.assertThat()
          .with(OptiqAssert.Config.REGULAR)
          .withMaterializations(JdbcTest.HR_MODEL, "m0", materialize)
          .query(query)
          .enableMaterializations(true)
          .explainContains(
              "EnumerableTableAccessRel(table=[[hr, m0]])")
          .sameResultWithMaterializationsDisabled();
    } finally {
      MaterializationService.INSTANCE.clear();
      Prepare.TRIM = false;
    }
  }

  /** Checks that a given query CAN NOT use a materialized view with a given
   * definition. */
  private void checkNoMaterialize(String materialize, String query) {
    try {
      Prepare.TRIM = true;
      OptiqAssert.assertThat()
          .with(OptiqAssert.Config.REGULAR)
          .withMaterializations(JdbcTest.HR_MODEL, "m0", materialize)
          .query(query)
          .enableMaterializations(true)
          .explainContains(
              "EnumerableTableAccessRel(table=[[hr, emps]])");
    } finally {
      MaterializationService.INSTANCE.clear();
      Prepare.TRIM = false;
    }
  }

  /** Runs the same test as {@link #testFilterQueryOnProjectView()} but more
   * concisely.*/
  @Test public void testFilterQueryOnProjectView0() {
    checkMaterialize(
        "select \"deptno\", \"empid\" from \"emps\"",
        "select \"empid\" + 1 as x from \"emps\" where \"deptno\" = 10");
  }

  /** As {@link #testFilterQueryOnProjectView()} but with extra column in
   * materialized view.*/
  @Test public void testFilterQueryOnProjectView1() {
    checkMaterialize(
        "select \"deptno\", \"empid\", \"name\" from \"emps\"",
        "select \"empid\" + 1 as x from \"emps\" where \"deptno\" = 10");
  }

  /** As {@link #testFilterQueryOnProjectView()} but with extra column in both
   * materialized view and query.*/
  @Test public void testFilterQueryOnProjectView2() {
    checkMaterialize(
        "select \"deptno\", \"empid\", \"name\" from \"emps\"",
        "select \"empid\" + 1 as x, \"name\" from \"emps\" where \"deptno\" = 10");
  }

  /** As {@link #testFilterQueryOnProjectView()} but materialized view contains
   * an expression and query.*/
  @Ignore
  @Test public void testFilterQueryOnProjectView3() {
    checkMaterialize(
        "select \"deptno\" - 10 as \"x\", \"empid\" + 1, \"name\" from \"emps\"",
        "select \"name\" from \"emps\" where \"deptno\" - 10 = 0");
  }

  /** As {@link #testFilterQueryOnProjectView3()} but materialized view cannot
   * be used because it does not contain required expression.*/
  @Test public void testFilterQueryOnProjectView4() {
    checkNoMaterialize(
        "select \"deptno\" - 10 as \"x\", \"empid\" + 1, \"name\" from \"emps\"",
        "select \"name\" from \"emps\" where \"deptno\" + 10 = 20");
  }

  @Test public void testFilterQueryOnFilterView() {
    checkMaterialize(
        "select \"deptno\", \"empid\", \"name\" from \"emps\" where \"deptno\" = 10",
        "select \"empid\" + 1 as x, \"name\" from \"emps\" where \"deptno\" = 10");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query. */
  @Ignore
  @Test public void testFilterQueryOnFilterView2() {
    checkMaterialize(
        "select \"deptno\", \"empid\", \"name\" from \"emps\" where \"deptno\" = 10",
        "select \"empid\" + 1 as x, \"name\" from \"emps\" where \"deptno\" = 10 and \"empid\" < 150");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is weaker in
   * view. */
  @Ignore
  @Test public void testFilterQueryOnFilterView3() {
    checkMaterialize(
        "select \"deptno\", \"empid\", \"name\" from \"emps\" where \"deptno\" = 10 or \"empid\" < 150",
        "select \"empid\" + 1 as x, \"name\" from \"emps\" where \"deptno\" = 10");
  }

  @Ignore
  @Test public void testSwapJoin() {
    String q1 =
        "select count(*) as c from \"foodmart\".\"sales_fact_1997\" as s join \"foodmart\".\"time_by_day\" as t on s.\"time_id\" = t.\"time_id\"";
    String q2 =
        "select count(*) as c from \"foodmart\".\"time_by_day\" as t join \"foodmart\".\"sales_fact_1997\" as s on t.\"time_id\" = s.\"time_id\"";
  }

  @Ignore
  @Test public void testDifferentColumnNames() {}

  @Ignore
  @Test public void testDifferentType() {}

  @Ignore
  @Test public void testPartialUnion() {}

  @Ignore
  @Test public void testNonDisjointUnion() {}

  @Ignore
  @Test public void testMaterializationReferencesTableInOtherSchema() {}

  /** Unit test for logic functions
   * {@link org.eigenbase.relopt.SubstitutionVisitor#mayBeSatisfiable} and
   * {@link org.eigenbase.relopt.SubstitutionVisitor#simplify}. */
  @Test public void testSatisfiable() {
    // TRUE may be satisfiable
    checkSatisfiable(rexBuilder.makeLiteral(true), "true");

    // FALSE is not satisfiable
    checkNotSatisfiable(rexBuilder.makeLiteral(false));

    // The expression "$0 = 1".
    final RexNode i0_eq_0 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.equalsOperator,
            rexBuilder.makeInputRef(
                typeFactory.createType(int.class), 0),
            rexBuilder.makeExactLiteral(BigDecimal.ZERO));

    // "$0 = 1" may be satisfiable
    checkSatisfiable(i0_eq_0, "=($0, 0)");

    // "$0 = 1 AND TRUE" may be satisfiable
    final RexNode e0 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.andOperator,
            i0_eq_0,
            rexBuilder.makeLiteral(true));
    checkSatisfiable(e0, "=($0, 0)");

    // "$0 = 1 AND FALSE" is not satisfiable
    final RexNode e1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.andOperator,
            i0_eq_0,
            rexBuilder.makeLiteral(false));
    checkNotSatisfiable(e1);

    // "$0 = 0 AND NOT $0 = 0" is not satisfiable
    final RexNode e2 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.andOperator,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.notOperator,
                i0_eq_0));
    checkNotSatisfiable(e2);

    // "TRUE AND NOT $0 = 0" may be satisfiable. Can simplify.
    final RexNode e3 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.andOperator,
            rexBuilder.makeLiteral(true),
            rexBuilder.makeCall(
                SqlStdOperatorTable.notOperator,
                i0_eq_0));
    checkSatisfiable(e3, "NOT(=($0, 0))");

    // The expression "$1 = 1".
    final RexNode i1_eq_1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.equalsOperator,
            rexBuilder.makeInputRef(
                typeFactory.createType(int.class), 1),
            rexBuilder.makeExactLiteral(BigDecimal.ONE));

    // "$0 = 0 AND $1 = 1 AND NOT $0 = 0" is not satisfiable
    final RexNode e4 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.andOperator,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator,
                i1_eq_1,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.notOperator, i0_eq_0)));
    checkNotSatisfiable(e4);

    // "$0 = 0 AND NOT $1 = 1" may be satisfiable. Can't simplify.
    final RexNode e5 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.andOperator,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.notOperator,
                i1_eq_1));
    checkSatisfiable(e5, "AND(=($0, 0), NOT(=($1, 1)))");

    // "$0 = 0 AND NOT ($0 = 0 AND $1 = 1)" may be satisfiable. Can simplify.
    final RexNode e6 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.andOperator,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.notOperator,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator,
                    i0_eq_0,
                    i1_eq_1)));
    checkSatisfiable(e6, "AND(=($0, 0), NOT(AND(=($0, 0), =($1, 1))))");

    // "$0 = 0 AND ($1 = 1 AND NOT ($0 = 0))" is not satisfiable.
    final RexNode e7 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.andOperator,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator,
                i1_eq_1,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.notOperator,
                    i0_eq_0)));
    checkNotSatisfiable(e7);

    // The expression "$2".
    final RexInputRef i2 =
        rexBuilder.makeInputRef(
            typeFactory.createType(boolean.class), 2);

    // The expression "$3".
    final RexInputRef i3 =
        rexBuilder.makeInputRef(
            typeFactory.createType(boolean.class), 3);

    // The expression "$4".
    final RexInputRef i4 =
        rexBuilder.makeInputRef(
            typeFactory.createType(boolean.class), 4);

    // "$0 = 0 AND $2 AND $3 AND NOT ($2 AND $3 AND $4) AND NOT ($2 AND $4)" may
    // be satisfiable. Can't simplify.
    final RexNode e8 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.andOperator,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator,
                i2,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator,
                    i3,
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.notOperator,
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.andOperator,
                            i2,
                            i3,
                            i4)),
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.notOperator,
                        i4))));
    checkSatisfiable(e8,
        "AND(AND(AND(AND(=($0, 0), $2), $3), NOT(AND($2, $3, $4))), NOT($4))");
  }

  private void checkNotSatisfiable(RexNode e) {
    assertFalse(SubstitutionVisitor.mayBeSatisfiable(e));
    final RexNode simple = SubstitutionVisitor.simplify(rexBuilder, e);
    assertFalse(RexLiteral.booleanValue(simple));
  }

  private void checkSatisfiable(RexNode e, String s) {
    assertTrue(SubstitutionVisitor.mayBeSatisfiable(e));
    final RexNode simple = SubstitutionVisitor.simplify(rexBuilder, e);
    assertEquals(s, simple.toString());
  }
}

// End MaterializationTest.java
