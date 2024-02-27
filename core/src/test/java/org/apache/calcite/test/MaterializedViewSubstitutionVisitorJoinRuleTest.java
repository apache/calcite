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

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit test for {@link org.apache.calcite.plan.JoinUnifyRule}.
 */
public class MaterializedViewSubstitutionVisitorJoinRuleTest {

  private static final HepProgram HEP_PROGRAM =
      new HepProgramBuilder()
          .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
          .addRuleInstance(CoreRules.FILTER_MERGE)
          .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
          .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
          .addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
          .addRuleInstance(CoreRules.PROJECT_MERGE)
          .addRuleInstance(CoreRules.PROJECT_REMOVE)
          .addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE)
          .addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE)
          .addRuleInstance(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS)
          .addRuleInstance(CoreRules.FILTER_TO_CALC)
          .addRuleInstance(CoreRules.PROJECT_TO_CALC)
          .addRuleInstance(CoreRules.FILTER_CALC_MERGE)
          .addRuleInstance(CoreRules.PROJECT_CALC_MERGE)
          .addRuleInstance(CoreRules.CALC_MERGE)
          .build();

  public static final MaterializedViewTester TESTER =
      new MaterializedViewTester() {
        @Override protected List<RelNode> optimize(RelNode queryRel,
            List<RelOptMaterialization> materializationList) {
          RelOptMaterialization materialization = materializationList.get(0);
          SubstitutionVisitor substitutionVisitor =
              new SubstitutionVisitor(canonicalize(materialization.queryRel),
                  canonicalize(queryRel));
          return substitutionVisitor
              .go(materialization.tableRel);
        }

        private RelNode canonicalize(RelNode rel) {
          final HepPlanner hepPlanner = new HepPlanner(HEP_PROGRAM);
          hepPlanner.setRoot(rel);
          return hepPlanner.findBestExp();
        }
      };

  /**
   * Creates a fixture.
   */
  protected MaterializedViewFixture fixture(String query) {
    return MaterializedViewFixture.create(query, TESTER);
  }

  /**
   * Creates a fixture with a given query.
   */
  protected final MaterializedViewFixture sql(String materialize,
      String query) {
    return fixture(query)
        .withMaterializations(ImmutableList.of(Pair.of(materialize, "MV0")));
  }

  // INNER TO LEFT
  /**
   * apply JoinExtendTrivialRule.
   */
  @Test public void testInnerToLeft0() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" left join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" inner join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    sql(mv, query).ok();
  }

  /**
   * apply JoinOnLeftCalcToJoinUnifyRule.
   */
  @Test public void testInnerToLeft1() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" left join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"A\".\"empid\", \"A\".\"deptno\", \"depts\".\"deptno\" from\n"
        + " (select \"empid\", \"deptno\" from \"emps\" where \"deptno\" > 10) A"
        + " join \"depts\"\n"
        + "on \"A\".\"deptno\" = \"depts\".\"deptno\"";
    sql(mv, query).ok();
  }

  /**
   * apply JoinOnRightCalcToJoinUnifyRule.
   */
  @Test public void testInnerToLeft2() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" left join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"B\".\"deptno\" from\n"
        + "\"emps\" join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" > 10) B\n"
        + "on \"emps\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).ok();
  }

  /**
   * apply JoinOnCalcsToJoinUnifyRule.
   */
  @Test public void testInnerToLeft3() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" left join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "(select \"empid\", \"deptno\" from \"emps\" where \"empid\" > 10) A\n"
        + "join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" > 10) B\n"
        + "on \"A\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).ok();
  }

  /**
   * apply JoinOnCalcsToJoinUnifyRule.
   */
  @Test public void testInnerToLeft4() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" left join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "(select \"empid\", \"deptno\", \"empid\" + 1 from \"emps\" where \"empid\" > 10) A\n"
        + "join\n"
        + "(select \"deptno\" , \"deptno\" + 1 from \"depts\" where \"deptno\" > 10) B\n"
        + "on \"A\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).ok();
  }

  // INNER TO RIGHT
  /**
   * apply JoinExtendTrivialRule.
   */
  @Test public void testInnerToRight0() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" right join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", \"depts\".\"deptno\" from\n"
        + "\"emps\" inner join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    sql(mv, query).ok();
  }

  /**
   * apply JoinOnLeftCalcToJoinUnifyRule.
   */
  @Test public void testInnerToRight1() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" left join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"A\".\"empid\", \"A\".\"deptno\", \"depts\".\"deptno\" from\n"
        + " (select \"empid\", \"deptno\" from \"emps\" where \"deptno\" > 10) A"
        + " join \"depts\"\n"
        + "on \"A\".\"deptno\" = \"depts\".\"deptno\"";
    sql(mv, query).ok();
  }

  /**
   * apply JoinOnRightCalcToJoinUnifyRule.
   */
  @Test public void testInnerToRight2() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" right join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"B\".\"deptno\" from\n"
        + "\"emps\" join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" > 10) B\n"
        + "on \"emps\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).ok();
  }

  /**
   * apply JoinOnCalcToJoinUnifyRule.
   */
  @Test public void testInnerToRight3() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" right join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "(select \"empid\", \"deptno\" from \"emps\" where \"empid\" > 10) A\n"
        + "join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" > 10) B\n"
        + "on \"A\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).ok();
  }

  /**
   * apply JoinExtendTrivialRule,
   * As {@link #testInnerToRight0()} but use relation key.
   */
  @Test public void testInnerToRight5() {
    String mv = ""
        + "select \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" right join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" inner join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    sql(mv, query).ok();
  }

  // LEFT TO FULL
  @Test public void testLeftToFull0() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" full join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"A\".\"empid\", \"A\".\"deptno\", \"depts\".\"deptno\" from\n"
        + " (select \"empid\", \"deptno\" from \"emps\" where \"deptno\" > 10) A"
        + " left join \"depts\"\n"
        + "on \"A\".\"deptno\" = \"depts\".\"deptno\"";
    sql(mv, query).ok();
  }

  @Test public void testLeftToFull1() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" full join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"B\".\"deptno\" from\n"
        + "\"emps\" left join \n "
        + "(select \"deptno\" from \"depts\" ) B\n"
        + "on \"emps\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).ok();
  }

  @Test public void testLeftToFull2() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" full join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" left join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    sql(mv, query).ok();
  }

  @Test public void testLeftToFull3() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" full join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\",  \"emps\".\"empid\" + 1 , \"emps\".\"deptno\", \"B\""
        + ".\"deptno\" from\n"
        + "\"emps\" left join \n "
        + "(select \"deptno\" from \"depts\" ) B\n"
        + "on \"emps\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).ok();
  }

  @Test public void testLeftToFull4() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" full join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", \"emps\".\"empid\" + 1, \"emps\".\"deptno\", \"depts\""
        + ".\"deptno\" from\n"
        + "\"emps\" left join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    sql(mv, query).ok();
  }

  // RIGHT TO FULL
  @Test public void testRightToFull0() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" full join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"B\".\"deptno\" from\n"
        + "\"emps\" right join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" > 10) B\n"
        + "on \"emps\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).ok();
  }

  @Test public void testRightToFull1() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" full join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" right join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    sql(mv, query).ok();
  }

  // INNER TO FULL
  @Test public void testInnerToFull0() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" full join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"A\".\"empid\", cc , \"A\".\"deptno\", \"depts\".\"deptno\" from\n"
        + " (select \"empid\", \"empid\" + 1 as cc , \"deptno\" from \"emps\") A "
        + "inner join \"depts\"\n"
        + "on \"A\".\"deptno\" = \"depts\".\"deptno\"";
    sql(mv, query).ok();
  }

  @Test public void testInnerToFull1() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" full join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", cc , \"emps\".\"deptno\", \"B\".\"deptno\" from\n"
        + "\"emps\" inner join\n"
        + "(select \"deptno\", \"deptno\"+1 as cc from \"depts\") B\n"
        + "on \"emps\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).ok();
  }

  @Test public void testInnerToFull2() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" full join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" inner join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    sql(mv, query).ok();
  }
}
