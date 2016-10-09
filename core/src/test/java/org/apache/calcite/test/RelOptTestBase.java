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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.Holder;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * RelOptTestBase is an abstract base for tests which exercise a planner and/or
 * rules via {@link DiffRepository}.
 */
abstract class RelOptTestBase extends SqlToRelTestBase {
  //~ Methods ----------------------------------------------------------------

  @Override protected Tester createTester() {
    return super.createTester().withDecorrelation(false);
  }

  /**
   * Checks the plan for a SQL statement before/after executing a given rule.
   *
   * @param rule Planner rule
   * @param sql  SQL query
   */
  protected void checkPlanning(
      RelOptRule rule,
      String sql) {
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleInstance(rule);

    checkPlanning(
        programBuilder.build(),
        sql);
  }

  /**
   * Checks the plan for a SQL statement before/after executing a given
   * program.
   *
   * @param program Planner program
   * @param sql     SQL query
   */
  protected void checkPlanning(HepProgram program, String sql) {
    checkPlanning(new HepPlanner(program), sql);
  }

  /**
   * Checks the plan for a SQL statement before/after executing a given
   * planner.
   *
   * @param planner Planner
   * @param sql     SQL query
   */
  protected void checkPlanning(RelOptPlanner planner, String sql) {
    checkPlanning(tester, null, planner, sql);
  }

  /**
   * Checks that the plan is the same before and after executing a given
   * planner. Useful for checking circumstances where rules should not fire.
   *
   * @param planner Planner
   * @param sql     SQL query
   */
  protected void checkPlanUnchanged(RelOptPlanner planner, String sql) {
    checkPlanning(tester, null, planner, sql, true);
  }

  /**
   * Checks the plan for a SQL statement before/after executing a given rule,
   * with a pre-program to prepare the tree.
   *
   * @param tester     Tester
   * @param preProgram Program to execute before comparing before state
   * @param planner    Planner
   * @param sql        SQL query
   */
  protected void checkPlanning(Tester tester, HepProgram preProgram,
      RelOptPlanner planner, String sql) {
    checkPlanning(tester, preProgram, planner, sql, false);
  }

  /**
   * Checks the plan for a SQL statement before/after executing a given rule,
   * with a pre-program to prepare the tree.
   *
   * @param tester     Tester
   * @param preProgram Program to execute before comparing before state
   * @param planner    Planner
   * @param sql        SQL query
   * @param unchanged  Whether the rule is to have no effect
   */
  protected void checkPlanning(Tester tester, HepProgram preProgram,
      RelOptPlanner planner, String sql, boolean unchanged) {
    final DiffRepository diffRepos = getDiffRepos();
    String sql2 = diffRepos.expand("sql", sql);
    final RelRoot root = tester.convertSqlToRel(sql2);
    final RelNode relInitial = root.rel;

    assertTrue(relInitial != null);

    List<RelMetadataProvider> list = Lists.newArrayList();
    list.add(DefaultRelMetadataProvider.INSTANCE);
    planner.registerMetadataProviders(list);
    RelMetadataProvider plannerChain =
        ChainedRelMetadataProvider.of(list);
    relInitial.getCluster().setMetadataProvider(plannerChain);

    RelNode relBefore;
    if (preProgram == null) {
      relBefore = relInitial;
    } else {
      HepPlanner prePlanner = new HepPlanner(preProgram);
      prePlanner.setRoot(relInitial);
      relBefore = prePlanner.findBestExp();
    }

    assertThat(relBefore, notNullValue());

    String planBefore = NL + RelOptUtil.toString(relBefore);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    SqlToRelTestBase.assertValid(relBefore);

    planner.setRoot(relBefore);
    RelNode relAfter = planner.findBestExp();

    String planAfter = NL + RelOptUtil.toString(relAfter);
    if (unchanged) {
      assertThat(planAfter, is(planBefore));
    } else {
      diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
      if (planBefore.equals(planAfter)) {
        throw new AssertionError("Expected plan before and after is the same.\n"
            + "You must use unchanged=true or call checkPlanUnchanged");
      }
    }
    SqlToRelTestBase.assertValid(relAfter);
  }

  /** Sets the SQL statement for a test. */
  Sql sql(String sql) {
    return new Sql(sql, null, true, ImmutableMap.<Hook, Function>of());
  }

  /** Allows fluent testing. */
  class Sql {
    private final String sql;
    private final HepPlanner hepPlanner;
    private final boolean expand;
    private final ImmutableMap<Hook, Function> hooks;

    Sql(String sql, HepPlanner hepPlanner, boolean expand,
        ImmutableMap<Hook, Function> hooks) {
      this.sql = sql;
      this.hepPlanner = hepPlanner;
      this.expand = expand;
      this.hooks = hooks;
    }

    public Sql with(HepPlanner hepPlanner) {
      return new Sql(sql, hepPlanner, expand, hooks);
    }

    public Sql with(HepProgram program) {
      return new Sql(sql, new HepPlanner(program), expand, hooks);
    }

    /** Adds a hook and a handler for that hook. Calcite will create a thread
     * hook (by calling {@link Hook#addThread(com.google.common.base.Function)})
     * just before running the query, and remove the hook afterwards. */
    public <T> Sql withHook(Hook hook, Function<T, Void> handler) {
      return new Sql(sql, hepPlanner, expand,
          ImmutableMap.<Hook, Function>builder().putAll(hooks)
              .put(hook, handler).build());
    }

    /** Returns a function that, when a hook is called, will "return" a given
     * value. (Because of the way hooks work, it "returns" the value by writing
     * into a {@link Holder}. */
    private <V> Function<Holder<V>, Void> propertyHook(final V v) {
      return new Function<Holder<V>, Void>() {
        public Void apply(Holder<V> holder) {
          holder.set(v);
          return null;
        }
      };
    }

    public <V> Sql withProperty(Hook hook, V value) {
      return withHook(hook, propertyHook(value));
    }

    public Sql expand(boolean expand) {
      return new Sql(sql, hepPlanner, expand, hooks);
    }

    public void check() {
      check(false);
    }

    public void checkUnchanged() {
      check(true);
    }

    private void check(boolean unchanged) {
      final List<Hook.Closeable> closeables = new ArrayList<>();
      try {
        for (Map.Entry<Hook, Function> entry : hooks.entrySet()) {
          closeables.add(entry.getKey().addThread(entry.getValue()));
        }
        checkPlanning(tester.withExpand(expand), null, hepPlanner, sql,
            unchanged);
      } finally {
        for (Hook.Closeable closeable : closeables) {
          closeable.close();
        }
      }
    }
  }
}

// End RelOptTestBase.java
