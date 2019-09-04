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

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Closer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

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

  protected Tester createDynamicTester() {
    return getTesterWithDynamicTable();
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
   * Checks the plan for a SQL statement before/after executing a given rule.
   *
   * @param rule Planner rule
   * @param sql  SQL query
   */
  protected void checkPlanningDynamic(
      RelOptRule rule,
      String sql) {
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleInstance(rule);

    checkPlanning(
        createDynamicTester(),
        null,
        new HepPlanner(programBuilder.build()),
        sql);
  }
  /**
   * Checks the plan for a SQL statement before/after executing a given rule.
   *
   * @param sql  SQL query
   */
  protected void checkPlanningDynamic(
      String sql) {
    checkPlanning(
        createDynamicTester(),
        null,
        new HepPlanner(HepProgram.builder().build()),
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
   * program, but with specified implicit type coercion flag.
   *
   * @param program      Planner program
   * @param sql          SQL query
   * @param typeCoercion Whether to open up the implicit type coercion
   */
  protected void checkPlanning(HepProgram program, String sql, boolean typeCoercion) {
    final Tester tester = typeCoercion ? this.tester : this.strictTester;
    checkPlanning(tester, null, new HepPlanner(program), sql);
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

    List<RelMetadataProvider> list = new ArrayList<>();
    list.add(DefaultRelMetadataProvider.INSTANCE);
    planner.registerMetadataProviders(list);
    RelMetadataProvider plannerChain =
        ChainedRelMetadataProvider.of(list);
    final RelOptCluster cluster = relInitial.getCluster();
    cluster.setMetadataProvider(plannerChain);

    RelNode relBefore;
    if (preProgram == null) {
      relBefore = relInitial;
    } else {
      HepPlanner prePlanner = new HepPlanner(preProgram);
      prePlanner.setRoot(relInitial);
      relBefore = prePlanner.findBestExp();
    }

    assertThat(relBefore, notNullValue());

    final String planBefore = NL + RelOptUtil.toString(relBefore);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    SqlToRelTestBase.assertValid(relBefore);

    planner.setRoot(relBefore);
    RelNode r = planner.findBestExp();
    if (tester.isLateDecorrelate()) {
      final String planMid = NL + RelOptUtil.toString(r);
      diffRepos.assertEquals("planMid", "${planMid}", planMid);
      SqlToRelTestBase.assertValid(r);
      final RelBuilder relBuilder =
          RelFactories.LOGICAL_BUILDER.create(cluster, null);
      r = RelDecorrelator.decorrelateQuery(r, relBuilder);
    }
    final String planAfter = NL + RelOptUtil.toString(r);
    if (unchanged) {
      assertThat(planAfter, is(planBefore));
    } else {
      diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
      if (planBefore.equals(planAfter)) {
        throw new AssertionError("Expected plan before and after is the same.\n"
            + "You must use unchanged=true or call checkUnchanged");
      }
    }
    SqlToRelTestBase.assertValid(r);
  }

  /** Sets the SQL statement for a test. */
  Sql sql(String sql) {
    return new Sql(sql, null, null,
        ImmutableMap.of(), ImmutableList.of());
  }

  /** Allows fluent testing. */
  class Sql {
    private final String sql;
    private HepProgram preProgram;
    private final HepPlanner hepPlanner;
    private final ImmutableMap<Hook, Consumer> hooks;
    private ImmutableList<Function<Tester, Tester>> transforms;

    Sql(String sql, HepProgram preProgram, HepPlanner hepPlanner,
        ImmutableMap<Hook, Consumer> hooks,
        ImmutableList<Function<Tester, Tester>> transforms) {
      this.sql = sql;
      this.preProgram = preProgram;
      this.hepPlanner = hepPlanner;
      this.hooks = hooks;
      this.transforms = transforms;
    }

    public Sql withPre(HepProgram preProgram) {
      return new Sql(sql, preProgram, hepPlanner, hooks, transforms);
    }

    public Sql with(HepPlanner hepPlanner) {
      return new Sql(sql, preProgram, hepPlanner, hooks, transforms);
    }

    public Sql with(HepProgram program) {
      return new Sql(sql, preProgram, new HepPlanner(program), hooks,
          transforms);
    }

    public Sql withRule(RelOptRule... rules) {
      final HepProgramBuilder builder = HepProgram.builder();
      for (RelOptRule rule : rules) {
        builder.addRuleInstance(rule);
      }
      return with(builder.build());
    }

    /** Adds a transform that will be applied to {@link #tester}
     * just before running the query. */
    private Sql withTransform(Function<Tester, Tester> transform) {
      return new Sql(sql, preProgram, hepPlanner, hooks,
          FlatLists.append(transforms, transform));
    }

    /** Adds a hook and a handler for that hook. Calcite will create a thread
     * hook (by calling {@link Hook#addThread(Consumer)})
     * just before running the query, and remove the hook afterwards. */
    public <T> Sql withHook(Hook hook, Consumer<T> handler) {
      return new Sql(sql, preProgram, hepPlanner,
          FlatLists.append(hooks, hook, handler), transforms);
    }

    /** @deprecated Use {@link #withHook(Hook, Consumer)}. */
    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public <T> Sql withHook(Hook hook,
        com.google.common.base.Function<T, Void> handler) {
      return withHook(hook, (Consumer<T>) handler::apply);
    }

    public <V> Sql withProperty(Hook hook, V value) {
      return withHook(hook, Hook.propertyJ(value));
    }

    public Sql expand(final boolean b) {
      return withTransform(tester -> tester.withExpand(b));
    }

    public Sql withLateDecorrelation(final boolean b) {
      return withTransform(tester -> tester.withLateDecorrelation(b));
    }

    public Sql withDecorrelation(final boolean b) {
      return withTransform(tester -> tester.withDecorrelation(b));
    }

    public Sql withTrim(final boolean b) {
      return withTransform(tester -> tester.withTrim(b));
    }

    public Sql withContext(final Context context) {
      return withTransform(tester -> tester.withContext(context));
    }

    public void check() {
      check(false);
    }

    public void checkUnchanged() {
      check(true);
    }

    @SuppressWarnings("unchecked")
    private void check(boolean unchanged) {
      try (Closer closer = new Closer()) {
        for (Map.Entry<Hook, Consumer> entry : hooks.entrySet()) {
          closer.add(entry.getKey().addThread(entry.getValue()));
        }
        Tester t = tester;
        for (Function<Tester, Tester> transform : transforms) {
          t = transform.apply(t);
        }
        checkPlanning(t, preProgram, hepPlanner, sql, unchanged);
      }
    }
  }

}

// End RelOptTestBase.java
