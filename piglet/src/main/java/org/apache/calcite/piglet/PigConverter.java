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
package org.apache.calcite.piglet;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreterRule;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.ToLogicalConverter;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSets;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Extension from PigServer to convert Pig scripts into logical relational
 * algebra plans and SQL statements.
 */
public class PigConverter extends PigServer {
  // Basic transformation and implementation rules to optimize for Pig-translated logical plans
  private static final List<RelOptRule> PIG_RULES =
      ImmutableList.of(
          ProjectToWindowRule.PROJECT,
          PigToSqlAggregateRule.INSTANCE,
          EnumerableRules.ENUMERABLE_VALUES_RULE,
          EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_CORRELATE_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE,
          EnumerableRules.ENUMERABLE_FILTER_RULE,
          EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_LIMIT_RULE,
          EnumerableRules.ENUMERABLE_COLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNION_RULE,
          EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
          EnumerableInterpreterRule.INSTANCE);

  private static final List<RelOptRule> TRANSFORM_RULES =
      ImmutableList.of(
          ProjectWindowTransposeRule.INSTANCE,
          FilterMergeRule.INSTANCE,
          ProjectMergeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          EnumerableRules.ENUMERABLE_VALUES_RULE,
          EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_CORRELATE_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE,
          EnumerableRules.ENUMERABLE_FILTER_RULE,
          EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_LIMIT_RULE,
          EnumerableRules.ENUMERABLE_COLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNION_RULE,
          EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
          EnumerableInterpreterRule.INSTANCE);

  private final PigRelBuilder builder;

  private PigConverter(FrameworkConfig config, ExecType execType)
      throws Exception {
    super(execType);
    this.builder = PigRelBuilder.create(config);
  }

  public static PigConverter create(FrameworkConfig config) throws Exception {
    return new PigConverter(config, ExecType.LOCAL);
  }

  public PigRelBuilder getBuilder() {
    return builder;
  }

  /**
   * Parses a Pig script and converts it into relational algebra plans,
   * optimizing the result.
   *
   * <p>Equivalent to {@code pigQuery2Rel(pigQuery, true, true, true)}.
   *
   * @param pigQuery Pig script
   *
   * @return A list of root nodes of the translated relational plans. Each of
   * these root corresponds to a sink operator (normally a STORE command) in the
   * Pig plan
   *
   * @throws IOException Exception during parsing or translating Pig
   */
  public List<RelNode> pigQuery2Rel(String pigQuery) throws IOException {
    return pigQuery2Rel(pigQuery, true, true, true);
  }

  /**
   * Parses a Pig script and converts it into relational algebra plans.
   *
   * @param pigQuery Pig script
   * @param planRewrite Whether to rewrite the translated plan
   * @param validate Whether to validate the Pig logical plan before doing
   *   translation
   * @param usePigRules Whether to use Pig Rules (see PigRelPlanner} to rewrite
   *   translated rel plan
   *
   * @return A list of root nodes of the translated relational plans. Each of
   * these root corresponds to a sink operator (normally a STORE command) in the
   * Pig plan
   *
   * @throws IOException Exception during parsing or translating Pig
   */
  public List<RelNode> pigQuery2Rel(String pigQuery, boolean planRewrite,
      boolean validate, boolean usePigRules) throws IOException {
    setBatchOn();
    registerQuery(pigQuery);
    final LogicalPlan pigPlan = getCurrentDAG().getLogicalPlan();
    if (validate) {
      pigPlan.validate(getPigContext(), scope, false);
    }
    return pigPlan2Rel(pigPlan, planRewrite, usePigRules);
  }

  /**
   * Gets a Pig script string from a file after doing param substitution.
   *
   * @param in Pig script file
   * @param params Param sub map
   */
  public String getPigScript(InputStream in, Map<String, String> params)
      throws IOException {
    return getPigContext().doParamSubstitution(in, paramMapToList(params), null);
  }

  /**
   * Parses a Pig script and converts it into relational algebra plans.
   *
   * @param fileName File name
   * @param params Param substitution map
   * @param planRewrite Whether to rewrite the translated plan
   *
   * @return A list of root nodes of the translated relational plans. Each of
   * these root corresponds to a sink operator (normally a STORE command) in the
   * Pig plan
   *
   * @throws IOException Exception during parsing or translating Pig
   */
  public List<RelNode> pigScript2Rel(String fileName, Map<String, String> params,
      boolean planRewrite) throws IOException {
    setBatchOn();
    registerScript(fileName, params);
    final LogicalPlan pigPlan = getCurrentDAG().getLogicalPlan();
    pigPlan.validate(getPigContext(), scope, false);

    return pigPlan2Rel(pigPlan, planRewrite, true);
  }

  private List<RelNode> pigPlan2Rel(LogicalPlan pigPlan, boolean planRewrite,
      boolean usePigRules) throws FrontendException {
    final PigRelOpWalker walker = new PigRelOpWalker(pigPlan);
    List<RelNode> relNodes =
        new PigRelOpVisitor(pigPlan, walker, builder).translate();
    final List<RelNode> storeRels = builder.getRelsForStores();
    relNodes = storeRels != null ? storeRels : relNodes;

    if (usePigRules) {
      relNodes = optimizePlans(relNodes, PIG_RULES);
    }
    if (planRewrite) {
      relNodes = optimizePlans(relNodes, TRANSFORM_RULES);
    }
    return relNodes;
  }

  /**
   * Converts a Pig script to a list of SQL statements.
   *
   * @param pigQuery Pig script
   * @param sqlDialect Dialect of SQL language
   * @throws IOException Exception during parsing or translating Pig
   */
  public List<String> pigToSql(String pigQuery, SqlDialect sqlDialect)
      throws IOException {
    final SqlPrettyWriter writer = new SqlPrettyWriter(sqlDialect);
    writer.setQuoteAllIdentifiers(false);
    writer.setAlwaysUseParentheses(false);
    writer.setSelectListItemsOnSeparateLines(false);
    writer.setIndentation(2);
    return pigToSql(pigQuery, writer);
  }

  /**
   * Converts a Pig script to a list of SQL statements.
   *
   * @param pigQuery Pig script
   * @param writer The SQL writer to decide dialect and format of SQL statements
   * @throws IOException Exception during parsing or translating Pig
   */
  private List<String> pigToSql(String pigQuery, SqlWriter writer)
      throws IOException {
    final RelToSqlConverter sqlConverter =
        new PigRelToSqlConverter(writer.getDialect());
    final List<RelNode> finalRels = pigQuery2Rel(pigQuery);
    final List<String> sqlStatements = new ArrayList<>();
    for (RelNode rel : finalRels) {
      final SqlNode sqlNode = sqlConverter.visitChild(0, rel).asStatement();
      sqlNode.unparse(writer, 0, 0);
      sqlStatements.add(writer.toString());
    }
    return sqlStatements;
  }

  private List<RelNode> optimizePlans(List<RelNode> originalRels,
      List<RelOptRule> rules) {
    final RelOptPlanner planner = originalRels.get(0).getCluster().getPlanner();
    // Remember old rule set of the planner before resetting it with new rules
    final List<RelOptRule> oldRules = planner.getRules();
    resetPlannerRules(planner, rules);
    final Program program = Programs.of(RuleSets.ofList(planner.getRules()));
    final List<RelNode> optimizedPlans = new ArrayList<>();
    for (RelNode rel : originalRels) {
      final RelCollation collation = rel instanceof Sort
          ? ((Sort) rel).collation
          : RelCollations.EMPTY;
      // Apply the planner to obtain the physical plan
      final RelNode physicalPlan = program.run(planner, rel,
          rel.getTraitSet().replace(EnumerableConvention.INSTANCE)
              .replace(collation).simplify(),
          ImmutableList.of(), ImmutableList.of());

      // Then convert the physical plan back to logical plan
      final RelNode logicalPlan = new ToLogicalConverter(builder).visit(physicalPlan);
      optimizedPlans.add(logicalPlan);
    }
    resetPlannerRules(planner, oldRules);
    return optimizedPlans;
  }

  private void resetPlannerRules(RelOptPlanner planner,
      List<RelOptRule> rulesToSet) {
    planner.clear();
    for (RelOptRule rule : rulesToSet) {
      planner.addRule(rule);
    }
  }
}

// End PigConverter.java
