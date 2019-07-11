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
import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
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
  private final LogicalPigRelBuilder builder;

  public PigConverter(LogicalPigRelBuilder builder) throws Exception {
    this(builder, ExecType.LOCAL);
  }

  PigConverter(LogicalPigRelBuilder builder, ExecType execType) throws Exception {
    super(execType);
    this.builder = builder;
    builder.clear();
  }

  public PigConverter() throws Exception {
    this(ExecType.LOCAL);
  }

  public PigConverter(ExecType execType) throws Exception {
    this(LogicalPigRelBuilder.create(), execType);
  }

  public LogicalPigRelBuilder getBuilder() {
    return builder;
  }

  public void setPlanner(RelOptPlanner planner) {
    builder.getCluster().setPlanner(planner);
  }

  /**
   * Parses a Pig script and converts it into relational algebra plans.
   *
   * @param pigQuery String represents a Pig script
   * @return A list of roots of the translated relational plans. Each of these root
   * nodes is corresponding to a sink operator (normally a STORE command) in the Pig plan
   * @throws IOException Exception during parsing or translating Pig.
   */
  public List<RelNode> pigQuery2Rel(String pigQuery) throws IOException {
    return pigQuery2Rel(pigQuery, true);
  }

  /**
   * Parses a Pig script and converts it into relational algebra plans.
   *
   * @param pigQuery String represents a Pig script
   * @param optimized Whether to rewrite the translated plan.
   * @return A list of roots of the translated relational plans. Each of these root
   * nodes is corresponding to a sink operator (normally a STORE command) in the Pig plan
   * @throws IOException Exception during parsing or translating Pig.
   */
  public List<RelNode> pigQuery2Rel(String pigQuery, boolean optimized) throws IOException {
    if (optimized) {
      return pigQuery2Rel(pigQuery, true, true, true);
    }
    return pigQuery2Rel(pigQuery, false, true, false);
  }

  /**
   * Parses a Pig script and converts it into relational algebra plans.
   *
   * @param pigQuery String represents a Pig script
   * @param planRewrite Whether to rewrite the translated plan.
   * @param validate Whether to validate the Pig logical plan before doing translation.
   * @param usePigRules Whether to use Pig Rules (see PigRelPlanner} to rewrite translated rel plan.
   * @return A list of roots of the translated relational plans. Each of these root
   * nodes is corresponding to a sink operator (normally a STORE command) in the Pig plan
   * @throws IOException Exception during parsing or translating Pig.
   */
  public List<RelNode> pigQuery2Rel(String pigQuery, boolean planRewrite, boolean validate,
      boolean usePigRules)
      throws IOException {
    setBatchOn();
    registerQuery(pigQuery);
    final LogicalPlan pigPlan = getCurrentDAG().getLogicalPlan();
    if (validate) {
      pigPlan.validate(getPigContext(), scope, false);
    }
    return pigPlan2Rel(pigPlan, planRewrite, usePigRules);
  }

  /**
   * Gets Pig script string from a file after doing param substitution
   * @param in Pig script file
   * @param params Param sub map
   */
  public String getPigScript(InputStream in, Map<String, String> params) throws IOException {
    return getPigContext().doParamSubstitution(in, paramMapToList(params), null);
  }

  /**
   * Parses a Pig script and converts it into relational algebra plans.
   *
   * @param fileName File name
   * @param params Param substitution map.
   * @param planRewrite Whether to rewrite the translated plan.
   * @return A list of roots of the translated relational plans. Each of these root
   * nodes is corresponding to a sink operator (normally a STORE command) in the Pig plan
   * @throws IOException Exception during parsing or translating Pig.
   */
  public List<RelNode> pigScript2Rel(String fileName, Map<String, String> params,
      boolean planRewrite) throws IOException {
    setBatchOn();
    registerScript(fileName, params);
    final LogicalPlan pigPlan = getCurrentDAG().getLogicalPlan();
    pigPlan.validate(getPigContext(), scope, false);

    return pigPlan2Rel(pigPlan, planRewrite, true);
  }

  private List<RelNode> pigPlan2Rel(LogicalPlan pigPlan, boolean planRewrite, boolean usePigRules)
      throws FrontendException {
    List<RelNode> relNodes =
        new PigRelOpVisitor(pigPlan, new PigRelOpWalker(pigPlan), builder).translate();
    final List<RelNode> storeRels = builder.getRelsForStores();
    relNodes = storeRels != null ? storeRels : relNodes;

    if (usePigRules) {
      relNodes = optimizePlans(relNodes, PigRelPlanner.PIG_RULES);
    }
    if (planRewrite) {
      relNodes = optimizePlans(relNodes, PigRelPlanner.TRANSFORM_RULES);
    }
    return relNodes;
  }

  /**
   * Converts a Pig script to a list of SQL statements.
   *
   * @param pigScript String represents a Pig script
   * @param sqlDialect Dialect of SQL languange
   * @throws IOException Exception during parsing or translating Pig.
   */
  public List<String> pigToSql(String pigScript, SqlDialect sqlDialect) throws IOException {
    final SqlPrettyWriter writer = new SqlPrettyWriter(sqlDialect);
    writer.setQuoteAllIdentifiers(false);
    writer.setAlwaysUseParentheses(false);
    writer.setSelectListItemsOnSeparateLines(false);
    writer.setIndentation(2);
    return pigToSql(pigScript, writer);
  }

  /**
   * Converts a Pig script to a list of SQL statements.
   *
   * @param pigScript String represents a Pig script
   * @param writer The Sql writer to decide dialect and format of SQLs
   * @throws IOException Exception during parsing or translating Pig.
   */
  private List<String> pigToSql(String pigScript, SqlWriter writer) throws IOException {
    final RelToSqlConverter sqlConverter = new PigRelToSqlConverter(writer.getDialect());
    final List<RelNode> finalRels = pigQuery2Rel(pigScript);
    final List<String> sqlStatments = new ArrayList<>();
    for (RelNode rel : finalRels) {
      final SqlNode sqlNode = sqlConverter.visitChild(0, rel).asStatement();
      sqlNode.unparse(writer, 0, 0);
      sqlStatments.add(writer.toString());
    }
    return  sqlStatments;
  }

  private List<RelNode> optimizePlans(List<RelNode> orgionalRels, List<RelOptRule> rules) {
    PigRelPlanner planner = PigRelPlanner.createPlanner(builder.getCluster().getPlanner(), rules);
    setPlanner(planner);
    final Program program = Programs.of(RuleSets.ofList(planner.getRules()));
    final List<RelNode> optimizedPlans = new ArrayList<>();
    for (RelNode rel : orgionalRels) {
      final RelCollation collation = rel instanceof Sort
                                         ? ((Sort) rel).collation
                                         : RelCollations.EMPTY;
      // Apply the planner to obtain the physical plan
      final RelNode physicalPlan = program.run(planner, rel,
          rel.getTraitSet().replace(EnumerableConvention.INSTANCE).replace(collation).simplify(),
          ImmutableList.<RelOptMaterialization>of(), ImmutableList.<RelOptLattice>of());

      // Then convert the physical plan back to logical plan
      final RelNode logicalPlan = new ToLogicalConverter().visit(physicalPlan);
      optimizedPlans.add(logicalPlan);
    }
    return optimizedPlans;
  }

  /**
   * RelNode visitor to convert any rel plan to a logical plan.
   */
  private static class ToLogicalConverter extends RelHomogeneousShuttle {
    @Override public RelNode visit(RelNode relNode) {
      if (relNode instanceof Aggregate) {
        final Aggregate agg = (Aggregate) relNode;
        return LogicalAggregate.create(visit(agg.getInput()), agg.getGroupSet(),
            agg.groupSets, agg.getAggCallList());
      }
      if (relNode instanceof TableScan) {
        final TableScan tableScan = (TableScan) relNode;
        return LogicalTableScan.create(tableScan.getCluster(), tableScan.getTable());
      }
      if (relNode instanceof Filter) {
        final Filter filter = (Filter) relNode;
        return LogicalFilter.create(visit(filter.getInput()), filter.getCondition());
      }
      if (relNode instanceof Project) {
        final Project project = (Project) relNode;
        return LogicalProject.create(visit(project.getInput()), project.getProjects(),
            project.getRowType());
      }
      if (relNode instanceof Join) {
        final Join join = (Join) relNode;
        final RelNode left = visit(join.getLeft());
        final RelNode right = visit(join.getRight());
        return LogicalJoin.create(left, right, join.getCondition(), join.getVariablesSet(),
            join.getJoinType());
      }
      if (relNode instanceof Correlate) {
        final Correlate corr = (Correlate) relNode;
        final RelNode left = visit(corr.getLeft());
        final RelNode right = visit(corr.getRight());
        return LogicalCorrelate.create(left, right, corr.getCorrelationId(),
            corr.getRequiredColumns(), corr.getJoinType());
      }
      if (relNode instanceof Sort) {
        final Sort sort = (Sort) relNode;
        return LogicalSort.create(visit(sort.getInput()), sort.getCollation(), sort.offset,
            sort.fetch);
      }
      if (relNode instanceof Union) {
        final Union union = (Union) relNode;
        final List<RelNode> newInputs = new ArrayList<>();
        for (RelNode rel : union.getInputs()) {
          newInputs.add(visit(rel));
        }
        return LogicalUnion.create(newInputs, union.all);
      }
      if (relNode instanceof Window) {
        final Window window = (Window) relNode;
        final RelNode input = visit(window.getInput());
        return LogicalWindow.create(input.getTraitSet(), input, window.constants,
            window.getRowType(), window.groups);
      }
      if (relNode instanceof Calc) {
        final Calc calc = (Calc) relNode;
        return LogicalCalc.create(visit(calc.getInput()), calc.getProgram());
      }
      if (relNode instanceof EnumerableInterpreter) {
        return visit(((EnumerableInterpreter) relNode).getInput());
      }
      if (relNode instanceof EnumerableLimit) {
        final EnumerableLimit limit = (EnumerableLimit) relNode;
        if (limit.getInput() instanceof Sort) {
          final Sort sort = (Sort) limit.getInput();
          return LogicalSort.create(visit(sort.getInput()), sort.getCollation(), limit.offset,
              limit.fetch);
        } else {
          return LogicalSort.create(visit(limit.getInput()), RelCollations.of(), limit.offset,
              limit.fetch);
        }
      }
      if (relNode instanceof Uncollect) {
        final Uncollect uncollect = (Uncollect) relNode;
        final RelNode input = visit(uncollect.getInput());
        return new Uncollect(input.getCluster(), input.getTraitSet(), input,
            uncollect.withOrdinality);
      }
      if (relNode instanceof Values) {
        final Values values = (Values) relNode;
        return LogicalValues.create(values.getCluster(), values.getRowType(), values.tuples);
      }
      throw new AssertionError("Need to implement " + relNode.getClass().getName());
    }
  }
}

// End PigConverter.java
