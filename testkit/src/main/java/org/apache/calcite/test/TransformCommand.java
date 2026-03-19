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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.TopDownGeneralDecorrelator;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import net.hydromatic.quidem.AbstractCommand;
import net.hydromatic.quidem.Quidem;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

/** Command ({@code !transform}) that prints a plan for a SQL statement,
 * optionally after applying rules.
 *
 * <p>The {@code args} string is a comma-separated list of config tokens and
 * rule names. {@code NONE} is a placeholder meaning "no rule at this position".
 * Config tokens (lowercase-starting) configure the SQL-to-RelNode conversion
 * and {@link org.apache.calcite.tools.RelBuilder RelBuilder} behavior.
 * All uppercase tokens are {@link org.apache.calcite.rel.rules.CoreRules}
 * field names collected into a single {@link HepPlanner} and applied to the
 * initial plan.
 *
 * <p>Supported config tokens:
 * <ul>
 *   <li>{@code aggregateUnique=true} &mdash; GROUP BY without aggregate
 *       functions creates a {@code LogicalAggregate} (not simplified to a
 *       Project); equivalent to
 *       {@code .withRelBuilderConfig(b -> b.withAggregateUnique(true))}
 *   <li>{@code bloat=N} &mdash; allows merging expressions up to N nodes
 *       larger; equivalent to
 *       {@code .withRelBuilderConfig(b -> b.withBloat(N))}
 *   <li>{@code decorrelate=true} &mdash; decorrelates after SQL-to-RelNode
 *       conversion by calling {@link SqlToRelConverter#decorrelate};
 *       equivalent to {@code .withDecorrelate(true)}
 *   <li>{@code expand=true} &mdash; expands sub-queries during
 *       SQL-to-RelNode conversion; equivalent to {@code .withExpand(true)}
 *   <li>{@code inSubQueryThreshold=N} &mdash; sets how many items an IN-list
 *       may have before it is converted to a join/sub-query; equivalent to
 *       {@code .withInSubQueryThreshold(N)}
 *   <li>{@code relBuilderSimplify=false} &mdash; disables
 *       {@link org.apache.calcite.rex.RexSimplify} during RelNode
 *       construction, so CASE/CAST expressions appear unsimplified;
 *       equivalent to {@code .withRelBuilderSimplify(false)}
 *   <li>{@code simplifyValues=false} &mdash; prevents Values rows from
 *       being simplified; equivalent to
 *       {@code .withRelBuilderConfig(b -> b.withSimplifyValues(false))}
 *   <li>{@code lateDecorrelate=true} &mdash; applies
 *       {@link RelDecorrelator#decorrelateQuery} after the main rules;
 *       equivalent to {@code .withLateDecorrelate(true)}
 *   <li>{@code topDownGeneralDecorrelate=true} &mdash; when used with
 *       {@code lateDecorrelate=true}, applies
 *       {@link TopDownGeneralDecorrelator#decorrelateQuery} instead of
 *       {@link RelDecorrelator#decorrelateQuery};
 *       equivalent to {@code .withTopDownGeneralDecorrelate(true)}
 *   <li>{@code operatorTable=BIG_QUERY} &mdash; uses BigQuery operator
 *       table; equivalent to
 *       {@code .withFactory(t -> t.withOperatorTable(o ->
 *       getOperatorTable(SqlLibrary.BIG_QUERY)))}
 *   <li>{@code AggregateExtractProjectRule.SCAN} &mdash; uses the
 *       {@link org.apache.calcite.rel.rules.AggregateExtractProjectRule#SCAN}
 *       rule instance
 *   <li>{@code DateRangeRules.FILTER_INSTANCE} &mdash; uses the
 *       {@link org.apache.calcite.rel.rules.DateRangeRules#FILTER_INSTANCE}
 *       rule instance
 *   <li>{@code MeasureRules.AGGREGATE} and other MeasureRules fields
 *   <li>{@code SingleValuesOptimizationRules.JOIN_LEFT_INSTANCE}
 *       and other SingleValuesOptimizationRules fields
 *   <li>{@code subQueryRules} &mdash; applies
 *       PROJECT/FILTER/JOIN_SUB_QUERY_TO_CORRELATE as pre-rules before the
 *       main rules; equivalent to {@code .withSubQueryRules()}
 *   <li>{@code trim=true} &mdash; trims unused fields from projections;
 *       equivalent to {@code .withTrim(true)}
 * </ul>
 *
 * <p>Examples: {@code !transform "NONE"} (initial plan, no rules),
 * {@code !transform "AGGREGATE_UNION_AGGREGATE"} (plan after one rule),
 * {@code !transform "aggregateUnique=true, NONE"} (initial plan with aggregateUnique),
 * {@code !transform "relBuilderSimplify=false, NONE, PROJECT_REDUCE_EXPRESSIONS"}
 * (plan after rule, with expressions not pre-simplified by RelBuilder). */
public class TransformCommand extends AbstractCommand {
  private static final Pattern STRIP_QUOTES = Pattern.compile("^\"|\"$");

  private final ImmutableList<String> lines;
  private final ImmutableList<String> content;
  private final String args;

  TransformCommand(List<String> lines, List<String> content, String args) {
    this.lines = ImmutableList.copyOf(lines);
    this.content = ImmutableList.copyOf(content);
    this.args = args;
  }

  @Override public void execute(Context x, boolean execute) throws Exception {
    if (execute) {
      final Config config = parseArgs(args);
      final Quidem.SqlCommand sqlCommand = x.previousSqlCommand();
      try (AutoCloseable ignored =
               Hook.REL_BUILDER_SIMPLIFY.addThread(
                   Hook.propertyJ(config.relBuilderSimplify))) {
        RelNode relNode = buildRelNode(config, sqlCommand);
        relNode = applyRules(config, relNode);
        final String s = RelOptUtil.toString(relNode);
        x.echo(ImmutableList.copyOf(s.split(System.lineSeparator())));
      }
    } else {
      x.echo(content);
    }
    x.echo(lines);
  }

  /** Parses the args string into a {@link Config}. */
  private static Config parseArgs(String args) {
    boolean aggregateUnique = false;
    boolean connectionConfig = false;
    boolean decorrelate = false;
    boolean relBuilderSimplify = true;
    boolean expand = false;
    boolean lateDecorrelate = false;
    boolean topDownGeneralDecorrelate = false;
    boolean operatorTableBigQuery = false;
    boolean throwIfNotUnique = true;
    boolean trim = false;
    boolean simplifyValues = true;
    boolean subQueryRules = false;
    int bloat = -1; // -1 means "use default"
    int inSubQueryThreshold = -1; // -1 means "use default"
    boolean bottomUp = false;
    @Nullable String functionsToReduceStr = null;
    boolean withinDistinctOnly = false;
    final List<RelOptRule> rules = new ArrayList<>();

    for (String token : STRIP_QUOTES.matcher(args).replaceAll("").split(",")) {
      final String name = token.trim();
      if (name.isEmpty() || name.equals("NONE")) {
        // skip placeholder
      } else if (Character.isLowerCase(name.charAt(0))) {
        // config token
        if (name.equals("aggregateUnique=true")) {
          aggregateUnique = true;
        } else if (name.equals("connectionConfig=true")) {
          connectionConfig = true;
        } else if (name.startsWith("bloat=")) {
          bloat = parseInt(name.substring("bloat=".length()));
        } else if (name.startsWith("inSubQueryThreshold=")) {
          inSubQueryThreshold =
              parseInt(name.substring("inSubQueryThreshold=".length()));
        } else if (name.equals("decorrelate=true")) {
          decorrelate = true;
        } else if (name.equals("decorrelate=false")) {
          decorrelate = false; // same as default, used for documentation
        } else if (name.equals("expand=true")) {
          expand = true;
        } else if (name.equals("expand=false")) {
          expand = false; // same as default, used for documentation
        } else if (name.equals("lateDecorrelate=true")) {
          lateDecorrelate = true;
        } else if (name.equals("topDownGeneralDecorrelate=true")) {
          topDownGeneralDecorrelate = true;
        } else if (name.equals("operatorTable=BIG_QUERY")) {
          operatorTableBigQuery = true;
        } else if (name.equals("relBuilderSimplify=false")) {
          relBuilderSimplify = false;
        } else if (name.equals("simplifyValues=false")) {
          simplifyValues = false;
        } else if (name.equals("subQueryRules")) {
          subQueryRules = true;
        } else if (name.equals("throwIfNotUnique=false")) {
          throwIfNotUnique = false;
        } else if (name.equals("trim=true")) {
          trim = true;
        } else if (name.equals("bottomUp=true")) {
          bottomUp = true;
        } else if (name.startsWith("functionsToReduce=")) {
          functionsToReduceStr = name.substring("functionsToReduce=".length());
        } else if (name.equals("withinDistinctOnly=true")) {
          withinDistinctOnly = true;
        } else {
          throw new IllegalArgumentException("Unknown config token: " + name);
        }
      } else {
        if (!throwIfNotUnique
            && name.equals("AGGREGATE_EXPAND_WITHIN_DISTINCT")) {
          rules.add(CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT.config
              .withThrowIfNotUnique(false).toRule());
        } else {
          rules.add(QuidemTest.getCoreRule(name));
        }
      }
    }

    // Add custom AggregateReduceFunctionsRule variants if requested
    if (functionsToReduceStr != null) {
      final EnumSet<SqlKind> functions = EnumSet.noneOf(SqlKind.class);
      if (!functionsToReduceStr.equals("NONE")) {
        for (String fn : functionsToReduceStr.split("\\|")) {
          functions.add(SqlKind.valueOf(fn.trim()));
        }
      }
      rules.add(AggregateReduceFunctionsRule.Config.DEFAULT
          .withOperandFor(LogicalAggregate.class)
          .withFunctionsToReduce(functions)
          .toRule());
    }
    if (withinDistinctOnly) {
      rules.add(AggregateReduceFunctionsRule.Config.DEFAULT
          .withExtraCondition(call -> call.distinctKeys != null)
          .toRule());
    }

    return new Config(aggregateUnique, connectionConfig, decorrelate,
        relBuilderSimplify, expand, lateDecorrelate, topDownGeneralDecorrelate,
        operatorTableBigQuery, trim, simplifyValues, subQueryRules, bloat,
        inSubQueryThreshold, bottomUp, ImmutableList.copyOf(rules));
  }

  /** Converts a SQL statement to a {@link RelNode} according to {@code config}. */
  private static RelNode buildRelNode(Config config,
      Quidem.SqlCommand sqlCommand) throws Exception {
    SqlTestFactory testFactory = SqlTestFactory.INSTANCE
        .withValidatorConfig(c -> c.withIdentifierExpansion(true))
        .withSqlToRelConfig(c -> c.withExpand(config.expand))
        .withSqlToRelConfig(c ->
            c.addRelBuilderConfigTransform(
                b -> b.withPruneInputOfAggregate(false)));
    if (config.aggregateUnique) {
      testFactory = testFactory.withSqlToRelConfig(c ->
          c.addRelBuilderConfigTransform(b -> b.withAggregateUnique(true)));
    }
    if (!config.simplifyValues) {
      testFactory = testFactory.withSqlToRelConfig(c ->
          c.addRelBuilderConfigTransform(b -> b.withSimplifyValues(false)));
    }
    if (config.trim) {
      testFactory = testFactory.withSqlToRelConfig(c ->
          c.withTrimUnusedFields(true));
    }
    if (config.bloat >= 0) {
      final int bloat = config.bloat;
      testFactory = testFactory.withSqlToRelConfig(c ->
          c.addRelBuilderConfigTransform(b -> b.withBloat(bloat)));
    }
    if (config.inSubQueryThreshold >= 0) {
      final int threshold = config.inSubQueryThreshold;
      testFactory = testFactory.withSqlToRelConfig(c ->
          c.withInSubQueryThreshold(threshold));
    }
    if (config.operatorTableBigQuery) {
      testFactory = testFactory.withOperatorTable(opTab ->
          SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
              SqlLibrary.BIG_QUERY));
    }
    if (config.connectionConfig) {
      testFactory = testFactory.withPlannerContext(c ->
          Contexts.of(CalciteConnectionConfig.DEFAULT, c));
    }

    final SqlToRelConverter converter = testFactory.createSqlToRelConverter();
    final SqlNode sqlQuery =
        testFactory.createParser(sqlCommand.sql).parseQuery();
    final SqlNode validatedQuery =
        requireNonNull(converter.validator).validate(sqlQuery);
    RelNode relNode =
        converter.convertQuery(validatedQuery, false, true).project();
    // decorrelate=true / trim=true: matching
    // AbstractSqlTester.convertSqlToRel2 behavior
    if (config.decorrelate || config.trim) {
      relNode = converter.flattenTypes(relNode, true);
    }
    if (config.decorrelate) {
      relNode = converter.decorrelate(sqlQuery, relNode);
    }
    if (config.trim) {
      relNode = converter.trimUnusedFields(true, relNode);
    }

    // Apply subQueryRules as a pre-pass before the main rules
    if (config.subQueryRules) {
      final HepProgramBuilder preBuilder = new HepProgramBuilder();
      preBuilder.addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);
      preBuilder.addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
      preBuilder.addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
      final HepPlanner prePlanner = new HepPlanner(preBuilder.build());
      prePlanner.setRoot(relNode);
      relNode = prePlanner.findBestExp();
    }

    return relNode;
  }

  /** Applies the rules in {@code config} to {@code relNode} and returns the
   * result. */
  private static RelNode applyRules(Config config, RelNode relNode) {
    if (!config.rules.isEmpty()) {
      final HepProgramBuilder builder = new HepProgramBuilder();
      if (config.bottomUp) {
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
      }
      for (RelOptRule rule : config.rules) {
        builder.addRuleInstance(rule);
      }
      final org.apache.calcite.plan.Context context =
          config.connectionConfig
              ? Contexts.of(CalciteConnectionConfig.DEFAULT)
              : Contexts.empty();
      final HepPlanner hepPlanner = new HepPlanner(builder.build(), context);
      hepPlanner.setRoot(relNode);
      relNode = hepPlanner.findBestExp();
    }

    if (config.lateDecorrelate) {
      final RelBuilder relBuilder =
          RelFactories.LOGICAL_BUILDER.create(relNode.getCluster(), null);
      relNode = config.topDownGeneralDecorrelate
          ? TopDownGeneralDecorrelator.decorrelateQuery(relNode, relBuilder)
          : RelDecorrelator.decorrelateQuery(relNode, relBuilder);
    }

    return relNode;
  }

  /** Parsed configuration for a {@code !transform} command. */
  private static class Config {
    final boolean aggregateUnique;
    final boolean connectionConfig;
    final boolean decorrelate;
    final boolean relBuilderSimplify;
    final boolean expand;
    final boolean lateDecorrelate;
    final boolean topDownGeneralDecorrelate;
    final boolean operatorTableBigQuery;
    final boolean trim;
    final boolean simplifyValues;
    final boolean subQueryRules;
    final int bloat;
    final int inSubQueryThreshold;
    final boolean bottomUp;
    final ImmutableList<RelOptRule> rules;

    Config(boolean aggregateUnique, boolean connectionConfig,
        boolean decorrelate, boolean relBuilderSimplify, boolean expand,
        boolean lateDecorrelate, boolean topDownGeneralDecorrelate,
        boolean operatorTableBigQuery, boolean trim, boolean simplifyValues,
        boolean subQueryRules, int bloat, int inSubQueryThreshold,
        boolean bottomUp, ImmutableList<RelOptRule> rules) {
      this.aggregateUnique = aggregateUnique;
      this.connectionConfig = connectionConfig;
      this.decorrelate = decorrelate;
      this.relBuilderSimplify = relBuilderSimplify;
      this.expand = expand;
      this.lateDecorrelate = lateDecorrelate;
      this.topDownGeneralDecorrelate = topDownGeneralDecorrelate;
      this.operatorTableBigQuery = operatorTableBigQuery;
      this.trim = trim;
      this.simplifyValues = simplifyValues;
      this.subQueryRules = subQueryRules;
      this.bloat = bloat;
      this.inSubQueryThreshold = inSubQueryThreshold;
      this.bottomUp = bottomUp;
      this.rules = rules;
    }
  }
}
