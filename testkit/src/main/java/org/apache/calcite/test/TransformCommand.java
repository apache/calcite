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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

/** Command ({@code !transform}) that prints a plan for a SQL statement,
 * optionally after applying rules.
 *
 * <p>The {@code args} string is a comma-separated list of config groups,
 * config tokens, and rule names. {@code NONE} is a placeholder meaning
 * "no rule at this position".
 *
 * <p>Config groups (uppercase class name with parenthesised params) configure
 * related sets of options:
 * <ul>
 *   <li>{@code RelBuilder(simplify=false)} &mdash; disables
 *       {@link org.apache.calcite.rex.RexSimplify} during RelNode
 *       construction; equivalent to {@code .withRelBuilderSimplify(false)}
 *   <li>{@code RelBuilder(simplifyValues=false)} &mdash; prevents Values rows
 *       from being simplified; equivalent to
 *       {@code .withRelBuilderConfig(b -> b.withSimplifyValues(false))}
 *   <li>{@code RelBuilder(aggregateUnique=true)} &mdash; GROUP BY without
 *       aggregate functions creates a {@code LogicalAggregate}; equivalent to
 *       {@code .withRelBuilderConfig(b -> b.withAggregateUnique(true))}
 *   <li>{@code RelBuilder(bloat=N)} &mdash; allows merging expressions up to
 *       N nodes larger; equivalent to
 *       {@code .withRelBuilderConfig(b -> b.withBloat(N))}
 *   <li>{@code Sql2Rel(expand=true)} &mdash; expands sub-queries during
 *       SQL-to-RelNode conversion; equivalent to {@code .withExpand(true)}
 *   <li>{@code Sql2Rel(decorrelate=true)} &mdash; decorrelates after
 *       SQL-to-RelNode conversion by calling
 *       {@link SqlToRelConverter#decorrelate}; equivalent to
 *       {@code .withDecorrelate(true)}
 *   <li>{@code Sql2Rel(trim=true)} &mdash; trims unused fields from
 *       projections; equivalent to {@code .withTrim(true)}
 *   <li>{@code Sql2Rel(inSubQueryThreshold=N)} &mdash; sets how many items
 *       an IN-list may have before it is converted to a join/sub-query;
 *       equivalent to {@code .withInSubQueryThreshold(N)}
 * </ul>
 *
 * <p>Multiple params may be combined in one group:
 * {@code Sql2Rel(expand=true, decorrelate=true, trim=true)}.
 *
 * <p>Remaining config tokens (lowercase, no parentheses):
 * <ul>
 *   <li>{@code connectionConfig=true} &mdash; creates the HepPlanner with
 *       {@link CalciteConnectionConfig#DEFAULT} as context (needed for
 *       DateRangeRules)
 *   <li>{@code lateDecorrelate=true} &mdash; applies
 *       {@link RelDecorrelator#decorrelateQuery} after the main rules
 *   <li>{@code topDownGeneralDecorrelate=true} &mdash; when used with
 *       {@code lateDecorrelate=true}, applies
 *       {@link TopDownGeneralDecorrelator#decorrelateQuery} instead
 *   <li>{@code operatorTable=BIG_QUERY} &mdash; uses BigQuery operator table
 *   <li>{@code subQueryRules} &mdash; applies
 *       PROJECT/FILTER/JOIN_SUB_QUERY_TO_CORRELATE as pre-rules
 *   <li>{@code bottomUp=true} &mdash; uses {@link HepMatchOrder#BOTTOM_UP}
 * </ul>
 *
 * <p>Parameterised rule tokens (uppercase class name with parenthesised
 * params) build a specific rule instance:
 * <ul>
 *   <li>{@code AggregateReduceFunctionsRule(functions=AVG|SUM)} &mdash;
 *       reduces only the named functions ({@code NONE} for none)
 *   <li>{@code AggregateReduceFunctionsRule(withinDistinctOnly=true)} &mdash;
 *       fires only on aggregate calls with {@code WITHIN DISTINCT} keys
 *   <li>{@code AGGREGATE_EXPAND_WITHIN_DISTINCT(throwIfNotUnique=false)}
 * </ul>
 *
 * <p>All remaining uppercase tokens without parentheses are
 * {@link CoreRules} field names (or {@code ClassName.FIELD} for other rule
 * classes).
 *
 * <p>Examples:
 * {@code !transform "NONE"} (initial plan, no rules),
 * {@code !transform "AGGREGATE_UNION_AGGREGATE"} (plan after one rule),
 * {@code !transform "RelBuilder(aggregateUnique=true), NONE"},
 * {@code !transform "RelBuilder(simplify=false), PROJECT_REDUCE_EXPRESSIONS"},
 * {@code !transform "Sql2Rel(expand=true, decorrelate=true, trim=true), FILTER_INTO_JOIN"}. */
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
        x.echo(
            ImmutableList.copyOf(
            RelOptUtil.toString(relNode).split(System.lineSeparator())));
      }
    } else {
      x.echo(content);
    }
    x.echo(lines);
  }

  /** Parses the args string into a {@link Config}. */
  private static Config parseArgs(String args) {
    final ConfigBuilder b = new ConfigBuilder();
    for (String name : splitTokens(args)) {
      if (name.isEmpty() || name.equals("NONE")) {
        // skip placeholder
      } else if (name.contains("(")) {
        // parameterised token: config group or rule with params
        applyParamToken(name, b);
      } else if (Character.isLowerCase(name.charAt(0))) {
        // simple config token
        applyConfigToken(name, b);
      } else {
        // plain rule name
        b.rules.add(QuidemTest.getCoreRule(name));
      }
    }
    return b.build();
  }

  /** Applies a simple lowercase config token to {@code b}. */
  private static void applyConfigToken(String name, ConfigBuilder b) {
    if (name.equals("connectionConfig=true")) {
      b.connectionConfig = true;
    } else if (name.equals("lateDecorrelate=true")) {
      b.lateDecorrelate = true;
    } else if (name.equals("topDownGeneralDecorrelate=true")) {
      b.topDownGeneralDecorrelate = true;
    } else if (name.equals("operatorTable=BIG_QUERY")) {
      b.operatorTableBigQuery = true;
    } else if (name.equals("subQueryRules")) {
      b.subQueryRules = true;
    } else if (name.equals("bottomUp=true")) {
      b.bottomUp = true;
    } else {
      throw new IllegalArgumentException("Unknown config token: " + name);
    }
  }

  /** Applies a parameterised token of the form {@code Name(key=value,...)}
   * to {@code b}.
   *
   * <p>Handles config groups ({@code RelBuilder}, {@code Sql2Rel}) and
   * parameterised rule tokens ({@code AggregateReduceFunctionsRule},
   * {@code AGGREGATE_EXPAND_WITHIN_DISTINCT}). */
  private static void applyParamToken(String token, ConfigBuilder b) {
    final int open = token.indexOf('(');
    final int close = token.lastIndexOf(')');
    if (close < 0 || close < open) {
      throw new IllegalArgumentException("Malformed token: " + token);
    }
    final String name = token.substring(0, open).trim();
    final String paramsStr = token.substring(open + 1, close).trim();
    final Map<String, String> params = parseParams(paramsStr);
    switch (name) {
    case "RelBuilder":
      for (Map.Entry<String, String> e : params.entrySet()) {
        switch (e.getKey()) {
        case "simplify":
          b.relBuilderSimplify = parseBoolean(e.getValue());
          break;
        case "simplifyValues":
          b.simplifyValues = parseBoolean(e.getValue());
          break;
        case "aggregateUnique":
          b.aggregateUnique = parseBoolean(e.getValue());
          break;
        case "bloat":
          b.bloat = parseInt(e.getValue());
          break;
        default:
          throw new IllegalArgumentException(
              "Unknown RelBuilder param: " + e.getKey());
        }
      }
      break;
    case "Sql2Rel":
      for (Map.Entry<String, String> e : params.entrySet()) {
        switch (e.getKey()) {
        case "expand":
          b.expand = parseBoolean(e.getValue());
          break;
        case "decorrelate":
          b.decorrelate = parseBoolean(e.getValue());
          break;
        case "trim":
          b.trim = parseBoolean(e.getValue());
          break;
        case "inSubQueryThreshold":
          b.inSubQueryThreshold = parseInt(e.getValue());
          break;
        default:
          throw new IllegalArgumentException(
              "Unknown Sql2Rel param: " + e.getKey());
        }
      }
      break;
    case "AggregateReduceFunctionsRule":
      if (params.containsKey("functions")) {
        final String functionsStr = params.get("functions");
        final EnumSet<SqlKind> functions = EnumSet.noneOf(SqlKind.class);
        if (!functionsStr.equals("NONE")) {
          for (String fn : functionsStr.split("\\|")) {
            functions.add(SqlKind.valueOf(fn.trim()));
          }
        }
        b.rules.add(AggregateReduceFunctionsRule.Config.DEFAULT
            .withOperandFor(LogicalAggregate.class)
            .withFunctionsToReduce(functions)
            .toRule());
      } else if ("true".equals(params.get("withinDistinctOnly"))) {
        b.rules.add(AggregateReduceFunctionsRule.Config.DEFAULT
            .withExtraCondition(call -> call.distinctKeys != null)
            .toRule());
      } else {
        throw new IllegalArgumentException(
            "Unknown params for AggregateReduceFunctionsRule: " + paramsStr);
      }
      break;
    case "AGGREGATE_EXPAND_WITHIN_DISTINCT":
      if ("false".equals(params.get("throwIfNotUnique"))) {
        b.rules.add(CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT.config
            .withThrowIfNotUnique(false).toRule());
      } else {
        throw new IllegalArgumentException(
            "Unknown params for AGGREGATE_EXPAND_WITHIN_DISTINCT: " + paramsStr);
      }
      break;
    default:
      throw new IllegalArgumentException(
          "Unknown parameterised token: " + name);
    }
  }

  /** Parses a {@code key=value,...} string into a map. */
  private static Map<String, String> parseParams(String paramsStr) {
    final Map<String, String> params = new LinkedHashMap<>();
    if (!paramsStr.isEmpty()) {
      for (String pair : paramsStr.split(",")) {
        final String p = pair.trim();
        final int eq = p.indexOf('=');
        if (eq >= 0) {
          params.put(p.substring(0, eq).trim(), p.substring(eq + 1).trim());
        } else if (!p.isEmpty()) {
          params.put(p, "true");
        }
      }
    }
    return params;
  }

  /** Splits {@code args} on commas, but not commas inside parentheses.
   * Also strips leading/trailing double-quotes and whitespace from each token. */
  private static List<String> splitTokens(String args) {
    final List<String> tokens = new ArrayList<>();
    final String stripped = STRIP_QUOTES.matcher(args).replaceAll("");
    int depth = 0;
    int start = 0;
    for (int i = 0; i < stripped.length(); i++) {
      final char c = stripped.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (c == ',' && depth == 0) {
        tokens.add(stripped.substring(start, i).trim());
        start = i + 1;
      }
    }
    final String last = stripped.substring(start).trim();
    if (!last.isEmpty()) {
      tokens.add(last);
    }
    return tokens;
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
    // decorrelate / trim: matching AbstractSqlTester.convertSqlToRel2 behavior
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

  /** Mutable state accumulated while parsing a {@code !transform} args string.
   * Converted to an immutable {@link Config} at the end of {@link #parseArgs}. */
  private static class ConfigBuilder {
    boolean aggregateUnique = false;
    boolean connectionConfig = false;
    boolean decorrelate = false;
    boolean relBuilderSimplify = true;
    boolean expand = false;
    boolean lateDecorrelate = false;
    boolean topDownGeneralDecorrelate = false;
    boolean operatorTableBigQuery = false;
    boolean trim = false;
    boolean simplifyValues = true;
    boolean subQueryRules = false;
    int bloat = -1; // -1 means "use default"
    int inSubQueryThreshold = -1; // -1 means "use default"
    boolean bottomUp = false;
    final List<RelOptRule> rules = new ArrayList<>();

    Config build() {
      return new Config(aggregateUnique, connectionConfig, decorrelate,
          relBuilderSimplify, expand, lateDecorrelate, topDownGeneralDecorrelate,
          operatorTableBigQuery, trim, simplifyValues, subQueryRules, bloat,
          inSubQueryThreshold, bottomUp, ImmutableList.copyOf(rules));
    }
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
