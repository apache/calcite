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
package org.apache.calcite.adapter.druid;

import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.interpreter.Compiler;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.DateTimeStringUtils;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.joda.time.Interval;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import static org.apache.calcite.sql.SqlKind.INPUT_REF;

/**
 * Relational expression representing a scan of a Druid data set.
 */
public class DruidQuery extends AbstractRelNode implements BindableRel {

  /**
   * Provides a standard list of supported Calcite operators that can be converted to
   * Druid Expressions. This can be used as is or re-adapted based on underline
   * engine operator syntax.
   */
  public static final List<DruidSqlOperatorConverter> DEFAULT_OPERATORS_LIST =
      ImmutableList.<DruidSqlOperatorConverter>builder()
          .add(new DirectOperatorConversion(SqlStdOperatorTable.EXP, "exp"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CONCAT, "concat"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.DIVIDE_INTEGER, "div"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LIKE, "like"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LN, "log"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.SQRT, "sqrt"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LOWER, "lower"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LOG10, "log10"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.REPLACE, "replace"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.UPPER, "upper"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.POWER, "pow"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.ABS, "abs"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.SIN, "sin"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.COS, "cos"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.TAN, "tan"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CASE, "case_searched"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CHAR_LENGTH, "strlen"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CHARACTER_LENGTH, "strlen"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.EQUALS, "=="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.NOT_EQUALS, "!="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.OR, "||"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.AND, "&&"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.LESS_THAN, "<"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "<="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.GREATER_THAN, ">"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ">="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.PLUS, "+"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.MINUS, "-"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.MULTIPLY, "*"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.DIVIDE, "/"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.MOD, "%"))
          .add(new DruidSqlCastConverter())
          .add(new ExtractOperatorConversion())
          .add(new UnaryPrefixOperatorConversion(SqlStdOperatorTable.NOT, "!"))
          .add(new UnaryPrefixOperatorConversion(SqlStdOperatorTable.UNARY_MINUS, "-"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_FALSE, "<= 0"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_NOT_TRUE, "<= 0"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_TRUE, "> 0"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_NOT_FALSE, "> 0"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_NULL, "== null"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_NOT_NULL, "!= null"))
          .add(new FloorOperatorConversion())
          .add(new CeilOperatorConversion())
          .add(new SubstringOperatorConversion())
          .build();
  protected QuerySpec querySpec;

  final RelOptTable table;
  final DruidTable druidTable;
  final ImmutableList<Interval> intervals;
  final ImmutableList<RelNode> rels;
  /**
   * This operator map provides DruidSqlOperatorConverter instance to convert a Calcite RexNode to
   * Druid Expression when possible.
   */
  final Map<SqlOperator, DruidSqlOperatorConverter> converterOperatorMap;

  private static final Pattern VALID_SIG = Pattern.compile("sf?p?(a?|ao)l?");
  private static final String EXTRACT_COLUMN_NAME_PREFIX = "extract";
  private static final String FLOOR_COLUMN_NAME_PREFIX = "floor";
  protected static final String DRUID_QUERY_FETCH = "druid.query.fetch";
  private static final int DAYS_IN_TEN_YEARS = 10 * 365;

  /**
   * Creates a DruidQuery.
   *
   * @param cluster        Cluster
   * @param traitSet       Traits
   * @param table          Table
   * @param druidTable     Druid table
   * @param intervals      Intervals for the query
   * @param rels           Internal relational expressions
   * @param converterOperatorMap mapping of Calcite Sql Operator to Druid Expression API.
   */
  protected DruidQuery(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable,
      List<Interval> intervals, List<RelNode> rels,
      Map<SqlOperator, DruidSqlOperatorConverter> converterOperatorMap) {
    super(cluster, traitSet);
    this.table = table;
    this.druidTable = druidTable;
    this.intervals = ImmutableList.copyOf(intervals);
    this.rels = ImmutableList.copyOf(rels);
    this.converterOperatorMap = Preconditions.checkNotNull(converterOperatorMap, "Operator map "
        + "can not be null");
    assert isValid(Litmus.THROW, null);
  }
  /** Returns a string describing the operations inside this query.
   *
   * <p>For example, "sfpaol" means {@link TableScan} (s)
   * followed by {@link Filter} (f)
   * followed by {@link Project} (p)
   * followed by {@link Aggregate} (a)
   * followed by {@link Project} (o)
   * followed by {@link Sort} (l).
   *
   * @see #isValidSignature(String)
   */
  String signature() {
    final StringBuilder b = new StringBuilder();
    boolean flag = false;
    for (RelNode rel : rels) {
      b.append(rel instanceof TableScan ? 's'
          : (rel instanceof Project && flag) ? 'o'
          : rel instanceof Filter ? 'f'
          : rel instanceof Aggregate ? 'a'
          : rel instanceof Sort ? 'l'
          : rel instanceof Project ? 'p'
          : '!');
      flag = flag || rel instanceof Aggregate;
    }
    return b.toString();
  }

  @Override public boolean isValid(Litmus litmus, Context context) {
    if (!super.isValid(litmus, context)) {
      return false;
    }
    final String signature = signature();
    if (!isValidSignature(signature)) {
      return litmus.fail("invalid signature [{}]", signature);
    }
    if (rels.isEmpty()) {
      return litmus.fail("must have at least one rel");
    }
    for (int i = 0; i < rels.size(); i++) {
      final RelNode r = rels.get(i);
      if (i == 0) {
        if (!(r instanceof TableScan)) {
          return litmus.fail("first rel must be TableScan, was ", r);
        }
        if (r.getTable() != table) {
          return litmus.fail("first rel must be based on table table");
        }
      } else {
        final List<RelNode> inputs = r.getInputs();
        if (inputs.size() != 1 || inputs.get(0) != rels.get(i - 1)) {
          return litmus.fail("each rel must have a single input");
        }
        if (r instanceof Aggregate) {
          final Aggregate aggregate = (Aggregate) r;
          if (aggregate.getGroupSets().size() != 1
              || aggregate.indicator) {
            return litmus.fail("no grouping sets");
          }
        }
        if (r instanceof Filter) {
          final Filter filter = (Filter) r;
          final DruidJsonFilter druidJsonFilter = DruidJsonFilter
              .toDruidFilters(filter.getCondition(), table.getRowType(), this);
          if (druidJsonFilter == null) {
            return litmus.fail("invalid filter [{}]", filter.getCondition());
          }
        }
        if (r instanceof Sort) {
          final Sort sort = (Sort) r;
          if (sort.offset != null && RexLiteral.intValue(sort.offset) != 0) {
            return litmus.fail("offset not supported");
          }
        }
      }
    }
    return true;
  }

  public Map<SqlOperator, DruidSqlOperatorConverter> getConverterOperatorMap() {
    return converterOperatorMap;
  }

  /** Returns whether a signature represents an sequence of relational operators
   * that can be translated into a valid Druid query. */
  static boolean isValidSignature(String signature) {
    return VALID_SIG.matcher(signature).matches();
  }

  /** Creates a DruidQuery. */
  public static DruidQuery create(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable, List<RelNode> rels) {
    final ImmutableMap converterOperatorMap = ImmutableMap.<SqlOperator,
        DruidSqlOperatorConverter>builder().putAll(
        Lists.transform(DEFAULT_OPERATORS_LIST, new Function<DruidSqlOperatorConverter,
            Map.Entry<SqlOperator, DruidSqlOperatorConverter>>() {
          @Nullable @Override public Map.Entry<SqlOperator, DruidSqlOperatorConverter> apply(
              final DruidSqlOperatorConverter input) {
            return Maps.immutableEntry(input.calciteOperator(), input);
          }
        })).build();
    return create(cluster, traitSet, table, druidTable, druidTable.intervals, rels,
        converterOperatorMap
    );
  }

  /** Creates a DruidQuery. */
  public static DruidQuery create(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable, List<RelNode> rels,
      Map<SqlOperator, DruidSqlOperatorConverter> converterOperatorMap) {
    return create(cluster, traitSet, table, druidTable, druidTable.intervals, rels,
        converterOperatorMap
    );
  }

  /**
   * Creates a DruidQuery.
   */
  private static DruidQuery create(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable, List<Interval> intervals,
      List<RelNode> rels, Map<SqlOperator, DruidSqlOperatorConverter> converterOperatorMap) {
    return new DruidQuery(cluster, traitSet, table, druidTable, intervals, rels,
        converterOperatorMap);
  }

  /** Extends a DruidQuery. */
  public static DruidQuery extendQuery(DruidQuery query, RelNode r) {
    final ImmutableList.Builder<RelNode> builder = ImmutableList.builder();
    return DruidQuery.create(query.getCluster(), r.getTraitSet().replace(query.getConvention()),
        query.getTable(), query.druidTable, query.intervals,
        builder.addAll(query.rels).add(r).build(), query.getConverterOperatorMap());
  }

  /** Extends a DruidQuery. */
  public static DruidQuery extendQuery(DruidQuery query,
      List<Interval> intervals) {
    return DruidQuery.create(query.getCluster(), query.getTraitSet(), query.getTable(),
        query.druidTable, intervals, query.rels, query.getConverterOperatorMap());
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override public RelDataType deriveRowType() {
    return getCluster().getTypeFactory().createStructType(
        Pair.right(Util.last(rels).getRowType().getFieldList()),
        getQuerySpec().fieldNames);
  }

  public TableScan getTableScan() {
    return (TableScan) rels.get(0);
  }

  public RelNode getTopNode() {
    return Util.last(rels);
  }

  @Override public RelOptTable getTable() {
    return table;
  }

  public DruidTable getDruidTable() {
    return druidTable;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    for (RelNode rel : rels) {
      if (rel instanceof TableScan) {
        TableScan tableScan = (TableScan) rel;
        pw.item("table", tableScan.getTable().getQualifiedName());
        pw.item("intervals", intervals);
      } else if (rel instanceof Filter) {
        pw.item("filter", ((Filter) rel).getCondition());
      } else if (rel instanceof Project) {
        if (((Project) rel).getInput() instanceof Aggregate) {
          pw.item("post_projects", ((Project) rel).getProjects());
        } else {
          pw.item("projects", ((Project) rel).getProjects());
        }
      } else if (rel instanceof Aggregate) {
        final Aggregate aggregate = (Aggregate) rel;
        pw.item("groups", aggregate.getGroupSet())
            .item("aggs", aggregate.getAggCallList());
      } else if (rel instanceof Sort) {
        final Sort sort = (Sort) rel;
        for (Ord<RelFieldCollation> ord
            : Ord.zip(sort.collation.getFieldCollations())) {
          pw.item("sort" + ord.i, ord.e.getFieldIndex());
        }
        for (Ord<RelFieldCollation> ord
            : Ord.zip(sort.collation.getFieldCollations())) {
          pw.item("dir" + ord.i, ord.e.shortString());
        }
        pw.itemIf("fetch", sort.fetch, sort.fetch != null);
      } else {
        throw new AssertionError("rel type not supported in Druid query "
            + rel);
      }
    }
    return pw;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return Util.last(rels)
        .computeSelfCost(planner, mq)
        // Cost increases with the number of fields queried.
        // A plan returning 100 or more columns will have 2x the cost of a
        // plan returning 2 columns.
        // A plan where all extra columns are pruned will be preferred.
        .multiplyBy(
            RelMdUtil.linear(querySpec.fieldNames.size(), 2, 100, 1d, 2d))
        .multiplyBy(getQueryTypeCostMultiplier())
        // a plan with sort pushed to druid is better than doing sort outside of druid
        .multiplyBy(Util.last(rels) instanceof Sort ? 0.1 : 1.0)
        .multiplyBy(getIntervalCostMultiplier());
  }

  private double getIntervalCostMultiplier() {
    int days = 0;
    for (Interval interval : intervals) {
      days += interval.toDuration().getStandardDays();
    }
    // Cost increases with the wider interval being queries.
    // A plan querying 10 or more years of data will have 10x the cost of a
    // plan returning 1 day data.
    // A plan where least interval is queries will be preferred.
    return RelMdUtil.linear(days, 1, DAYS_IN_TEN_YEARS, 0.1d, 1d);
  }

  private double getQueryTypeCostMultiplier() {
    // Cost of Select > GroupBy > Timeseries > TopN
    switch (querySpec.queryType) {
    case SELECT:
      return .1;
    case GROUP_BY:
      return .08;
    case TIMESERIES:
      return .06;
    case TOP_N:
      return .04;
    default:
      return .2;
    }
  }

  @Override public void register(RelOptPlanner planner) {
    for (RelOptRule rule : DruidRules.RULES) {
      planner.addRule(rule);
    }
    for (RelOptRule rule : Bindables.RULES) {
      planner.addRule(rule);
    }
  }

  @Override public Class<Object[]> getElementType() {
    return Object[].class;
  }

  @Override public Enumerable<Object[]> bind(DataContext dataContext) {
    return table.unwrap(ScannableTable.class).scan(dataContext);
  }

  @Override public Node implement(InterpreterImplementor implementor) {
    return new DruidQueryNode(implementor.compiler, this);
  }

  public QuerySpec getQuerySpec() {
    if (querySpec == null) {
      querySpec = deriveQuerySpec();
      assert querySpec != null : this;
    }
    return querySpec;
  }

  protected QuerySpec deriveQuerySpec() {
    final RelDataType rowType = table.getRowType();
    int i = 1;

    RexNode filter = null;
    if (i < rels.size() && rels.get(i) instanceof Filter) {
      final Filter filterRel = (Filter) rels.get(i++);
      filter = filterRel.getCondition();
    }

    List<RexNode> projects = null;
    if (i < rels.size() && rels.get(i) instanceof Project) {
      final Project project = (Project) rels.get(i++);
      projects = project.getProjects();
    }

    ImmutableBitSet groupSet = null;
    List<AggregateCall> aggCalls = null;
    List<String> aggNames = null;
    if (i < rels.size() && rels.get(i) instanceof Aggregate) {
      final Aggregate aggregate = (Aggregate) rels.get(i++);
      groupSet = aggregate.getGroupSet();
      aggCalls = aggregate.getAggCallList();
      aggNames = Util.skip(aggregate.getRowType().getFieldNames(),
          groupSet.cardinality());
    }

    Project postProject = null;
    if (i < rels.size() && rels.get(i) instanceof Project) {
      postProject = (Project) rels.get(i++);
    }

    List<Integer> collationIndexes = null;
    List<Direction> collationDirections = null;
    ImmutableBitSet.Builder numericCollationBitSetBuilder = ImmutableBitSet.builder();
    Integer fetch = null;
    if (i < rels.size() && rels.get(i) instanceof Sort) {
      final Sort sort = (Sort) rels.get(i++);
      collationIndexes = new ArrayList<>();
      collationDirections = new ArrayList<>();
      for (RelFieldCollation fCol : sort.collation.getFieldCollations()) {
        collationIndexes.add(fCol.getFieldIndex());
        collationDirections.add(fCol.getDirection());
        if (sort.getRowType().getFieldList().get(fCol.getFieldIndex()).getType().getFamily()
            == SqlTypeFamily.NUMERIC) {
          numericCollationBitSetBuilder.set(fCol.getFieldIndex());
        }
      }
      fetch = sort.fetch != null ? RexLiteral.intValue(sort.fetch) : null;
    }

    if (i != rels.size()) {
      throw new AssertionError("could not implement all rels");
    }

    return getQuery(rowType, filter, projects, groupSet, aggCalls, aggNames,
        collationIndexes, collationDirections, numericCollationBitSetBuilder.build(), fetch,
        postProject);
  }

  public QueryType getQueryType() {
    return getQuerySpec().queryType;
  }

  public String getQueryString() {
    return getQuerySpec().queryString;
  }

  protected CalciteConnectionConfig getConnectionConfig() {
    return getCluster().getPlanner().getContext().unwrap(CalciteConnectionConfig.class);
  }

  protected QuerySpec getQuery(RelDataType rowType, RexNode filter, List<RexNode> projects,
      ImmutableBitSet groupSet, List<AggregateCall> aggCalls, List<String> aggNames,
      List<Integer> collationIndexes, List<Direction> collationDirections,
      ImmutableBitSet numericCollationIndexes, Integer fetch, Project postProject) {
    final CalciteConnectionConfig config = getConnectionConfig();
    QueryType queryType = QueryType.SCAN;
    final Translator translator = new Translator(druidTable, rowType, config.timeZone(), this);
    List<String> fieldNames = rowType.getFieldNames();
    Set<String> usedFieldNames = Sets.newHashSet(fieldNames);

    // Handle filter
    final DruidJson jsonFilter;
    if (filter != null) {
      jsonFilter = DruidJsonFilter.toDruidFilters(filter, table.getRowType(), this);
      Preconditions.checkNotNull(
          jsonFilter, DateTimeStringUtils.format("Druid Filter is null instead of [%s]", filter));
    } else {
      jsonFilter = null;
    }

    // Then we handle project
    if (projects != null) {
      translator.clearFieldNameLists();
      final ImmutableList.Builder<String> builder = ImmutableList.builder();
      for (RexNode project : projects) {
        builder.add(translator.translate(project, true, false));
      }
      fieldNames = builder.build();
    }

    // Finally we handle aggregate and sort. Handling of these
    // operators is more complex, since we need to extract
    // the conditions to know whether the query will be
    // executed as a Timeseries, TopN, or GroupBy in Druid
    final List<DimensionSpec> dimensions = new ArrayList<>();
    final List<JsonAggregation> aggregations = new ArrayList<>();
    final List<JsonPostAggregation> postAggs = new ArrayList<>();
    Granularity finalGranularity = Granularities.all();
    Direction timeSeriesDirection = null;
    JsonLimit limit = null;
    TimeExtractionDimensionSpec timeExtractionDimensionSpec = null;
    if (groupSet != null) {
      assert aggCalls != null;
      assert aggNames != null;
      assert aggCalls.size() == aggNames.size();

      int timePositionIdx = -1;
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      if (projects != null) {
        for (int groupKey : groupSet) {
          final String fieldName = fieldNames.get(groupKey);
          final RexNode project = projects.get(groupKey);
          if (project instanceof RexInputRef) {
            // Reference could be to the timestamp or druid dimension but no druid metric
            final RexInputRef ref = (RexInputRef) project;
            final String originalFieldName = druidTable.getRowType(getCluster().getTypeFactory())
                .getFieldList().get(ref.getIndex()).getName();
            if (originalFieldName.equals(druidTable.timestampFieldName)) {
              finalGranularity = Granularities.all();
              String extractColumnName = SqlValidatorUtil.uniquify(EXTRACT_COLUMN_NAME_PREFIX,
                  usedFieldNames, SqlValidatorUtil.EXPR_SUGGESTER);
              timeExtractionDimensionSpec = TimeExtractionDimensionSpec.makeFullTimeExtract(
                  extractColumnName, config.timeZone());
              dimensions.add(timeExtractionDimensionSpec);
              builder.add(extractColumnName);
              assert timePositionIdx == -1;
              timePositionIdx = groupKey;
            } else {
              dimensions.add(new DefaultDimensionSpec(fieldName));
              builder.add(fieldName);
            }
          } else if (project instanceof RexCall) {
            // Call, check if we should infer granularity
            final RexCall call = (RexCall) project;
            final Granularity funcGranularity =
                DruidDateTimeUtils.extractGranularity(call, config.timeZone());
            if (funcGranularity != null) {
              final String extractColumnName;
              switch (call.getKind()) {
              case EXTRACT:
                // case extract field from time column
                finalGranularity = Granularities.all();
                extractColumnName =
                    SqlValidatorUtil.uniquify(EXTRACT_COLUMN_NAME_PREFIX + "_"
                        + funcGranularity.getType().lowerName, usedFieldNames,
                        SqlValidatorUtil.EXPR_SUGGESTER);
                timeExtractionDimensionSpec = TimeExtractionDimensionSpec.makeTimeExtract(
                    funcGranularity, extractColumnName, config.timeZone());
                dimensions.add(timeExtractionDimensionSpec);
                builder.add(extractColumnName);
                break;
              case FLOOR:
                // case floor time column
                if (groupSet.cardinality() > 1) {
                  // case we have more than 1 group by key -> then will have druid group by
                  extractColumnName =
                      SqlValidatorUtil.uniquify(FLOOR_COLUMN_NAME_PREFIX
                          + "_" + funcGranularity.getType().lowerName,
                          usedFieldNames, SqlValidatorUtil.EXPR_SUGGESTER);
                  dimensions.add(
                      TimeExtractionDimensionSpec.makeTimeFloor(funcGranularity,
                          extractColumnName, config.timeZone()));
                  finalGranularity = Granularities.all();
                  builder.add(extractColumnName);
                } else {
                  // case timeseries we can not use extraction function
                  finalGranularity = funcGranularity;
                  builder.add(fieldName);
                }
                assert timePositionIdx == -1;
                timePositionIdx = groupKey;
                break;
              default:
                throw new AssertionError();
              }

            } else {
              dimensions.add(new DefaultDimensionSpec(fieldName));
              builder.add(fieldName);
            }
          } else {
            throw new AssertionError("incompatible project expression: " + project);
          }
        }
      } else {
        for (int groupKey : groupSet) {
          final String s = fieldNames.get(groupKey);
          if (s.equals(druidTable.timestampFieldName)) {
            finalGranularity = Granularities.all();
            // Generate unique name as timestampFieldName is taken
            String extractColumnName = SqlValidatorUtil.uniquify(EXTRACT_COLUMN_NAME_PREFIX,
                usedFieldNames, SqlValidatorUtil.EXPR_SUGGESTER);
            timeExtractionDimensionSpec = TimeExtractionDimensionSpec.makeFullTimeExtract(
                extractColumnName, config.timeZone());
            dimensions.add(timeExtractionDimensionSpec);
            builder.add(extractColumnName);
            assert timePositionIdx == -1;
            timePositionIdx = groupKey;
          } else {
            dimensions.add(new DefaultDimensionSpec(s));
            builder.add(s);
          }
        }
      }

      for (Pair<AggregateCall, String> agg : Pair.zip(aggCalls, aggNames)) {
        final JsonAggregation jsonAggregation =
            getJsonAggregation(fieldNames, agg.right, agg.left, projects);
        aggregations.add(jsonAggregation);
        builder.add(jsonAggregation.name);
      }

      fieldNames = builder.build();

      if (postProject != null) {
        builder = ImmutableList.builder();
        for (Pair<RexNode, String> pair : postProject.getNamedProjects()) {
          String fieldName = pair.right;
          RexNode rex = pair.left;
          builder.add(fieldName);
          // Render Post JSON object when PostProject exists. In DruidPostAggregationProjectRule
          // all check has been done to ensure all RexCall rexNode can be pushed in.
          if (rex instanceof RexCall) {
            DruidQuery.JsonPostAggregation jsonPost = getJsonPostAggregation(fieldName, rex,
                postProject.getInput());
            postAggs.add(jsonPost);
          }
        }
        fieldNames = builder.build();
      }

      ImmutableList<JsonCollation> collations = null;
      boolean sortsMetric = false;
      if (collationIndexes != null) {
        assert collationDirections != null;
        ImmutableList.Builder<JsonCollation> colBuilder =
            ImmutableList.builder();
        for (Pair<Integer, Direction> p : Pair.zip(collationIndexes, collationDirections)) {
          final String dimensionOrder = numericCollationIndexes.get(p.left) ? "numeric"
              : "alphanumeric";
          colBuilder.add(
              new JsonCollation(fieldNames.get(p.left),
                  p.right == Direction.DESCENDING ? "descending" : "ascending", dimensionOrder));
          if (p.left >= groupSet.cardinality() && p.right == Direction.DESCENDING) {
            // Currently only support for DESC in TopN
            sortsMetric = true;
          } else if (p.left == timePositionIdx) {
            assert timeSeriesDirection == null;
            timeSeriesDirection = p.right;
          }
        }
        collations = colBuilder.build();
      }

      limit = new JsonLimit("default", fetch, collations);

      if (dimensions.isEmpty() && (collations == null || timeSeriesDirection != null)) {
        queryType = QueryType.TIMESERIES;
        assert fetch == null;
      } else if (dimensions.size() == 1
          && finalGranularity.equals(Granularities.all())
          && sortsMetric
          && collations.size() == 1
          && fetch != null
          && config.approximateTopN()) {
        queryType = QueryType.TOP_N;
      } else {
        queryType = QueryType.GROUP_BY;
      }
    } else {
      assert aggCalls == null;
      assert aggNames == null;
      assert collationIndexes == null || collationIndexes.isEmpty();
      assert collationDirections == null || collationDirections.isEmpty();
    }

    final StringWriter sw = new StringWriter();
    final JsonFactory factory = new JsonFactory();
    try {
      final JsonGenerator generator = factory.createGenerator(sw);

      switch (queryType) {
      case TIMESERIES:
        generator.writeStartObject();

        generator.writeStringField("queryType", "timeseries");
        generator.writeStringField("dataSource", druidTable.dataSource);
        generator.writeBooleanField("descending", timeSeriesDirection != null
            && timeSeriesDirection == Direction.DESCENDING);
        writeField(generator, "granularity", finalGranularity);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "aggregations", aggregations);
        writeFieldIf(generator, "postAggregations", postAggs.size() > 0 ? postAggs : null);
        writeField(generator, "intervals", intervals);

        generator.writeFieldName("context");
        // The following field is necessary to conform with SQL semantics (CALCITE-1589)
        generator.writeStartObject();
        final boolean isCountStar = finalGranularity.equals(Granularities.all())
            && aggregations.size() == 1
            && aggregations.get(0).type.equals("count");
        //Count(*) returns 0 if result set is empty thus need to set skipEmptyBuckets to false
        generator.writeBooleanField("skipEmptyBuckets", !isCountStar);
        generator.writeEndObject();

        generator.writeEndObject();
        break;

      case TOP_N:
        generator.writeStartObject();

        generator.writeStringField("queryType", "topN");
        generator.writeStringField("dataSource", druidTable.dataSource);
        writeField(generator, "granularity", finalGranularity);
        writeField(generator, "dimension", dimensions.get(0));
        generator.writeStringField("metric", fieldNames.get(collationIndexes.get(0)));
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "aggregations", aggregations);
        writeFieldIf(generator, "postAggregations", postAggs.size() > 0 ? postAggs : null);
        writeField(generator, "intervals", intervals);
        generator.writeNumberField("threshold", fetch);

        generator.writeEndObject();
        break;

      case GROUP_BY:
        generator.writeStartObject();
        generator.writeStringField("queryType", "groupBy");
        generator.writeStringField("dataSource", druidTable.dataSource);
        writeField(generator, "granularity", finalGranularity);
        writeField(generator, "dimensions", dimensions);
        writeFieldIf(generator, "limitSpec", limit);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "aggregations", aggregations);
        writeFieldIf(generator, "postAggregations", postAggs.size() > 0 ? postAggs : null);
        writeField(generator, "intervals", intervals);
        writeFieldIf(generator, "having", null);

        generator.writeEndObject();
        break;

      case SELECT:
        generator.writeStartObject();

        generator.writeStringField("queryType", "select");
        generator.writeStringField("dataSource", druidTable.dataSource);
        generator.writeBooleanField("descending", false);
        writeField(generator, "intervals", intervals);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "dimensions", translator.dimensions);
        writeField(generator, "metrics", translator.metrics);
        writeField(generator, "granularity", finalGranularity);

        generator.writeFieldName("pagingSpec");
        generator.writeStartObject();
        generator.writeNumberField("threshold", fetch != null ? fetch
            : CalciteConnectionProperty.DRUID_FETCH.wrap(new Properties()).getInt());
        generator.writeBooleanField("fromNext", true);
        generator.writeEndObject();

        generator.writeFieldName("context");
        generator.writeStartObject();
        generator.writeBooleanField(DRUID_QUERY_FETCH, fetch != null);
        generator.writeEndObject();

        generator.writeEndObject();
        break;

      case SCAN:
        generator.writeStartObject();

        generator.writeStringField("queryType", "scan");
        generator.writeStringField("dataSource", druidTable.dataSource);
        writeField(generator, "intervals", intervals);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "columns",
            Lists.transform(fieldNames, new Function<String, String>() {
              @Override public String apply(String s) {
                return s.equals(druidTable.timestampFieldName)
                    ? DruidTable.DEFAULT_TIMESTAMP_COLUMN : s;
              }
            }));
        writeField(generator, "granularity", finalGranularity);
        generator.writeStringField("resultFormat", "compactedList");
        if (fetch != null) {
          generator.writeNumberField("limit", fetch);
        }

        generator.writeEndObject();
        break;

      default:
        throw new AssertionError("unknown query type " + queryType);
      }

      generator.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return new QuerySpec(queryType, sw.toString(), fieldNames);
  }

  protected JsonAggregation getJsonAggregation(List<String> fieldNames,
      String name, AggregateCall aggCall, List<RexNode> projects) {
    final List<String> list = new ArrayList<>();
    for (Integer arg : aggCall.getArgList()) {
      list.add(fieldNames.get(arg));
    }
    final String only = Iterables.getFirst(list, null);
    final boolean fractional;
    final RelDataType type = aggCall.getType();
    final SqlTypeName sqlTypeName = type.getSqlTypeName();
    if (SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains(sqlTypeName)) {
      fractional = true;
    } else if (SqlTypeFamily.INTEGER.getTypeNames().contains(sqlTypeName)) {
      fractional = false;
    } else if (SqlTypeFamily.EXACT_NUMERIC.getTypeNames().contains(sqlTypeName)) {
      // Decimal
      assert sqlTypeName == SqlTypeName.DECIMAL;
      if (type.getScale() == 0) {
        fractional = false;
      } else {
        fractional = true;
      }
    } else {
      // Cannot handle this aggregate function type
      throw new AssertionError("unknown aggregate type " + type);
    }

    JsonAggregation aggregation;

    CalciteConnectionConfig config = getConnectionConfig();

    // Convert from a complex metric
    ComplexMetric complexMetric = druidTable.resolveComplexMetric(only, aggCall);

    switch (aggCall.getAggregation().getKind()) {
    case COUNT:
      if (aggCall.isDistinct()) {
        if (aggCall.isApproximate() || config.approximateDistinctCount()) {
          if (complexMetric == null) {
            aggregation = new JsonCardinalityAggregation("cardinality", name, list);
          } else {
            aggregation = new JsonAggregation(complexMetric.getMetricType(), name,
                    complexMetric.getMetricName());
          }
          break;
        } else {
          // Gets thrown if one of the rules allows a count(distinct ...) through
          // when approximate results were not told be acceptable.
          throw new UnsupportedOperationException("Cannot push " + aggCall
              + " because an approximate count distinct is not acceptable.");
        }
      }
      if (aggCall.getArgList().size() == 1) {
        // case we have count(column) push it as count(*) where column is not null
        final DruidJsonFilter matchNulls = DruidJsonFilter.getSelectorFilter(only, null, null);
        final DruidJsonFilter filterOutNulls = DruidJsonFilter.toNotDruidFilter(matchNulls);
        aggregation = new JsonFilteredAggregation(filterOutNulls,
            new JsonAggregation("count", name, only));
      } else {
        aggregation = new JsonAggregation("count", name, only);
      }

      break;
    case SUM:
    case SUM0:
      aggregation = new JsonAggregation(fractional ? "doubleSum" : "longSum", name, only);
      break;
    case MIN:
      aggregation = new JsonAggregation(fractional ? "doubleMin" : "longMin", name, only);
      break;
    case MAX:
      aggregation = new JsonAggregation(fractional ? "doubleMax" : "longMax", name, only);
      break;
    default:
      throw new AssertionError("unknown aggregate " + aggCall);
    }

    // Check for filters
    if (aggCall.hasFilter()) {
      RexNode filterNode = projects.get(aggCall.filterArg);
      DruidJsonFilter druidFilter = DruidJsonFilter
          .toDruidFilters(projects.get(aggCall.filterArg), table.getRowType(), this);
      Preconditions.checkNotNull(
          druidFilter, DateTimeStringUtils.format("Druid Filter is null instead of [%s]",
              filterNode));
      aggregation = new JsonFilteredAggregation(druidFilter, aggregation);
    }

    return aggregation;
  }

  public JsonPostAggregation getJsonPostAggregation(String name, RexNode rexNode, RelNode rel) {
    if (rexNode instanceof RexCall) {
      List<JsonPostAggregation> fields = new ArrayList<>();
      for (RexNode ele : ((RexCall) rexNode).getOperands()) {
        JsonPostAggregation field = getJsonPostAggregation("", ele, rel);
        if (field == null) {
          throw new RuntimeException("Unchecked types that cannot be parsed as Post Aggregator");
        }
        fields.add(field);
      }
      switch (rexNode.getKind()) {
      case PLUS:
        return new JsonArithmetic(name, "+", fields, null);
      case MINUS:
        return new JsonArithmetic(name, "-", fields, null);
      case DIVIDE:
        return new JsonArithmetic(name, "quotient", fields, null);
      case TIMES:
        return new JsonArithmetic(name, "*", fields, null);
      case CAST:
        return getJsonPostAggregation(name, ((RexCall) rexNode).getOperands().get(0),
            rel);
      default:
      }
    } else if (rexNode instanceof RexInputRef) {
      // Subtract only number of grouping columns as offset because for now only Aggregates
      // without grouping sets (i.e. indicator columns size is zero) are allowed to pushed
      // in Druid Query.
      Integer indexSkipGroup = ((RexInputRef) rexNode).getIndex()
          - ((Aggregate) rel).getGroupCount();
      AggregateCall aggCall = ((Aggregate) rel).getAggCallList().get(indexSkipGroup);
      // Use either the hyper unique estimator, or the theta sketch one.
      // Hyper unique is used by default.
      if (aggCall.isDistinct()
          && aggCall.getAggregation().getKind() == SqlKind.COUNT) {
        final String fieldName = rel.getRowType().getFieldNames()
                .get(((RexInputRef) rexNode).getIndex());

        List<String> fieldNames = ((Aggregate) rel).getInput().getRowType().getFieldNames();
        String complexName = fieldNames.get(aggCall.getArgList().get(0));
        ComplexMetric metric = druidTable.resolveComplexMetric(complexName, aggCall);

        if (metric != null) {
          switch (metric.getDruidType()) {
          case THETA_SKETCH:
            return new JsonThetaSketchEstimate("", fieldName);
          case HYPER_UNIQUE:
            return new JsonHyperUniqueCardinality("", fieldName);
          default:
            throw new AssertionError("Can not translate complex metric type: "
                    + metric.getDruidType());
          }
        }
        // Count distinct on a non-complex column.
        return new JsonHyperUniqueCardinality("", fieldName);
      }
      return new JsonFieldAccessor("",
          rel.getRowType().getFieldNames().get(((RexInputRef) rexNode).getIndex()));
    } else if (rexNode instanceof RexLiteral) {
      // Druid constant post aggregator only supports numeric value for now.
      // (http://druid.io/docs/0.10.0/querying/post-aggregations.html) Accordingly, all
      // numeric type of RexLiteral can only have BigDecimal value, so filter out unsupported
      // constant by checking the type of RexLiteral value.
      if (((RexLiteral) rexNode).getValue3() instanceof BigDecimal) {
        return new JsonConstant("",
            ((BigDecimal) ((RexLiteral) rexNode).getValue3()).doubleValue());
      }
    }
    throw new RuntimeException("Unchecked types that cannot be parsed as Post Aggregator");
  }

  protected static void writeField(JsonGenerator generator, String fieldName,
      Object o) throws IOException {
    generator.writeFieldName(fieldName);
    writeObject(generator, o);
  }

  protected static void writeFieldIf(JsonGenerator generator, String fieldName,
      Object o) throws IOException {
    if (o != null) {
      writeField(generator, fieldName, o);
    }
  }

  protected static void writeArray(JsonGenerator generator, List<?> elements)
      throws IOException {
    generator.writeStartArray();
    for (Object o : elements) {
      writeObject(generator, o);
    }
    generator.writeEndArray();
  }

  protected static void writeObject(JsonGenerator generator, Object o)
      throws IOException {
    if (o instanceof String) {
      String s = (String) o;
      generator.writeString(s);
    } else if (o instanceof Interval) {
      generator.writeString(o.toString());
    } else if (o instanceof Integer) {
      Integer i = (Integer) o;
      generator.writeNumber(i);
    } else if (o instanceof List) {
      writeArray(generator, (List<?>) o);
    } else if (o instanceof DruidJson) {
      ((DruidJson) o).write(generator);
    } else {
      throw new AssertionError("not a json object: " + o);
    }
  }

  /** Generates a JSON string to query metadata about a data source. */
  static String metadataQuery(String dataSourceName,
      List<Interval> intervals) {
    final StringWriter sw = new StringWriter();
    final JsonFactory factory = new JsonFactory();
    try {
      final JsonGenerator generator = factory.createGenerator(sw);
      generator.writeStartObject();
      generator.writeStringField("queryType", "segmentMetadata");
      generator.writeStringField("dataSource", dataSourceName);
      generator.writeBooleanField("merge", true);
      generator.writeBooleanField("lenientAggregatorMerge", true);
      generator.writeArrayFieldStart("analysisTypes");
      generator.writeString("aggregators");
      generator.writeEndArray();
      writeFieldIf(generator, "intervals", intervals);
      generator.writeEndObject();
      generator.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sw.toString();
  }

  /** Druid query specification. */
  public static class QuerySpec {
    final QueryType queryType;
    final String queryString;
    final List<String> fieldNames;

    QuerySpec(QueryType queryType, String queryString,
        List<String> fieldNames) {
      this.queryType = Preconditions.checkNotNull(queryType);
      this.queryString = Preconditions.checkNotNull(queryString);
      this.fieldNames = ImmutableList.copyOf(fieldNames);
    }

    @Override public int hashCode() {
      return Objects.hash(queryType, queryString, fieldNames);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof QuerySpec
          && queryType == ((QuerySpec) obj).queryType
          && queryString.equals(((QuerySpec) obj).queryString)
          && fieldNames.equals(((QuerySpec) obj).fieldNames);
    }

    @Override public String toString() {
      return "{queryType: " + queryType
          + ", queryString: " + queryString
          + ", fieldNames: " + fieldNames + "}";
    }

    public String getQueryString(String pagingIdentifier, int offset) {
      if (pagingIdentifier == null) {
        return queryString;
      }
      return queryString.replace("\"threshold\":",
          "\"pagingIdentifiers\":{\"" + pagingIdentifier + "\":" + offset
              + "},\"threshold\":");
    }
  }

  /** Translates scalar expressions to Druid field references. */
  @VisibleForTesting
  protected static class Translator {
    final List<String> dimensions = new ArrayList<>();
    final List<String> metrics = new ArrayList<>();
    final DruidTable druidTable;
    final RelDataType rowType;
    final String timeZone;
    final DruidQuery druidQuery;
    final SimpleDateFormat dateFormatter;

    Translator(DruidTable druidTable, RelDataType rowType, String timeZone, DruidQuery druidQuery) {
      this.druidTable = druidTable;
      this.rowType = rowType;
      this.druidQuery = druidQuery;
      for (RelDataTypeField f : rowType.getFieldList()) {
        final String fieldName = f.getName();
        if (druidTable.isMetric(fieldName)) {
          metrics.add(fieldName);
        } else if (!druidTable.timestampFieldName.equals(fieldName)
            && !DruidTable.DEFAULT_TIMESTAMP_COLUMN.equals(fieldName)) {
          dimensions.add(fieldName);
        }
      }
      this.timeZone = timeZone;
      this.dateFormatter = new SimpleDateFormat(TimeExtractionFunction.ISO_TIME_FORMAT,
          Locale.ROOT);
      if (timeZone != null) {
        this.dateFormatter.setTimeZone(TimeZone.getTimeZone(timeZone));
      }
    }

    protected void clearFieldNameLists() {
      dimensions.clear();
      metrics.clear();
    }

    /** Formats timestamp values to druid format using
     * {@link DruidQuery.Translator#dateFormatter}. This is needed when pushing
     * timestamp comparisons to druid using a TimeFormatExtractionFunction that
     * returns a string value. */
    @SuppressWarnings("incomplete-switch")
    String translate(RexNode e, boolean set, boolean formatDateString) {
      int index = -1;
      switch (e.getKind()) {
      case INPUT_REF:
        final RexInputRef ref = (RexInputRef) e;
        index = ref.getIndex();
        break;
      case CAST:
        return tr(e, 0, set, formatDateString);
      case LITERAL:
        final RexLiteral rexLiteral = (RexLiteral) e;
        if (!formatDateString) {
          return Objects.toString(rexLiteral.getValue3());
        } else {
          // Case when we are passing to druid as an extractionFunction
          // Need to format the timestamp String in druid format.
          TimestampString timestampString = DruidDateTimeUtils
              .literalValue(e, TimeZone.getTimeZone(timeZone));
          if (timestampString == null) {
            throw new AssertionError(
                "Cannot translate Literal" + e + " of type "
                    + rexLiteral.getTypeName() + " to TimestampString");
          }
          return dateFormatter.format(timestampString.getMillisSinceEpoch());
        }
      case FLOOR:
      case EXTRACT:
        final RexCall call = (RexCall) e;
        assert DruidDateTimeUtils.extractGranularity(call, timeZone) != null;
        index = RelOptUtil.InputFinder.bits(e).asList().get(0);
        break;
      case IS_TRUE:
        return ""; // the fieldName for which this is the filter will be added separately
      }
      if (index == -1) {
        return null;
      }
      final String fieldName = rowType.getFieldList().get(index).getName();
      if (set) {
        if (druidTable.metricFieldNames.contains(fieldName)) {
          metrics.add(fieldName);
        } else if (!druidTable.timestampFieldName.equals(fieldName)
            && !DruidTable.DEFAULT_TIMESTAMP_COLUMN.equals(fieldName)) {
          dimensions.add(fieldName);
        }
      }
      return fieldName;
    }

    private String tr(RexNode call, int index, boolean formatDateString) {
      return tr(call, index, false, formatDateString);
    }

    private String tr(RexNode call, int index, boolean set, boolean formatDateString) {
      return translate(((RexCall) call).getOperands().get(index), set, formatDateString);
    }
  }

  /** Interpreter node that executes a Druid query and sends the results to a
   * {@link Sink}. */
  private static class DruidQueryNode implements Node {
    private final Sink sink;
    private final DruidQuery query;
    private final QuerySpec querySpec;

    DruidQueryNode(Compiler interpreter, DruidQuery query) {
      this.query = query;
      this.sink = interpreter.sink(query);
      this.querySpec = query.getQuerySpec();
      Hook.QUERY_PLAN.run(querySpec);
    }

    public void run() throws InterruptedException {
      final List<ColumnMetaData.Rep> fieldTypes = new ArrayList<>();
      for (RelDataTypeField field : query.getRowType().getFieldList()) {
        fieldTypes.add(getPrimitive(field));
      }
      final DruidConnectionImpl connection =
          new DruidConnectionImpl(query.druidTable.schema.url,
              query.druidTable.schema.coordinatorUrl);
      final boolean limitQuery = containsLimit(querySpec);
      final DruidConnectionImpl.Page page = new DruidConnectionImpl.Page();
      do {
        final String queryString =
            querySpec.getQueryString(page.pagingIdentifier, page.offset);
        connection.request(querySpec.queryType, queryString, sink,
            querySpec.fieldNames, fieldTypes, page);
      } while (!limitQuery
          && page.pagingIdentifier != null
          && page.totalRowCount > 0);
    }

    private static boolean containsLimit(QuerySpec querySpec) {
      return querySpec.queryString.contains("\"context\":{\""
          + DRUID_QUERY_FETCH + "\":true");
    }

    private ColumnMetaData.Rep getPrimitive(RelDataTypeField field) {
      switch (field.getType().getSqlTypeName()) {
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP;
      case BIGINT:
        return ColumnMetaData.Rep.LONG;
      case INTEGER:
        return ColumnMetaData.Rep.INTEGER;
      case SMALLINT:
        return ColumnMetaData.Rep.SHORT;
      case TINYINT:
        return ColumnMetaData.Rep.BYTE;
      case REAL:
        return ColumnMetaData.Rep.FLOAT;
      case DOUBLE:
      case FLOAT:
        return ColumnMetaData.Rep.DOUBLE;
      default:
        return null;
      }
    }
  }

  /** Aggregation element of a Druid "groupBy" or "topN" query. */
  private static class JsonAggregation implements DruidJson {
    final String type;
    final String name;
    final String fieldName;

    private JsonAggregation(String type, String name, String fieldName) {
      this.type = type;
      this.name = name;
      this.fieldName = fieldName;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      generator.writeStringField("name", name);
      writeFieldIf(generator, "fieldName", fieldName);
      generator.writeEndObject();
    }
  }

  /** Collation element of a Druid "groupBy" query. */
  private static class JsonLimit implements DruidJson {
    final String type;
    final Integer limit;
    final ImmutableList<JsonCollation> collations;

    private JsonLimit(String type, Integer limit, ImmutableList<JsonCollation> collations) {
      this.type = type;
      this.limit = limit;
      this.collations = collations;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      writeFieldIf(generator, "limit", limit);
      writeFieldIf(generator, "columns", collations);
      generator.writeEndObject();
    }
  }

  /** Collation element of a Druid "groupBy" query. */
  private static class JsonCollation implements DruidJson {
    final String dimension;
    final String direction;
    final String dimensionOrder;

    private JsonCollation(String dimension, String direction, String dimensionOrder) {
      this.dimension = dimension;
      this.direction = direction;
      this.dimensionOrder = dimensionOrder;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("dimension", dimension);
      writeFieldIf(generator, "direction", direction);
      writeFieldIf(generator, "dimensionOrder", dimensionOrder);
      generator.writeEndObject();
    }
  }

  /** Aggregation element that calls the "cardinality" function. */
  private static class JsonCardinalityAggregation extends JsonAggregation {
    final List<String> fieldNames;

    private JsonCardinalityAggregation(String type, String name,
        List<String> fieldNames) {
      super(type, name, null);
      this.fieldNames = fieldNames;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      generator.writeStringField("name", name);
      writeFieldIf(generator, "fieldNames", fieldNames);
      generator.writeEndObject();
    }
  }

  /** Aggregation element that contains a filter */
  private static class JsonFilteredAggregation extends JsonAggregation {
    final DruidJsonFilter filter;
    final JsonAggregation aggregation;

    private JsonFilteredAggregation(DruidJsonFilter filter, JsonAggregation aggregation) {
      // Filtered aggregations don't use the "name" and "fieldName" fields directly,
      // but rather use the ones defined in their "aggregation" field.
      super("filtered", aggregation.name, aggregation.fieldName);
      this.filter = filter;
      this.aggregation = aggregation;
      // The aggregation cannot be a JsonFilteredAggregation
      assert !(aggregation instanceof JsonFilteredAggregation);
    }

    @Override public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      writeField(generator, "filter", filter);
      writeField(generator, "aggregator", aggregation);
      generator.writeEndObject();
    }
  }

  /** Post-Aggregator Post aggregator abstract writer */
  protected abstract static class JsonPostAggregation implements DruidJson {
    final String type;
    String name;

    private JsonPostAggregation(String name, String type) {
      this.type = type;
      this.name = name;
    }

    // Expects all subclasses to write the EndObject item
    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      generator.writeStringField("name", name);
    }

    public void setName(String name) {
      this.name = name;
    }

    public abstract JsonPostAggregation copy();
  }

  /** FieldAccessor Post aggregator writer */
  private static class JsonFieldAccessor extends JsonPostAggregation {
    final String fieldName;

    private JsonFieldAccessor(String name, String fieldName) {
      super(name, "fieldAccess");
      this.fieldName = fieldName;
    }

    public void write(JsonGenerator generator) throws IOException {
      super.write(generator);
      generator.writeStringField("fieldName", fieldName);
      generator.writeEndObject();
    }

    /**
     * Leaf node in Post-aggs Json Tree, return an identical leaf node.
     */

    public JsonPostAggregation copy() {
      return new JsonFieldAccessor(this.name, this.fieldName);
    }
  }

  /** Constant Post aggregator writer */
  private static class JsonConstant extends JsonPostAggregation {
    final double value;

    private JsonConstant(String name, double value) {
      super(name, "constant");
      this.value = value;
    }

    public void write(JsonGenerator generator) throws IOException {
      super.write(generator);
      generator.writeNumberField("value", value);
      generator.writeEndObject();
    }

    /**
     * Leaf node in Post-aggs Json Tree, return an identical leaf node.
     */

    public JsonPostAggregation copy() {
      return new JsonConstant(this.name, this.value);
    }
  }

  /** Greatest/Leastest Post aggregator writer */
  private static class JsonGreatestLeast extends JsonPostAggregation {
    final List<JsonPostAggregation> fields;
    final boolean fractional;
    final boolean greatest;

    private JsonGreatestLeast(String name, List<JsonPostAggregation> fields,
                              boolean fractional, boolean greatest) {
      super(name, greatest ? (fractional ? "doubleGreatest" : "longGreatest")
          : (fractional ? "doubleLeast" : "longLeast"));
      this.fields = fields;
      this.fractional = fractional;
      this.greatest = greatest;
    }

    public void write(JsonGenerator generator) throws IOException {
      super.write(generator);
      writeFieldIf(generator, "fields", fields);
      generator.writeEndObject();
    }

    /**
     * Non-leaf node in Post-aggs Json Tree, recursively copy the leaf node.
     */

    public JsonPostAggregation copy() {
      ImmutableList.Builder<JsonPostAggregation> builder = ImmutableList.builder();
      for (JsonPostAggregation field : fields) {
        builder.add(field.copy());
      }
      return new JsonGreatestLeast(name, builder.build(), fractional, greatest);
    }
  }

  /** Arithmetic Post aggregator writer */
  private static class JsonArithmetic extends JsonPostAggregation {
    final String fn;
    final List<JsonPostAggregation> fields;
    final String ordering;

    private JsonArithmetic(String name, String fn, List<JsonPostAggregation> fields,
                           String ordering) {
      super(name, "arithmetic");
      this.fn = fn;
      this.fields = fields;
      this.ordering = ordering;
    }

    public void write(JsonGenerator generator) throws IOException {
      super.write(generator);
      generator.writeStringField("fn", fn);
      writeFieldIf(generator, "fields", fields);
      writeFieldIf(generator, "ordering", ordering);
      generator.writeEndObject();
    }

    /**
     * Non-leaf node in Post-aggs Json Tree, recursively copy the leaf node.
     */

    public JsonPostAggregation copy() {
      ImmutableList.Builder<JsonPostAggregation> builder = ImmutableList.builder();
      for (JsonPostAggregation field : fields) {
        builder.add(field.copy());
      }
      return new JsonArithmetic(name, fn, builder.build(), ordering);
    }
  }

  /** HyperUnique Cardinality Post aggregator writer */
  private static class JsonHyperUniqueCardinality extends JsonPostAggregation {
    final String fieldName;

    private JsonHyperUniqueCardinality(String name, String fieldName) {
      super(name, "hyperUniqueCardinality");
      this.fieldName = fieldName;
    }

    public void write(JsonGenerator generator) throws IOException {
      super.write(generator);
      generator.writeStringField("fieldName", fieldName);
      generator.writeEndObject();
    }

    /**
     * Leaf node in Post-aggs Json Tree, return an identical leaf node.
     */

    public JsonPostAggregation copy() {
      return new JsonHyperUniqueCardinality(this.name, this.fieldName);
    }
  }

  /** Theta Sketch Estimator for Post aggregation */
  private static class JsonThetaSketchEstimate extends JsonPostAggregation {
    final String fieldName;

    private JsonThetaSketchEstimate(String name, String fieldName) {
      super(name, "thetaSketchEstimate");
      this.fieldName = fieldName;
    }

    @Override public JsonPostAggregation copy() {
      return new JsonThetaSketchEstimate(name, fieldName);
    }

    @Override public void write(JsonGenerator generator) throws IOException {
      super.write(generator);
      // Druid spec for ThetaSketchEstimate requires a field accessor
      writeField(generator, "field", new JsonFieldAccessor("", fieldName));
      generator.writeEndObject();
    }
  }

  /**
   * @return index of the timestamp ref or -1 if not present
   */
  public int getTimestampFieldIndex() {
    for (int i = 0; i < this.getRowType().getFieldCount(); i++) {
      if (this.druidTable.timestampFieldName.equals(
          this.getRowType().getFieldList().get(i).getName())) {
        return i;
      }
    }
    return -1;
  }
}

// End DruidQuery.java
