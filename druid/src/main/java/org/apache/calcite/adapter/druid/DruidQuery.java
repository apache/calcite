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
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
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
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Relational expression representing a scan of a Druid data set.
 */
public class DruidQuery extends AbstractRelNode implements BindableRel {

  protected QuerySpec querySpec;

  final RelOptTable table;
  final DruidTable druidTable;
  final ImmutableList<LocalInterval> intervals;
  final ImmutableList<RelNode> rels;

  private static final Pattern VALID_SIG = Pattern.compile("sf?p?a?l?");
  protected static final String DRUID_QUERY_FETCH = "druid.query.fetch";

  /**
   * Creates a DruidQuery.
   *
   * @param cluster        Cluster
   * @param traitSet       Traits
   * @param table          Table
   * @param druidTable     Druid table
   * @param intervals      Intervals for the query
   * @param rels           Internal relational expressions
   */
  protected DruidQuery(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable,
      List<LocalInterval> intervals, List<RelNode> rels) {
    super(cluster, traitSet);
    this.table = table;
    this.druidTable = druidTable;
    this.intervals = ImmutableList.copyOf(intervals);
    this.rels = ImmutableList.copyOf(rels);

    assert isValid(Litmus.THROW, null);
  }

  /** Returns a string describing the operations inside this query.
   *
   * <p>For example, "sfpal" means {@link TableScan} (s)
   * followed by {@link Filter} (f)
   * followed by {@link Project} (p)
   * followed by {@link Aggregate} (a)
   * followed by {@link Sort} (l).
   *
   * @see #isValidSignature(String)
   */
  String signature() {
    final StringBuilder b = new StringBuilder();
    for (RelNode rel : rels) {
      b.append(rel instanceof TableScan ? 's'
          : rel instanceof Project ? 'p'
              : rel instanceof Filter ? 'f'
                  : rel instanceof Aggregate ? 'a'
                      : rel instanceof Sort ? 'l'
                          : '!');
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
          for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.filterArg >= 0) {
              return litmus.fail("no filtered aggregate functions");
            }
          }
        }
        if (r instanceof Filter) {
          final Filter filter = (Filter) r;
          if (!isValidFilter(filter.getCondition())) {
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

  boolean isValidFilter(RexNode e) {
    return isValidFilter(e, false);
  }

  boolean isValidFilter(RexNode e, boolean boundedComparator) {
    switch (e.getKind()) {
    case INPUT_REF:
    case LITERAL:
      return true;
    case AND:
    case OR:
    case NOT:
    case EQUALS:
    case NOT_EQUALS:
    case IN:
      return areValidFilters(((RexCall) e).getOperands(), false);
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case BETWEEN:
      return areValidFilters(((RexCall) e).getOperands(), true);
    case CAST:
      return isValidCast((RexCall) e, boundedComparator);
    default:
      return false;
    }
  }

  private boolean areValidFilters(List<RexNode> es, boolean boundedComparator) {
    for (RexNode e : es) {
      if (!isValidFilter(e, boundedComparator)) {
        return false;
      }
    }
    return true;
  }

  private boolean isValidCast(RexCall e, boolean boundedComparator) {
    assert e.isA(SqlKind.CAST);
    if (e.getOperands().get(0).isA(SqlKind.INPUT_REF)
        && e.getType().getFamily() == SqlTypeFamily.CHARACTER) {
      // CAST of input to character type
      return true;
    }
    if (e.getOperands().get(0).isA(SqlKind.INPUT_REF)
        && e.getType().getFamily() == SqlTypeFamily.NUMERIC
        && boundedComparator) {
      // CAST of input to numeric type, it is part of a bounded comparison
      return true;
    }
    if (e.getOperands().get(0).isA(SqlKind.LITERAL)
        && e.getType().getFamily() == SqlTypeFamily.TIMESTAMP) {
      // CAST of literal to timestamp type
      return true;
    }
    // Currently other CAST operations cannot be pushed to Druid
    return false;
  }

  /** Returns whether a signature represents an sequence of relational operators
   * that can be translated into a valid Druid query. */
  static boolean isValidSignature(String signature) {
    return VALID_SIG.matcher(signature).matches();
  }

  /** Creates a DruidQuery. */
  public static DruidQuery create(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable, List<RelNode> rels) {
    return new DruidQuery(cluster, traitSet, table, druidTable, druidTable.intervals, rels);
  }

  /** Creates a DruidQuery. */
  private static DruidQuery create(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable, List<LocalInterval> intervals,
      List<RelNode> rels) {
    return new DruidQuery(cluster, traitSet, table, druidTable, intervals, rels);
  }

  /** Extends a DruidQuery. */
  public static DruidQuery extendQuery(DruidQuery query, RelNode r) {
    final ImmutableList.Builder<RelNode> builder = ImmutableList.builder();
    return DruidQuery.create(query.getCluster(), r.getTraitSet().replace(query.getConvention()),
        query.getTable(), query.druidTable, query.intervals,
        builder.addAll(query.rels).add(r).build());
  }

  /** Extends a DruidQuery. */
  public static DruidQuery extendQuery(DruidQuery query,
      List<LocalInterval> intervals) {
    return DruidQuery.create(query.getCluster(), query.getTraitSet(), query.getTable(),
        query.druidTable, intervals, query.rels);
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
        pw.item("projects", ((Project) rel).getProjects());
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
        .multiplyBy(getQueryTypeCostMultiplier());
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
    return new DruidQueryNode(implementor.interpreter, this);
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

    List<Integer> collationIndexes = null;
    List<Direction> collationDirections = null;
    Integer fetch = null;
    if (i < rels.size() && rels.get(i) instanceof Sort) {
      final Sort sort = (Sort) rels.get(i++);
      collationIndexes = new ArrayList<>();
      collationDirections = new ArrayList<>();
      for (RelFieldCollation fCol: sort.collation.getFieldCollations()) {
        collationIndexes.add(fCol.getFieldIndex());
        collationDirections.add(fCol.getDirection());
      }
      fetch = sort.fetch != null ? RexLiteral.intValue(sort.fetch) : null;
    }

    if (i != rels.size()) {
      throw new AssertionError("could not implement all rels");
    }

    return getQuery(rowType, filter, projects, groupSet, aggCalls, aggNames,
        collationIndexes, collationDirections, fetch);
  }

  public QueryType getQueryType() {
    return getQuerySpec().queryType;
  }

  public String getQueryString() {
    return getQuerySpec().queryString;
  }

  protected QuerySpec getQuery(RelDataType rowType, RexNode filter, List<RexNode> projects,
      ImmutableBitSet groupSet, List<AggregateCall> aggCalls, List<String> aggNames,
      List<Integer> collationIndexes, List<Direction> collationDirections, Integer fetch) {
    final CalciteConnectionConfig config =
        getCluster().getPlanner().getContext()
            .unwrap(CalciteConnectionConfig.class);
    QueryType queryType = QueryType.SELECT;
    final Translator translator = new Translator(druidTable, rowType);
    List<String> fieldNames = rowType.getFieldNames();

    // Handle filter
    Json jsonFilter = null;
    if (filter != null) {
      jsonFilter = translator.translateFilter(filter);
    }

    // Then we handle project
    if (projects != null) {
      translator.metrics.clear();
      translator.dimensions.clear();
      final ImmutableList.Builder<String> builder = ImmutableList.builder();
      for (RexNode project : projects) {
        builder.add(translator.translate(project, true));
      }
      fieldNames = builder.build();
    }

    // Finally we handle aggregate and sort. Handling of these
    // operators is more complex, since we need to extract
    // the conditions to know whether the query will be
    // executed as a Timeseries, TopN, or GroupBy in Druid
    final List<String> dimensions = new ArrayList<>();
    final List<JsonAggregation> aggregations = new ArrayList<>();
    Granularity granularity = Granularity.ALL;
    Direction timeSeriesDirection = null;
    JsonLimit limit = null;
    if (groupSet != null) {
      assert aggCalls != null;
      assert aggNames != null;
      assert aggCalls.size() == aggNames.size();

      int timePositionIdx = -1;
      final ImmutableList.Builder<String> builder = ImmutableList.builder();
      if (projects != null) {
        for (int groupKey : groupSet) {
          final String s = fieldNames.get(groupKey);
          final RexNode project = projects.get(groupKey);
          if (project instanceof RexInputRef) {
            // Reference, it could be to the timestamp column or any other dimension
            final RexInputRef ref = (RexInputRef) project;
            final String origin = druidTable.getRowType(getCluster().getTypeFactory())
                .getFieldList().get(ref.getIndex()).getName();
            if (origin.equals(druidTable.timestampFieldName)) {
              granularity = Granularity.NONE;
              builder.add(s);
              assert timePositionIdx == -1;
              timePositionIdx = groupKey;
            } else {
              dimensions.add(s);
              builder.add(s);
            }
          } else if (project instanceof RexCall) {
            // Call, check if we should infer granularity
            final RexCall call = (RexCall) project;
            final Granularity funcGranularity =
                DruidDateTimeUtils.extractGranularity(call);
            if (funcGranularity != null) {
              granularity = funcGranularity;
              builder.add(s);
              assert timePositionIdx == -1;
              timePositionIdx = groupKey;
            } else {
              dimensions.add(s);
              builder.add(s);
            }
          } else {
            throw new AssertionError("incompatible project expression: " + project);
          }
        }
      } else {
        for (int groupKey : groupSet) {
          final String s = fieldNames.get(groupKey);
          if (s.equals(druidTable.timestampFieldName)) {
            granularity = Granularity.NONE;
            builder.add(s);
            assert timePositionIdx == -1;
            timePositionIdx = groupKey;
          } else {
            dimensions.add(s);
            builder.add(s);
          }
        }
      }

      for (Pair<AggregateCall, String> agg : Pair.zip(aggCalls, aggNames)) {
        final JsonAggregation jsonAggregation =
            getJsonAggregation(fieldNames, agg.right, agg.left);
        aggregations.add(jsonAggregation);
        builder.add(jsonAggregation.name);
      }

      fieldNames = builder.build();

      ImmutableList<JsonCollation> collations = null;
      boolean sortsMetric = false;
      if (collationIndexes != null) {
        assert collationDirections != null;
        ImmutableList.Builder<JsonCollation> colBuilder =
            ImmutableList.builder();
        for (Pair<Integer, Direction> p : Pair.zip(collationIndexes, collationDirections)) {
          colBuilder.add(
              new JsonCollation(fieldNames.get(p.left),
                  p.right == Direction.DESCENDING ? "descending" : "ascending"));
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
          && granularity == Granularity.ALL
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
        generator.writeStringField("granularity", granularity.value);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "aggregations", aggregations);
        writeFieldIf(generator, "postAggregations", null);
        writeField(generator, "intervals", intervals);

        generator.writeFieldName("context");
        // The following field is necessary to conform with SQL semantics (CALCITE-1589)
        generator.writeStartObject();
        generator.writeBooleanField("skipEmptyBuckets", true);
        generator.writeEndObject();

        generator.writeEndObject();
        break;

      case TOP_N:
        generator.writeStartObject();

        generator.writeStringField("queryType", "topN");
        generator.writeStringField("dataSource", druidTable.dataSource);
        generator.writeStringField("granularity", granularity.value);
        generator.writeStringField("dimension", dimensions.get(0));
        generator.writeStringField("metric", fieldNames.get(collationIndexes.get(0)));
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "aggregations", aggregations);
        writeFieldIf(generator, "postAggregations", null);
        writeField(generator, "intervals", intervals);
        generator.writeNumberField("threshold", fetch);

        generator.writeEndObject();
        break;

      case GROUP_BY:
        generator.writeStartObject();

        if (aggregations.isEmpty()) {
          // Druid requires at least one aggregation, otherwise gives:
          //   Must have at least one AggregatorFactory
          aggregations.add(
              new JsonAggregation("longSum", "dummy_agg", "dummy_agg"));
        }

        generator.writeStringField("queryType", "groupBy");
        generator.writeStringField("dataSource", druidTable.dataSource);
        generator.writeStringField("granularity", granularity.value);
        writeField(generator, "dimensions", dimensions);
        writeFieldIf(generator, "limitSpec", limit);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "aggregations", aggregations);
        writeFieldIf(generator, "postAggregations", null);
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
        generator.writeStringField("granularity", granularity.value);

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
      String name, AggregateCall aggCall) {
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
    switch (aggCall.getAggregation().getKind()) {
    case COUNT:
      if (aggCall.isDistinct()) {
        return new JsonCardinalityAggregation("cardinality", name, list);
      }
      return new JsonAggregation("count", name, only);
    case SUM:
    case SUM0:
      return new JsonAggregation(fractional ? "doubleSum" : "longSum", name, only);
    case MIN:
      return new JsonAggregation(fractional ? "doubleMin" : "longMin", name, only);
    case MAX:
      return new JsonAggregation(fractional ? "doubleMax" : "longMax", name, only);
    default:
      throw new AssertionError("unknown aggregate " + aggCall);
    }
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
    } else if (o instanceof LocalInterval) {
      generator.writeString(o.toString());
    } else if (o instanceof Integer) {
      Integer i = (Integer) o;
      generator.writeNumber(i);
    } else if (o instanceof List) {
      writeArray(generator, (List<?>) o);
    } else if (o instanceof Json) {
      ((Json) o).write(generator);
    } else {
      throw new AssertionError("not a json object: " + o);
    }
  }

  /** Generates a JSON string to query metadata about a data source. */
  static String metadataQuery(String dataSourceName,
      List<LocalInterval> intervals) {
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

    Translator(DruidTable druidTable, RelDataType rowType) {
      this.druidTable = druidTable;
      this.rowType = rowType;
      for (RelDataTypeField f : rowType.getFieldList()) {
        final String fieldName = f.getName();
        if (druidTable.metricFieldNames.contains(fieldName)) {
          metrics.add(fieldName);
        } else if (!druidTable.timestampFieldName.equals(fieldName)
            && !DruidTable.DEFAULT_TIMESTAMP_COLUMN.equals(fieldName)) {
          dimensions.add(fieldName);
        }
      }
    }

    String translate(RexNode e, boolean set) {
      switch (e.getKind()) {
      case INPUT_REF:
        final RexInputRef ref = (RexInputRef) e;
        final String fieldName =
            rowType.getFieldList().get(ref.getIndex()).getName();
        if (set) {
          if (druidTable.metricFieldNames.contains(fieldName)) {
            metrics.add(fieldName);
          } else if (!druidTable.timestampFieldName.equals(fieldName)
              && !DruidTable.DEFAULT_TIMESTAMP_COLUMN.equals(fieldName)) {
            dimensions.add(fieldName);
          }
        }
        return fieldName;

      case CAST:
        return tr(e, 0, set);

      case LITERAL:
        return ((RexLiteral) e).getValue2().toString();

      case FLOOR:
        final RexCall call = (RexCall) e;
        assert DruidDateTimeUtils.extractGranularity(call) != null;
        return tr(call, 0, set);

      default:
        throw new AssertionError("invalid expression " + e);
      }
    }

    private JsonFilter translateFilter(RexNode e) {
      final RexCall call;
      switch (e.getKind()) {
      case EQUALS:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case IN:
      case BETWEEN:
        call = (RexCall) e;
        int posRef;
        int posConstant;
        if (RexUtil.isConstant(call.getOperands().get(1))) {
          posRef = 0;
          posConstant = 1;
        } else if (RexUtil.isConstant(call.getOperands().get(0))) {
          posRef = 1;
          posConstant = 0;
        } else {
          throw new AssertionError("it is not a valid comparison: " + e);
        }
        final boolean numeric =
            call.getOperands().get(posRef).getType().getFamily()
                == SqlTypeFamily.NUMERIC;
        switch (e.getKind()) {
        case EQUALS:
          return new JsonSelector("selector", tr(e, posRef), tr(e, posConstant));
        case NOT_EQUALS:
          return new JsonCompositeFilter("not",
              new JsonSelector("selector", tr(e, posRef), tr(e, posConstant)));
        case GREATER_THAN:
          return new JsonBound("bound", tr(e, posRef), tr(e, posConstant),
              true, null, false, numeric);
        case GREATER_THAN_OR_EQUAL:
          return new JsonBound("bound", tr(e, posRef), tr(e, posConstant),
              false, null, false, numeric);
        case LESS_THAN:
          return new JsonBound("bound", tr(e, posRef), null, false,
              tr(e, posConstant), true, numeric);
        case LESS_THAN_OR_EQUAL:
          return new JsonBound("bound", tr(e, posRef), null, false,
              tr(e, posConstant), false, numeric);
        case IN:
          ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
          for (RexNode rexNode: call.getOperands()) {
            if (rexNode.getKind() == SqlKind.LITERAL) {
              listBuilder.add(((RexLiteral) rexNode).getValue2().toString());
            }
          }
          return new JsonInFilter("in", tr(e, posRef), listBuilder.build());
        case BETWEEN:
          return new JsonBound("bound", tr(e, posRef), tr(e, 2), false,
              tr(e, 3), false, numeric);
        default:
          throw new AssertionError();
        }
      case AND:
      case OR:
      case NOT:
        call = (RexCall) e;
        return new JsonCompositeFilter(e.getKind().lowerName,
            translateFilters(call.getOperands()));
      default:
        throw new AssertionError("cannot translate filter: " + e);
      }
    }

    private String tr(RexNode call, int index) {
      return tr(call, index, false);
    }

    private String tr(RexNode call, int index, boolean set) {
      return translate(((RexCall) call).getOperands().get(index), set);
    }

    private List<JsonFilter> translateFilters(List<RexNode> operands) {
      final ImmutableList.Builder<JsonFilter> builder =
          ImmutableList.builder();
      for (RexNode operand : operands) {
        builder.add(translateFilter(operand));
      }
      return builder.build();
    }
  }

  /** Interpreter node that executes a Druid query and sends the results to a
   * {@link Sink}. */
  private static class DruidQueryNode implements Node {
    private final Sink sink;
    private final DruidQuery query;
    private final QuerySpec querySpec;

    DruidQueryNode(Interpreter interpreter, DruidQuery query) {
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
      if (field.getName().equals(query.druidTable.timestampFieldName)) {
        return ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP;
      }
      switch (field.getType().getSqlTypeName()) {
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

  /** Object that knows how to write itself to a
   * {@link com.fasterxml.jackson.core.JsonGenerator}. */
  private interface Json {
    void write(JsonGenerator generator) throws IOException;
  }

  /** Aggregation element of a Druid "groupBy" or "topN" query. */
  private static class JsonAggregation implements Json {
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
  private static class JsonLimit implements Json {
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
  private static class JsonCollation implements Json {
    final String dimension;
    final String direction;

    private JsonCollation(String dimension, String direction) {
      this.dimension = dimension;
      this.direction = direction;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("dimension", dimension);
      writeFieldIf(generator, "direction", direction);
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

  /** Filter element of a Druid "groupBy" or "topN" query. */
  private abstract static class JsonFilter implements Json {
    final String type;

    private JsonFilter(String type) {
      this.type = type;
    }
  }

  /** Equality filter. */
  private static class JsonSelector extends JsonFilter {
    private final String dimension;
    private final String value;

    private JsonSelector(String type, String dimension, String value) {
      super(type);
      this.dimension = dimension;
      this.value = value;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      generator.writeStringField("dimension", dimension);
      generator.writeStringField("value", value);
      generator.writeEndObject();
    }
  }

  /** Bound filter. */
  @VisibleForTesting
  protected static class JsonBound extends JsonFilter {
    private final String dimension;
    private final String lower;
    private final boolean lowerStrict;
    private final String upper;
    private final boolean upperStrict;
    private final boolean alphaNumeric;

    private JsonBound(String type, String dimension, String lower,
        boolean lowerStrict, String upper, boolean upperStrict,
        boolean alphaNumeric) {
      super(type);
      this.dimension = dimension;
      this.lower = lower;
      this.lowerStrict = lowerStrict;
      this.upper = upper;
      this.upperStrict = upperStrict;
      this.alphaNumeric = alphaNumeric;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      generator.writeStringField("dimension", dimension);
      if (lower != null) {
        generator.writeStringField("lower", lower);
        generator.writeBooleanField("lowerStrict", lowerStrict);
      }
      if (upper != null) {
        generator.writeStringField("upper", upper);
        generator.writeBooleanField("upperStrict", upperStrict);
      }
      generator.writeBooleanField("alphaNumeric", alphaNumeric);
      generator.writeEndObject();
    }
  }

  /** Filter that combines other filters using a boolean operator. */
  private static class JsonCompositeFilter extends JsonFilter {
    private final List<? extends JsonFilter> fields;

    private JsonCompositeFilter(String type,
        Iterable<? extends JsonFilter> fields) {
      super(type);
      this.fields = ImmutableList.copyOf(fields);
    }

    private JsonCompositeFilter(String type, JsonFilter... fields) {
      this(type, ImmutableList.copyOf(fields));
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      switch (type) {
      case "NOT":
        writeField(generator, "field", fields.get(0));
        break;
      default:
        writeField(generator, "fields", fields);
      }
      generator.writeEndObject();
    }
  }

  /** IN filter. */
  protected static class JsonInFilter extends JsonFilter {
    private final String dimension;
    private final List<String> values;

    private JsonInFilter(String type, String dimension, List<String> values) {
      super(type);
      this.dimension = dimension;
      this.values = values;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      generator.writeStringField("dimension", dimension);
      writeField(generator, "values", values);
      generator.writeEndObject();
    }
  }

}

// End DruidQuery.java
