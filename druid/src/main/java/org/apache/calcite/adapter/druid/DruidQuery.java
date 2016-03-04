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
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

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
  private final RelOptTable table;
  final DruidTable druidTable;
  final ImmutableList<RelNode> rels;

  private static final Pattern VALID_SIG = Pattern.compile("sf?p?a?");

  /**
   * Creates a DruidQuery.
   *
   * @param cluster        Cluster
   * @param traitSet       Traits
   * @param table          Table
   * @param druidTable     Druid table
   * @param rels           Internal relational expressions
   */
  private DruidQuery(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable, List<RelNode> rels) {
    super(cluster, traitSet);
    this.table = table;
    this.druidTable = druidTable;
    this.rels = ImmutableList.copyOf(rels);

    assert isValid(Litmus.THROW);
  }

  /** Returns a string describing the operations inside this query.
   *
   * <p>For example, "sfa" means {@link TableScan} (s)
   * followed by {@link Filter} (f)
   * followed by {@link Aggregate} (a).
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
          : '!');
    }
    return b.toString();
  }

  @Override public boolean isValid(Litmus litmus) {
    if (!super.isValid(litmus)) {
      return false;
    }
    final String signature = signature();
    if (!isValidSignature(signature)) {
      return litmus.fail("invalid signature [%s]", signature);
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
            return litmus.fail("invalid filter [%s]", filter.getCondition());
          }
        }
      }
    }
    return true;
  }

  boolean isValidFilter(RexNode e) {
    switch (e.getKind()) {
    case INPUT_REF:
    case LITERAL:
      return true;
    case AND:
    case OR:
    case EQUALS:
    case NOT_EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case CAST:
      return areValidFilters(((RexCall) e).getOperands());
    default:
      return false;
    }
  }

  private boolean areValidFilters(List<RexNode> es) {
    for (RexNode e : es) {
      if (!isValidFilter(e)) {
        return false;
      }
    }
    return true;
  }

  /** Returns whether a signature represents an sequence of relational operators
   * that can be translated into a valid Druid query. */
  static boolean isValidSignature(String signature) {
    return VALID_SIG.matcher(signature).matches();
  }

  /** Creates a DruidQuery. */
  static DruidQuery create(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable, List<RelNode> rels) {
    return new DruidQuery(cluster, traitSet, table, druidTable, rels);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override public RelDataType deriveRowType() {
    return Util.last(rels).getRowType();
  }

  @Override public RelOptTable getTable() {
    return table;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (RelNode rel : rels) {
      if (rel instanceof TableScan) {
        TableScan tableScan = (TableScan) rel;
        pw.item("table", tableScan.getTable().getQualifiedName());
      } else if (rel instanceof Filter) {
        pw.item("filter", ((Filter) rel).getCondition());
      } else if (rel instanceof Project) {
        pw.item("projects", ((Project) rel).getProjects());
      } else if (rel instanceof Aggregate) {
        final Aggregate aggregate = (Aggregate) rel;
        pw.item("groups", aggregate.getGroupSet())
            .item("aggs", aggregate.getAggCallList());
      } else {
        throw new AssertionError("rel type not supported in Druid query "
            + rel);
      }
    }
    return pw;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return Util.last(rels).computeSelfCost(planner, mq).multiplyBy(.1);
  }

  @Override public void register(RelOptPlanner planner) {
    for (RelOptRule rule : DruidRules.RULES) {
      planner.addRule(rule);
    }
    for (RelOptRule rule : Bindables.RULES) {
      planner.addRule(rule);
    }
  }

  public Class<Object[]> getElementType() {
    return Object[].class;
  }

  public Enumerable<Object[]> bind(DataContext dataContext) {
    return table.unwrap(ScannableTable.class).scan(dataContext);
  }

  public Node implement(InterpreterImplementor implementor) {
    return new DruidQueryNode(implementor.interpreter, this);
  }

  private QuerySpec getQuerySpec() {
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

    if (i != rels.size()) {
      throw new AssertionError("could not implement all rels");
    }
    return getQuery(rowType, filter, projects, groupSet, aggCalls, aggNames);
  }

  private QuerySpec getQuery(RelDataType rowType, RexNode filter,
      List<RexNode> projects, ImmutableBitSet groupSet,
      List<AggregateCall> aggCalls, List<String> aggNames) {
    QueryType queryType = QueryType.SELECT;
    final Translator translator = new Translator(druidTable, rowType);
    List<String> fieldNames = rowType.getFieldNames();

    Json jsonFilter = null;
    if (filter != null) {
      jsonFilter = translator.translateFilter(filter);
      translator.metrics.clear();
      translator.dimensions.clear();
    }

    if (projects != null) {
      final ImmutableList.Builder<String> builder = ImmutableList.builder();
      for (RexNode project : projects) {
        builder.add(translator.translate(project));
      }
      fieldNames = builder.build();
    }

    final List<String> dimensions = new ArrayList<>();
    final List<JsonAggregation> aggregations = new ArrayList<>();

    if (groupSet != null) {
      assert aggCalls != null;
      assert aggNames != null;
      assert aggCalls.size() == aggNames.size();
      queryType = QueryType.GROUP_BY;

      final ImmutableList.Builder<String> builder = ImmutableList.builder();
      for (int groupKey : groupSet) {
        final String s = fieldNames.get(groupKey);
        dimensions.add(s);
        builder.add(s);
      }
      for (Pair<AggregateCall, String> agg : Pair.zip(aggCalls, aggNames)) {
        final JsonAggregation jsonAggregation =
            getJsonAggregation(fieldNames, agg.right, agg.left);
        aggregations.add(jsonAggregation);
        builder.add(jsonAggregation.name);
      }
      fieldNames = builder.build();
    } else {
      assert aggCalls == null;
      assert aggNames == null;
    }

    final StringWriter sw = new StringWriter();
    final JsonFactory factory = new JsonFactory();
    try {
      final JsonGenerator generator = factory.createGenerator(sw);

      switch (queryType) {
      case GROUP_BY:
        generator.writeStartObject();

        if (aggregations.isEmpty()) {
          // Druid requires at least one aggregation, otherwise gives:
          //   Must have at least one AggregatorFactory
          aggregations.add(
              new JsonAggregation("longSum", "unit_sales", "unit_sales"));
        }

        generator.writeStringField("queryType", "groupBy");
        generator.writeStringField("dataSource", druidTable.dataSource);
        generator.writeStringField("granularity", "all");
        writeField(generator, "dimensions", dimensions);
        writeFieldIf(generator, "limitSpec", null);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "aggregations", aggregations);
        writeFieldIf(generator, "postAggregations", null);
        writeField(generator, "intervals",
            ImmutableList.of(druidTable.interval));
        writeFieldIf(generator, "having", null);

        generator.writeEndObject();
        break;

      case SELECT:
        generator.writeStartObject();

        generator.writeStringField("queryType", "select");
        generator.writeStringField("dataSource", druidTable.dataSource);
        generator.writeStringField("descending", "false");
        writeField(generator, "intervals",
            ImmutableList.of(druidTable.interval));
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "dimensions", translator.dimensions);
        writeField(generator, "metrics", translator.metrics);
        generator.writeStringField("granularity", "all");

        generator.writeFieldName("pagingSpec");
        generator.writeStartObject();
        final int fetch =
            Integer.parseInt(
                CalciteConnectionProperty.DRUID_FETCH.wrap(new Properties())
                    .getString());
        generator.writeNumberField("threshold", fetch);
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

  private JsonAggregation getJsonAggregation(List<String> fieldNames,
      String name, AggregateCall aggCall) {
    final List<String> list = new ArrayList<>();
    for (Integer arg : aggCall.getArgList()) {
      list.add(fieldNames.get(arg));
    }
    final String only = Iterables.getFirst(list, null);
    final boolean b = aggCall.getType().getSqlTypeName() == SqlTypeName.DOUBLE;
    switch (aggCall.getAggregation().getKind()) {
    case COUNT:
      if (aggCall.isDistinct()) {
        return new JsonCardinalityAggregation("cardinality", name, list);
      }
      return new JsonAggregation("count", name, only);
    case SUM:
    case SUM0:
      return new JsonAggregation(b ? "doubleSum" : "longSum", name, only);
    case MIN:
      return new JsonAggregation(b ? "doubleMin" : "longMin", name, only);
    case MAX:
      return new JsonAggregation(b ? "doubleMax" : "longMax", name, only);
    default:
      throw new AssertionError("unknown aggregate " + aggCall);
    }
  }

  private static void writeField(JsonGenerator generator, String fieldName,
      Object o) throws IOException {
    generator.writeFieldName(fieldName);
    writeObject(generator, o);
  }

  private static void writeFieldIf(JsonGenerator generator, String fieldName,
      Object o) throws IOException {
    if (o != null) {
      writeField(generator, fieldName, o);
    }
  }

  private static void writeArray(JsonGenerator generator, List<?> elements)
      throws IOException {
    generator.writeStartArray();
    for (Object o : elements) {
      writeObject(generator, o);
    }
    generator.writeEndArray();
  }

  private static void writeObject(JsonGenerator generator, Object o)
      throws IOException {
    if (o instanceof String) {
      String s = (String) o;
      generator.writeString(s);
    } else if (o instanceof List) {
      writeArray(generator, (List) o);
    } else if (o instanceof Json) {
      ((Json) o).write(generator);
    } else {
      throw new AssertionError("not a json object: " + o);
    }
  }

  static boolean canProjectAll(List<RexNode> nodes) {
    for (RexNode e : nodes) {
      if (!(e instanceof RexInputRef)) {
        return false;
      }
    }
    return true;
  }

  static Pair<List<RexNode>, List<RexNode>> splitProjects(
      final RexBuilder rexBuilder, final RelNode input, List<RexNode> nodes) {
    final RelOptUtil.InputReferencedVisitor visitor =
        new RelOptUtil.InputReferencedVisitor();
    for (RexNode node : nodes) {
      node.accept(visitor);
    }
    if (visitor.inputPosReferenced.size() == input.getRowType().getFieldCount()) {
      // All inputs are referenced
      return null;
    }
    final List<RexNode> belowNodes = new ArrayList<>();
    final List<Integer> positions =
        Lists.newArrayList(visitor.inputPosReferenced);
    for (int i : positions) {
      belowNodes.add(rexBuilder.makeInputRef(input, i));
    }
    final List<RexNode> aboveNodes = new ArrayList<>();
    for (RexNode node : nodes) {
      aboveNodes.add(
          node.accept(
              new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef ref) {
                  return rexBuilder.makeInputRef(input,
                      positions.indexOf(ref.getIndex()));
                }
              }));
    }
    return Pair.of(aboveNodes, belowNodes);
  }

  /** Druid query specification. */
  public static class QuerySpec {
    final QueryType queryType;
    public final String queryString;
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

    String getQueryString(String pagingIdentifier, int offset) {
      if (pagingIdentifier == null) {
        return queryString;
      }
      return queryString.replace("\"threshold\":",
          "\"pagingIdentifiers\":{\"" + pagingIdentifier + "\":" + offset
              + "},\"threshold\":");
    }
  }

  /** Translates scalar expressions to Druid field references. */
  private static class Translator {
    final List<String> dimensions = new ArrayList<>();
    final List<String> metrics = new ArrayList<>();
    final DruidTable druidTable;
    final RelDataType rowType;

    Translator(DruidTable druidTable, RelDataType rowType) {
      this.druidTable = druidTable;
      this.rowType = rowType;
    }

    String translate(RexNode e) {
      switch (e.getKind()) {
      case INPUT_REF:
        final RexInputRef ref = (RexInputRef) e;
        final String fieldName =
            rowType.getFieldList().get(ref.getIndex()).getName();
        if (druidTable.metricFieldNames.contains(fieldName)) {
          metrics.add(fieldName);
        } else {
          dimensions.add(fieldName);
        }
        return fieldName;

      case CAST:
        return tr(e, 0);

      case LITERAL:
        return ((RexLiteral) e).getValue2().toString();

      default:
        throw new AssertionError("invalid expression " + e);
      }
    }

    private JsonFilter translateFilter(RexNode e) {
      final RexCall call;
      switch (e.getKind()) {
      case EQUALS:
        return new JsonSelector("selector", tr(e, 0), tr(e, 1));
      case NOT_EQUALS:
        return new JsonCompositeFilter("not",
            ImmutableList.of(new JsonSelector("selector", tr(e, 0), tr(e, 1))));
      case GREATER_THAN:
        return new JsonBound("bound", tr(e, 0), tr(e, 1), true, null, false,
            false);
      case GREATER_THAN_OR_EQUAL:
        return new JsonBound("bound", tr(e, 0), tr(e, 1), false, null, false,
            false);
      case LESS_THAN:
        return new JsonBound("bound", tr(e, 0), null, false, tr(e, 1), true,
            false);
      case LESS_THAN_OR_EQUAL:
        return new JsonBound("bound", tr(e, 0), null, false, tr(e, 1), false,
            false);
      case AND:
      case OR:
      case NOT:
        call = (RexCall) e;
        return new JsonCompositeFilter(e.getKind().toString().toLowerCase(),
            translateFilters(call.getOperands()));
      default:
        throw new AssertionError("cannot translate filter: " + e);
      }
    }

    private String tr(RexNode call, int index) {
      return translate(((RexCall) call).getOperands().get(index));
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
      try {
        final DruidConnectionImpl connection =
            new DruidConnectionImpl(query.druidTable.schema.url);
        final DruidConnectionImpl.Page page = new DruidConnectionImpl.Page();
        int previousOffset;
        do {
          previousOffset = page.offset;
          final String queryString =
              querySpec.getQueryString(page.pagingIdentifier, page.offset);
          connection.request(querySpec.queryType, queryString, sink,
              querySpec.fieldNames, page);
        } while (page.pagingIdentifier != null && page.offset > previousOffset);
      } catch (IOException e) {
        throw Throwables.propagate(e);
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
  private static class JsonBound extends JsonFilter {
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
        List<? extends JsonFilter> fields) {
      super(type);
      this.fields = fields;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      switch ("type") {
      case "NOT":
        writeField(generator, "field", fields.get(0));
        break;
      default:
        writeField(generator, "fields", fields);
      }
      generator.writeEndObject();
    }
  }

}

// End DruidQuery.java
