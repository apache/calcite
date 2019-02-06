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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Table mapped onto a Druid table.
 */
public class DruidTable extends AbstractTable implements TranslatableTable {

  public static final String DEFAULT_TIMESTAMP_COLUMN = "__time";
  public static final Interval DEFAULT_INTERVAL =
      new Interval(new DateTime("1900-01-01", ISOChronology.getInstanceUTC()),
          new DateTime("3000-01-01", ISOChronology.getInstanceUTC()));

  final DruidSchema schema;
  final String dataSource;
  final RelProtoDataType protoRowType;
  final ImmutableSet<String> metricFieldNames;
  final ImmutableList<Interval> intervals;
  final String timestampFieldName;
  final ImmutableMap<String, List<ComplexMetric>> complexMetrics;
  final ImmutableMap<String, SqlTypeName> allFields;

  /**
   * Creates a Druid table.
   *
   * @param schema Druid schema that contains this table
   * @param dataSource Druid data source name
   * @param protoRowType Field names and types
   * @param metricFieldNames Names of fields that are metrics
   * @param intervals Default interval if query does not constrain the time, or null
   * @param timestampFieldName Name of the column that contains the time
   */
  public DruidTable(DruidSchema schema, String dataSource,
      RelProtoDataType protoRowType, Set<String> metricFieldNames,
      String timestampFieldName, List<Interval> intervals,
      Map<String, List<ComplexMetric>> complexMetrics, Map<String, SqlTypeName> allFields) {
    this.timestampFieldName = Objects.requireNonNull(timestampFieldName);
    this.schema = Objects.requireNonNull(schema);
    this.dataSource = Objects.requireNonNull(dataSource);
    this.protoRowType = protoRowType;
    this.metricFieldNames = ImmutableSet.copyOf(metricFieldNames);
    this.intervals = intervals != null ? ImmutableList.copyOf(intervals)
        : ImmutableList.of(DEFAULT_INTERVAL);
    this.complexMetrics = complexMetrics == null ? ImmutableMap.of()
            : ImmutableMap.copyOf(complexMetrics);
    this.allFields = allFields == null ? ImmutableMap.of()
            : ImmutableMap.copyOf(allFields);
  }

  /** Creates a {@link DruidTable} by using the given {@link DruidConnectionImpl}
   * to populate the other parameters. The parameters may be partially populated.
   *
   * @param druidSchema Druid schema
   * @param dataSourceName Data source name in Druid, also table name
   * @param intervals Intervals, or null to use default
   * @param fieldMap Partially populated map of fields (dimensions plus metrics)
   * @param metricNameSet Partially populated set of metric names
   * @param timestampColumnName Name of timestamp column, or null
   * @param connection Connection used to find column definitions; Must be non-null
   * @param complexMetrics List of complex metrics in Druid (thetaSketch, hyperUnique)
   *
   * @return A table
   */
  static Table create(DruidSchema druidSchema, String dataSourceName,
      List<Interval> intervals, Map<String, SqlTypeName> fieldMap,
      Set<String> metricNameSet, String timestampColumnName,
      DruidConnectionImpl connection, Map<String, List<ComplexMetric>> complexMetrics) {
    assert connection != null;

    connection.metadata(dataSourceName, timestampColumnName, intervals,
            fieldMap, metricNameSet, complexMetrics);

    return DruidTable.create(druidSchema, dataSourceName, intervals, fieldMap,
            metricNameSet, timestampColumnName, complexMetrics);
  }

  /** Creates a {@link DruidTable} by copying the given parameters.
   *
   * @param druidSchema Druid schema
   * @param dataSourceName Data source name in Druid, also table name
   * @param intervals Intervals, or null to use default
   * @param fieldMap Fully populated map of fields (dimensions plus metrics)
   * @param metricNameSet Fully populated set of metric names
   * @param timestampColumnName Name of timestamp column, or null
   * @param complexMetrics List of complex metrics in Druid (thetaSketch, hyperUnique)
   *
   * @return A table
   */
  static Table create(DruidSchema druidSchema, String dataSourceName,
                      List<Interval> intervals, Map<String, SqlTypeName> fieldMap,
                      Set<String> metricNameSet, String timestampColumnName,
                      Map<String, List<ComplexMetric>> complexMetrics) {
    final ImmutableMap<String, SqlTypeName> fields =
            ImmutableMap.copyOf(fieldMap);
    return new DruidTable(druidSchema,
        dataSourceName,
        new MapRelProtoDataType(fields, timestampColumnName),
        ImmutableSet.copyOf(metricNameSet),
        timestampColumnName,
        intervals,
        complexMetrics,
        fieldMap);
  }

  /**
   * Returns the appropriate {@link ComplexMetric} that is mapped from the given <code>alias</code>
   * if it exists, and is used in the expected context with the given {@link AggregateCall}.
   * Otherwise returns <code>null</code>.
   * */
  public ComplexMetric resolveComplexMetric(String alias, AggregateCall call) {
    List<ComplexMetric> potentialMetrics = getComplexMetricsFrom(alias);

    // It's possible that multiple complex metrics match the AggregateCall,
    // but for now we only return the first that matches
    for (ComplexMetric complexMetric : potentialMetrics) {
      if (complexMetric.canBeUsed(call)) {
        return complexMetric;
      }
    }

    return null;
  }

  @Override public boolean isRolledUp(String column) {
    // The only rolled up columns we care about are Complex Metrics (aka sketches).
    // But we also need to check if this column name is a dimension
    return complexMetrics.get(column) != null
            && allFields.get(column) != SqlTypeName.VARCHAR;
  }

  @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
      SqlNode parent, CalciteConnectionConfig config) {
    assert isRolledUp(column);
    // Our rolled up columns are only allowed in COUNT(DISTINCT ...) aggregate functions.
    // We only allow this when approximate results are acceptable.
    return ((config != null
                && config.approximateDistinctCount()
                && isCountDistinct(call))
            || call.getOperator() == SqlStdOperatorTable.APPROX_COUNT_DISTINCT)
        && call.getOperandList().size() == 1 // for COUNT(a_1, a_2, ... a_n). n should be 1
        && isValidParentKind(parent);
  }

  private boolean isValidParentKind(SqlNode node) {
    return node.getKind() == SqlKind.SELECT
            || node.getKind() == SqlKind.FILTER
            || isSupportedPostAggOperation(node.getKind());
  }

  private boolean isCountDistinct(SqlCall call) {
    return call.getKind() == SqlKind.COUNT
            && call.getFunctionQuantifier() != null
            && call.getFunctionQuantifier().getValue() == SqlSelectKeyword.DISTINCT;
  }

  // Post aggs support +, -, /, * so we should allow the parent of a count distinct to be any one of
  // those.
  private boolean isSupportedPostAggOperation(SqlKind kind) {
    return kind == SqlKind.PLUS
            || kind == SqlKind.MINUS
            || kind == SqlKind.DIVIDE
            || kind == SqlKind.TIMES;
  }

  /**
   * Returns the list of {@link ComplexMetric} that match the given <code>alias</code> if it exists,
   * otherwise returns an empty list, never <code>null</code>
   * */
  public List<ComplexMetric> getComplexMetricsFrom(String alias) {
    return complexMetrics.containsKey(alias)
            ? complexMetrics.get(alias)
            : new ArrayList<>();
  }

  /**
   * Returns true if and only if the given <code>alias</code> is a reference to a registered
   * {@link ComplexMetric}
   * */
  public boolean isComplexMetric(String alias) {
    return complexMetrics.get(alias) != null;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataType rowType = protoRowType.apply(typeFactory);
    final List<String> fieldNames = rowType.getFieldNames();
    Preconditions.checkArgument(fieldNames.contains(timestampFieldName));
    Preconditions.checkArgument(fieldNames.containsAll(metricFieldNames));
    return rowType;
  }

  public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    final TableScan scan = LogicalTableScan.create(cluster, relOptTable);
    return DruidQuery.create(cluster,
        cluster.traitSetOf(BindableConvention.INSTANCE), relOptTable, this,
        ImmutableList.of(scan));
  }

  public boolean isMetric(String name) {
    return metricFieldNames.contains(name);
  }

  /** Creates a {@link RelDataType} from a map of
   * field names and types. */
  private static class MapRelProtoDataType implements RelProtoDataType {
    private final ImmutableMap<String, SqlTypeName> fields;
    private final String timestampColumn;

    MapRelProtoDataType(ImmutableMap<String, SqlTypeName> fields) {
      this.fields = fields;
      this.timestampColumn = DruidTable.DEFAULT_TIMESTAMP_COLUMN;
    }

    MapRelProtoDataType(ImmutableMap<String, SqlTypeName> fields, String timestampColumn) {
      this.fields = fields;
      this.timestampColumn = timestampColumn;
    }

    public RelDataType apply(RelDataTypeFactory typeFactory) {
      final RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (Map.Entry<String, SqlTypeName> field : fields.entrySet()) {
        final String key = field.getKey();
        builder.add(key, field.getValue())
            // Druid's time column is always not null and the only column called __time.
            .nullable(!timestampColumn.equals(key));
      }
      return builder.build();
    }
  }
}

// End DruidTable.java
