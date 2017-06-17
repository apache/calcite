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

import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Table mapped onto a Druid table.
 */
public class DruidTable extends AbstractTable implements TranslatableTable {

  public static final String DEFAULT_TIMESTAMP_COLUMN = "__time";
  public static final LocalInterval DEFAULT_INTERVAL =
      LocalInterval.create("1900-01-01", "3000-01-01");

  final DruidSchema schema;
  final String dataSource;
  final RelProtoDataType protoRowType;
  final ImmutableSet<String> metricFieldNames;
  final ImmutableList<LocalInterval> intervals;
  final String timestampFieldName;

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
      String timestampFieldName, List<LocalInterval> intervals) {
    this.timestampFieldName = Preconditions.checkNotNull(timestampFieldName);
    this.schema = Preconditions.checkNotNull(schema);
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.protoRowType = protoRowType;
    this.metricFieldNames = ImmutableSet.copyOf(metricFieldNames);
    this.intervals = intervals != null ? ImmutableList.copyOf(intervals)
        : ImmutableList.of(DEFAULT_INTERVAL);
  }

  /** Creates a {@link DruidTable}
   *
   * @param druidSchema Druid schema
   * @param dataSourceName Data source name in Druid, also table name
   * @param intervals Intervals, or null to use default
   * @param fieldMap Mutable map of fields (dimensions plus metrics);
   *        may be partially populated already
   * @param metricNameSet Mutable set of metric names;
   *        may be partially populated already
   * @param timestampColumnName Name of timestamp column, or null
   * @param connection If not null, use this connection to find column
   *                   definitions
   * @return A table
   */
  static Table create(DruidSchema druidSchema, String dataSourceName,
      List<LocalInterval> intervals, Map<String, SqlTypeName> fieldMap,
      Set<String> metricNameSet, String timestampColumnName,
      DruidConnectionImpl connection) {
    if (connection != null) {
      connection.metadata(dataSourceName, timestampColumnName, intervals, fieldMap, metricNameSet);
    }
    final ImmutableMap<String, SqlTypeName> fields =
        ImmutableMap.copyOf(fieldMap);
    return new DruidTable(druidSchema, dataSourceName,
        new MapRelProtoDataType(fields), ImmutableSet.copyOf(metricNameSet),
        timestampColumnName, intervals);
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
        ImmutableList.<RelNode>of(scan));
  }

  /** Creates a {@link RelDataType} from a map of
   * field names and types. */
  private static class MapRelProtoDataType implements RelProtoDataType {
    private final ImmutableMap<String, SqlTypeName> fields;

    MapRelProtoDataType(ImmutableMap<String, SqlTypeName> fields) {
      this.fields = fields;
    }

    public RelDataType apply(RelDataTypeFactory typeFactory) {
      final RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
      for (Map.Entry<String, SqlTypeName> field : fields.entrySet()) {
        builder.add(field.getKey(), field.getValue()).nullable(true);
      }
      return builder.build();
    }
  }
}

// End DruidTable.java
