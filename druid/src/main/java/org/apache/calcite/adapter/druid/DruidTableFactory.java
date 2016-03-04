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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link TableFactory} for Druid.
 *
 * <p>A table corresponds to what Druid calls a "data source".
 */
public class DruidTableFactory implements TableFactory {
  public Table create(SchemaPlus schema, String name, Map operand,
      RelDataType rowType) {
    final DruidSchema druidSchema = schema.unwrap(DruidSchema.class);
    // If "dataSource" operand is present it overrides the table name.
    final String dataSource = (String) operand.get("dataSource");
    final Set<String> metricNameBuilder = new LinkedHashSet<>();
    String timestampColumnName = (String) operand.get("timestampColumn");
    final ImmutableMap.Builder<String, SqlTypeName> fieldBuilder =
        ImmutableMap.builder();
    if (operand.get("dimensions") != null) {
      //noinspection unchecked
      final List<String> dimensions = (List<String>) operand.get("dimensions");
      for (String dimension : dimensions) {
        fieldBuilder.put(dimension, SqlTypeName.VARCHAR);
      }
    }
    if (operand.get("metrics") != null) {
      final List metrics = (List) operand.get("metrics");
      for (Object metric : metrics) {
        final SqlTypeName sqlTypeName;
        final String metricName;
        if (metric instanceof Map) {
          Map map2 = (Map) metric;
          if (!(map2.get("name") instanceof String)) {
            throw new IllegalArgumentException("metric must have name");
          }
          metricName = (String) map2.get("name");

          final Object type = map2.get("type");
          if ("long".equals(type)) {
            sqlTypeName = SqlTypeName.BIGINT;
          } else if ("double".equals(type)) {
            sqlTypeName = SqlTypeName.DOUBLE;
          } else {
            sqlTypeName = SqlTypeName.BIGINT;
          }
        } else {
          metricName = (String) metric;
          sqlTypeName = SqlTypeName.BIGINT;
        }
        fieldBuilder.put(metricName, sqlTypeName);
        metricNameBuilder.add(metricName);
      }
    }
    fieldBuilder.put(timestampColumnName, SqlTypeName.VARCHAR);
    final ImmutableMap<String, SqlTypeName> fields = fieldBuilder.build();
    String interval = Util.first((String) operand.get("interval"),
        "1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z");
    return new DruidTable(druidSchema, Util.first(dataSource, name),
        new MapRelProtoDataType(fields),
        ImmutableSet.copyOf(metricNameBuilder), interval, timestampColumnName);
  }

  /** Creates a {@link org.apache.calcite.rel.type.RelDataType} from a map of
   * field names and types. */
  private static class MapRelProtoDataType implements RelProtoDataType {
    private final ImmutableMap<String, SqlTypeName> fields;

    public MapRelProtoDataType(ImmutableMap<String, SqlTypeName> fields) {
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

// End DruidTableFactory.java
