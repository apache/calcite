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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
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
  @SuppressWarnings("unused")
  public static final DruidTableFactory INSTANCE = new DruidTableFactory();

  private DruidTableFactory() {}

  public Table create(SchemaPlus schema, String name, Map operand,
      RelDataType rowType) {
    final DruidSchema druidSchema = schema.unwrap(DruidSchema.class);
    // If "dataSource" operand is present it overrides the table name.
    final String dataSource = (String) operand.get("dataSource");
    final Set<String> metricNameBuilder = new LinkedHashSet<>();
    final Map<String, SqlTypeName> fieldBuilder = new LinkedHashMap<>();
    final String timestampColumnName;
    if (operand.get("timestampColumn") != null) {
      timestampColumnName = (String) operand.get("timestampColumn");
    } else {
      timestampColumnName = DruidTable.DEFAULT_TIMESTAMP_COLUMN;
    }
    fieldBuilder.put(timestampColumnName, SqlTypeName.TIMESTAMP);
    final Object dimensionsRaw = operand.get("dimensions");
    if (dimensionsRaw instanceof List) {
      //noinspection unchecked
      final List<String> dimensions = (List<String>) dimensionsRaw;
      for (String dimension : dimensions) {
        fieldBuilder.put(dimension, SqlTypeName.VARCHAR);
      }
    }
    final Object metricsRaw = operand.get("metrics");
    if (metricsRaw instanceof List) {
      final List metrics = (List) metricsRaw;
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
    final String dataSourceName = Util.first(dataSource, name);
    DruidConnectionImpl c;
    if (dimensionsRaw == null || metricsRaw == null) {
      c = new DruidConnectionImpl(druidSchema.url, druidSchema.url.replace(":8082", ":8081"));
    } else {
      c = null;
    }
    final Object interval = operand.get("interval");
    final List<LocalInterval> intervals;
    if (interval instanceof String) {
      intervals = ImmutableList.of(LocalInterval.create((String) interval));
    } else {
      intervals = null;
    }
    return DruidTable.create(druidSchema, dataSourceName, intervals,
        fieldBuilder, metricNameBuilder, timestampColumnName, c);
  }

}

// End DruidTableFactory.java
