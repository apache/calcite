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

import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import java.util.ArrayList;
import java.util.HashMap;
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

  // name that is also the same name as a complex metric
  public Table create(SchemaPlus schema, String name, Map operand,
      RelDataType rowType) {
    final DruidSchema druidSchema = schema.unwrap(DruidSchema.class);
    // If "dataSource" operand is present it overrides the table name.
    final String dataSource = (String) operand.get("dataSource");
    final Set<String> metricNameBuilder = new LinkedHashSet<>();
    final Map<String, SqlTypeName> fieldBuilder = new LinkedHashMap<>();
    final Map<String, List<ComplexMetric>> complexMetrics = new HashMap<>();
    final String timestampColumnName;
    final SqlTypeName timestampColumnType;
    final Object timestampInfo = operand.get("timestampColumn");
    if (timestampInfo != null) {
      if (timestampInfo instanceof Map) {
        Map map = (Map) timestampInfo;
        if (!(map.get("name") instanceof String)) {
          throw new IllegalArgumentException("timestampColumn array must have name");
        }
        timestampColumnName = (String) map.get("name");
        if (!(map.get("type") instanceof String)
            || map.get("type").equals("timestamp with local time zone")) {
          timestampColumnType = SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        } else if (map.get("type").equals("timestamp")) {
          timestampColumnType = SqlTypeName.TIMESTAMP;
        } else {
          throw new IllegalArgumentException("unexpected type for timestampColumn array");
        }
      } else {
        // String (for backwards compatibility)
        timestampColumnName = (String) timestampInfo;
        timestampColumnType = SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
      }
    } else {
      timestampColumnName = DruidTable.DEFAULT_TIMESTAMP_COLUMN;
      timestampColumnType = SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }
    fieldBuilder.put(timestampColumnName, timestampColumnType);
    final Object dimensionsRaw = operand.get("dimensions");
    if (dimensionsRaw instanceof List) {
      // noinspection unchecked
      final List<String> dimensions = (List<String>) dimensionsRaw;
      for (String dimension : dimensions) {
        fieldBuilder.put(dimension, SqlTypeName.VARCHAR);
      }
    }

    // init the complex metric map
    final Object complexMetricsRaw = operand.get("complexMetrics");
    if (complexMetricsRaw instanceof List) {
      // noinspection unchecked
      final List<String> complexMetricList = (List<String>) complexMetricsRaw;
      for (String metric : complexMetricList) {
        complexMetrics.put(metric, new ArrayList<>());
      }
    }

    final Object metricsRaw = operand.get("metrics");
    if (metricsRaw instanceof List) {
      final List metrics = (List) metricsRaw;
      for (Object metric : metrics) {
        DruidType druidType = DruidType.LONG;
        final String metricName;
        String fieldName = null;

        if (metric instanceof Map) {
          Map map2 = (Map) metric;
          if (!(map2.get("name") instanceof String)) {
            throw new IllegalArgumentException("metric must have name");
          }
          metricName = (String) map2.get("name");

          final String type = (String) map2.get("type");
          fieldName = (String) map2.get("fieldName");

          druidType = DruidType.getTypeFromMetric(type);
        } else {
          metricName = (String) metric;
        }

        if (!druidType.isComplex()) {
          fieldBuilder.put(metricName, druidType.sqlType);
          metricNameBuilder.add(metricName);
        } else {
          assert fieldName != null;
          // Only add the complex metric if there exists an alias for it
          if (complexMetrics.containsKey(fieldName)) {
            SqlTypeName type = fieldBuilder.get(fieldName);
            if (type != SqlTypeName.VARCHAR) {
              fieldBuilder.put(fieldName, SqlTypeName.VARBINARY);
              // else, this complex metric is also a dimension, so it's type should remain as
              // VARCHAR, but it'll also be added as a complex metric.
            }
            complexMetrics.get(fieldName).add(new ComplexMetric(metricName, druidType));
          }
        }
      }
    }
    final Object interval = operand.get("interval");
    final List<Interval> intervals;
    if (interval instanceof String) {
      intervals = ImmutableList.of(
          new Interval((String) interval, ISOChronology.getInstanceUTC()));
    } else {
      intervals = null;
    }

    final String dataSourceName = Util.first(dataSource, name);

    if (dimensionsRaw == null || metricsRaw == null) {
      DruidConnectionImpl connection = new DruidConnectionImpl(druidSchema.url,
              druidSchema.url.replace(":8082", ":8081"));
      return DruidTable.create(druidSchema, dataSourceName, intervals, fieldBuilder,
              metricNameBuilder, timestampColumnName, connection, complexMetrics);
    } else {
      return DruidTable.create(druidSchema, dataSourceName, intervals, fieldBuilder,
              metricNameBuilder, timestampColumnName, complexMetrics);
    }
  }
}

// End DruidTableFactory.java
