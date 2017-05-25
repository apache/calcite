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
import org.apache.calcite.util.Pair;
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
    final Map<String, Pair<SqlTypeName, DruidType>> fieldTypeBuilder = new LinkedHashMap<>();
    final String timestampColumnName;
    if (operand.get("timestampColumn") != null) {
      timestampColumnName = (String) operand.get("timestampColumn");
    } else {
      timestampColumnName = DruidTable.DEFAULT_TIMESTAMP_COLUMN;
    }
    fieldTypeBuilder.put(timestampColumnName,
            new Pair<>(SqlTypeName.TIMESTAMP, DruidType.STRING));
    final Object dimensionsRaw = operand.get("dimensions");
    if (dimensionsRaw instanceof List) {
      //noinspection unchecked
      final List<String> dimensions = (List<String>) dimensionsRaw;
      for (String dimension : dimensions) {
        // Druid dimensions are type STRING by default
        fieldTypeBuilder.put(dimension, new Pair<>(SqlTypeName.VARCHAR, DruidType.STRING));
      }
    }
    final Object metricsRaw = operand.get("metrics");
    if (metricsRaw instanceof List) {
      final List metrics = (List) metricsRaw;
      for (Object metric : metrics) {
        final SqlTypeName sqlTypeName;
        final String metricName;
        DruidType druidType = DruidType.LONG;
        if (metric instanceof Map) {
          Map map2 = (Map) metric;
          if (!(map2.get("name") instanceof String)) {
            throw new IllegalArgumentException("metric must have name");
          }
          metricName = (String) map2.get("name");
          // type should be non null if metric is a map
          final String type = (String) map2.get("type");
          assert type != null;
          // count metrics should be queried with longSum at query time
          if (type.startsWith("long") || type.equals("count")) {
            sqlTypeName = SqlTypeName.BIGINT;
          } else if (type.startsWith("double")) {
            sqlTypeName = SqlTypeName.DOUBLE;
            druidType = DruidType.FLOAT;
          } else {
            sqlTypeName = SqlTypeName.BIGINT;
            druidType = DruidType.valueOf(type);
          }
        } else {
          metricName = (String) metric;
          sqlTypeName = SqlTypeName.BIGINT;
        }
        metricNameBuilder.add(metricName);
        fieldTypeBuilder.put(metricName, new Pair<>(sqlTypeName, druidType));
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
        fieldTypeBuilder, metricNameBuilder, timestampColumnName, c);
  }

}

// End DruidTableFactory.java
