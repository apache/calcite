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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Schema mapped onto a Druid instance.
 */
public class DruidSchema extends AbstractSchema {
  final String url;
  final String coordinatorUrl;
  private final boolean discoverTables;
  private Map<String, Table> tableMap = null;

  /**
   * Creates a Druid schema.
   *
   * @param url URL of query REST service, e.g. "http://localhost:8082"
   * @param coordinatorUrl URL of coordinator REST service,
   *                       e.g. "http://localhost:8081"
   * @param discoverTables If true, ask Druid what tables exist;
   *                       if false, only create tables explicitly in the model
   */
  public DruidSchema(String url, String coordinatorUrl,
      boolean discoverTables) {
    this.url = Objects.requireNonNull(url);
    this.coordinatorUrl = Objects.requireNonNull(coordinatorUrl);
    this.discoverTables = discoverTables;
  }

  @Override protected Map<String, Table> getTableMap() {
    if (!discoverTables) {
      return ImmutableMap.of();
    }

    if (tableMap == null) {
      final DruidConnectionImpl connection = new DruidConnectionImpl(url, coordinatorUrl);
      Set<String> tableNames = connection.tableNames();

      tableMap = Maps.asMap(
          ImmutableSet.copyOf(tableNames),
          CacheBuilder.newBuilder()
              .build(CacheLoader.from(name -> table(name, connection))));
    }

    return tableMap;
  }

  private Table table(String tableName, DruidConnectionImpl connection) {
    final Map<String, SqlTypeName> fieldMap = new LinkedHashMap<>();
    final Set<String> metricNameSet = new LinkedHashSet<>();
    final Map<String, List<ComplexMetric>> complexMetrics = new HashMap<>();

    connection.metadata(tableName, DruidTable.DEFAULT_TIMESTAMP_COLUMN,
        null, fieldMap, metricNameSet, complexMetrics);

    return DruidTable.create(DruidSchema.this, tableName, null,
        fieldMap, metricNameSet, DruidTable.DEFAULT_TIMESTAMP_COLUMN,
        complexMetrics);
  }
}

// End DruidSchema.java
