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
import org.apache.calcite.util.Compatible;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Schema mapped onto a Druid instance.
 */
public class DruidSchema extends AbstractSchema {
  final String url;
  final String coordinatorUrl;
  private final boolean discoverTables;

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
    this.url = Preconditions.checkNotNull(url);
    this.coordinatorUrl = Preconditions.checkNotNull(coordinatorUrl);
    this.discoverTables = discoverTables;
  }

  @Override protected Map<String, Table> getTableMap() {
    if (!discoverTables) {
      return ImmutableMap.of();
    }
    final DruidConnectionImpl connection =
        new DruidConnectionImpl(url, coordinatorUrl);
    return Compatible.INSTANCE.asMap(
        ImmutableSet.copyOf(connection.tableNames()),
        CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Table>() {
              public Table load(@Nonnull String tableName) throws Exception {
                final Map<String, SqlTypeName> fieldMap = new LinkedHashMap<>();
                final Set<String> metricNameSet = new LinkedHashSet<>();
                connection.metadata(tableName, DruidTable.DEFAULT_TIMESTAMP_COLUMN,
                    null, fieldMap, metricNameSet);
                return DruidTable.create(DruidSchema.this, tableName, null,
                    fieldMap, metricNameSet, DruidTable.DEFAULT_TIMESTAMP_COLUMN, connection);
              }
            }));
  }
}

// End DruidSchema.java
