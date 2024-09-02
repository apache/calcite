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
package org.apache.calcite.adapter.redis;

import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.apache.commons.lang3.StringUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Schema mapped onto a set of URLs / HTML tables. Each table in the schema
 * is an HTML table on a URL.
 */
class RedisSchema extends AbstractSchema {
  public final String host;
  public final int port;
  public final int database;
  public final String password;
  public final List<Map<String, Object>> tables;

  RedisSchema(String host,
      int port,
      int database,
      String password,
      List<Map<String, Object>> tables) {
    this.host = host;
    this.port = port;
    this.database = database;
    this.password = password;
    this.tables = tables;
  }

  @Override protected Map<String, Table> getTableMap() {
    JsonCustomTable[] jsonCustomTables = new JsonCustomTable[tables.size()];
    Set<String> tableNames = Arrays.stream(tables.toArray(jsonCustomTables))
        .map(e -> e.name).collect(Collectors.toSet());
    return Maps.asMap(ImmutableSet.copyOf(tableNames),
        CacheBuilder.newBuilder()
            .build(CacheLoader.from(this::table)));
  }

  private Table table(String tableName) {
    RedisConfig redisConfig = new RedisConfig(host, port, database, password);
    return RedisTable.create(RedisSchema.this, tableName, redisConfig, null);
  }

  public RedisTableFieldInfo getTableFieldInfo(String tableName) {
    RedisTableFieldInfo tableFieldInfo = new RedisTableFieldInfo();
    List<LinkedHashMap<String, Object>> fields = new ArrayList<>();
    String dataFormat = "";
    String keyDelimiter = "";
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<JsonCustomTable> jsonCustomTables =
        (List<JsonCustomTable>) (List) this.tables;
    for (JsonCustomTable jsonCustomTable : jsonCustomTables) {
      if (jsonCustomTable.name.equals(tableName)) {
        Map<String, Object> map =
            requireNonNull(jsonCustomTable.operand, "operand");
        if (map.get("dataFormat") == null) {
          throw new RuntimeException("dataFormat is null");
        }
        if (map.get("fields") == null) {
          throw new RuntimeException("fields is null");
        }
        dataFormat = map.get("dataFormat").toString();
        fields = (List<LinkedHashMap<String, Object>>) map.get("fields");
        if (map.get("keyDelimiter") != null) {
          keyDelimiter = map.get("keyDelimiter").toString();
        }
        break;
      }
    }
    tableFieldInfo.setTableName(tableName);
    tableFieldInfo.setDataFormat(dataFormat);
    tableFieldInfo.setFields(fields);
    if (StringUtils.isNotEmpty(keyDelimiter)) {
      tableFieldInfo.setKeyDelimiter(keyDelimiter);
    }
    return tableFieldInfo;
  }
}
