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
package org.apache.calcite.adapter.kvrocks;

import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Schema that exposes Kvrocks keys as relational tables.
 *
 * <p>Each entry in the {@code tables} list of the model JSON becomes a
 * {@link KvrocksTable}. The schema holds connection parameters shared by
 * all tables and owns a single {@link KvrocksJedisManager} that is reused
 * across all scans.
 */
class KvrocksSchema extends AbstractSchema {
  private static final String DATA_FORMAT = "dataFormat";
  private static final String FIELDS = "fields";
  private static final String KEY_DELIMITER = "keyDelimiter";
  private static final String OPERAND = "operand";

  final String host;
  final int port;
  final int database;
  final @Nullable String password;
  final List<Map<String, Object>> tables;
  private final KvrocksJedisManager manager;

  KvrocksSchema(String host, int port, int database,
      @Nullable String password,
      List<Map<String, Object>> tables) {
    this.host = host;
    this.port = port;
    this.database = database;
    this.password = password;
    this.tables = tables;
    this.manager =
        new KvrocksJedisManager(host, port, database, password);
  }

  /** Returns the shared connection manager for this schema. */
  KvrocksJedisManager getManager() {
    return manager;
  }

  @Override protected Map<String, Table> getTableMap() {
    JsonCustomTable[] array = new JsonCustomTable[tables.size()];
    Set<String> tableNames = Arrays.stream(tables.toArray(array))
        .map(t -> t.name)
        .collect(Collectors.toSet());
    return Maps.asMap(ImmutableSet.copyOf(tableNames),
        CacheBuilder.newBuilder()
            .build(CacheLoader.from(this::table)));
  }

  private Table table(String tableName) {
    KvrocksConfig config =
        new KvrocksConfig(host, port, database, password);
    return KvrocksTable.create(KvrocksSchema.this, tableName,
        config, null);
  }

  /**
   * Extracts field metadata for the named table from the model definition.
   *
   * @throws RuntimeException if dataFormat is missing/invalid or fields
   *         are empty
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  KvrocksTableFieldInfo getTableFieldInfo(String tableName) {
    KvrocksTableFieldInfo info = new KvrocksTableFieldInfo();
    List<LinkedHashMap<String, Object>> fields = new ArrayList<>();
    String dataFormat = "";
    String keyDelimiter = "";

    List<JsonCustomTable> jsonTables =
        (List<JsonCustomTable>) (List) this.tables;
    for (JsonCustomTable jsonTable : jsonTables) {
      if (jsonTable.name.equals(tableName)) {
        Map<String, Object> operand =
            requireNonNull(jsonTable.operand, OPERAND);

        Object rawFormat = operand.get(DATA_FORMAT);
        if (rawFormat == null || rawFormat.toString().isEmpty()) {
          throw new RuntimeException(
              "dataFormat is invalid, it must be raw, csv or json");
        }
        KvrocksDataFormat fmt =
            KvrocksDataFormat.fromTypeName(rawFormat.toString());
        if (fmt == null) {
          throw new RuntimeException(
              "dataFormat is invalid, it must be raw, csv or json");
        }
        Object rawFields = operand.get(FIELDS);
        if (rawFields == null
            || (rawFields instanceof List
                && ((List) rawFields).isEmpty())) {
          throw new RuntimeException("fields is null");
        }

        dataFormat = operand.get(DATA_FORMAT).toString();
        fields =
            (List<LinkedHashMap<String, Object>>) operand.get(FIELDS);
        if (operand.get(KEY_DELIMITER) != null) {
          keyDelimiter = operand.get(KEY_DELIMITER).toString();
        }
        break;
      }
    }

    info.setTableName(tableName);
    info.setDataFormat(dataFormat);
    info.setFields(fields);
    if (keyDelimiter != null && !keyDelimiter.isEmpty()) {
      info.setKeyDelimiter(keyDelimiter);
    }
    return info;
  }
}
