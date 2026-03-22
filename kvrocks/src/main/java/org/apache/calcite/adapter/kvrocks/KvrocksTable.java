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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A Calcite {@link Table} backed by a single Kvrocks key.
 *
 * <p>Implements {@link ScannableTable}: every SQL query triggers a full
 * read of the underlying Kvrocks data structure.
 */
public class KvrocksTable extends AbstractTable implements ScannableTable {

  final KvrocksSchema schema;
  final String tableName;
  final @Nullable RelProtoDataType protoRowType;
  final ImmutableMap<String, Object> allFields;
  final String dataFormat;
  final KvrocksConfig kvrocksConfig;

  KvrocksTable(KvrocksSchema schema, String tableName,
      @Nullable RelProtoDataType protoRowType,
      Map<String, Object> allFields, String dataFormat,
      KvrocksConfig kvrocksConfig) {
    this.schema = schema;
    this.tableName = tableName;
    this.protoRowType = protoRowType;
    this.allFields = allFields == null
        ? ImmutableMap.of() : ImmutableMap.copyOf(allFields);
    this.dataFormat = dataFormat;
    this.kvrocksConfig = kvrocksConfig;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    final List<String> names = new ArrayList<>(allFields.size());
    final List<RelDataType> types = new ArrayList<>(allFields.size());
    for (Map.Entry<String, Object> entry : allFields.entrySet()) {
      names.add(entry.getKey());
      types.add(typeFactory.createJavaType(entry.getValue().getClass()));
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new KvrocksEnumerator(kvrocksConfig, schema, tableName);
      }
    };
  }

  /** Creates a table from schema-level metadata. */
  static Table create(KvrocksSchema schema, String tableName,
      KvrocksConfig config, @Nullable RelProtoDataType protoRowType) {
    KvrocksTableFieldInfo fieldInfo = schema.getTableFieldInfo(tableName);
    Map<String, Object> allFields =
        KvrocksEnumerator.deduceRowType(fieldInfo);
    return new KvrocksTable(schema, tableName, protoRowType,
        allFields, fieldInfo.getDataFormat(), config);
  }

  /** Creates a table from an operand map (used by {@link KvrocksTableFactory}). */
  static Table create(KvrocksSchema schema, String tableName,
      Map operand, @Nullable RelProtoDataType protoRowType) {
    KvrocksConfig config =
        new KvrocksConfig(schema.host, schema.port, schema.database,
        schema.password, schema.namespace);
    return create(schema, tableName, config, protoRowType);
  }
}
