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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table mapped onto a redis table.
 */
public class RedisTable extends AbstractTable
    implements ScannableTable {

  final RedisSchema schema;
  final String tableName;
  final RelProtoDataType protoRowType;
  final ImmutableMap<String, Object> allFields;
  final String dataFormat;
  final RedisConfig redisConfig;
  RedisEnumerator redisEnumerator;

  public RedisTable(
      RedisSchema schema,
      String tableName,
      RelProtoDataType protoRowType,
      Map<String, Object> allFields,
      String dataFormat,
      RedisConfig redisConfig) {
    this.schema = schema;
    this.tableName = tableName;
    this.protoRowType = protoRowType;
    this.allFields = allFields == null ? ImmutableMap.of()
        : ImmutableMap.copyOf(allFields);
    this.dataFormat = dataFormat;
    this.redisConfig = redisConfig;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    final List<RelDataType> types = new ArrayList<RelDataType>(allFields.size());
    final List<String> names = new ArrayList<String>(allFields.size());

    for (Object key : allFields.keySet()) {
      final RelDataType type = typeFactory.createJavaType(allFields.get(key).getClass());
      names.add(key.toString());
      types.add(type);
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  static Table create(
      RedisSchema schema,
      String tableName,
      RedisConfig redisConfig,
      RelProtoDataType protoRowType) {
    RedisTableFieldInfo tableFieldInfo = schema.getTableFieldInfo(tableName);
    Map<String, Object> allFields = RedisEnumerator.deduceRowType(tableFieldInfo);
    return new RedisTable(schema, tableName, protoRowType,
        allFields, tableFieldInfo.getDataFormat(), redisConfig);
  }

  static Table create(
      RedisSchema schema,
      String tableName,
      Map operand,
      RelProtoDataType protoRowType) {
    RedisConfig redisConfig = new RedisConfig(schema.host, schema.port,
        schema.database, schema.password);
    return create(schema, tableName, redisConfig, protoRowType);
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new RedisEnumerator(redisConfig, schema, tableName);
      }
    };
  }
}
