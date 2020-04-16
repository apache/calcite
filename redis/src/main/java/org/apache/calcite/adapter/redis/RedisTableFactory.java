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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

/**
 * Implementation of {@link TableFactory} for Redis.
 *
 * <p>A table corresponds to what Redis calls a "data source".
 */
public class RedisTableFactory implements TableFactory {
  @SuppressWarnings("unused")
  public static final RedisTableFactory INSTANCE = new RedisTableFactory();

  private RedisTableFactory() {
  }

  // name that is also the same name as a complex metric
  @Override public Table create(SchemaPlus schema, String tableName, Map operand,
      RelDataType rowType) {
    final RedisSchema redisSchema = schema.unwrap(RedisSchema.class);
    final RelProtoDataType protoRowType =
        rowType != null ? RelDataTypeImpl.proto(rowType) : null;
    return RedisTable.create(redisSchema, tableName, operand, protoRowType);
  }
}
