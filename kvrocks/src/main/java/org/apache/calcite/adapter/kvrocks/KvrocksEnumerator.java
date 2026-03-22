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

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;

import static java.util.Objects.requireNonNull;

/**
 * Enumerator that reads rows from a single Kvrocks key.
 *
 * <p>On construction it borrows a {@link Jedis} connection from the
 * schema-level {@link KvrocksJedisManager}, eagerly fetches all data
 * via {@link KvrocksDataProcess}, then returns the connection to the
 * pool. Iteration is over the materialised list.
 */
class KvrocksEnumerator implements Enumerator<Object[]> {
  private final Enumerator<Object[]> enumerator;

  KvrocksEnumerator(KvrocksConfig config, KvrocksSchema schema,
      String tableName) {
    KvrocksTableFieldInfo fieldInfo =
        schema.getTableFieldInfo(tableName);
    KvrocksJedisManager manager = schema.getManager();

    try (Jedis jedis = manager.getResource()) {
      if (config.getPassword() != null
          && !config.getPassword().isEmpty()) {
        jedis.auth(config.getPassword());
      }
      KvrocksDataProcess process =
          new KvrocksDataProcess(jedis, fieldInfo);
      List<Object[]> rows = process.read();
      enumerator = Linq4j.enumerator(rows);
    }
  }

  /**
   * Derives the column name/type map from field metadata.
   *
   * <p>For {@link KvrocksDataFormat#RAW} a single {@code "key"} column is
   * produced; for CSV and JSON the configured field list is used.
   */
  static Map<String, Object> deduceRowType(
      KvrocksTableFieldInfo fieldInfo) {
    final Map<String, Object> columns = new LinkedHashMap<>();
    KvrocksDataFormat format =
        requireNonNull(KvrocksDataFormat.fromTypeName(fieldInfo.getDataFormat()));
    if (format == KvrocksDataFormat.RAW) {
      columns.put("key", "key");
    } else {
      for (LinkedHashMap<String, Object> field : fieldInfo.getFields()) {
        columns.put(field.get("name").toString(),
            field.get("type").toString());
      }
    }
    return columns;
  }

  @Override public Object[] current() {
    return enumerator.current();
  }

  @Override public boolean moveNext() {
    return enumerator.moveNext();
  }

  @Override public void reset() {
    enumerator.reset();
  }

  @Override public void close() {
    enumerator.close();
  }
}
