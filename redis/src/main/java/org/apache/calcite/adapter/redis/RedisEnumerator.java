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

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link RedisEnumerator}.
 */
class RedisEnumerator implements Enumerator<Object[]> {
  private final Enumerator<Object[]> enumerator;

  RedisEnumerator(RedisConfig redisConfig, RedisSchema schema, String tableName) {
    RedisTableFieldInfo tableFieldInfo = schema.getTableFieldInfo(tableName);

    RedisJedisManager redisManager =
        new RedisJedisManager(redisConfig.getHost(), redisConfig.getPort(),
            redisConfig.getDatabase(), redisConfig.getPassword());

    try (Jedis jedis = redisManager.getResource()) {
      if (StringUtils.isNotEmpty(redisConfig.getPassword())) {
        jedis.auth(redisConfig.getPassword());
      }
      RedisDataProcess dataProcess = new RedisDataProcess(jedis, tableFieldInfo);
      List<Object[]> objs = dataProcess.read();
      enumerator = Linq4j.enumerator(objs);
    }
  }

  static Map<String, Object> deduceRowType(RedisTableFieldInfo tableFieldInfo) {
    final Map<String, Object> fieldBuilder = new LinkedHashMap<>();
    String dataFormat = tableFieldInfo.getDataFormat();
    RedisDataFormat redisDataFormat =
        requireNonNull(RedisDataFormat.fromTypeName(dataFormat));
    if (redisDataFormat == RedisDataFormat.RAW) {
      fieldBuilder.put("key", "key");
    } else {
      for (LinkedHashMap<String, Object> field : tableFieldInfo.getFields()) {
        fieldBuilder.put(field.get("name").toString(), field.get("type").toString());
      }
    }
    return fieldBuilder;
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
