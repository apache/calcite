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

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import redis.clients.jedis.Jedis;

import static java.util.Objects.requireNonNull;

/**
 * The class with RedisDataProcess.
 */
public class RedisDataProcess {
  final String tableName;
  final String dataFormat;
  final String keyDelimiter;
  final RedisDataType dataType;
  final RedisDataFormat redisDataFormat;
  final List<LinkedHashMap<String, Object>> fields;
  private final Jedis jedis;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public RedisDataProcess(Jedis jedis, RedisTableFieldInfo tableFieldInfo) {
    this.jedis = jedis;
    String type = jedis.type(tableFieldInfo.getTableName());
    fields = tableFieldInfo.getFields();
    dataFormat = tableFieldInfo.getDataFormat();
    tableName = tableFieldInfo.getTableName();
    keyDelimiter = tableFieldInfo.getKeyDelimiter();
    dataType = requireNonNull(RedisDataType.fromTypeName(type));
    redisDataFormat =
        requireNonNull(
            RedisDataFormat.fromTypeName(tableFieldInfo.getDataFormat()));
    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
        .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
        .configure(JsonParser.Feature.ALLOW_COMMENTS, true);
  }

  public List<Object[]> read() {
    switch (dataType) {
    case STRING:
      return parse(jedis.keys(tableName));
    case LIST:
      return parse(jedis.lrange(tableName, 0, -1));
    case SET:
      return parse(jedis.smembers(tableName));
    case SORTED_SET:
      return parse(jedis.zrange(tableName, 0, -1));
    case HASH:
      return parse(jedis.hvals(tableName));
    default:
      return new ArrayList<>();
    }
  }

  private Object[] parseJson(String value) {
    assert StringUtils.isNotEmpty(value);
    Object[] arr = new Object[fields.size()];
    try {
      JsonNode jsonNode = objectMapper.readTree(value);
      Object obj;
      for (int i = 0; i < arr.length; i++) {
        obj = fields.get(i).get("mapping");
        if (obj == null) {
          arr[i] = "";
        } else {
          arr[i] = jsonNode.findValue(fields.get(i).get("mapping").toString());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Parsing json failed: ", e);
    }
    return arr;
  }

  private Object[] parseCsv(String value) {
    assert StringUtils.isNotEmpty(value);
    String[] values = value.split(keyDelimiter);
    Object[] arr = new Object[fields.size()];
    assert values.length == arr.length;
    for (int i = 0; i < arr.length; i++) {
      arr[i] = values[i] == null ? "" : values[i];
    }
    return arr;
  }

  List<Object[]> parse(Iterable<String> keys) {
    List<Object[]> objects = new ArrayList<>();
    for (String key : keys) {
      if (dataType == RedisDataType.STRING) {
        key = jedis.get(key);
      }
      switch (redisDataFormat) {
      case RAW:
        objects.add(new Object[]{key});
        break;
      case JSON:
        objects.add(parseJson(key));
        break;
      case CSV:
        objects.add(parseCsv(key));
        break;
      default:
        break;
      }
    }
    return objects;
  }

  public List<Object[]> parse(List<String> keys) {
    List<Object[]> objects = new ArrayList<>();
    for (String key : keys) {
      if (dataType == RedisDataType.STRING) {
        key = jedis.get(key);
      }
      switch (redisDataFormat) {
      case RAW:
        objects.add(new Object[]{key});
        break;
      case JSON:
        objects.add(parseJson(key));
        break;
      case CSV:
        objects.add(parseCsv(key));
        break;
      default:
        break;
      }
    }
    return objects;
  }
}
