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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

import static java.util.Objects.requireNonNull;

/**
 * Reads data from a single Kvrocks key and converts it to relational rows.
 *
 * <p>The reading strategy depends on the Kvrocks data type of the key
 * (detected via the {@code TYPE} command), and the parsing strategy depends
 * on the configured {@link KvrocksDataFormat}.
 */
public class KvrocksDataProcess {
  private final Jedis jedis;
  private final String tableName;
  private final String keyDelimiter;
  private final KvrocksDataType dataType;
  private final KvrocksDataFormat dataFormat;
  private final List<LinkedHashMap<String, Object>> fields;
  private final ObjectMapper objectMapper;

  KvrocksDataProcess(Jedis jedis, KvrocksTableFieldInfo fieldInfo) {
    this.jedis = jedis;
    this.tableName = fieldInfo.getTableName();
    this.keyDelimiter = fieldInfo.getKeyDelimiter();
    this.fields = fieldInfo.getFields();

    String type = jedis.type(tableName);
    this.dataType =
        requireNonNull(KvrocksDataType.fromTypeName(type));
    this.dataFormat =
        requireNonNull(
            KvrocksDataFormat.fromTypeName(
                fieldInfo.getDataFormat()));

    this.objectMapper = new ObjectMapper();
    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
  }

  /** Reads all values for the key and returns them as relational rows. */
  public List<Object[]> read() {
    switch (dataType) {
    case STRING:
      return readString();
    case LIST:
      return parseValues(jedis.lrange(tableName, 0, -1));
    case SET:
      return parseValues(jedis.smembers(tableName));
    case SORTED_SET:
      return parseValues(jedis.zrange(tableName, 0, -1));
    case HASH:
      return parseValues(jedis.hvals(tableName));
    case STREAM:
      return readStream();
    default:
      return new ArrayList<>();
    }
  }

  /** STRING type: the key name itself is the lookup key; value is the data. */
  private List<Object[]> readString() {
    List<Object[]> rows = new ArrayList<>();
    for (String key : jedis.keys(tableName)) {
      String value = jedis.get(key);
      if (value != null) {
        rows.add(parseRow(value));
      }
    }
    return rows;
  }

  /** Reads entries from a Kvrocks Stream via XRANGE. */
  private List<Object[]> readStream() {
    List<Object[]> rows = new ArrayList<>();
    List<StreamEntry> entries =
        jedis.xrange(tableName, (StreamEntryID) null,
            (StreamEntryID) null, Integer.MAX_VALUE);
    for (StreamEntry entry : entries) {
      try {
        String json =
            objectMapper.writeValueAsString(entry.getFields());
        rows.add(parseRow(json));
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to serialize stream entry", e);
      }
    }
    return rows;
  }

  /** Parses a collection of string values into rows. */
  private List<Object[]> parseValues(Iterable<String> values) {
    List<Object[]> rows = new ArrayList<>();
    for (String value : values) {
      rows.add(parseRow(value));
    }
    return rows;
  }

  /** Converts a single string value into a row array. */
  private Object[] parseRow(String value) {
    switch (dataFormat) {
    case RAW:
      return new Object[]{value};
    case CSV:
      return parseCsv(value);
    case JSON:
      return parseJson(value);
    default:
      throw new AssertionError("unexpected format: " + dataFormat);
    }
  }

  private Object[] parseCsv(String value) {
    String[] parts = value.split(keyDelimiter);
    Object[] row = new Object[fields.size()];
    for (int i = 0; i < row.length; i++) {
      row[i] = i < parts.length ? parts[i] : "";
    }
    return row;
  }

  private Object[] parseJson(String value) {
    Object[] row = new Object[fields.size()];
    try {
      JsonNode node = objectMapper.readTree(value);
      for (int i = 0; i < row.length; i++) {
        Object mapping = fields.get(i).get("mapping");
        if (mapping == null) {
          row[i] = "";
        } else {
          JsonNode found = node.findValue(mapping.toString());
          row[i] = found != null ? found : "";
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse JSON value: " + value, e);
    }
    return row;
  }
}
