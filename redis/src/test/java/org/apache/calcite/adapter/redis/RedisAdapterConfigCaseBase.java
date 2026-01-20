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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.test.CalciteAssert;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import redis.clients.jedis.Protocol;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for the {@code org.apache.calcite.adapter.redis} package.
 */
public class RedisAdapterConfigCaseBase extends RedisAdapterCaseBase {

  private String model;

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7388">[CALCITE-7388]
   * Redis Adapter operand config should not support empty string</a>. */
  protected void readModelFromJsonString(String jsonString) {
    String strResult = null;
    try {
      ObjectMapper objMapper = new ObjectMapper();
      objMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
          .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
          .configure(JsonParser.Feature.ALLOW_COMMENTS, true);
      JsonNode rootNode = objMapper.readTree(jsonString);
      strResult =
          rootNode.toString().replace(Integer.toString(Protocol.DEFAULT_PORT),
              Integer.toString(getRedisServerPort()));
    } catch (Exception e) {
      throw new RuntimeException("Failed to read model from json string", e);
    }
    model = strResult;
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7388">[CALCITE-7388]
   * Redis Adapter operand config should not support empty string</a>. */
  @Test void testDataFormatEmptyException() {
    String jsonString = "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"redis\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"type\": \"custom\","
        + "      \"name\": \"foodmart\","
        + "      \"factory\": \"org.apache.calcite.adapter.redis.RedisSchemaFactory\","
        + "      \"operand\": {"
        + "        \"host\": \"localhost\","
        + "        \"port\": 6379 ,"
        + "        \"database\": 0,"
        + "        \"password\": \"\""
        + "      },"
        + "      \"tables\": ["
        + "        {"
        + "          \"name\": \"csv_05\","
        + "          \"type\": \"custom\","
        + "          \"factory\": \"org.apache.calcite.adapter.redis.RedisTableFactory\","
        + "          \"operand\": {"
        + "            \"dataFormat\": \"\","
        + "            \"keyDelimiter\": \":\","
        + "            \"fields\": [\n"
        + "              {\n"
        + "                \"name\": \"DEPTNO\",\n"
        + "                \"type\": \"varchar\",\n"
        + "                \"mapping\": 0\n"
        + "              },\n"
        + "              {\n"
        + "                \"name\": \"NAME\",\n"
        + "                \"type\": \"varchar\",\n"
        + "                \"mapping\": 1\n"
        + "              }\n"
        + "            ]\n"
        + "          }"
        + "        }"
        + "      ]"
        + "    }"
        + "  ]"
        + "}";
    readModelFromJsonString(jsonString);
    assertNotNull(model, "model cannot be null!");
    CalciteAssert.model(model)
        .enable(CalciteSystemProperty.TEST_REDIS.value())
        .connectThrows("dataFormat is invalid, it must be raw, csv or json");
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7388">[CALCITE-7388]
   * Redis Adapter operand config should not support empty string</a>. */
  @Test void testDataFieldsEmptyException() {
    String jsonString = "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"redis\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"type\": \"custom\","
        + "      \"name\": \"foodmart\","
        + "      \"factory\": \"org.apache.calcite.adapter.redis.RedisSchemaFactory\","
        + "      \"operand\": {"
        + "        \"host\": \"localhost\","
        + "        \"port\": 6379 ,"
        + "        \"database\": 0,"
        + "        \"password\": \"\""
        + "      },"
        + "      \"tables\": ["
        + "        {"
        + "          \"name\": \"csv_05\","
        + "          \"type\": \"custom\","
        + "          \"factory\": \"org.apache.calcite.adapter.redis.RedisTableFactory\","
        + "          \"operand\": {"
        + "            \"dataFormat\": \"csv\","
        + "            \"keyDelimiter\": \":\","
        + "            \"fields\": []\n"
        + "          }"
        + "        }"
        + "      ]"
        + "    }"
        + "  ]"
        + "}";
    readModelFromJsonString(jsonString);
    assertNotNull(model, "model cannot be null!");
    CalciteAssert.model(model)
        .enable(CalciteSystemProperty.TEST_REDIS.value())
        .connectThrows("fields is null");
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7388">[CALCITE-7388]
   * Redis Adapter operand config should not support empty string</a>. */
  @Test void testDataFormatInvalidException() {
    String jsonString = "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"redis\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"type\": \"custom\","
        + "      \"name\": \"foodmart\","
        + "      \"factory\": \"org.apache.calcite.adapter.redis.RedisSchemaFactory\","
        + "      \"operand\": {"
        + "        \"host\": \"localhost\","
        + "        \"port\": 6379 ,"
        + "        \"database\": 0,"
        + "        \"password\": \"\""
        + "      },"
        + "      \"tables\": ["
        + "        {"
        + "          \"name\": \"csv_05\","
        + "          \"type\": \"custom\","
        + "          \"factory\": \"org.apache.calcite.adapter.redis.RedisTableFactory\","
        + "          \"operand\": {"
        + "            \"dataFormat\": \"CSV\","
        + "            \"keyDelimiter\": \":\","
        + "            \"fields\": [\n"
        + "              {\n"
        + "                \"name\": \"DEPTNO\",\n"
        + "                \"type\": \"varchar\",\n"
        + "                \"mapping\": 0\n"
        + "              },\n"
        + "              {\n"
        + "                \"name\": \"NAME\",\n"
        + "                \"type\": \"varchar\",\n"
        + "                \"mapping\": 1\n"
        + "              }\n"
        + "            ]\n"
        + "          }"
        + "        }"
        + "      ]"
        + "    }"
        + "  ]"
        + "}";
    readModelFromJsonString(jsonString);
    assertNotNull(model, "model cannot be null!");
    CalciteAssert.model(model)
        .enable(CalciteSystemProperty.TEST_REDIS.value())
        .connectThrows("dataFormat is invalid, it must be raw, csv or json");
  }
}
