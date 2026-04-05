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

import org.apache.calcite.test.CalciteAssert;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests that invalid model configurations are rejected with clear errors.
 */
public class KvrocksAdapterConfigCaseBase extends KvrocksAdapterCaseBase {

  private String configModel;

  private void readModelFromJsonString(String jsonString) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
      mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
      mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
      JsonNode root = mapper.readTree(jsonString);
      configModel = root.toString()
          .replace("6666",
              Integer.toString(getKvrocksServerPort()));
    } catch (Exception e) {
      throw new RuntimeException("Failed to read model from JSON string", e);
    }
  }

  @Test void testDataFormatEmptyException() {
    assumeTrue(isKvrocksRunning());
    String json = "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"kvrocks\","
        + "  \"schemas\": [{"
        + "    \"type\": \"custom\","
        + "    \"name\": \"foodmart\","
        + "    \"factory\": \"org.apache.calcite.adapter.kvrocks.KvrocksSchemaFactory\","
        + "    \"operand\": {"
        + "      \"host\": \"localhost\","
        + "      \"port\": 6666,"
        + "      \"database\": 0,"
        + "      \"password\": \"\""
        + "    },"
        + "    \"tables\": [{"
        + "      \"name\": \"csv_05\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.kvrocks.KvrocksTableFactory\","
        + "      \"operand\": {"
        + "        \"dataFormat\": \"\","
        + "        \"keyDelimiter\": \":\","
        + "        \"fields\": ["
        + "          {\"name\": \"DEPTNO\", \"type\": \"varchar\", \"mapping\": 0},"
        + "          {\"name\": \"NAME\", \"type\": \"varchar\", \"mapping\": 1}"
        + "        ]"
        + "      }"
        + "    }]"
        + "  }]"
        + "}";
    readModelFromJsonString(json);
    assertNotNull(configModel, "model cannot be null");
    CalciteAssert.model(configModel)
        .enable(isKvrocksRunning())
        .connectThrows("dataFormat is invalid, it must be raw, csv or json");
  }

  @Test void testDataFieldsEmptyException() {
    assumeTrue(isKvrocksRunning());
    String json = "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"kvrocks\","
        + "  \"schemas\": [{"
        + "    \"type\": \"custom\","
        + "    \"name\": \"foodmart\","
        + "    \"factory\": \"org.apache.calcite.adapter.kvrocks.KvrocksSchemaFactory\","
        + "    \"operand\": {"
        + "      \"host\": \"localhost\","
        + "      \"port\": 6666,"
        + "      \"database\": 0,"
        + "      \"password\": \"\""
        + "    },"
        + "    \"tables\": [{"
        + "      \"name\": \"csv_05\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.kvrocks.KvrocksTableFactory\","
        + "      \"operand\": {"
        + "        \"dataFormat\": \"csv\","
        + "        \"keyDelimiter\": \":\","
        + "        \"fields\": []"
        + "      }"
        + "    }]"
        + "  }]"
        + "}";
    readModelFromJsonString(json);
    assertNotNull(configModel, "model cannot be null");
    CalciteAssert.model(configModel)
        .enable(isKvrocksRunning())
        .connectThrows("fields is null");
  }

  @Test void testDataFormatInvalidException() {
    assumeTrue(isKvrocksRunning());
    String json = "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"kvrocks\","
        + "  \"schemas\": [{"
        + "    \"type\": \"custom\","
        + "    \"name\": \"foodmart\","
        + "    \"factory\": \"org.apache.calcite.adapter.kvrocks.KvrocksSchemaFactory\","
        + "    \"operand\": {"
        + "      \"host\": \"localhost\","
        + "      \"port\": 6666,"
        + "      \"database\": 0,"
        + "      \"password\": \"\""
        + "    },"
        + "    \"tables\": [{"
        + "      \"name\": \"csv_05\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.kvrocks.KvrocksTableFactory\","
        + "      \"operand\": {"
        + "        \"dataFormat\": \"CSV\","
        + "        \"keyDelimiter\": \":\","
        + "        \"fields\": ["
        + "          {\"name\": \"DEPTNO\", \"type\": \"varchar\", \"mapping\": 0},"
        + "          {\"name\": \"NAME\", \"type\": \"varchar\", \"mapping\": 1}"
        + "        ]"
        + "      }"
        + "    }]"
        + "  }]"
        + "}";
    readModelFromJsonString(json);
    assertNotNull(configModel, "model cannot be null");
    CalciteAssert.model(configModel)
        .enable(isKvrocksRunning())
        .connectThrows("dataFormat is invalid, it must be raw, csv or json");
  }
}
