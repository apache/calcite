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
package org.apache.calcite.adapter.openapi;

import org.apache.calcite.linq4j.tree.Types;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * Builtin methods in the OpenAPI adapter.
 */
enum OpenAPIMethod {
  OPENAPI_QUERYABLE_FIND(OpenAPITable.OpenAPIQueryable.class, "find",
      Map.class, List.class, List.class, Long.class, Long.class);

  public final Method method;

  public static final String OPENAPI_PACKAGE = "org.apache.calcite.adapter.openapi";

  OpenAPIMethod(Class clazz, String methodName, Class... argumentTypes) {
    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
  }
}

/**
 * Utility functions for converting OpenAPI results to Calcite enumerables.
 */
class OpenAPIEnumerators {

  private OpenAPIEnumerators() {}

  /**
   * Creates a function that extracts fields from JSON nodes.
   *
   * @param fields List of fields to extract, or null for all fields
   * @return Function that converts JsonNode to appropriate return type
   */
  static org.apache.calcite.linq4j.function.Function1<com.fasterxml.jackson.databind.JsonNode, Object>
  getter(List<Map.Entry<String, Class>> fields) {

    if (fields == null || fields.isEmpty()) {
      // Return the entire JSON object as a map
      return jsonNode -> {
        Map<String, Object> map = new java.util.HashMap<>();
        jsonNode.fields().forEachRemaining(entry -> {
          map.put(entry.getKey(), convertJsonValue(entry.getValue()));
        });
        return map;
      };
    } else if (fields.size() == 1) {
      // Single field extraction
      String fieldName = fields.get(0).getKey();
      Class fieldClass = fields.get(0).getValue();
      return jsonNode -> {
        com.fasterxml.jackson.databind.JsonNode fieldNode = jsonNode.get(fieldName);
        return convertJsonValue(fieldNode, fieldClass);
      };
    } else {
      // Multiple field extraction - return as Object[]
      return jsonNode -> {
        Object[] result = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          String fieldName = fields.get(i).getKey();
          Class fieldClass = fields.get(i).getValue();
          com.fasterxml.jackson.databind.JsonNode fieldNode = jsonNode.get(fieldName);
          result[i] = convertJsonValue(fieldNode, fieldClass);
        }
        return result;
      };
    }
  }

  /**
   * Converts a JsonNode to a Java object of the appropriate type.
   */
  private static Object convertJsonValue(com.fasterxml.jackson.databind.JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    } else if (node.isBoolean()) {
      return node.asBoolean();
    } else if (node.isIntegralNumber()) {
      return node.asLong();
    } else if (node.isFloatingPointNumber()) {
      return node.asDouble();
    } else if (node.isTextual()) {
      return node.asText();
    } else if (node.isArray()) {
      java.util.List<Object> list = new java.util.ArrayList<>();
      node.elements().forEachRemaining(element -> list.add(convertJsonValue(element)));
      return list;
    } else if (node.isObject()) {
      java.util.Map<String, Object> map = new java.util.HashMap<>();
      node.fields().forEachRemaining(entry ->
          map.put(entry.getKey(), convertJsonValue(entry.getValue())));
      return map;
    } else {
      return node.asText(); // Fallback to string representation
    }
  }

  /**
   * Converts a JsonNode to a specific Java type.
   */
  private static Object convertJsonValue(com.fasterxml.jackson.databind.JsonNode node, Class targetClass) {
    Object value = convertJsonValue(node);
    if (value == null || targetClass.isInstance(value)) {
      return value;
    }

    // Attempt type conversion
    if (targetClass == String.class) {
      return value.toString();
    } else if (targetClass == Integer.class || targetClass == int.class) {
      if (value instanceof Number) {
        return ((Number) value).intValue();
      }
      return Integer.valueOf(value.toString());
    } else if (targetClass == Long.class || targetClass == long.class) {
      if (value instanceof Number) {
        return ((Number) value).longValue();
      }
      return Long.valueOf(value.toString());
    } else if (targetClass == Double.class || targetClass == double.class) {
      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      }
      return Double.valueOf(value.toString());
    } else if (targetClass == Boolean.class || targetClass == boolean.class) {
      if (value instanceof Boolean) {
        return value;
      }
      return Boolean.valueOf(value.toString());
    }

    // Default: return as-is and hope for the best
    return value;
  }
}
