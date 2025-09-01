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
package org.apache.calcite.adapter.file.format.json;

import org.apache.calcite.rel.type.RelDataType;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Container for shared parsed JSON data that can be accessed by multiple tables.
 * This allows parsing a JSON file once and creating multiple table views from it.
 */
public class SharedJsonData {
  private final JsonNode rootNode;
  private final Map<String, JsonNode> pathCache = new ConcurrentHashMap<>();
  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * Creates a SharedJsonData container with the parsed JSON root node.
   *
   * @param rootNode The parsed JSON root node
   */
  public SharedJsonData(JsonNode rootNode) {
    this.rootNode = rootNode;
  }

  /**
   * Get data at a specific JSONPath, with caching.
   * Supports path expressions like:
   * - $.data.users (JSONPath style)
   * - $.users[0].name (with array indices)
   * - $.users[*] (wildcards for all array elements)
   *
   * @param jsonPath The JSONPath expression
   * @return The JsonNode at the specified path, or null if not found
   */
  public @Nullable JsonNode getDataAtPath(String jsonPath) {
    if (jsonPath == null || jsonPath.equals("$")) {
      return rootNode;
    }

    return pathCache.computeIfAbsent(jsonPath, path -> {
      try {
        // Check for wildcard patterns first
        if (path.contains("[*]")) {
          return handleWildcardPath(path);
        }

        // Convert JSONPath to JsonPointer format
        String pointerPath = convertJsonPathToPointer(path);

        if (pointerPath.isEmpty() || pointerPath.equals("/")) {
          return rootNode;
        }

        // Use Jackson's JsonPointer for efficient navigation
        JsonPointer pointer = JsonPointer.compile(pointerPath);
        JsonNode result = rootNode.at(pointer);

        // at() returns MissingNode if path doesn't exist
        return result.isMissingNode() ? null : result;
      } catch (Exception e) {
        // Path not found or evaluation error
        return null;
      }
    });
  }

  /**
   * Convert JSONPath notation to JsonPointer notation.
   * $.data.users[0] -> /data/users/0
   * $.data.users -> /data/users
   *
   * @param jsonPath JSONPath expression
   * @return JsonPointer path
   */
  private String convertJsonPathToPointer(String jsonPath) {
    // Remove $ prefix
    String path = jsonPath.startsWith("$") ? jsonPath.substring(1) : jsonPath;

    // Remove leading dot
    if (path.startsWith(".")) {
      path = path.substring(1);
    }

    if (path.isEmpty()) {
      return "";
    }

    // Convert dots to slashes
    path = path.replace(".", "/");

    // Convert array notation [n] to /n
    path = path.replaceAll("\\[(\\d+)\\]", "/$1");

    // Ensure path starts with /
    if (!path.startsWith("/")) {
      path = "/" + path;
    }

    return path;
  }

  /**
   * Handle wildcard paths like $.users[*].name
   * Returns an array containing all matching values.
   *
   * @param jsonPath Path containing wildcards
   * @return JsonNode array of matching values, or null if none found
   */
  private @Nullable JsonNode handleWildcardPath(String jsonPath) {
    // Split path at the wildcard
    int wildcardIndex = jsonPath.indexOf("[*]");
    if (wildcardIndex == -1) {
      return null;
    }

    String basePath = jsonPath.substring(0, wildcardIndex);
    String remainingPath = jsonPath.substring(wildcardIndex + 3); // Skip [*]

    // Get the array node
    JsonNode arrayNode = getDataAtPath(basePath);
    if (arrayNode == null || !arrayNode.isArray()) {
      return null;
    }

    // If there's no remaining path, return all array elements
    if (remainingPath.isEmpty()) {
      return arrayNode;
    }

    // Extract values from each array element
    ObjectMapper mapper = new ObjectMapper();
    List<JsonNode> results = new ArrayList<>();

    for (JsonNode element : arrayNode) {
      if (remainingPath.startsWith(".")) {
        // Navigate within each element
        String elementPath = "$" + remainingPath;
        // Create a temporary SharedJsonData for the element
        SharedJsonData tempData = new SharedJsonData(element);
        JsonNode value = tempData.getDataAtPath(elementPath);
        if (value != null) {
          results.add(value);
        }
      }
    }

    // Return results as array node
    return results.isEmpty() ? null : mapper.valueToTree(results);
  }

  /**
   * Get row iterator for data at a specific path.
   *
   * @param jsonPath The JSONPath expression
   * @param rowType The expected row type
   * @return An iterator over rows of Object arrays
   */
  public Iterator<Object[]> getRowIterator(String jsonPath, RelDataType rowType) {
    JsonNode pathData = getDataAtPath(jsonPath);
    if (pathData == null) {
      return new ArrayList<Object[]>().iterator();
    }

    return new JsonPathIterator(pathData, rowType);
  }


  /**
   * Get the root JSON node.
   *
   * @return The root JsonNode
   */
  public JsonNode getRootNode() {
    return rootNode;
  }

  /**
   * Check if a path exists in the JSON structure.
   *
   * @param jsonPath The JSONPath expression
   * @return true if the path exists, false otherwise
   */
  public boolean pathExists(String jsonPath) {
    return getDataAtPath(jsonPath) != null;
  }

  /**
   * Get the size of array/object at a specific path.
   *
   * @param jsonPath The JSONPath expression
   * @return The size of the collection at the path, or 0 if not a collection
   */
  public int getPathSize(String jsonPath) {
    JsonNode pathData = getDataAtPath(jsonPath);
    if (pathData == null) {
      return 0;
    }

    if (pathData.isArray()) {
      return pathData.size();
    } else if (pathData.isObject()) {
      return pathData.size();
    }

    return 0;
  }

  /**
   * Iterator for converting JsonNode data to Object[] rows.
   */
  private static class JsonPathIterator implements Iterator<Object[]> {
    private final Iterator<JsonNode> nodeIterator;
    private final RelDataType rowType;
    private final int fieldCount;

    JsonPathIterator(JsonNode data, RelDataType rowType) {
      this.rowType = rowType;
      this.fieldCount = rowType.getFieldCount();

      if (data.isArray()) {
        this.nodeIterator = data.iterator();
      } else {
        // Single object - wrap in a single-element iterator
        List<JsonNode> singleNode = new ArrayList<>();
        singleNode.add(data);
        this.nodeIterator = singleNode.iterator();
      }
    }

    @Override public boolean hasNext() {
      return nodeIterator.hasNext();
    }

    @Override public Object[] next() {
      JsonNode node = nodeIterator.next();
      Object[] row = new Object[fieldCount];

      for (int i = 0; i < fieldCount; i++) {
        String fieldName = rowType.getFieldList().get(i).getName();
        JsonNode fieldNode = node.get(fieldName);

        if (fieldNode != null) {
          row[i] = convertJsonNode(fieldNode);
        }
      }

      return row;
    }

    private Object convertJsonNode(JsonNode node) {
      if (node.isNull()) {
        return null;
      } else if (node.isBoolean()) {
        return node.asBoolean();
      } else if (node.isNumber()) {
        if (node.isIntegralNumber()) {
          return node.asLong();
        } else {
          return node.asDouble();
        }
      } else if (node.isTextual()) {
        return node.asText();
      } else if (node.isArray() || node.isObject()) {
        return node.toString();
      }
      return null;
    }
  }
}
