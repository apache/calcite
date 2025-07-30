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
package org.apache.calcite.adapter.file;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility to flatten nested JSON structures.
 * Objects are flattened using dot notation, arrays are converted to delimited strings.
 */
public class JsonFlattener {
  private final String delimiter;
  private final int maxDepth;
  private final String nullValue;

  public JsonFlattener() {
    this(",", 3, "");
  }

  public JsonFlattener(String delimiter, int maxDepth, String nullValue) {
    this.delimiter = delimiter;
    this.maxDepth = maxDepth;
    this.nullValue = nullValue;
  }

  /**
   * Flattens a nested map structure.
   *
   * @param input The map to flatten
   * @return A new map with flattened keys
   */
  public Map<String, Object> flatten(Map<String, Object> input) {
    Map<String, Object> output = new LinkedHashMap<>();
    flattenObject("", input, output, 0);
    return output;
  }

  private void flattenObject(String prefix, Map<String, Object> obj,
                            Map<String, Object> output, int depth) {
    if (depth > maxDepth) {
      // If we've reached max depth, store the object as JSON string
      if (!prefix.isEmpty()) {
        output.put(prefix, obj.toString());
      }
      return;
    }

    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
      Object value = entry.getValue();

      if (value == null) {
        output.put(key, null);
      } else if (value instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> mapValue = (Map<String, Object>) value;
        if (mapValue.isEmpty()) {
          // Skip empty objects
          continue;
        }
        flattenObject(key, mapValue, output, depth + 1);
      } else if (value instanceof List) {
        String flattened = flattenArray((List<?>) value);
        if (flattened != null) {
          output.put(key, flattened);
        }
      } else {
        output.put(key, value);
      }
    }
  }

  private String flattenArray(List<?> array) {
    if (array.isEmpty()) {
      return "";
    }

    // Check if it's an array of objects - don't flatten those
    if (array.stream().anyMatch(item -> item instanceof Map)) {
      return null; // Signal to skip this field
    }

    return array.stream()
        .map(v -> v == null ? nullValue : escapeValue(v.toString()))
        .collect(Collectors.joining(delimiter));
  }

  private String escapeValue(String value) {
    // If the value contains our delimiter, wrap it in quotes
    if (value.contains(delimiter)) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
    return value;
  }
}
