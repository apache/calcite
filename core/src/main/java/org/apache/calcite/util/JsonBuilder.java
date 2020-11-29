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
package org.apache.calcite.util;

import org.apache.calcite.avatica.util.Spaces;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder for JSON documents (represented as {@link List}, {@link Map},
 * {@link String}, {@link Boolean}, {@link Long}).
 */
public class JsonBuilder {
  /**
   * Creates a JSON object (represented by a {@link Map}).
   */
  public Map<String, @Nullable Object> map() {
    // Use LinkedHashMap to preserve order.
    return new LinkedHashMap<>();
  }

  /**
   * Creates a JSON object (represented by a {@link List}).
   */
  public List<@Nullable Object> list() {
    return new ArrayList<>();
  }

  /**
   * Adds a key/value pair to a JSON object.
   */
  public JsonBuilder put(Map<String, @Nullable Object> map, String name, @Nullable Object value) {
    map.put(name, value);
    return this;
  }

  /**
   * Adds a key/value pair to a JSON object if the value is not null.
   */
  public JsonBuilder putIf(
      Map<String, @Nullable Object> map, String name, @Nullable Object value) {
    if (value != null) {
      map.put(name, value);
    }
    return this;
  }

  /**
   * Serializes an object consisting of maps, lists and atoms into a JSON
   * string.
   *
   * <p>We should use a JSON library such as Jackson when Mondrian needs
   * one elsewhere.</p>
   */
  public String toJsonString(Object o) {
    StringBuilder buf = new StringBuilder();
    append(buf, 0, o);
    return buf.toString();
  }

  /**
   * Appends a JSON object to a string builder.
   */
  public void append(StringBuilder buf, int indent, @Nullable Object o) {
    if (o == null) {
      buf.append("null");
    } else if (o instanceof Map) {
      //noinspection unchecked
      appendMap(buf, indent, (Map) o);
    } else if (o instanceof List) {
      appendList(buf, indent, (List<?>) o);
    } else if (o instanceof String) {
      buf.append('"')
          .append(
              ((String) o).replace("\"", "\\\"")
                  .replace("\n", "\\n"))
          .append('"');
    } else {
      assert o instanceof Number || o instanceof Boolean;
      buf.append(o);
    }
  }

  private void appendMap(
      StringBuilder buf, int indent, Map<String, @Nullable Object> map) {
    if (map.isEmpty()) {
      buf.append("{}");
      return;
    }
    buf.append("{");
    newline(buf, indent + 1);
    int n = 0;
    for (Map.Entry<String, @Nullable Object> entry : map.entrySet()) {
      if (n++ > 0) {
        buf.append(",");
        newline(buf, indent + 1);
      }
      append(buf, 0, entry.getKey());
      buf.append(": ");
      append(buf, indent + 1, entry.getValue());
    }
    newline(buf, indent);
    buf.append("}");
  }

  private void newline(StringBuilder buf, int indent) {
    Spaces.append(buf.append('\n'), indent * 2);
  }

  private void appendList(
      StringBuilder buf, int indent, List<?> list) {
    if (list.isEmpty()) {
      buf.append("[]");
      return;
    }
    buf.append("[");
    newline(buf, indent + 1);
    int n = 0;
    for (Object o : list) {
      if (n++ > 0) {
        buf.append(",");
        newline(buf, indent + 1);
      }
      append(buf, indent + 1, o);
    }
    newline(buf, indent);
    buf.append("]");
  }
}
