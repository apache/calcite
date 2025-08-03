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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.linq4j.Enumerator;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Enumerator that fetches and returns SharePoint list items.
 */
public class SharePointListEnumerator implements Enumerator<Object[]> {
  private final SharePointListMetadata metadata;
  private final MicrosoftGraphListClient client;
  private Iterator<Map<String, Object>> iterator;
  private Object[] current;

  public SharePointListEnumerator(SharePointListMetadata metadata, MicrosoftGraphListClient client) {
    this.metadata = metadata;
    this.client = client;
    reset();
  }

  @Override public Object[] current() {
    return current;
  }

  @Override public boolean moveNext() {
    if (iterator.hasNext()) {
      Map<String, Object> item = iterator.next();
      current = convertToRow(item);
      return true;
    }
    return false;
  }

  @Override public void reset() {
    try {
      List<Map<String, Object>> items = client.getListItems(metadata.getListId());
      iterator = items.iterator();
    } catch (Exception e) {
      throw new RuntimeException("Failed to fetch SharePoint list items", e);
    }
  }

  @Override public void close() {
    // Nothing to close
  }

  private Object[] convertToRow(Map<String, Object> item) {
    List<SharePointColumn> columns = metadata.getColumns();
    Object[] row = new Object[columns.size() + 1]; // +1 for ID column

    // First column is always the ID
    row[0] = item.get("id");

    // Then the other columns
    for (int i = 0; i < columns.size(); i++) {
      SharePointColumn column = columns.get(i);
      Object value = item.get(column.getInternalName());
      row[i + 1] = convertValue(value, column.getType());
    }

    return row;
  }

  private Object convertValue(Object value, String type) {
    if (value == null) {
      return null;
    }

    switch (type.toLowerCase(Locale.ROOT)) {
    case "number":
    case "currency":
      return value instanceof Number ? ((Number) value).doubleValue()
          : Double.parseDouble(value.toString());
    case "integer":
    case "counter":
      return value instanceof Number ? ((Number) value).intValue()
          : Integer.parseInt(value.toString());
    case "boolean":
      return value instanceof Boolean ? value : Boolean.parseBoolean(value.toString());
    case "datetime":
      // SharePoint returns dates as ISO 8601 strings
      return java.sql.Timestamp.valueOf(value.toString().replace("T", " ").replace("Z", ""));
    case "lookup":
    case "user":
      // These are complex objects in SharePoint, extract display value
      if (value instanceof Map) {
        Map<String, Object> lookupValue = (Map<String, Object>) value;
        return lookupValue.get("Title") != null
            ? lookupValue.get("Title") : lookupValue.get("LookupValue");
      }
      return value.toString();
    case "multichoice":
      // Multiple choice fields return arrays
      if (value instanceof List) {
        return String.join(", ", (List<String>) value);
      }
      return value.toString();
    default:
      return value.toString();
    }
  }
}
