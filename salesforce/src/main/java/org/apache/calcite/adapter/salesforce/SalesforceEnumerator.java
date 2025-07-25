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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Enumerator that reads from Salesforce.
 */
public class SalesforceEnumerator implements Enumerator<Object[]> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SalesforceEnumerator.class);

  private final SalesforceConnection connection;
  private final String soql;
  private final RelDataType rowType;

  private SalesforceConnection.QueryResult currentResult;
  private Iterator<Map<String, Object>> currentIterator;
  private Object[] current;

  public SalesforceEnumerator(SalesforceConnection connection, String soql,
      RelDataType rowType) {
    this.connection = connection;
    this.soql = soql;
    this.rowType = rowType;
  }

  @Override public Object[] current() {
    return current;
  }

  @Override public boolean moveNext() {
    try {
      // Initial query
      if (currentResult == null) {
        LOGGER.debug("Executing SOQL: {}", soql);
        currentResult = connection.query(soql);
        currentIterator = currentResult.records.iterator();
      }

      // Check if we have more records in current batch
      if (currentIterator.hasNext()) {
        Map<String, Object> record = currentIterator.next();
        current = convertRecord(record);
        return true;
      }

      // Check if there are more batches
      if (!currentResult.done && currentResult.nextRecordsUrl != null) {
        LOGGER.debug("Fetching next batch: {}", currentResult.nextRecordsUrl);
        currentResult = connection.queryMore(currentResult.nextRecordsUrl);
        currentIterator = currentResult.records.iterator();
        return moveNext();
      }

      // No more records
      return false;

    } catch (IOException e) {
      throw new RuntimeException("Failed to query Salesforce", e);
    }
  }

  private Object[] convertRecord(Map<String, Object> record) {
    List<RelDataTypeField> fields = rowType.getFieldList();
    Object[] row = new Object[fields.size()];

    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      Object value = record.get(field.getName());

      // Handle nested objects (e.g., Owner.Name)
      if (value instanceof Map) {
        // For now, just use the Id of referenced objects
        Map<String, Object> nestedObject = (Map<String, Object>) value;
        value = nestedObject.get("Id");
      }

      // Convert value to expected type
      row[i] = convertValue(value, field.getType());
    }

    return row;
  }

  private Object convertValue(Object value, RelDataType type) {
    if (value == null) {
      return null;
    }

    // Salesforce returns most values as strings, so we need to convert
    String stringValue = value.toString();

    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return Boolean.parseBoolean(stringValue);

      case INTEGER:
        return Integer.parseInt(stringValue);

      case BIGINT:
        return Long.parseLong(stringValue);

      case DOUBLE:
      case FLOAT:
        return Double.parseDouble(stringValue);

      case DECIMAL:
        return new java.math.BigDecimal(stringValue);

      case DATE:
      case TIMESTAMP:
        // Parse Salesforce date/datetime format
        return parseDate(stringValue);

      default:
        // VARCHAR and others
        return stringValue;
    }
  }

  private Object parseDate(String dateString) {
    // Salesforce dates are in ISO format
    // For now, return as string - proper date parsing would use java.time
    return dateString;
  }

  @Override public void reset() {
    throw new UnsupportedOperationException("reset not supported");
  }

  @Override public void close() {
    // Nothing to close
  }
}
