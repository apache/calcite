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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.linq4j.Enumerator;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simple Iceberg enumerator that reads records directly using Iceberg's data API.
 * This provides snapshot-aware reading for the MVP implementation.
 */
public class IcebergEnumerator implements Enumerator<Object[]> {
  private final CloseableIterable<Record> records;
  private final Iterator<Record> iterator;
  private final Schema schema;
  private final Schema projectedSchema;
  private final AtomicBoolean cancelFlag;
  private final @Nullable int[] projectedColumns;
  private @Nullable Object[] current;

  public IcebergEnumerator(Table icebergTable, 
                          @Nullable Long snapshotId, 
                          @Nullable String asOfTimestamp,
                          AtomicBoolean cancelFlag) {
    this(icebergTable, snapshotId, asOfTimestamp, cancelFlag, null);
  }
  
  public IcebergEnumerator(Table icebergTable, 
                          @Nullable Long snapshotId, 
                          @Nullable String asOfTimestamp,
                          AtomicBoolean cancelFlag,
                          @Nullable int[] projectedColumns) {
    this(icebergTable, snapshotId, asOfTimestamp, cancelFlag, projectedColumns, null);
  }
  
  public IcebergEnumerator(Table icebergTable, 
                          @Nullable Long snapshotId, 
                          @Nullable String asOfTimestamp,
                          AtomicBoolean cancelFlag,
                          @Nullable int[] projectedColumns,
                          @Nullable Expression filter) {
    this.schema = icebergTable.schema();
    this.projectedColumns = projectedColumns;
    this.cancelFlag = cancelFlag;
    
    // Build projected schema if column projection is specified
    if (projectedColumns != null && projectedColumns.length > 0) {
      List<Types.NestedField> projectedFields = new ArrayList<>();
      List<Types.NestedField> allFields = schema.columns();
      
      for (int columnIndex : projectedColumns) {
        if (columnIndex >= 0 && columnIndex < allFields.size()) {
          projectedFields.add(allFields.get(columnIndex));
        }
      }
      this.projectedSchema = new Schema(projectedFields);
    } else {
      this.projectedSchema = schema;
    }
    
    // Build the reader with snapshot filtering, column projection, and filters
    IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(icebergTable);
    
    // Apply column projection if specified
    if (projectedColumns != null && projectedColumns.length > 0) {
      scanBuilder = scanBuilder.select(getProjectedColumnNames());
    }
    
    // Apply filter for partition pruning and row filtering
    if (filter != null) {
      scanBuilder = scanBuilder.where(filter);
    }
    
    if (snapshotId != null) {
      // Use specific snapshot
      scanBuilder = scanBuilder.useSnapshot(snapshotId);
    } else if (asOfTimestamp != null) {
      // Use snapshot at specific time
      try {
        long timestampMillis = Instant.parse(asOfTimestamp).toEpochMilli();
        scanBuilder = scanBuilder.asOfTime(timestampMillis);
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid timestamp format: " + asOfTimestamp 
            + ". Expected ISO-8601 format like '2024-01-01T00:00:00Z'", e);
      }
    }
    // If neither specified, uses current snapshot (default)
    
    this.records = scanBuilder.build();
    this.iterator = records.iterator();
  }

  @Override
  public Object[] current() {
    if (current == null) {
      throw new IllegalStateException("No current record");
    }
    return current;
  }

  @Override
  public boolean moveNext() {
    if (cancelFlag.get()) {
      return false;
    }
    
    if (iterator.hasNext()) {
      Record record = iterator.next();
      current = convertRecord(record);
      return true;
    }
    
    current = null;
    return false;
  }

  private String[] getProjectedColumnNames() {
    if (projectedColumns == null || projectedColumns.length == 0) {
      return null;
    }
    
    List<Types.NestedField> allFields = schema.columns();
    String[] columnNames = new String[projectedColumns.length];
    
    for (int i = 0; i < projectedColumns.length; i++) {
      int columnIndex = projectedColumns[i];
      if (columnIndex >= 0 && columnIndex < allFields.size()) {
        columnNames[i] = allFields.get(columnIndex).name();
      }
    }
    
    return columnNames;
  }

  private Object[] convertRecord(Record record) {
    // Use projected schema for field iteration
    List<Types.NestedField> fields = projectedSchema.columns();
    Object[] row = new Object[fields.size()];
    
    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField field = fields.get(i);
      Object value = record.getField(field.name());
      row[i] = convertValue(value);
    }
    
    return row;
  }

  private @Nullable Object convertValue(@Nullable Object value) {
    if (value == null) {
      return null;
    }
    
    // For MVP, do basic conversions
    // TODO: Add more sophisticated type conversions as needed
    return value;
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("Reset not supported on IcebergEnumerator");
  }

  @Override
  public void close() {
    try {
      records.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to close Iceberg records", e);
    }
  }
}