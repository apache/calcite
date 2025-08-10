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
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Enumerator that reads records from an Iceberg table.
 */
public class IcebergEnumerator implements Enumerator<Object[]> {
  private final CloseableIterable<Record> records;
  private final Iterator<Record> iterator;
  private final Schema schema;
  private final AtomicBoolean cancelFlag;
  private @Nullable Object[] current;

  public IcebergEnumerator(CloseableIterable<Record> records, 
                          Schema schema,
                          AtomicBoolean cancelFlag) {
    this.records = records;
    this.iterator = records.iterator();
    this.schema = schema;
    this.cancelFlag = cancelFlag;
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

  private Object[] convertRecord(Record record) {
    List<Types.NestedField> fields = schema.columns();
    Object[] row = new Object[fields.size()];
    
    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField field = fields.get(i);
      Object value = record.getField(field.name());
      row[i] = convertValue(value, field.type());
    }
    
    return row;
  }

  private @Nullable Object convertValue(@Nullable Object value, org.apache.iceberg.types.Type type) {
    if (value == null) {
      return null;
    }
    
    switch (type.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
        return value;
        
      case DATE:
        if (value instanceof LocalDate) {
          return Date.valueOf((LocalDate) value);
        } else if (value instanceof Integer) {
          // Days since epoch
          LocalDate date = LocalDate.ofEpochDay((Integer) value);
          return Date.valueOf(date);
        }
        return value;
        
      case TIMESTAMP:
        if (value instanceof LocalDateTime) {
          return Timestamp.valueOf((LocalDateTime) value);
        } else if (value instanceof OffsetDateTime) {
          return Timestamp.from(((OffsetDateTime) value).toInstant());
        } else if (value instanceof Long) {
          // Microseconds since epoch
          return new Timestamp(((Long) value) / 1000);
        }
        return value;
        
      case DECIMAL:
        if (value instanceof BigDecimal) {
          return value;
        } else if (value instanceof ByteBuffer) {
          // Convert ByteBuffer to BigDecimal
          Types.DecimalType decimalType = (Types.DecimalType) type;
          return convertBytesToDecimal((ByteBuffer) value, 
              decimalType.precision(), decimalType.scale());
        }
        return value;
        
      case BINARY:
      case FIXED:
        if (value instanceof ByteBuffer) {
          ByteBuffer buffer = (ByteBuffer) value;
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          return bytes;
        }
        return value;
        
      case UUID:
        if (value instanceof UUID) {
          return value.toString();
        }
        return value;
        
      case STRUCT:
        if (value instanceof Record) {
          Types.StructType structType = (Types.StructType) type;
          Record structRecord = (Record) value;
          Object[] structValues = new Object[structType.fields().size()];
          for (int i = 0; i < structType.fields().size(); i++) {
            Types.NestedField field = structType.fields().get(i);
            structValues[i] = convertValue(
                structRecord.getField(field.name()), field.type());
          }
          return structValues;
        }
        return value;
        
      case LIST:
        if (value instanceof List) {
          Types.ListType listType = (Types.ListType) type;
          List<?> list = (List<?>) value;
          Object[] array = new Object[list.size()];
          for (int i = 0; i < list.size(); i++) {
            array[i] = convertValue(list.get(i), listType.elementType());
          }
          return array;
        }
        return value;
        
      case MAP:
        if (value instanceof Map) {
          // Return as-is, Calcite will handle Map types
          return value;
        }
        return value;
        
      default:
        return value;
    }
  }

  private BigDecimal convertBytesToDecimal(ByteBuffer bytes, int precision, int scale) {
    // Convert ByteBuffer to BigDecimal
    byte[] byteArray = new byte[bytes.remaining()];
    bytes.get(byteArray);
    BigDecimal unscaled = new BigDecimal(new java.math.BigInteger(byteArray));
    return unscaled.movePointLeft(scale);
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("Reset not supported");
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