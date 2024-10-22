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
package org.apache.calcite.adapter.cassandra;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

/** Enumerator that reads from a Cassandra column family. */
class CassandraEnumerator implements Enumerator<Object> {
  private final Iterator<Row> iterator;
  private final List<RelDataTypeField> fieldTypes;
  @Nullable private Row current;

  /** Creates a CassandraEnumerator.
   *
   * @param results Cassandra result set ({@link com.datastax.oss.driver.api.core.cql.ResultSet})
   * @param protoRowType The type of resulting rows
   */
  CassandraEnumerator(ResultSet results, RelProtoDataType protoRowType) {
    this.iterator = results.iterator();
    this.current = null;

    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();
  }

  /** Produces the next row from the results.
   *
   * @return A new row from the results
   */
  @Override public Object current() {
    if (fieldTypes.size() == 1) {
      // If we just have one field, produce it directly
      return currentRowField(0);
    } else {
      // Build an array with all fields in this row
      Object[] row = new Object[fieldTypes.size()];
      for (int i = 0; i < fieldTypes.size(); i++) {
        row[i] = currentRowField(i);
      }

      return row;
    }
  }

  /** Get a field for the current row from the underlying object.
   *
   * @param index Index of the field within the Row object
   */
  private @Nullable Object currentRowField(int index) {
    requireNonNull(current, "current");
    final Object o =
         current.get(index,
             CodecRegistry.DEFAULT.codecFor(
                 current.getColumnDefinitions().get(index).getType()));

    return convertToEnumeratorObject(o);
  }

  /** Convert an object into the expected internal representation.
   *
   * @param obj Object to convert, if needed
   */
  private @Nullable Object convertToEnumeratorObject(@Nullable Object obj) {
    if (obj instanceof ByteBuffer) {
      ByteBuffer buf = (ByteBuffer) obj;
      byte [] bytes = new byte[buf.remaining()];
      buf.get(bytes, 0, bytes.length);
      return new ByteString(bytes);
    } else if (obj instanceof LocalDate) {
      // converts dates to the expected numeric format
      return ((LocalDate) obj).toEpochDay();
    } else if (obj instanceof Date) {
      @SuppressWarnings("JdkObsolete")
      long milli = ((Date) obj).toInstant().toEpochMilli();
      return milli;
    } else if (obj instanceof Instant) {
      return ((Instant) obj).toEpochMilli();
    } else if (obj instanceof LocalTime) {
      return ((LocalTime) obj).toNanoOfDay();
    } else if (obj instanceof LinkedHashSet) {
      // MULTISET is handled as an array
      return ((LinkedHashSet<?>) obj).toArray();
    } else if (obj instanceof TupleValue) {
      // STRUCT can be handled as an array
      final TupleValue tupleValue = (TupleValue) obj;
      int numComponents = tupleValue.getType().getComponentTypes().size();
      return IntStream.range(0, numComponents)
          .mapToObj(i ->
              tupleValue.get(i,
                  CodecRegistry.DEFAULT.codecFor(
                      tupleValue.getType().getComponentTypes().get(i))))
          .map(this::convertToEnumeratorObject)
          .map(Objects::requireNonNull) // "null" cannot appear inside collections
          .toArray();
    }

    return obj;
  }

  @Override public boolean moveNext() {
    if (iterator.hasNext()) {
      current = iterator.next();
      return true;
    } else {
      return false;
    }
  }

  @Override public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override public void close() {
    // Nothing to do here
  }
}
