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
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.IntStream;

/** Enumerator that reads from a Cassandra column family. */
class CassandraEnumerator implements Enumerator<Object> {
  private Iterator<Row> iterator;
  private Row current;
  private List<RelDataTypeField> fieldTypes;

  /** Creates a CassandraEnumerator.
   *
   * @param results Cassandra result set ({@link com.datastax.driver.core.ResultSet})
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
  public Object current() {
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
  private Object currentRowField(int index) {
    final Object o =  current.get(index,
        CassandraSchema.CODEC_REGISTRY.codecFor(
            current.getColumnDefinitions().getType(index)));

    return convertToEnumeratorObject(o);
  }

  /** Convert an object into the expected internal representation.
   *
   * @param obj Object to convert, if needed
   */
  private Object convertToEnumeratorObject(Object obj) {
    if (obj instanceof ByteBuffer) {
      ByteBuffer buf = (ByteBuffer) obj;
      byte [] bytes = new byte[buf.remaining()];
      buf.get(bytes, 0, bytes.length);
      return new ByteString(bytes);
    } else if (obj instanceof LocalDate) {
      // converts dates to the expected numeric format
      return ((LocalDate) obj).getMillisSinceEpoch()
          / DateTimeUtils.MILLIS_PER_DAY;
    } else if (obj instanceof Date) {
      @SuppressWarnings("JdkObsolete")
      long milli = ((Date) obj).toInstant().toEpochMilli();
      return milli;
    } else if (obj instanceof LinkedHashSet) {
      // MULTISET is handled as an array
      return ((LinkedHashSet) obj).toArray();
    } else if (obj instanceof TupleValue) {
      // STRUCT can be handled as an array
      final TupleValue tupleValue = (TupleValue) obj;
      int numComponents = tupleValue.getType().getComponentTypes().size();
      return IntStream.range(0, numComponents)
          .mapToObj(i ->
              tupleValue.get(i,
                  CassandraSchema.CODEC_REGISTRY.codecFor(
                      tupleValue.getType().getComponentTypes().get(i)))
          ).map(this::convertToEnumeratorObject)
          .toArray();
    }

    return obj;
  }

  public boolean moveNext() {
    if (iterator.hasNext()) {
      current = iterator.next();
      return true;
    } else {
      return false;
    }
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  public void close() {
    // Nothing to do here
  }
}
