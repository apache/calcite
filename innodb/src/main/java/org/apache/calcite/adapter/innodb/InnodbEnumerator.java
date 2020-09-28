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
package org.apache.calcite.adapter.innodb;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.util.Utils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

/**
 * Enumerator that reads from InnoDB data file.
 */
class InnodbEnumerator implements Enumerator<Object> {
  private final Iterator<GenericRecord> iterator;
  private GenericRecord current;
  private final List<RelDataTypeField> fieldTypes;

  /**
   * Creates an InnodbEnumerator.
   *
   * @param resultIterator result iterator
   * @param protoRowType   the type of resulting rows
   */
  InnodbEnumerator(Iterator<GenericRecord> resultIterator, RelProtoDataType protoRowType) {
    this.iterator = resultIterator;
    this.current = null;

    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();
  }

  /**
   * Produce the next row from the results
   *
   * @return a new row from the results
   */
  public Object current() {
    if (fieldTypes.size() == 1) {
      // If we just have one field, produce it directly
      return currentRowField(fieldTypes.get(0));
    } else {
      // Build an array with all fields in this row
      Object[] row = new Object[fieldTypes.size()];
      for (int i = 0; i < fieldTypes.size(); i++) {
        row[i] = currentRowField(fieldTypes.get(i));
      }
      return row;
    }
  }

  /**
   * Get a field for the current row from the underlying object.
   */
  private Object currentRowField(RelDataTypeField relDataTypeField) {
    final Object o = current.get(relDataTypeField.getName());
    return convertToEnumeratorObject(o, relDataTypeField.getType());
  }

  /**
   * Convert an object into the expected internal representation.
   *
   * @param obj         object to convert, if needed
   * @param relDataType data type
   */
  private Object convertToEnumeratorObject(Object obj, RelDataType relDataType) {
    if (obj == null) {
      return null;
    }
    SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
    switch (sqlTypeName) {
    case BINARY:
    case VARBINARY:
      return new ByteString((byte[]) obj);
    case TIMESTAMP:
      Timestamp timestamp = Utils.convertDateTime((String) obj,
          relDataType.getPrecision());
      return shift(timestamp).getTime();
    case TIME:
      Time time = Utils.convertTime((String) obj,
          relDataType.getPrecision());
      return shift(time).getTime();
    case DATE:
      Date date = Date.valueOf(LocalDate.parse((String) obj));
      return DateTimeUtils.dateStringToUnixDate(date.toString());
    default:
      return obj;
    }
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

  private static Timestamp shift(Timestamp v) {
    if (v == null) {
      return null;
    }
    long time = v.getTime();
    int offset = TimeZone.getDefault().getOffset(time);
    return new Timestamp(time + offset);
  }

  private static Time shift(Time v) {
    if (v == null) {
      return null;
    }
    long time = v.getTime();
    int offset = TimeZone.getDefault().getOffset(time);
    return new Time((time + offset) % DateTimeUtils.MILLIS_PER_DAY);
  }
}
