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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;

import org.apache.commons.lang3.time.FastDateFormat;

import au.com.bytecode.opencsv.CSVReader;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * Enumerator that reads from a CSV file.
 * 从 CSV 文件读取数据的枚举器。
 *
 * @param <E> Row type 行类型
 */
public class CsvEnumerator<E> implements Enumerator<E> {
  private final CSVReader reader;
  private final List<String> filterValues;
  private final AtomicBoolean cancelFlag;
  private final RowConverter<E> rowConverter;
  private E current;

  private static final FastDateFormat TIME_FORMAT_DATE;
  private static final FastDateFormat TIME_FORMAT_TIME;
  private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

  static {
    final TimeZone gmt = TimeZone.getTimeZone("GMT");
    TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
    TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
    TIME_FORMAT_TIMESTAMP =
        FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
  }

  public CsvEnumerator(Source source, AtomicBoolean cancelFlag,
      List<CsvFieldType> fieldTypes, List<Integer> fields) {
    //noinspection unchecked
    this(source, cancelFlag, false, null,
        (RowConverter<E>) converter(fieldTypes, fields));
  }

  public CsvEnumerator(Source source, AtomicBoolean cancelFlag, boolean stream,
      String[] filterValues, RowConverter<E> rowConverter) {
    this.cancelFlag = cancelFlag;
    this.rowConverter = rowConverter;
    this.filterValues = filterValues == null ? null
        : ImmutableNullableList.copyOf(filterValues);
    try {
      if (stream) {
        this.reader = new CsvStreamReader(source);
      } else {
        this.reader = openCsv(source);
      }
      this.reader.readNext(); // skip header row
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static RowConverter<?> converter(List<CsvFieldType> fieldTypes,
      List<Integer> fields) {
    if (fields.size() == 1) {
      final int field = fields.get(0);
      return new SingleColumnRowConverter(fieldTypes.get(field), field);
    } else {
      return arrayConverter(fieldTypes, fields, false);
    }
  }

  /**
   * @param fieldTypes
   * @param fields
   * @param stream
   * @return
   */
  public static RowConverter<Object[]> arrayConverter(List<CsvFieldType> fieldTypes, List<Integer> fields, boolean stream) {
    return new ArrayRowConverter(fieldTypes, fields, stream);
  }

  /** Deduces the names and types of a table's columns by reading the first line
   * of a CSV file. */
  static RelDataType deduceRowType(JavaTypeFactory typeFactory, Source source,
      List<CsvFieldType> fieldTypes) {
    return deduceRowType(typeFactory, source, fieldTypes, false);
  }

  /**
   * Deduces(推断) the names and types of a table's columns by reading the first line of a CSV file.
   * 通过读取csv文件第一行，推断 schema 的信息。
   *
   * @param typeFactory
   * @param source
   * @param fieldTypes 存放结果
   * @param stream
   * @return
   */
  public static RelDataType deduceRowType(JavaTypeFactory typeFactory, Source source, List<CsvFieldType> fieldTypes, Boolean stream) {
    final List<RelDataType> types = new ArrayList<>();
    final List<String> names = new ArrayList<>();
    if (stream) {
      names.add(FileSchemaFactory.ROWTIME_COLUMN_NAME);
      types.add(typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
    }
    try (CSVReader reader = openCsv(source)) {
      String[] strings = reader.readNext();
      if (strings == null) {
        strings = new String[]{"EmptyFileHasNoColumns:boolean"};
      }
      for (String string : strings) {
        final String name;
        final CsvFieldType fieldType;
        final int colon = string.indexOf(':');
        if (colon >= 0) {
          name = string.substring(0, colon);
          String typeString = string.substring(colon + 1);
          fieldType = CsvFieldType.of(typeString);
          if (fieldType == null) {
            System.out.println("WARNING: Found unknown type: "
                + typeString + " in file: " + source.path()
                + " for column: " + name
                + ". Will assume the type of column is string");
          }
        } else {
          name = string;
          fieldType = null;
        }
        final RelDataType type;
        if (fieldType == null) {
          type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        } else {
          type = fieldType.toType(typeFactory);
        }
        names.add(name);
        types.add(type);
        if (fieldTypes != null) {
          fieldTypes.add(fieldType);
        }
      }
    } catch (IOException e) {
      // ignore
    }
    if (names.isEmpty()) {
      names.add("line");
      types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  static CSVReader openCsv(Source source) throws IOException {
    Objects.requireNonNull(source, "source");
    return new CSVReader(source.reader());
  }

  @Override public E current() {
    return current;
  }

  @Override public boolean moveNext() {
    try {
    outer:
      for (;;) {
        if (cancelFlag.get()) {
          return false;
        }
        final String[] strings = reader.readNext();
        if (strings == null) {
          if (reader instanceof CsvStreamReader) {
            try {
              Thread.sleep(CsvStreamReader.DEFAULT_MONITOR_DELAY);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            continue;
          }
          current = null;
          reader.close();
          return false;
        }
        if (filterValues != null) {
          for (int i = 0; i < strings.length; i++) {
            String filterValue = filterValues.get(i);
            if (filterValue != null) {
              if (!filterValue.equals(strings[i])) {
                continue outer;
              }
            }
          }
        }
        current = rowConverter.convertRow(strings);
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing CSV reader", e);
    }
  }

  // todo 改成 IntStream
  /** Returns an array of integers {0, ..., n - 1}. */
  public static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  /** Row converter.
   *
   * @param <E> element type */
  abstract static class RowConverter<E> {
    abstract E convertRow(String[] rows);

    @SuppressWarnings("JdkObsolete")
    protected Object convert(CsvFieldType fieldType, String string) {
      if (fieldType == null) {
        return string;
      }
      switch (fieldType) {
      case BOOLEAN:
        if (string.length() == 0) {
          return null;
        }
        return Boolean.parseBoolean(string);
      case BYTE:
        if (string.length() == 0) {
          return null;
        }
        return Byte.parseByte(string);
      case SHORT:
        if (string.length() == 0) {
          return null;
        }
        return Short.parseShort(string);
      case INT:
        if (string.length() == 0) {
          return null;
        }
        return Integer.parseInt(string);
      case LONG:
        if (string.length() == 0) {
          return null;
        }
        return Long.parseLong(string);
      case FLOAT:
        if (string.length() == 0) {
          return null;
        }
        return Float.parseFloat(string);
      case DOUBLE:
        if (string.length() == 0) {
          return null;
        }
        return Double.parseDouble(string);
      case DATE:
        if (string.length() == 0) {
          return null;
        }
        try {
          Date date = TIME_FORMAT_DATE.parse(string);
          return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
        } catch (ParseException e) {
          return null;
        }
      case TIME:
        if (string.length() == 0) {
          return null;
        }
        try {
          Date date = TIME_FORMAT_TIME.parse(string);
          return (int) date.getTime();
        } catch (ParseException e) {
          return null;
        }
      case TIMESTAMP:
        if (string.length() == 0) {
          return null;
        }
        try {
          Date date = TIME_FORMAT_TIMESTAMP.parse(string);
          return date.getTime();
        } catch (ParseException e) {
          return null;
        }
      case STRING:
      default:
        return string;
      }
    }
  }

  /** Array row converter. */
  static class ArrayRowConverter extends RowConverter<Object[]> {
    /** Field types. List must not be null, but any element may be null. */
    private final List<CsvFieldType> fieldTypes;
    private final ImmutableIntList fields;
    /** Whether the row to convert is from a stream. */
    private final boolean stream;

    ArrayRowConverter(List<CsvFieldType> fieldTypes, List<Integer> fields,
        boolean stream) {
      this.fieldTypes = ImmutableNullableList.copyOf(fieldTypes);
      this.fields = ImmutableIntList.copyOf(fields);
      this.stream = stream;
    }

    @Override public Object[] convertRow(String[] strings) {
      if (stream) {
        return convertStreamRow(strings);
      } else {
        return convertNormalRow(strings);
      }
    }

    public Object[] convertNormalRow(String[] strings) {
      final Object[] objects = new Object[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        int field = fields.get(i);
        objects[i] = convert(fieldTypes.get(field), strings[field]);
      }
      return objects;
    }

    public Object[] convertStreamRow(String[] strings) {
      final Object[] objects = new Object[fields.size() + 1];
      objects[0] = System.currentTimeMillis();
      for (int i = 0; i < fields.size(); i++) {
        int field = fields.get(i);
        objects[i + 1] = convert(fieldTypes.get(field), strings[field]);
      }
      return objects;
    }
  }

  /** Single column row converter. */
  private static class SingleColumnRowConverter extends RowConverter<Object> {
    private final CsvFieldType fieldType;
    private final int fieldIndex;

    private SingleColumnRowConverter(CsvFieldType fieldType, int fieldIndex) {
      this.fieldType = fieldType;
      this.fieldIndex = fieldIndex;
    }

    @Override public Object convertRow(String[] strings) {
      return convert(fieldType, strings[fieldIndex]);
    }
  }
}
