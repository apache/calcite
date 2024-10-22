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
import org.apache.calcite.util.trace.CalciteLogger;

import org.apache.commons.lang3.time.FastDateFormat;

import au.com.bytecode.opencsv.CSVReader;

import com.google.common.annotations.VisibleForTesting;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Byte.parseByte;
import static java.lang.Double.parseDouble;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Short.parseShort;
import static java.util.Objects.requireNonNull;

/** Enumerator that reads from a CSV file.
 *
 * @param <E> Row type
 */
public class CsvEnumerator<E> implements Enumerator<E> {
  private static final CalciteLogger LOGGER =
      new CalciteLogger(LoggerFactory.getLogger(CsvEnumerator.class));

  private final CSVReader reader;
  private final @Nullable List<@Nullable String> filterValues;
  private final AtomicBoolean cancelFlag;
  private final RowConverter<E> rowConverter;
  private @Nullable E current;

  private static final FastDateFormat TIME_FORMAT_DATE;
  private static final FastDateFormat TIME_FORMAT_TIME;
  private static final FastDateFormat TIME_FORMAT_TIMESTAMP;
  private static final Pattern DECIMAL_TYPE_PATTERN = Pattern
      .compile("\"decimal\\(([0-9]+),([0-9]+)\\)");

  static {
    final TimeZone gmt = TimeZone.getTimeZone("GMT");
    TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
    TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
    TIME_FORMAT_TIMESTAMP =
        FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
  }

  public CsvEnumerator(Source source, AtomicBoolean cancelFlag,
      List<RelDataType> fieldTypes, List<Integer> fields) {
    //noinspection unchecked
    this(source, cancelFlag, false, null,
        (RowConverter<E>) converter(fieldTypes, fields));
  }

  public CsvEnumerator(Source source, AtomicBoolean cancelFlag, boolean stream,
      @Nullable String @Nullable [] filterValues, RowConverter<E> rowConverter) {
    this.cancelFlag = cancelFlag;
    this.rowConverter = rowConverter;
    this.filterValues =
        filterValues == null ? null
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

  private static RowConverter<?> converter(List<RelDataType> fieldTypes,
      List<Integer> fields) {
    if (fields.size() == 1) {
      final int field = fields.get(0);
      return new SingleColumnRowConverter(fieldTypes.get(field), field);
    } else {
      return arrayConverter(fieldTypes, fields, false);
    }
  }

  public static RowConverter<@Nullable Object[]> arrayConverter(
      List<RelDataType> fieldTypes, List<Integer> fields, boolean stream) {
    return new ArrayRowConverter(fieldTypes, fields, stream);
  }

  /** Deduces the names and types of a table's columns by reading the first line
   * of a CSV file. */
  public static RelDataType deduceRowType(JavaTypeFactory typeFactory,
      Source source, @Nullable List<RelDataType> fieldTypes, Boolean stream) {
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
        final RelDataType fieldType;
        final int colon = string.indexOf(':');
        if (colon >= 0) {
          name = string.substring(0, colon);
          String typeString = string.substring(colon + 1);
          Matcher decimalMatcher = DECIMAL_TYPE_PATTERN.matcher(typeString);
          if (decimalMatcher.matches()) {
            int precision = parseInt(decimalMatcher.group(1));
            int scale = parseInt(decimalMatcher.group(2));
            fieldType = parseDecimalSqlType(typeFactory, precision, scale);
          } else {
            switch (typeString) {
            case "string":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
              break;
            case "boolean":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.BOOLEAN);
              break;
            case "byte":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.TINYINT);
              break;
            case "char":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.CHAR);
              break;
            case "short":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.SMALLINT);
              break;
            case "int":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.INTEGER);
              break;
            case "long":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.BIGINT);
              break;
            case "float":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.REAL);
              break;
            case "double":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.DOUBLE);
              break;
            case "date":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.DATE);
              break;
            case "timestamp":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.TIMESTAMP);
              break;
            case "time":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.TIME);
              break;
            default:
              LOGGER.warn(
                  "Found unknown type: {} in file: {} for column: {}. Will assume the type of "
                      + "column is string.",
                  typeString, source.path(), name);
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
              break;
            }
          }
        } else {
          name = string;
          fieldType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        }
        names.add(name);
        types.add(fieldType);
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
    requireNonNull(source, "source");
    return new CSVReader(source.reader());
  }

  @Override public E current() {
    return castNonNull(current);
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

  /** Returns an array of integers {0, ..., n - 1}. */
  public static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  private static RelDataType toNullableRelDataType(JavaTypeFactory typeFactory,
      SqlTypeName sqlTypeName) {
    return typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlTypeName), true);
  }

  /** Row converter.
   *
   * @param <E> element type */
  abstract static class RowConverter<E> {
    abstract E convertRow(@Nullable String[] rows);

    @SuppressWarnings("JavaUtilDate")
    protected @Nullable Object convert(@Nullable RelDataType fieldType, @Nullable String string) {
      if (fieldType == null || string == null) {
        return string;
      }
      switch (fieldType.getSqlTypeName()) {
      case BOOLEAN:
        if (string.length() == 0) {
          return null;
        }
        return parseBoolean(string);
      case TINYINT:
        if (string.length() == 0) {
          return null;
        }
        return parseByte(string);
      case SMALLINT:
        if (string.length() == 0) {
          return null;
        }
        return parseShort(string);
      case INTEGER:
        if (string.length() == 0) {
          return null;
        }
        return parseInt(string);
      case BIGINT:
        if (string.length() == 0) {
          return null;
        }
        return parseLong(string);
      case REAL:
        if (string.length() == 0) {
          return null;
        }
        return parseFloat(string);
      case FLOAT:
      case DOUBLE:
        if (string.length() == 0) {
          return null;
        }
        return parseDouble(string);
      case DECIMAL:
        if (string.length() == 0) {
          return null;
        }
        return parseDecimal(fieldType.getPrecision(), fieldType.getScale(), string);
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
      case VARCHAR:
      default:
        return string;
      }
    }
  }

  private static RelDataType parseDecimalSqlType(JavaTypeFactory typeFactory, int precision,
      int scale) {
    checkArgument(precision > 0, "DECIMAL type must have precision > 0. Found %s", precision);
    checkArgument(scale >= 0, "DECIMAL type must have scale >= 0. Found %s", scale);
    checkArgument(precision >= scale,
        "DECIMAL type must have precision >= scale. Found precision (%s) and scale (%s).",
        precision, scale);
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale), true);
  }

  @VisibleForTesting
  protected static BigDecimal parseDecimal(int precision, int scale, String string) {
    BigDecimal result = new BigDecimal(string);
    // If the parsed value has more fractional digits than the specified scale, round ties away
    // from 0.
    if (result.scale() > scale) {
      LOGGER.warn(
          "Decimal value {} exceeds declared scale ({}). Performing rounding to keep the "
              + "first {} fractional digits.",
          result, scale, scale);
      result = result.setScale(scale, RoundingMode.HALF_UP);
    }
    // Throws an exception if the parsed value has more digits to the left of the decimal point
    // than the specified value.
    if (result.precision() - result.scale() > precision - scale) {
      throw new IllegalArgumentException(String
          .format(Locale.ROOT, "Decimal value %s exceeds declared precision (%d) and scale (%d).",
              result, precision, scale));
    }
    return result;
  }

  /** Array row converter. */
  static class ArrayRowConverter extends RowConverter<@Nullable Object[]> {

    /** Field types. List must not be null, but any element may be null. */
    private final List<RelDataType> fieldTypes;
    private final ImmutableIntList fields;
    /** Whether the row to convert is from a stream. */
    private final boolean stream;

    ArrayRowConverter(List<RelDataType> fieldTypes, List<Integer> fields,
        boolean stream) {
      this.fieldTypes = ImmutableNullableList.copyOf(fieldTypes);
      this.fields = ImmutableIntList.copyOf(fields);
      this.stream = stream;
    }

    @Override public @Nullable Object[] convertRow(@Nullable String[] strings) {
      if (stream) {
        return convertStreamRow(strings);
      } else {
        return convertNormalRow(strings);
      }
    }

    public @Nullable Object[] convertNormalRow(@Nullable String[] strings) {
      final @Nullable Object[] objects = new Object[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        int field = fields.get(i);
        objects[i] = convert(fieldTypes.get(field), strings[field]);
      }
      return objects;
    }

    public @Nullable Object[] convertStreamRow(@Nullable String[] strings) {
      final @Nullable Object[] objects = new Object[fields.size() + 1];
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
    private final RelDataType fieldType;
    private final int fieldIndex;

    private SingleColumnRowConverter(RelDataType fieldType, int fieldIndex) {
      this.fieldType = fieldType;
      this.fieldIndex = fieldIndex;
    }

    @Override public @Nullable Object convertRow(@Nullable String[] strings) {
      return convert(fieldType, strings[fieldIndex]);
    }
  }
}
