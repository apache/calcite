/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.runtime;

import com.google.common.collect.ImmutableMap;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Metadata for a column.
 * (Compare with {@link java.sql.ResultSetMetaData}.)
 */
public class ColumnMetaData {
  public final int ordinal; // 0-based
  public final boolean autoIncrement;
  public final boolean caseSensitive;
  public final boolean searchable;
  public final boolean currency;
  public final int nullable;
  public final boolean signed;
  public final int displaySize;
  public final String label;
  public final String columnName;
  public final String schemaName;
  public final int precision;
  public final int scale;
  public final String tableName;
  public final String catalogName;
  public final int type;
  public final String typeName;
  public final boolean readOnly;
  public final boolean writable;
  public final boolean definitelyWritable;
  public final String columnClassName;

  /** The type of the field that holds the value. Not a JDBC property. */
  public final Rep representation;

  public ColumnMetaData(
      int ordinal,
      boolean autoIncrement,
      boolean caseSensitive,
      boolean searchable,
      boolean currency,
      int nullable,
      boolean signed,
      int displaySize,
      String label,
      String columnName,
      String schemaName,
      int precision,
      int scale,
      String tableName,
      String catalogName,
      int type,
      String typeName,
      boolean readOnly,
      boolean writable,
      boolean definitelyWritable,
      String columnClassName,
      Rep representation) {
    this.ordinal = ordinal;
    this.autoIncrement = autoIncrement;
    this.caseSensitive = caseSensitive;
    this.searchable = searchable;
    this.currency = currency;
    this.nullable = nullable;
    this.signed = signed;
    this.displaySize = displaySize;
    this.label = label;
    // Per the JDBC spec this should be just columnName.
    // For example, the query
    //     select 1 as x, c as y from t
    // should give columns
    //     (label=x, column=null, table=null)
    //     (label=y, column=c table=t)
    // But DbUnit requires every column to have a name. Duh.
    this.columnName = first(columnName, label);
    this.schemaName = schemaName;
    this.precision = precision;
    this.scale = scale;
    this.tableName = tableName;
    this.catalogName = catalogName;
    this.type = type;
    this.typeName = typeName;
    this.readOnly = readOnly;
    this.writable = writable;
    this.definitelyWritable = definitelyWritable;
    this.columnClassName = columnClassName;
    this.representation = representation;

    assert representation != null;
  }

  private static <T> T first(T t0, T t1) {
    return t0 != null ? t0 : t1;
  }

  /** Description of the type used to internally represent a value. For example,
   * a {@link java.sql.Date} might be represented as a {@link #PRIMITIVE_INT}
   * if not nullable, or a {@link #JAVA_SQL_DATE}. */
  public enum Rep {
    PRIMITIVE_BOOLEAN(boolean.class),
    PRIMITIVE_BYTE(byte.class),
    PRIMITIVE_CHAR(char.class),
    PRIMITIVE_SHORT(short.class),
    PRIMITIVE_INT(int.class),
    PRIMITIVE_LONG(long.class),
    PRIMITIVE_FLOAT(float.class),
    PRIMITIVE_DOUBLE(double.class),
    BOOLEAN(Boolean.class),
    BYTE(Byte.class),
    CHARACTER(Character.class),
    SHORT(Short.class),
    INTEGER(Integer.class),
    LONG(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    JAVA_SQL_TIME(Time.class),
    JAVA_SQL_TIMESTAMP(Timestamp.class),
    JAVA_SQL_DATE(java.sql.Date.class),
    JAVA_UTIL_DATE(java.util.Date.class),
    STRING(String.class),
    OBJECT(Object.class);

    private final Class clazz;

    public static final Map<Class, Rep> VALUE_MAP;

    static {
      ImmutableMap.Builder<Class, Rep> builder = ImmutableMap.builder();
      for (Rep rep : values()) {
        builder.put(rep.clazz, rep);
      }
      VALUE_MAP = builder.build();
    }

    Rep(Class clazz) {
      this.clazz = clazz;
    }
  }
}

// End ColumnMetaData.java
