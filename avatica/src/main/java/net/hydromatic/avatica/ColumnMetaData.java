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
package net.hydromatic.avatica;

import java.lang.reflect.Type;
import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
  public final boolean readOnly;
  public final boolean writable;
  public final boolean definitelyWritable;
  public final String columnClassName;
  public final AvaticaType type;

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
      AvaticaType type,
      boolean readOnly,
      boolean writable,
      boolean definitelyWritable,
      String columnClassName) {
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
    this.readOnly = readOnly;
    this.writable = writable;
    this.definitelyWritable = definitelyWritable;
    this.columnClassName = columnClassName;
  }

  private static <T> T first(T t0, T t1) {
    return t0 != null ? t0 : t1;
  }

  /** Creates a {@link ScalarType}. */
  public static ScalarType scalar(int type, String typeName, Rep rep) {
    return new ScalarType(type, typeName, rep);
  }

  /** Creates a {@link StructType}. */
  public static StructType struct(List<ColumnMetaData> columns) {
    return new StructType(columns, "STRUCT", Types.STRUCT,
        ColumnMetaData.Rep.OBJECT);
  }

  /** Creates an {@link ArrayType}. */
  public static ArrayType array(AvaticaType componentType, String typeName,
      Rep rep) {
    return new ArrayType(Types.ARRAY, typeName, rep, componentType);
  }

  /** Creates a ColumnMetaData for result sets that are not based on a struct
   * but need to have a single 'field' for purposes of
   * {@link ResultSetMetaData}. */
  public static ColumnMetaData dummy(AvaticaType type, boolean nullable) {
    return new ColumnMetaData(
        0,
        false,
        true,
        false,
        false,
        nullable
            ? DatabaseMetaData.columnNullable
            : DatabaseMetaData.columnNoNulls,
        true,
        -1,
        null,
        null,
        null,
        -1,
        -1,
        null,
        null, type,
        true,
        false,
        false,
        null);
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
      Map<Class, Rep> builder = new HashMap<Class, Rep>();
      for (Rep rep : values()) {
        builder.put(rep.clazz, rep);
      }
      VALUE_MAP = Collections.unmodifiableMap(builder);
    }

    Rep(Class clazz) {
      this.clazz = clazz;
    }

    public static Rep of(Type clazz) {
      //noinspection SuspiciousMethodCalls
      final Rep rep = VALUE_MAP.get(clazz);
      return rep != null ? rep : OBJECT;
    }
  }

  /** Base class for a column type. */
  public static class AvaticaType {
    public final int type;
    public final String typeName;

    /** The type of the field that holds the value. Not a JDBC property. */
    public final Rep representation;

    protected AvaticaType(int type, String typeName, Rep representation) {
      this.type = type;
      this.typeName = typeName;
      this.representation = representation;
      assert representation != null;
    }
  }

  /** Scalar type. */
  public static class ScalarType extends AvaticaType {
    public ScalarType(int type, String typeName, Rep representation) {
      super(type, typeName, representation);
    }
  }

  /** Record type. */
  public static class StructType extends AvaticaType {
    public final List<ColumnMetaData> columns;

    private StructType(List<ColumnMetaData> columns, String typeName, int type,
        Rep representation) {
      super(type, typeName, representation);
      this.columns = columns;
    }
  }

  /** Array type. */
  public static class ArrayType extends AvaticaType {
    public final AvaticaType component;

    private ArrayType(int type, String typeName, Rep representation,
        AvaticaType component) {
      super(type, typeName, representation);
      this.component = component;
    }
  }
}

// End ColumnMetaData.java
