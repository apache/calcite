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
package org.apache.calcite.avatica;

import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.util.ByteString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.protobuf.Descriptors.Descriptor;

import java.lang.reflect.Type;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata for a column.
 *
 * <p>(Compare with {@link java.sql.ResultSetMetaData}.)
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

  @JsonCreator
  public ColumnMetaData(
      @JsonProperty("ordinal") int ordinal,
      @JsonProperty("autoIncrement") boolean autoIncrement,
      @JsonProperty("caseSensitive") boolean caseSensitive,
      @JsonProperty("searchable") boolean searchable,
      @JsonProperty("currency") boolean currency,
      @JsonProperty("nullable") int nullable,
      @JsonProperty("signed") boolean signed,
      @JsonProperty("displaySize") int displaySize,
      @JsonProperty("label") String label,
      @JsonProperty("columnName") String columnName,
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("precision") int precision,
      @JsonProperty("scale") int scale,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("catalogName") String catalogName,
      @JsonProperty("type") AvaticaType type,
      @JsonProperty("readOnly") boolean readOnly,
      @JsonProperty("writable") boolean writable,
      @JsonProperty("definitelyWritable") boolean definitelyWritable,
      @JsonProperty("columnClassName") String columnClassName) {
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

  public Common.ColumnMetaData toProto() {
    Common.ColumnMetaData.Builder builder = Common.ColumnMetaData.newBuilder();

    // Primitive fields (can't be null)
    builder.setOrdinal(ordinal)
      .setAutoIncrement(autoIncrement)
      .setCaseSensitive(caseSensitive)
      .setSearchable(searchable)
      .setCurrency(currency)
      .setNullable(nullable)
      .setSigned(signed)
      .setDisplaySize(displaySize)
      .setPrecision(precision)
      .setScale(scale)
      .setReadOnly(readOnly)
      .setWritable(writable)
      .setDefinitelyWritable(definitelyWritable);

    // Potentially null fields
    if (null != label) {
      builder.setLabel(label);
    }

    if (null != columnName) {
      builder.setColumnName(columnName);
    }

    if (null != schemaName) {
      builder.setSchemaName(schemaName);
    }

    if (null != tableName) {
      builder.setTableName(tableName);
    }

    if (null != catalogName) {
      builder.setCatalogName(catalogName);
    }

    if (null != type) {
      builder.setType(type.toProto());
    }

    if (null != columnClassName) {
      builder.setColumnClassName(columnClassName);
    }

    return builder.build();
  }

  public static ColumnMetaData fromProto(Common.ColumnMetaData proto) {
    AvaticaType nestedType = AvaticaType.fromProto(proto.getType());
    final Descriptor desc = proto.getDescriptorForType();

    String catalogName = null;
    if (proto.hasField(desc.findFieldByNumber(Common.ColumnMetaData.CATALOG_NAME_FIELD_NUMBER))) {
      catalogName = proto.getCatalogName();
    }

    String schemaName = null;
    if (proto.hasField(desc.findFieldByNumber(Common.ColumnMetaData.SCHEMA_NAME_FIELD_NUMBER))) {
      schemaName = proto.getSchemaName();
    }

    String label = null;
    if (proto.hasField(desc.findFieldByNumber(Common.ColumnMetaData.LABEL_FIELD_NUMBER))) {
      label = proto.getLabel();
    }

    String columnName = null;
    if (proto.hasField(desc.findFieldByNumber(Common.ColumnMetaData.COLUMN_NAME_FIELD_NUMBER))) {
      columnName = proto.getColumnName();
    }

    String tableName = null;
    if (proto.hasField(desc.findFieldByNumber(Common.ColumnMetaData.TABLE_NAME_FIELD_NUMBER))) {
      tableName = proto.getTableName();
    }

    String columnClassName = null;
    if (proto.hasField(
        desc.findFieldByNumber(Common.ColumnMetaData.COLUMN_CLASS_NAME_FIELD_NUMBER))) {
      columnClassName = proto.getColumnClassName();
    }

    // Recreate the ColumnMetaData
    return new ColumnMetaData(proto.getOrdinal(), proto.getAutoIncrement(),
        proto.getCaseSensitive(), proto.getSearchable(), proto.getCurrency(), proto.getNullable(),
        proto.getSigned(), proto.getDisplaySize(), label, columnName,
        schemaName, proto.getPrecision(), proto.getScale(), tableName,
        catalogName, nestedType, proto.getReadOnly(), proto.getWritable(),
        proto.getDefinitelyWritable(), columnClassName);
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (autoIncrement ? 1231 : 1237);
    result = prime * result + (caseSensitive ? 1231 : 1237);
    result = prime * result + ((catalogName == null) ? 0 : catalogName.hashCode());
    result = prime * result + ((columnClassName == null) ? 0 : columnClassName.hashCode());
    result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
    result = prime * result + (currency ? 1231 : 1237);
    result = prime * result + (definitelyWritable ? 1231 : 1237);
    result = prime * result + displaySize;
    result = prime * result + ((label == null) ? 0 : label.hashCode());
    result = prime * result + nullable;
    result = prime * result + ordinal;
    result = prime * result + precision;
    result = prime * result + (readOnly ? 1231 : 1237);
    result = prime * result + scale;
    result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
    result = prime * result + (searchable ? 1231 : 1237);
    result = prime * result + (signed ? 1231 : 1237);
    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    result = prime * result + (writable ? 1231 : 1237);
    return result;
  }

  @Override public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof ColumnMetaData) {
      ColumnMetaData other = (ColumnMetaData) obj;

      if (autoIncrement != other.autoIncrement) {
        return false;
      }

      if (caseSensitive != other.caseSensitive) {
        return false;
      }

      if (null == catalogName) {
        if (null != other.catalogName) {
          return false;
        }
      } else if (!catalogName.equals(other.catalogName)) {
        return false;
      }

      if (null == columnClassName) {
        if (null != other.columnClassName) {
          return false;
        }
      } else if (!columnClassName.equals(other.columnClassName)) {
        return false;
      }

      if (null == columnName) {
        if (null != other.columnName) {
          return false;
        }
      } else if (!columnName.equals(other.columnName)) {
        return false;
      }

      if (currency != other.currency) {
        return false;
      }

      if (definitelyWritable != other.definitelyWritable) {
        return false;
      }

      if (displaySize != other.displaySize) {
        return false;
      }

      if (null == label) {
        if (null != other.label) {
          return false;
        }
      } else if (!label.equals(other.label)) {
        return false;
      }

      if (nullable != other.nullable) {
        return false;
      }

      if (ordinal != other.ordinal) {
        return false;
      }

      if (precision != other.precision) {
        return false;
      }

      if (readOnly != other.readOnly) {
        return false;
      }

      if (scale != other.scale) {
        return false;
      }

      if (null == schemaName) {
        if (null != other.schemaName) {
          return false;
        }
      } else if (!schemaName.equals(other.schemaName)) {
        return false;
      }

      if (searchable != other.searchable) {
        return false;
      }

      if (signed != other.signed) {
        return false;
      }

      if (null == tableName) {
        if (null != other.tableName) {
          return false;
        }
      } else if (!tableName.equals(other.tableName)) {
        return false;
      }

      if (null == type) {
        if (null != other.type) {
          return false;
        }
      } else if (!type.equals(other.type)) {
        return false;
      }

      if (writable != other.writable) {
        return false;
      }

      return true;
    }

    return false;
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
    return new StructType(columns);
  }

  /** Creates an {@link ArrayType}. */
  public static ArrayType array(AvaticaType componentType, String typeName,
      Rep rep) {
    return new ArrayType(Types.ARRAY, typeName, rep, componentType);
  }

  /** Creates a ColumnMetaData for result sets that are not based on a struct
   * but need to have a single 'field' for purposes of
   * {@link java.sql.ResultSetMetaData}. */
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
        null,
        type,
        true,
        false,
        false,
        type.columnClassName());
  }

  public ColumnMetaData setRep(Rep rep) {
    return new ColumnMetaData(ordinal, autoIncrement, caseSensitive, searchable,
        currency, nullable, signed, displaySize, label, columnName, schemaName,
        precision, scale, tableName, catalogName, type.setRep(rep), readOnly,
        writable, definitelyWritable, columnClassName);
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
    BYTE_STRING(ByteString.class),
    STRING(String.class),

    /** Values are represented as some sub-class of {@link Number}.
     * The JSON encoding does this. */
    NUMBER(Number.class),
    OBJECT(Object.class);

    public final Class clazz;

    public static final Map<Class, Rep> VALUE_MAP;

    static {
      Map<Class, Rep> builder = new HashMap<>();
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

    /** Returns the value of a column of this type from a result set. */
    public Object jdbcGet(ResultSet resultSet, int i) throws SQLException {
      switch (this) {
      case PRIMITIVE_BOOLEAN:
        return resultSet.getBoolean(i);
      case PRIMITIVE_BYTE:
        return resultSet.getByte(i);
      case PRIMITIVE_SHORT:
        return resultSet.getShort(i);
      case PRIMITIVE_INT:
        return resultSet.getInt(i);
      case PRIMITIVE_LONG:
        return resultSet.getLong(i);
      case PRIMITIVE_FLOAT:
        return resultSet.getFloat(i);
      case PRIMITIVE_DOUBLE:
        return resultSet.getDouble(i);
      case BOOLEAN:
        final boolean aBoolean = resultSet.getBoolean(i);
        return resultSet.wasNull() ? null : aBoolean;
      case BYTE:
        final byte aByte = resultSet.getByte(i);
        return resultSet.wasNull() ? null : aByte;
      case SHORT:
        final short aShort = resultSet.getShort(i);
        return resultSet.wasNull() ? null : aShort;
      case INTEGER:
        final int anInt = resultSet.getInt(i);
        return resultSet.wasNull() ? null : anInt;
      case LONG:
        final long aLong = resultSet.getLong(i);
        return resultSet.wasNull() ? null : aLong;
      case FLOAT:
        final float aFloat = resultSet.getFloat(i);
        return resultSet.wasNull() ? null : aFloat;
      case DOUBLE:
        final double aDouble = resultSet.getDouble(i);
        return resultSet.wasNull() ? null : aDouble;
      case JAVA_SQL_DATE:
        return resultSet.getDate(i);
      case JAVA_SQL_TIME:
        return resultSet.getTime(i);
      case JAVA_SQL_TIMESTAMP:
        return resultSet.getTimestamp(i);
      default:
        return resultSet.getObject(i);
      }
    }

    public Common.Rep toProto() {
      return Common.Rep.valueOf(name());
    }

    public static Rep fromProto(Common.Rep proto) {
      return Rep.valueOf(proto.name());
    }
  }

  /** Base class for a column type. */
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      property = "type",
      defaultImpl = ScalarType.class)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = ScalarType.class, name = "scalar"),
      @JsonSubTypes.Type(value = StructType.class, name = "struct"),
      @JsonSubTypes.Type(value = ArrayType.class, name = "array") })
  public static class AvaticaType {
    public final int id;
    public final String name;

    /** The type of the field that holds the value. Not a JDBC property. */
    public final Rep rep;

    public AvaticaType(int id, String name, Rep rep) {
      this.id = id;
      this.name = Objects.requireNonNull(name);
      this.rep = Objects.requireNonNull(rep);
    }

    public String columnClassName() {
      return SqlType.valueOf(id).boxedClass().getName();
    }

    public AvaticaType setRep(Rep rep) {
      throw new UnsupportedOperationException();
    }

    public Common.AvaticaType toProto() {
      Common.AvaticaType.Builder builder = Common.AvaticaType.newBuilder();

      builder.setName(name);
      builder.setId(id);
      builder.setRep(rep.toProto());

      return builder.build();
    }

    public static AvaticaType fromProto(Common.AvaticaType proto) {
      Common.Rep repProto = proto.getRep();
      Rep rep = Rep.valueOf(repProto.name());
      AvaticaType type;

      if (proto.hasComponent()) {
        // ArrayType
        // recurse on the type for the array elements
        AvaticaType nestedType = AvaticaType.fromProto(proto.getComponent());
        type = ColumnMetaData.array(nestedType, proto.getName(), rep);
      } else if (proto.getColumnsCount() > 0) {
        // StructType
        List<ColumnMetaData> columns = new ArrayList<>(proto.getColumnsCount());
        for (Common.ColumnMetaData protoColumn : proto.getColumnsList()) {
          columns.add(ColumnMetaData.fromProto(protoColumn));
        }
        type = ColumnMetaData.struct(columns);
      } else {
        // ScalarType
        type = ColumnMetaData.scalar(proto.getId(), proto.getName(), rep);
      }

      return type;
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + id;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((rep == null) ? 0 : rep.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof AvaticaType) {
        AvaticaType other = (AvaticaType) o;

        if (id != other.id) {
          return false;
        }

        if (name == null) {
          if (other.name != null) {
            return false;
          }
        } else if (!name.equals(other.name)) {
          return false;
        }

        if (rep != other.rep) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** Scalar type. */
  public static class ScalarType extends AvaticaType {
    @JsonCreator
    public ScalarType(@JsonProperty("id") int id,
        @JsonProperty("name") String name,
        @JsonProperty("rep") Rep rep) {
      super(id, name, rep);
    }

    @Override public AvaticaType setRep(Rep rep) {
      return new ScalarType(id, name, rep);
    }
  }

  /** Record type. */
  public static class StructType extends AvaticaType {
    public final List<ColumnMetaData> columns;

    @JsonCreator
    public StructType(List<ColumnMetaData> columns) {
      super(Types.STRUCT, "STRUCT", ColumnMetaData.Rep.OBJECT);
      this.columns = columns;
    }

    @Override public Common.AvaticaType toProto() {
      Common.AvaticaType.Builder builder = Common.AvaticaType.newBuilder(super.toProto());
      for (ColumnMetaData valueType : columns) {
        builder.addColumns(valueType.toProto());
      }
      return builder.build();
    }

    @Override public int hashCode() {
      return 31 * (super.hashCode() + (null == columns ? 0 : columns.hashCode()));
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!super.equals(o)) {
        return false;
      }

      if (o instanceof StructType) {
        StructType other = (StructType) o;

        if (null == columns) {
          if (null != other.columns) {
            return false;
          }
        }

        return columns.equals(other.columns);
      }

      return false;
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

    @Override public Common.AvaticaType toProto() {
      Common.AvaticaType.Builder builder = Common.AvaticaType.newBuilder(super.toProto());

      builder.setComponent(component.toProto());

      return builder.build();
    }

    @Override public int hashCode() {
      return 31 * (super.hashCode() + (null == component ? 0 : component.hashCode()));
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!super.equals(o)) {
        return false;
      }

      if (o instanceof ArrayType) {
        ArrayType other = (ArrayType) o;

        if (null == component) {
          if (null != other.component) {
            return false;
          }
        }

        return component.equals(other.component);
      }

      return false;
    }
  }
}

// End ColumnMetaData.java
