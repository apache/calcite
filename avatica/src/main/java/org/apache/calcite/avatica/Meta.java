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
import org.apache.calcite.avatica.remote.ProtobufService;
import org.apache.calcite.avatica.remote.TypedValue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Command handler for getting various metadata. Should be implemented by each
 * driver.
 *
 * <p>Also holds other abstract methods that are not related to metadata
 * that each provider must implement. This is not ideal.</p>
 */
public interface Meta {

  /**
   * Returns a map of static database properties.
   *
   * <p>The provider can omit properties whose value is the same as the
   * default.
   */
  Map<DatabaseProperty, Object> getDatabaseProperties();

  /** Per {@link DatabaseMetaData#getTables(String, String, String, String[])}. */
  MetaResultSet getTables(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      List<String> typeList);

  /** Per {@link DatabaseMetaData#getColumns(String, String, String, String)}. */
  MetaResultSet getColumns(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern);

  MetaResultSet getSchemas(String catalog, Pat schemaPattern);

  /** Per {@link DatabaseMetaData#getCatalogs()}. */
  MetaResultSet getCatalogs();

  /** Per {@link DatabaseMetaData#getTableTypes()}. */
  MetaResultSet getTableTypes();

  /** Per {@link DatabaseMetaData#getProcedures(String, String, String)}. */
  MetaResultSet getProcedures(String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern);

  /** Per {@link DatabaseMetaData#getProcedureColumns(String, String, String, String)}. */
  MetaResultSet getProcedureColumns(String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern,
      Pat columnNamePattern);

  /** Per {@link DatabaseMetaData#getColumnPrivileges(String, String, String, String)}. */
  MetaResultSet getColumnPrivileges(String catalog,
      String schema,
      String table,
      Pat columnNamePattern);

  /** Per {@link DatabaseMetaData#getTablePrivileges(String, String, String)}. */
  MetaResultSet getTablePrivileges(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern);

  /** Per
   * {@link DatabaseMetaData#getBestRowIdentifier(String, String, String, int, boolean)}. */
  MetaResultSet getBestRowIdentifier(String catalog,
      String schema,
      String table,
      int scope,
      boolean nullable);

  /** Per {@link DatabaseMetaData#getVersionColumns(String, String, String)}. */
  MetaResultSet getVersionColumns(String catalog, String schema, String table);

  /** Per {@link DatabaseMetaData#getPrimaryKeys(String, String, String)}. */
  MetaResultSet getPrimaryKeys(String catalog, String schema, String table);

  /** Per {@link DatabaseMetaData#getImportedKeys(String, String, String)}. */
  MetaResultSet getImportedKeys(String catalog, String schema, String table);

  /** Per {@link DatabaseMetaData#getExportedKeys(String, String, String)}. */
  MetaResultSet getExportedKeys(String catalog, String schema, String table);

  /** Per
   * {@link DatabaseMetaData#getCrossReference(String, String, String, String, String, String)}. */
  MetaResultSet getCrossReference(String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable);

  /** Per {@link DatabaseMetaData#getTypeInfo()}. */
  MetaResultSet getTypeInfo();

  /** Per {@link DatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)}. */
  MetaResultSet getIndexInfo(String catalog,
      String schema,
      String table,
      boolean unique,
      boolean approximate);

  /** Per {@link DatabaseMetaData#getUDTs(String, String, String, int[])}. */
  MetaResultSet getUDTs(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      int[] types);

  /** Per {@link DatabaseMetaData#getSuperTypes(String, String, String)}. */
  MetaResultSet getSuperTypes(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern);

  /** Per {@link DatabaseMetaData#getSuperTables(String, String, String)}. */
  MetaResultSet getSuperTables(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern);

  /** Per {@link DatabaseMetaData#getAttributes(String, String, String, String)}. */
  MetaResultSet getAttributes(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      Pat attributeNamePattern);

  /** Per {@link DatabaseMetaData#getClientInfoProperties()}. */
  MetaResultSet getClientInfoProperties();

  /** Per {@link DatabaseMetaData#getFunctions(String, String, String)}. */
  MetaResultSet getFunctions(String catalog,
      Pat schemaPattern,
      Pat functionNamePattern);

  /** Per {@link DatabaseMetaData#getFunctionColumns(String, String, String, String)}. */
  MetaResultSet getFunctionColumns(String catalog,
      Pat schemaPattern,
      Pat functionNamePattern,
      Pat columnNamePattern);

  /** Per {@link DatabaseMetaData#getPseudoColumns(String, String, String, String)}. */
  MetaResultSet getPseudoColumns(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern);

  /** Creates an iterable for a result set.
   *
   * <p>The default implementation just returns {@code iterable}, which it
   * requires to be not null; derived classes may instead choose to execute the
   * relational expression in {@code signature}. */
  Iterable<Object> createIterable(StatementHandle handle, Signature signature,
      List<TypedValue> parameterValues, Frame firstFrame);

  /** Prepares a statement.
   *
   * @param ch Connection handle
   * @param sql SQL query
   * @param maxRowCount Negative for no limit (different meaning than JDBC)
   * @return Signature of prepared statement
   */
  StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount);

  /** Prepares and executes a statement.
   *
   * @param h Statement handle
   * @param sql SQL query
   * @param maxRowCount Negative for no limit (different meaning than JDBC)
   * @param callback Callback to lock, clear and assign cursor
   *
   * @return Result containing statement ID, and if a query, a result set and
   *     first frame of data
   */
  ExecuteResult prepareAndExecute(StatementHandle h, String sql,
      long maxRowCount, PrepareCallback callback);

  /** Returns a frame of rows.
   *
   * <p>The frame describes whether there may be another frame. If there is not
   * another frame, the current iteration is done when we have finished the
   * rows in the this frame.
   *
   * <p>The default implementation always returns null.
   *
   * @param h Statement handle
   * @param parameterValues A list of parameter values, if statement is to be
   *                        executed; otherwise null
   * @param offset Zero-based offset of first row in the requested frame
   * @param fetchMaxRowCount Maximum number of rows to return; negative means
   * no limit
   * @return Frame, or null if there are no more
   */
  Frame fetch(StatementHandle h, List<TypedValue> parameterValues, long offset,
      int fetchMaxRowCount);

  /** Called during the creation of a statement to allocate a new handle.
   *
   * @param ch Connection handle
   */
  StatementHandle createStatement(ConnectionHandle ch);

  /** Close a statement.
   */
  void closeStatement(StatementHandle h);

  /** Close a connection */
  void closeConnection(ConnectionHandle ch);

  /** Sync client and server view of connection properties.
   *
   * <p>Note: this interface is considered "experimental" and may undergo further changes as this
   * functionality is extended to other aspects of state management for
   * {@link java.sql.Connection}, {@link java.sql.Statement}, and {@link java.sql.ResultSet}.</p>
   */
  ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps);

  /** Factory to create instances of {@link Meta}. */
  interface Factory {
    Meta create(List<String> args);
  }

  /** Wrapper to remind API calls that a parameter is a pattern (allows '%' and
   * '_' wildcards, per the JDBC spec) rather than a string to be matched
   * exactly. */
  class Pat {
    public final String s;

    private Pat(String s) {
      this.s = s;
    }

    @Override public String toString() {
      return "Pat[" + s + "]";
    }

    @JsonCreator
    public static Pat of(@JsonProperty("s") String name) {
      return new Pat(name);
    }
  }

  /** Database property.
   *
   * <p>Values exist for methods, such as
   * {@link DatabaseMetaData#getSQLKeywords()}, which always return the same
   * value at all times and across connections.
   *
   * @see #getDatabaseProperties()
   */
  enum DatabaseProperty {
    /** Database property containing the value of
     * {@link DatabaseMetaData#getNumericFunctions()}. */
    GET_NUMERIC_FUNCTIONS(""),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getStringFunctions()}. */
    GET_STRING_FUNCTIONS(""),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getSystemFunctions()}. */
    GET_SYSTEM_FUNCTIONS(""),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getTimeDateFunctions()}. */
    GET_TIME_DATE_FUNCTIONS(""),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getSQLKeywords()}. */
    GET_S_Q_L_KEYWORDS(""),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getDefaultTransactionIsolation()}. */
    GET_DEFAULT_TRANSACTION_ISOLATION(Connection.TRANSACTION_NONE);

    public final Class<?> type;
    public final Object defaultValue;
    public final Method method;

    <T> DatabaseProperty(T defaultValue) {
      this.defaultValue = defaultValue;
      final String methodName = AvaticaUtils.toCamelCase(name());
      try {
        this.method = DatabaseMetaData.class.getMethod(methodName);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
      this.type = AvaticaUtils.box(method.getReturnType());
      assert defaultValue == null || defaultValue.getClass() == type;
    }

    /** Returns a value of this property, using the default value if the map
     * does not contain an explicit value. */
    public <T> T getProp(Meta meta, Class<T> aClass) {
      return getProp(meta.getDatabaseProperties(), aClass);
    }

    /** Returns a value of this property, using the default value if the map
     * does not contain an explicit value. */
    public <T> T getProp(Map<DatabaseProperty, Object> map, Class<T> aClass) {
      assert aClass == type;
      Object v = map.get(this);
      if (v == null) {
        v = defaultValue;
      }
      return aClass.cast(v);
    }

    public static DatabaseProperty fromProto(Common.DatabaseProperty proto) {
      return DatabaseProperty.valueOf(proto.getName());
    }

    public Common.DatabaseProperty toProto() {
      return Common.DatabaseProperty.newBuilder().setName(name()).build();
    }
  }

  /** Response from execute.
   *
   * <p>Typically a query will have a result set and rowCount = -1;
   * a DML statement will have a rowCount and no result sets.
   */
  class ExecuteResult {
    public final List<MetaResultSet> resultSets;

    public ExecuteResult(List<MetaResultSet> resultSets) {
      this.resultSets = resultSets;
    }
  }

  /** Meta data from which a result set can be constructed.
   *
   * <p>If {@code updateCount} is not -1, the result is just a count. A result
   * set cannot be constructed. */
  class MetaResultSet {
    public final String connectionId;
    public final int statementId;
    public final boolean ownStatement;
    public final Frame firstFrame;
    public final Signature signature;
    public final long updateCount;

    @Deprecated // to be removed before 2.0
    protected MetaResultSet(String connectionId, int statementId,
        boolean ownStatement, Signature signature, Frame firstFrame,
        int updateCount) {
      this(connectionId, statementId, ownStatement, signature, firstFrame,
          (long) updateCount);
    }

    protected MetaResultSet(String connectionId, int statementId,
        boolean ownStatement, Signature signature, Frame firstFrame,
        long updateCount) {
      this.signature = signature;
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.ownStatement = ownStatement;
      this.firstFrame = firstFrame; // may be null even if signature is not null
      this.updateCount = updateCount;
    }

    public static MetaResultSet create(String connectionId, int statementId,
        boolean ownStatement, Signature signature, Frame firstFrame) {
      return new MetaResultSet(connectionId, statementId, ownStatement,
          Objects.requireNonNull(signature), firstFrame, -1L);
    }

    public static MetaResultSet count(String connectionId, int statementId,
        long updateCount) {
      assert updateCount >= 0
          : "Meta.count(" + connectionId + ", " + statementId + ", "
          + updateCount + ")";
      return new MetaResultSet(connectionId, statementId, false, null, null,
          updateCount);
    }
  }

  /** Information necessary to convert an {@link Iterable} into a
   * {@link org.apache.calcite.avatica.util.Cursor}. */
  final class CursorFactory {
    public final Style style;
    public final Class clazz;
    @JsonIgnore
    public final List<Field> fields;
    public final List<String> fieldNames;

    private CursorFactory(Style style, Class clazz, List<Field> fields,
        List<String> fieldNames) {
      assert (fieldNames != null)
          == (style == Style.RECORD_PROJECTION || style == Style.MAP);
      assert (fields != null) == (style == Style.RECORD_PROJECTION);
      this.style = Objects.requireNonNull(style);
      this.clazz = clazz;
      this.fields = fields;
      this.fieldNames = fieldNames;
    }

    @JsonCreator
    public static CursorFactory create(@JsonProperty("style") Style style,
        @JsonProperty("clazz") Class clazz,
        @JsonProperty("fieldNames") List<String> fieldNames) {
      switch (style) {
      case OBJECT:
        return OBJECT;
      case ARRAY:
        return ARRAY;
      case LIST:
        return LIST;
      case RECORD:
        return record(clazz);
      case RECORD_PROJECTION:
        return record(clazz, null, fieldNames);
      case MAP:
        return map(fieldNames);
      default:
        throw new AssertionError("unknown style: " + style);
      }
    }

    public static final CursorFactory OBJECT =
        new CursorFactory(Style.OBJECT, null, null, null);

    public static final CursorFactory ARRAY =
        new CursorFactory(Style.ARRAY, null, null, null);

    public static final CursorFactory LIST =
        new CursorFactory(Style.LIST, null, null, null);

    public static CursorFactory record(Class resultClazz) {
      return new CursorFactory(Style.RECORD, resultClazz, null, null);
    }

    public static CursorFactory record(Class resultClass, List<Field> fields,
        List<String> fieldNames) {
      if (fields == null) {
        fields = new ArrayList<>();
        for (String fieldName : fieldNames) {
          try {
            fields.add(resultClass.getField(fieldName));
          } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
          }
        }
      }
      return new CursorFactory(Style.RECORD_PROJECTION, resultClass, fields,
          fieldNames);
    }

    public static CursorFactory map(List<String> fieldNames) {
      return new CursorFactory(Style.MAP, null, null, fieldNames);
    }

    public static CursorFactory deduce(List<ColumnMetaData> columns,
        Class resultClazz) {
      if (columns.size() == 1) {
        return OBJECT;
      } else if (resultClazz != null && !resultClazz.isArray()) {
        return record(resultClazz);
      } else {
        return ARRAY;
      }
    }

    public Common.CursorFactory toProto() {
      Common.CursorFactory.Builder builder = Common.CursorFactory.newBuilder();

      if (null != clazz) {
        builder.setClassName(clazz.getName());
      }
      builder.setStyle(style.toProto());
      if (null != fieldNames) {
        builder.addAllFieldNames(fieldNames);
      }

      return builder.build();
    }

    public static CursorFactory fromProto(Common.CursorFactory proto) {
      // Reconstruct CursorFactory
      Class<?> clz = null;

      FieldDescriptor clzFieldDesc = proto.getDescriptorForType()
          .findFieldByNumber(Common.CursorFactory.CLASS_NAME_FIELD_NUMBER);

      if (proto.hasField(clzFieldDesc)) {
        try {
          clz = Class.forName(proto.getClassName());
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      return CursorFactory.create(Style.fromProto(proto.getStyle()), clz,
          proto.getFieldNamesList());
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((clazz == null) ? 0 : clazz.hashCode());
      result = prime * result + ((fieldNames == null) ? 0 : fieldNames.hashCode());
      result = prime * result + ((fields == null) ? 0 : fields.hashCode());
      result = prime * result + ((style == null) ? 0 : style.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof CursorFactory) {
        CursorFactory other = (CursorFactory) o;

        if (null == clazz) {
          if (null != other.clazz) {
            return false;
          }
        } else if (!clazz.equals(other.clazz)) {
          return false;
        }

        if (null == fieldNames) {
          if (null != other.fieldNames) {
            return false;
          }
        } else if (!fieldNames.equals(other.fieldNames)) {
          return false;
        }

        if (null == fields) {
          if (null != other.fields) {
            return false;
          }
        } else if (!fields.equals(other.fields)) {
          return false;
        }

        return style == other.style;
      }

      return false;
    }
  }

  /** How logical fields are represented in the objects returned by the
   * iterator. */
  enum Style {
    OBJECT,
    RECORD,
    RECORD_PROJECTION,
    ARRAY,
    LIST,
    MAP;

    public Common.CursorFactory.Style toProto() {
      return Common.CursorFactory.Style.valueOf(name());
    }

    public static Style fromProto(Common.CursorFactory.Style proto) {
      return Style.valueOf(proto.name());
    }
  }

  /** Result of preparing a statement. */
  public class Signature {
    public final List<ColumnMetaData> columns;
    public final String sql;
    public final List<AvaticaParameter> parameters;
    public final transient Map<String, Object> internalParameters;
    public final CursorFactory cursorFactory;

    /** Creates a Signature. */
    public Signature(List<ColumnMetaData> columns,
        String sql,
        List<AvaticaParameter> parameters,
        Map<String, Object> internalParameters,
        CursorFactory cursorFactory) {
      this.columns = columns;
      this.sql = sql;
      this.parameters = parameters;
      this.internalParameters = internalParameters;
      this.cursorFactory = cursorFactory;
    }

    /** Used by Jackson to create a Signature by de-serializing JSON. */
    @JsonCreator
    public static Signature create(
        @JsonProperty("columns") List<ColumnMetaData> columns,
        @JsonProperty("sql") String sql,
        @JsonProperty("parameters") List<AvaticaParameter> parameters,
        @JsonProperty("cursorFactory") CursorFactory cursorFactory) {
      return new Signature(columns, sql, parameters,
          Collections.<String, Object>emptyMap(), cursorFactory);
    }

    /** Returns a copy of this Signature, substituting given CursorFactory. */
    public Signature setCursorFactory(CursorFactory cursorFactory) {
      return new Signature(columns, sql, parameters, internalParameters,
          cursorFactory);
    }

    /** Creates a copy of this Signature with null lists and maps converted to
     * empty. */
    public Signature sanitize() {
      if (columns == null || parameters == null || internalParameters == null) {
        return new Signature(sanitize(columns), sql, sanitize(parameters),
            sanitize(internalParameters), cursorFactory);
      }
      return this;
    }

    private <E> List<E> sanitize(List<E> list) {
      return list == null ? Collections.<E>emptyList() : list;
    }

    private <K, V> Map<K, V> sanitize(Map<K, V> map) {
      return map == null ? Collections.<K, V>emptyMap() : map;
    }

    public Common.Signature toProto() {
      Common.Signature.Builder builder = Common.Signature.newBuilder();

      if (null != sql) {
        builder.setSql(sql);
      }

      if (null != cursorFactory) {
        builder.setCursorFactory(cursorFactory.toProto());
      }

      if (null != columns) {
        for (ColumnMetaData column : columns) {
          builder.addColumns(column.toProto());
        }
      }

      if (null != parameters) {
        for (AvaticaParameter parameter : parameters) {
          builder.addParameters(parameter.toProto());
        }
      }

      return builder.build();
    }

    public static Signature fromProto(Common.Signature protoSignature) {
      List<ColumnMetaData> metadata = new ArrayList<>(protoSignature.getColumnsCount());
      for (Common.ColumnMetaData protoMetadata : protoSignature.getColumnsList()) {
        metadata.add(ColumnMetaData.fromProto(protoMetadata));
      }

      List<AvaticaParameter> parameters = new ArrayList<>(protoSignature.getParametersCount());
      for (Common.AvaticaParameter protoParam : protoSignature.getParametersList()) {
        parameters.add(AvaticaParameter.fromProto(protoParam));
      }

      final Descriptor desc = protoSignature.getDescriptorForType();

      String sql = null;
      if (ProtobufService.hasField(protoSignature, desc, Common.Signature.SQL_FIELD_NUMBER)) {
        sql = protoSignature.getSql();
      }

      CursorFactory cursorFactory = null;
      if (ProtobufService.hasField(protoSignature, desc,
            Common.Signature.CURSOR_FACTORY_FIELD_NUMBER)) {
        cursorFactory = CursorFactory.fromProto(protoSignature.getCursorFactory());
      }

      return Signature.create(metadata, sql, parameters, cursorFactory);
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((columns == null) ? 0 : columns.hashCode());
      result = prime * result + ((cursorFactory == null) ? 0 : cursorFactory.hashCode());
      result = prime * result + ((parameters == null) ? 0 : parameters.hashCode());
      result = prime * result + ((sql == null) ? 0 : sql.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof Signature) {
        Signature other = (Signature) o;

        if (null == columns) {
          if (null != other.columns) {
            return false;
          }
        } else if (!columns.equals(other.columns)) {
          return false;
        }

        if (null == cursorFactory) {
          if (null != other.cursorFactory) {
            return false;
          }
        } else if (!cursorFactory.equals(other.cursorFactory)) {
          return false;
        }

        if (null == parameters) {
          if (null != other.parameters) {
            return false;
          }
        } else if (!parameters.equals(other.parameters)) {
          return false;
        }

        if (null == sql) {
          if (null != other.sql) {
            return false;
          }
        } else if (!sql.equals(other.sql)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** A collection of rows. */
  public class Frame {
    /** Frame that has zero rows and is the last frame. */
    public static final Frame EMPTY =
        new Frame(0, true, Collections.emptyList());

    /** Frame that has zero rows but may have another frame. */
    public static final Frame MORE =
        new Frame(0, false, Collections.emptyList());

    /** Zero-based offset of first row. */
    public final long offset;
    /** Whether this is definitely the last frame of rows.
     * If true, there are no more rows.
     * If false, there may or may not be more rows. */
    public final boolean done;
    /** The rows. */
    public final Iterable<Object> rows;

    public Frame(long offset, boolean done, Iterable<Object> rows) {
      this.offset = offset;
      this.done = done;
      this.rows = rows;
    }

    @JsonCreator
    public static Frame create(@JsonProperty("offset") int offset,
        @JsonProperty("done") boolean done,
        @JsonProperty("rows") List<Object> rows) {
      if (offset == 0 && done && rows.isEmpty()) {
        return EMPTY;
      }
      return new Frame(offset, done, rows);
    }

    public Common.Frame toProto() {
      Common.Frame.Builder builder = Common.Frame.newBuilder();

      builder.setDone(done).setOffset(offset);

      for (Object row : this.rows) {
        if (null == row) {
          // Does this need to be persisted for some reason?
          continue;
        }

        if (row instanceof Object[]) {
          final Common.Row.Builder rowBuilder = Common.Row.newBuilder();

          for (Object element : (Object[]) row) {
            final Common.TypedValue.Builder valueBuilder = Common.TypedValue.newBuilder();

            // Numbers
            if (element instanceof Byte) {
              valueBuilder.setType(Common.Rep.BYTE).setNumberValue(((Byte) element).longValue());
            } else if (element instanceof Short) {
              valueBuilder.setType(Common.Rep.SHORT).setNumberValue(((Short) element).longValue());
            } else if (element instanceof Integer) {
              valueBuilder.setType(Common.Rep.INTEGER)
                .setNumberValue(((Integer) element).longValue());
            } else if (element instanceof Long) {
              valueBuilder.setType(Common.Rep.LONG).setNumberValue((Long) element);
            } else if (element instanceof Double) {
              valueBuilder.setType(Common.Rep.DOUBLE)
                .setDoubleValue(((Double) element).doubleValue());
            } else if (element instanceof Float) {
              valueBuilder.setType(Common.Rep.FLOAT).setNumberValue(((Float) element).longValue());
            } else if (element instanceof BigDecimal) {
              valueBuilder.setType(Common.Rep.NUMBER)
                .setDoubleValue(((BigDecimal) element).doubleValue());
            // Strings
            } else if (element instanceof String) {
              valueBuilder.setType(Common.Rep.STRING)
                .setStringValue((String) element);
            } else if (element instanceof Character) {
              valueBuilder.setType(Common.Rep.CHARACTER)
                .setStringValue(((Character) element).toString());
            // Bytes
            } else if (element instanceof byte[]) {
              valueBuilder.setType(Common.Rep.BYTE_STRING)
                .setBytesValues(ByteString.copyFrom((byte[]) element));
            // Boolean
            } else if (element instanceof Boolean) {
              valueBuilder.setType(Common.Rep.BOOLEAN).setBoolValue((boolean) element);
            } else if (null == element) {
              valueBuilder.setType(Common.Rep.NULL);
            // Unhandled
            } else {
              throw new RuntimeException("Unhandled type in Frame: " + element.getClass());
            }

            // Add value to row
            rowBuilder.addValue(valueBuilder.build());
          }

          // Collect all rows
          builder.addRows(rowBuilder.build());
        } else {
          // Can a "row" be a primitive? A struct? Only an Array?
          throw new RuntimeException("Only arrays are supported");
        }
      }

      return builder.build();
    }

    public static Frame fromProto(Common.Frame proto) {
      List<Object> parsedRows = new ArrayList<>(proto.getRowsCount());
      for (Common.Row protoRow : proto.getRowsList()) {
        ArrayList<Object> row = new ArrayList<>(protoRow.getValueCount());
        for (Common.TypedValue protoElement : protoRow.getValueList()) {
          Object element;

          // TODO Should these be primitives or Objects?
          switch (protoElement.getType()) {
          case BYTE:
            element = Long.valueOf(protoElement.getNumberValue()).byteValue();
            break;
          case SHORT:
            element = Long.valueOf(protoElement.getNumberValue()).shortValue();
            break;
          case INTEGER:
            element = Long.valueOf(protoElement.getNumberValue()).intValue();
            break;
          case LONG:
            element = protoElement.getNumberValue();
            break;
          case FLOAT:
            element = Long.valueOf(protoElement.getNumberValue()).floatValue();
            break;
          case DOUBLE:
            element = Double.valueOf(protoElement.getDoubleValue());
            break;
          case NUMBER:
            // TODO more cases here to expand on? BigInteger?
            element = BigDecimal.valueOf(protoElement.getDoubleValue());
            break;
          case STRING:
            element = protoElement.getStringValue();
            break;
          case CHARACTER:
            // A single character in the string
            element = protoElement.getStringValue().charAt(0);
            break;
          case BYTE_STRING:
            element = protoElement.getBytesValues().toByteArray();
            break;
          case BOOLEAN:
            element = protoElement.getBoolValue();
            break;
          case NULL:
            element = null;
            break;
          default:
            throw new RuntimeException("Unhandled type: " + protoElement.getType());
          }

          row.add(element);
        }

        parsedRows.add(row);
      }

      return new Frame(proto.getOffset(), proto.getDone(), parsedRows);
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (done ? 1231 : 1237);
      result = prime * result + (int) (offset ^ (offset >>> 32));
      result = prime * result + ((rows == null) ? 0 : rows.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof Frame) {
        Frame other = (Frame) o;

        if (null == rows) {
          if (null != other.rows) {
            return false;
          }
        } else {
          Iterator<Object> iter1 = rows.iterator();
          Iterator<Object> iter2 = other.rows.iterator();
          while (iter1.hasNext() && iter2.hasNext()) {
            Object obj1 = iter1.next();
            Object obj2 = iter2.next();

            // Can't just call equals on an array
            if (obj1 instanceof Object[]) {
              if (obj2 instanceof Object[]) {
                // Compare array and array
                if (!Arrays.equals((Object[]) obj1, (Object[]) obj2)) {
                  return false;
                }
              } else if (obj2 instanceof List) {
                // compare array and list
                @SuppressWarnings("unchecked")
                List<Object> obj2List = (List<Object>) obj2;
                if (!Arrays.equals((Object[]) obj1, obj2List.toArray(new Object[0]))) {
                  return false;
                }
              } else {
                // compare array and something that isn't an array will always fail
                return false;
              }
            } else if (obj1 instanceof List) {
              if (obj2 instanceof Object[]) {
                // Compare list and array
                @SuppressWarnings("unchecked")
                List<Object> obj1List = (List<Object>) obj1;
                if (!Arrays.equals(obj1List.toArray(new Object[0]), (Object[]) obj2)) {
                  return false;
                }
              } else if (!obj1.equals(obj2)) {
                // compare list and something else, let it fall to equals()
                return false;
              }
            } else if (!obj1.equals(obj2)) {
              // Not an array, leave it to equals()
              return false;
            }
          }

          // More elements in one of the iterables
          if (iter1.hasNext() || iter2.hasNext()) {
            return false;
          }
        }

        return offset == other.offset && done == other.done;
      }

      return false;
    }
  }

  /** Connection handle. */
  class ConnectionHandle {
    public final String id;

    @Override public String toString() {
      return id;
    }

    @JsonCreator
    public ConnectionHandle(@JsonProperty("id") String id) {
      this.id = id;
    }
  }

  /** Statement handle. */
  class StatementHandle {
    public final String connectionId;
    public final int id;

    // not final because LocalService#apply(PrepareRequest)
    /** Only present for PreparedStatement handles, null otherwise. */
    public Signature signature;

    @Override public String toString() {
      return connectionId + "::" + Integer.toString(id);
    }

    @JsonCreator
    public StatementHandle(
        @JsonProperty("connectionId") String connectionId,
        @JsonProperty("id") int id,
        @JsonProperty("signature") Signature signature) {
      this.connectionId = connectionId;
      this.id = id;
      this.signature = signature;
    }

    public Common.StatementHandle toProto() {
      return Common.StatementHandle.newBuilder().setConnectionId(connectionId)
          .setId(id).setSignature(signature.toProto()).build();
    }

    public static StatementHandle fromProto(Common.StatementHandle protoHandle) {
      return new StatementHandle(protoHandle.getConnectionId(), protoHandle.getId(),
          Signature.fromProto(protoHandle.getSignature()));
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      result = prime * result + id;
      result = prime * result + ((signature == null) ? 0 : signature.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof StatementHandle) {
        StatementHandle other = (StatementHandle) o;

        if (null == connectionId) {
          if (null != other.connectionId) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

        if (null == signature) {
          if (null != other.signature) {
            return false;
          }
        } else if (!signature.equals(other.signature)) {
          return false;
        }

        return id == other.id;
      }

      return false;
    }
  }

  /** A pojo containing various client-settable {@link java.sql.Connection} properties.
   *
   * <p>{@code java.lang} types are used here so that {@code null} can be used to indicate
   * a value has no been set.</p>
   *
   * <p>Note: this interface is considered "experimental" and may undergo further changes as this
   * functionality is extended to other aspects of state management for
   * {@link java.sql.Connection}, {@link java.sql.Statement}, and {@link java.sql.ResultSet}.</p>
   */
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      property = "connProps",
      defaultImpl = ConnectionPropertiesImpl.class)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = ConnectionPropertiesImpl.class, name = "connPropsImpl")
  })
  interface ConnectionProperties {

    /** Overwrite fields in {@code this} with any non-null fields in {@code that}
     *
     * @return {@code this}
     */
    ConnectionProperties merge(ConnectionProperties that);

    /** @return {@code true} when no properies have been set, {@code false} otherwise. */
    @JsonIgnore
    boolean isEmpty();

    /** Set {@code autoCommit} status.
     *
     * @return {@code this}
     */
    ConnectionProperties setAutoCommit(boolean val);

    Boolean isAutoCommit();

    /** Set {@code readOnly} status.
     *
     * @return {@code this}
     */
    ConnectionProperties setReadOnly(boolean val);

    Boolean isReadOnly();

    /** Set {@code transactionIsolation} status.
     *
     * @return {@code this}
     */
    ConnectionProperties setTransactionIsolation(int val);

    Integer getTransactionIsolation();

    /** Set {@code catalog}.
     *
     * @return {@code this}
     */
    ConnectionProperties setCatalog(String val);

    String getCatalog();

    /** Set {@code schema}.
     *
     * @return {@code this}
     */
    ConnectionProperties setSchema(String val);

    String getSchema();

    Common.ConnectionProperties toProto();
  }

  /** API to put a result set into a statement, being careful to enforce
   * thread-safety and not to overwrite existing open result sets. */
  interface PrepareCallback {
    Object getMonitor();
    void clear() throws SQLException;
    void assign(Signature signature, Frame firstFrame, long updateCount)
        throws SQLException;
    void execute() throws SQLException;
  }
}

// End Meta.java
