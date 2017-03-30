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
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.FilteredConstants;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

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
  Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle ch);

  /** Per {@link DatabaseMetaData#getTables(String, String, String, String[])}. */
  MetaResultSet getTables(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      List<String> typeList);

  /** Per {@link DatabaseMetaData#getColumns(String, String, String, String)}. */
  MetaResultSet getColumns(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern);

  MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern);

  /** Per {@link DatabaseMetaData#getCatalogs()}. */
  MetaResultSet getCatalogs(ConnectionHandle ch);

  /** Per {@link DatabaseMetaData#getTableTypes()}. */
  MetaResultSet getTableTypes(ConnectionHandle ch);

  /** Per {@link DatabaseMetaData#getProcedures(String, String, String)}. */
  MetaResultSet getProcedures(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern);

  /** Per {@link DatabaseMetaData#getProcedureColumns(String, String, String, String)}. */
  MetaResultSet getProcedureColumns(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern,
      Pat columnNamePattern);

  /** Per {@link DatabaseMetaData#getColumnPrivileges(String, String, String, String)}. */
  MetaResultSet getColumnPrivileges(ConnectionHandle ch,
      String catalog,
      String schema,
      String table,
      Pat columnNamePattern);

  /** Per {@link DatabaseMetaData#getTablePrivileges(String, String, String)}. */
  MetaResultSet getTablePrivileges(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern);

  /** Per
   * {@link DatabaseMetaData#getBestRowIdentifier(String, String, String, int, boolean)}. */
  MetaResultSet getBestRowIdentifier(ConnectionHandle ch,
      String catalog,
      String schema,
      String table,
      int scope,
      boolean nullable);

  /** Per {@link DatabaseMetaData#getVersionColumns(String, String, String)}. */
  MetaResultSet getVersionColumns(ConnectionHandle ch, String catalog, String schema, String table);

  /** Per {@link DatabaseMetaData#getPrimaryKeys(String, String, String)}. */
  MetaResultSet getPrimaryKeys(ConnectionHandle ch, String catalog, String schema, String table);

  /** Per {@link DatabaseMetaData#getImportedKeys(String, String, String)}. */
  MetaResultSet getImportedKeys(ConnectionHandle ch, String catalog, String schema, String table);

  /** Per {@link DatabaseMetaData#getExportedKeys(String, String, String)}. */
  MetaResultSet getExportedKeys(ConnectionHandle ch, String catalog, String schema, String table);

  /** Per
   * {@link DatabaseMetaData#getCrossReference(String, String, String, String, String, String)}. */
  MetaResultSet getCrossReference(ConnectionHandle ch,
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable);

  /** Per {@link DatabaseMetaData#getTypeInfo()}. */
  MetaResultSet getTypeInfo(ConnectionHandle ch);

  /** Per {@link DatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)}. */
  MetaResultSet getIndexInfo(ConnectionHandle ch, String catalog,
      String schema,
      String table,
      boolean unique,
      boolean approximate);

  /** Per {@link DatabaseMetaData#getUDTs(String, String, String, int[])}. */
  MetaResultSet getUDTs(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      int[] types);

  /** Per {@link DatabaseMetaData#getSuperTypes(String, String, String)}. */
  MetaResultSet getSuperTypes(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern);

  /** Per {@link DatabaseMetaData#getSuperTables(String, String, String)}. */
  MetaResultSet getSuperTables(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern);

  /** Per {@link DatabaseMetaData#getAttributes(String, String, String, String)}. */
  MetaResultSet getAttributes(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      Pat attributeNamePattern);

  /** Per {@link DatabaseMetaData#getClientInfoProperties()}. */
  MetaResultSet getClientInfoProperties(ConnectionHandle ch);

  /** Per {@link DatabaseMetaData#getFunctions(String, String, String)}. */
  MetaResultSet getFunctions(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat functionNamePattern);

  /** Per {@link DatabaseMetaData#getFunctionColumns(String, String, String, String)}. */
  MetaResultSet getFunctionColumns(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat functionNamePattern,
      Pat columnNamePattern);

  /** Per {@link DatabaseMetaData#getPseudoColumns(String, String, String, String)}. */
  MetaResultSet getPseudoColumns(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern);

  /** Creates an iterable for a result set.
   *
   * <p>The default implementation just returns {@code iterable}, which it
   * requires to be not null; derived classes may instead choose to execute the
   * relational expression in {@code signature}. */
  Iterable<Object> createIterable(StatementHandle stmt, QueryState state, Signature signature,
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
   * @deprecated See {@link #prepareAndExecute(StatementHandle, String, long, int, PrepareCallback)}
   */
  @Deprecated // to be removed before 2.0
  ExecuteResult prepareAndExecute(StatementHandle h, String sql,
      long maxRowCount, PrepareCallback callback) throws NoSuchStatementException;

  /** Prepares and executes a statement.
   *
   * @param h Statement handle
   * @param sql SQL query
   * @param maxRowCount Maximum number of rows for the entire query. Negative for no limit
   *    (different meaning than JDBC).
   * @param maxRowsInFirstFrame Maximum number of rows for the first frame. This value should
   *    always be less than or equal to {@code maxRowCount} as the number of results are guaranteed
   *    to be restricted by {@code maxRowCount} and the underlying database.
   * @param callback Callback to lock, clear and assign cursor
   *
   * @return Result containing statement ID, and if a query, a result set and
   *     first frame of data
   */
  ExecuteResult prepareAndExecute(StatementHandle h, String sql,
      long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback)
      throws NoSuchStatementException;

  /** Prepares a statement and then executes a number of SQL commands in one pass.
   *
   * @param h Statement handle
   * @param sqlCommands SQL commands to run
   * @return An array of update counts containing one element for each command in the batch.
   */
  ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h, List<String> sqlCommands)
      throws NoSuchStatementException;

  /** Executes a collection of bound parameter values on a prepared statement.
   *
   * @param h Statement handle
   * @param parameterValues A collection of list of typed values, one list per batch
   * @return An array of update counts containing one element for each command in the batch.
   */
  ExecuteBatchResult executeBatch(StatementHandle h, List<List<TypedValue>> parameterValues)
      throws NoSuchStatementException;

  /** Returns a frame of rows.
   *
   * <p>The frame describes whether there may be another frame. If there is not
   * another frame, the current iteration is done when we have finished the
   * rows in the this frame.
   *
   * <p>The default implementation always returns null.
   *
   * @param h Statement handle
   * @param offset Zero-based offset of first row in the requested frame
   * @param fetchMaxRowCount Maximum number of rows to return; negative means
   * no limit
   * @return Frame, or null if there are no more
   */
  Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) throws
      NoSuchStatementException, MissingResultsException;

  /** Executes a prepared statement.
   *
   * @param h Statement handle
   * @param parameterValues A list of parameter values; may be empty, not null
   * @param maxRowCount Maximum number of rows to return; negative means
   * no limit
   * @return Execute result
   * @deprecated See {@link #execute(StatementHandle, List, int)}
   */
  @Deprecated // to be removed before 2.0
  ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues,
      long maxRowCount) throws NoSuchStatementException;

  /** Executes a prepared statement.
   *
   * @param h Statement handle
   * @param parameterValues A list of parameter values; may be empty, not null
   * @param maxRowsInFirstFrame Maximum number of rows to return in the Frame.
   * @return Execute result
   */
  ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues,
      int maxRowsInFirstFrame) throws NoSuchStatementException;

  /** Called during the creation of a statement to allocate a new handle.
   *
   * @param ch Connection handle
   */
  StatementHandle createStatement(ConnectionHandle ch);

  /** Closes a statement.
   *
   * <p>If the statement handle is not known, or is already closed, does
   * nothing.
   *
   * @param h Statement handle
   */
  void closeStatement(StatementHandle h);

  /**
   * Opens (creates) a connection. The client allocates its own connection ID which the server is
   * then made aware of through the {@link ConnectionHandle}. The Map {@code info} argument is
   * analogous to the {@link Properties} typically passed to a "normal" JDBC Driver. Avatica
   * specific properties should not be included -- only properties for the underlying driver.
   *
   * @param ch A ConnectionHandle encapsulates information about the connection to be opened
   *    as provided by the client.
   * @param info A Map corresponding to the Properties typically passed to a JDBC Driver.
   */
  void openConnection(ConnectionHandle ch, Map<String, String> info);

  /** Closes a connection */
  void closeConnection(ConnectionHandle ch);

  /**
   * Re-sets the {@link ResultSet} on a Statement. Not a JDBC method.
   *
   * @return True if there are results to fetch after resetting to the given offset. False otherwise
   */
  boolean syncResults(StatementHandle sh, QueryState state, long offset)
      throws NoSuchStatementException;

  /**
   * Makes all changes since the last commit/rollback permanent. Analogous to
   * {@link Connection#commit()}.
   *
   * @param ch A reference to the real JDBC Connection
   */
  void commit(ConnectionHandle ch);

  /**
   * Undoes all changes since the last commit/rollback. Analogous to
   * {@link Connection#rollback()};
   *
   * @param ch A reference to the real JDBC Connection
   */
  void rollback(ConnectionHandle ch);

  /** Synchronizes client and server view of connection properties.
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
   * @see #getDatabaseProperties(Meta.ConnectionHandle)
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
    GET_DEFAULT_TRANSACTION_ISOLATION(Connection.TRANSACTION_NONE),

    /** Database property which is the Avatica version */
    AVATICA_VERSION(FilteredConstants.VERSION),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getDriverVersion()}. */
    GET_DRIVER_VERSION(""),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getDriverMinorVersion()}. */
    GET_DRIVER_MINOR_VERSION(-1),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getDriverMajorVersion()}. */
    GET_DRIVER_MAJOR_VERSION(-1),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getDriverName()}. */
    GET_DRIVER_NAME(""),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getDatabaseMinorVersion()}. */
    GET_DATABASE_MINOR_VERSION(-1),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getDatabaseMajorVersion()}. */
    GET_DATABASE_MAJOR_VERSION(-1),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getDatabaseProductName()}. */
    GET_DATABASE_PRODUCT_NAME(""),

    /** Database property containing the value of
     * {@link DatabaseMetaData#getDatabaseProductVersion()}. */
    GET_DATABASE_PRODUCT_VERSION("");

    public final Class<?> type;
    public final Object defaultValue;
    public final Method method;
    public final boolean isJdbc;

    <T> DatabaseProperty(T defaultValue) {
      this.defaultValue = defaultValue;
      final String methodName = AvaticaUtils.toCamelCase(name());
      Method localMethod = null;
      try {
        localMethod = DatabaseMetaData.class.getMethod(methodName);
      } catch (NoSuchMethodException e) {
        // Pass, localMethod stays null.
      }

      if (null == localMethod) {
        this.method = null;
        this.type = null;
        this.isJdbc = false;
      } else {
        this.method = localMethod;
        this.type = AvaticaUtils.box(method.getReturnType());
        this.isJdbc = true;
      }

      // It's either: 1) not a JDBC method, 2) has no default value,
      // 3) the defaultValue is of the expected type
      assert !isJdbc || defaultValue == null || defaultValue.getClass() == type;
    }

    /** Returns a value of this property, using the default value if the map
     * does not contain an explicit value. */
    public <T> T getProp(Meta meta, ConnectionHandle ch, Class<T> aClass) {
      return getProp(meta.getDatabaseProperties(ch), aClass);
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

  /**
   * Response from a collection of SQL commands or parameter values in a single batch.
   */
  class ExecuteBatchResult {
    public final long[] updateCounts;

    public ExecuteBatchResult(long[] updateCounts) {
      this.updateCounts = Objects.requireNonNull(updateCounts);
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
    private static final FieldDescriptor CLASS_NAME_DESCRIPTOR = Common.CursorFactory.
        getDescriptor().findFieldByNumber(Common.CursorFactory.CLASS_NAME_FIELD_NUMBER);

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
      }
      if (resultClazz == null) {
        return ARRAY;
      }
      if (resultClazz.isArray()) {
        return ARRAY;
      }
      if (List.class.isAssignableFrom(resultClazz)) {
        return LIST;
      }
      return record(resultClazz);
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

      if (proto.hasField(CLASS_NAME_DESCRIPTOR)) {
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
      return Objects.hash(clazz, fieldNames, fields, style);
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof CursorFactory
          && Objects.equals(clazz, ((CursorFactory) o).clazz)
          && Objects.equals(fieldNames, ((CursorFactory) o).fieldNames)
          && Objects.equals(fields, ((CursorFactory) o).fields)
          && style == ((CursorFactory) o).style;
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
    private static final FieldDescriptor SQL_DESCRIPTOR = Common.Signature
        .getDescriptor().findFieldByNumber(Common.Signature.SQL_FIELD_NUMBER);
    private static final FieldDescriptor CURSOR_FACTORY_DESCRIPTOR = Common.Signature
        .getDescriptor().findFieldByNumber(Common.Signature.CURSOR_FACTORY_FIELD_NUMBER);

    public final List<ColumnMetaData> columns;
    public final String sql;
    public final List<AvaticaParameter> parameters;
    public final transient Map<String, Object> internalParameters;
    public final CursorFactory cursorFactory;

    public final Meta.StatementType statementType;

    /** Creates a Signature. */
    public Signature(List<ColumnMetaData> columns,
        String sql,
        List<AvaticaParameter> parameters,
        Map<String, Object> internalParameters,
        CursorFactory cursorFactory,
        Meta.StatementType statementType) {
      this.columns = columns;
      this.sql = sql;
      this.parameters = parameters;
      this.internalParameters = internalParameters;
      this.cursorFactory = cursorFactory;
      this.statementType = statementType;
    }

    /** Used by Jackson to create a Signature by de-serializing JSON. */
    @JsonCreator
    public static Signature create(
        @JsonProperty("columns") List<ColumnMetaData> columns,
        @JsonProperty("sql") String sql,
        @JsonProperty("parameters") List<AvaticaParameter> parameters,
        @JsonProperty("cursorFactory") CursorFactory cursorFactory,
        @JsonProperty("statementType") Meta.StatementType statementType) {
      return new Signature(columns, sql, parameters,
          Collections.<String, Object>emptyMap(), cursorFactory, statementType);
    }

    /** Returns a copy of this Signature, substituting given CursorFactory. */
    public Signature setCursorFactory(CursorFactory cursorFactory) {
      return new Signature(columns, sql, parameters, internalParameters,
          cursorFactory, statementType);
    }

    /** Creates a copy of this Signature with null lists and maps converted to
     * empty. */
    public Signature sanitize() {
      if (columns == null || parameters == null || internalParameters == null
          || statementType == null) {
        return new Signature(sanitize(columns), sql, sanitize(parameters),
            sanitize(internalParameters), cursorFactory,
            Meta.StatementType.SELECT);
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

      String sql = null;
      if (protoSignature.hasField(SQL_DESCRIPTOR)) {
        sql = protoSignature.getSql();
      }

      CursorFactory cursorFactory = null;
      if (protoSignature.hasField(CURSOR_FACTORY_DESCRIPTOR)) {
        cursorFactory = CursorFactory.fromProto(protoSignature.getCursorFactory());
      }
      final Meta.StatementType statementType =
          Meta.StatementType.fromProto(protoSignature.getStatementType());

      return Signature.create(metadata, sql, parameters, cursorFactory, statementType);
    }

    @Override public int hashCode() {
      return Objects.hash(columns, cursorFactory, parameters, sql);
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof Signature
          && Objects.equals(columns, ((Signature) o).columns)
          && Objects.equals(cursorFactory, ((Signature) o).cursorFactory)
          && Objects.equals(parameters, ((Signature) o).parameters)
          && Objects.equals(sql, ((Signature) o).sql);
    }
  }

  /** A collection of rows. */
  class Frame {
    private static final FieldDescriptor HAS_ARRAY_VALUE_DESCRIPTOR = Common.ColumnValue
        .getDescriptor().findFieldByNumber(Common.ColumnValue.HAS_ARRAY_VALUE_FIELD_NUMBER);
    private static final FieldDescriptor SCALAR_VALUE_DESCRIPTOR = Common.ColumnValue
        .getDescriptor().findFieldByNumber(Common.ColumnValue.SCALAR_VALUE_FIELD_NUMBER);
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
    public static Frame create(@JsonProperty("offset") long offset,
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
            final Common.ColumnValue.Builder columnBuilder = Common.ColumnValue.newBuilder();

            if (element instanceof List) {
              columnBuilder.setHasArrayValue(true);
              List<?> list = (List<?>) element;
              // Add each element in the list/array to the column's value
              for (Object listItem : list) {
                final Common.TypedValue scalarListItem = serializeScalar(listItem);
                columnBuilder.addArrayValue(scalarListItem);
                // Add the deprecated 'value' repeated attribute for backwards compat
                columnBuilder.addValue(scalarListItem);
              }
            } else {
              // The default value, but still explicit.
              columnBuilder.setHasArrayValue(false);
              // Only one value for this column, a scalar.
              final Common.TypedValue scalarVal = serializeScalar(element);
              columnBuilder.setScalarValue(scalarVal);
              // Add the deprecated 'value' repeated attribute for backwards compat
              columnBuilder.addValue(scalarVal);
            }

            // Add value to row
            rowBuilder.addValue(columnBuilder.build());
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

    static Common.TypedValue serializeScalar(Object element) {
      final Common.TypedValue.Builder valueBuilder = Common.TypedValue.newBuilder();

      // Let TypedValue handle the serialization for us.
      TypedValue.toProto(valueBuilder, element);

      return valueBuilder.build();
    }

    public static Frame fromProto(Common.Frame proto) {
      List<Object> parsedRows = new ArrayList<>(proto.getRowsCount());
      for (Common.Row protoRow : proto.getRowsList()) {
        ArrayList<Object> row = new ArrayList<>(protoRow.getValueCount());
        for (Common.ColumnValue protoColumn : protoRow.getValueList()) {
          final Object value;
          if (!isNewStyleColumn(protoColumn)) {
            // Backward compatibility
            value = parseOldStyleColumn(protoColumn);
          } else {
            // Current style parsing (separate scalar and array values)
            value = parseColumn(protoColumn);
          }

          row.add(value);
        }

        parsedRows.add(row);
      }

      return new Frame(proto.getOffset(), proto.getDone(), parsedRows);
    }

    /**
     * Determines whether this message contains the new attributes in the
     * message. We can't directly test for the negative because our
     * {@code hasField} trick does not work on repeated fields.
     *
     * @param column The protobuf column object
     * @return True if the message is the new style, false otherwise.
     */
    static boolean isNewStyleColumn(Common.ColumnValue column) {
      return column.hasField(HAS_ARRAY_VALUE_DESCRIPTOR)
          || column.hasField(SCALAR_VALUE_DESCRIPTOR);
    }

    /**
     * For Calcite 1.5, we made the mistake of using array length to determine when the value for a
     * column is a scalar or an array. This method performs the old parsing for backwards
     * compatibility.
     *
     * @param column The protobuf ColumnValue object
     * @return The parsed value for this column
     */
    static Object parseOldStyleColumn(Common.ColumnValue column) {
      if (column.getValueCount() > 1) {
        List<Object> array = new ArrayList<>(column.getValueCount());
        for (Common.TypedValue columnValue : column.getValueList()) {
          array.add(deserializeScalarValue(columnValue));
        }
        return array;
      } else {
        return deserializeScalarValue(column.getValue(0));
      }
    }

    /**
     * Parses the value for a ColumnValue using the separated array and scalar attributes.
     *
     * @param column The protobuf ColumnValue object
     * @return The parse value for this column
     */
    static Object parseColumn(Common.ColumnValue column) {
      // Verify that we have one or the other (scalar or array)
      validateColumnValue(column);

      if (!column.hasField(SCALAR_VALUE_DESCRIPTOR)) {
        // Array
        List<Object> array = new ArrayList<>(column.getArrayValueCount());
        for (Common.TypedValue arrayValue : column.getArrayValueList()) {
          array.add(deserializeScalarValue(arrayValue));
        }
        return array;
      } else {
        // Scalar
        return deserializeScalarValue(column.getScalarValue());
      }
    }

    /**
     * Verifies that a ColumnValue has only a scalar or array value, not both and not neither.
     *
     * @param column The protobuf ColumnValue object
     * @throws IllegalArgumentException When the above condition is not met
     */
    static void validateColumnValue(Common.ColumnValue column) {
      final boolean hasScalar = column.hasField(SCALAR_VALUE_DESCRIPTOR);
      final boolean hasArrayValue = column.getHasArrayValue();

      // These should always be different
      if (hasScalar == hasArrayValue) {
        throw new IllegalArgumentException("A column must have a scalar or array value, not "
            + (hasScalar ? "both" : "neither"));
      }
    }

    static Object deserializeScalarValue(Common.TypedValue protoElement) {
      // ByteString is a single case where TypedValue is representing the data differently
      // (in its "local" form) than Frame does. We need to unwrap the Base64 encoding.
      if (Common.Rep.BYTE_STRING == protoElement.getType()) {
        // Protobuf is sending native bytes (not b64) across the wire. B64 bytes is only for
        // TypedValue's benefit
        return protoElement.getBytesValue().toByteArray();
      }
      // Again, let TypedValue deserialize things for us.
      return TypedValue.fromProto(protoElement).value;
    }

    @Override public int hashCode() {
      return Objects.hash(done, offset, rows);
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof Frame
          && equalRows(rows, ((Frame) o).rows)
          && offset == ((Frame) o).offset
          && done == ((Frame) o).done;
    }

    private static boolean equalRows(Iterable<Object> rows, Iterable<Object> otherRows) {
      if (null == rows) {
        if (null != otherRows) {
          return false;
        }
      } else {
        Iterator<Object> iter1 = rows.iterator();
        Iterator<Object> iter2 = otherRows.iterator();
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
              if (!Arrays.equals((Object[]) obj1, obj2List.toArray())) {
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
              if (!Arrays.equals(obj1List.toArray(), (Object[]) obj2)) {
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

      return true;
    }
  }

  /** Connection handle. */
  public class ConnectionHandle {
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
  // Visible for testing
  public class StatementHandle {
    private static final FieldDescriptor SIGNATURE_DESCRIPTOR = Common.StatementHandle
        .getDescriptor().findFieldByNumber(Common.StatementHandle.SIGNATURE_FIELD_NUMBER);
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
      Common.StatementHandle.Builder builder = Common.StatementHandle.newBuilder()
          .setConnectionId(connectionId).setId(id);
      if (null != signature) {
        builder.setSignature(signature.toProto());
      }
      return builder.build();
    }

    public static StatementHandle fromProto(Common.StatementHandle protoHandle) {
      // Signature is optional in the update path for executes.
      Signature signature = null;
      if (protoHandle.hasField(SIGNATURE_DESCRIPTOR)) {
        signature = Signature.fromProto(protoHandle.getSignature());
      }
      return new StatementHandle(protoHandle.getConnectionId(), protoHandle.getId(), signature);
    }

    @Override public int hashCode() {
      return Objects.hash(connectionId, id, signature);
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof StatementHandle
          && Objects.equals(connectionId, ((StatementHandle) o).connectionId)
          && Objects.equals(signature, ((StatementHandle) o).signature)
          && id == ((StatementHandle) o).id;
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

  /** Type of statement. */
  enum StatementType {
    SELECT, INSERT, UPDATE, DELETE, UPSERT, MERGE, OTHER_DML, IS_DML,
    CREATE, DROP, ALTER, OTHER_DDL, CALL;

    public boolean canUpdate() {
      switch(this) {
      case INSERT:
        return true;
      case IS_DML:
        return true;
      default:
        return false;
      }
    }

    public Common.StatementType toProto() {
      return Common.StatementType.valueOf(name());
    }

    public static StatementType fromProto(Common.StatementType proto) {
      return StatementType.valueOf(proto.name());
    }
  }
}

// End Meta.java
