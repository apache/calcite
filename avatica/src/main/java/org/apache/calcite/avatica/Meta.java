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

import org.apache.calcite.avatica.remote.TypedValue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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

  MetaResultSet getTables(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      List<String> typeList);

  MetaResultSet getColumns(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern);

  MetaResultSet getSchemas(String catalog, Pat schemaPattern);

  MetaResultSet getCatalogs();

  MetaResultSet getTableTypes();

  MetaResultSet getProcedures(String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern);

  MetaResultSet getProcedureColumns(String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern,
      Pat columnNamePattern);

  MetaResultSet getColumnPrivileges(String catalog,
      String schema,
      String table,
      Pat columnNamePattern);

  MetaResultSet getTablePrivileges(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern);

  MetaResultSet getBestRowIdentifier(String catalog,
      String schema,
      String table,
      int scope,
      boolean nullable);

  MetaResultSet getVersionColumns(String catalog, String schema, String table);

  MetaResultSet getPrimaryKeys(String catalog, String schema, String table);

  MetaResultSet getImportedKeys(String catalog, String schema, String table);

  MetaResultSet getExportedKeys(String catalog, String schema, String table);

  MetaResultSet getCrossReference(String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable);

  MetaResultSet getTypeInfo();

  MetaResultSet getIndexInfo(String catalog,
      String schema,
      String table,
      boolean unique,
      boolean approximate);

  MetaResultSet getUDTs(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      int[] types);

  MetaResultSet getSuperTypes(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern);

  MetaResultSet getSuperTables(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern);

  MetaResultSet getAttributes(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      Pat attributeNamePattern);

  MetaResultSet getClientInfoProperties();

  MetaResultSet getFunctions(String catalog,
      Pat schemaPattern,
      Pat functionNamePattern);

  MetaResultSet getFunctionColumns(String catalog,
      Pat schemaPattern,
      Pat functionNamePattern,
      Pat columnNamePattern);

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
  StatementHandle prepare(ConnectionHandle ch, String sql, int maxRowCount);

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
      int maxRowCount, PrepareCallback callback);

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
  Frame fetch(StatementHandle h, List<TypedValue> parameterValues, int offset,
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
    public final int updateCount;

    protected MetaResultSet(String connectionId, int statementId,
        boolean ownStatement, Signature signature, Frame firstFrame,
        int updateCount) {
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
          Objects.requireNonNull(signature), firstFrame, -1);
    }

    public static MetaResultSet count(String connectionId, int statementId,
        int updateCount) {
      assert updateCount >= 0;
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
  }

  /** How logical fields are represented in the objects returned by the
   * iterator. */
  enum Style {
    OBJECT,
    RECORD,
    RECORD_PROJECTION,
    ARRAY,
    LIST,
    MAP
  }

  /** Result of preparing a statement. */
  class Signature {
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
  }

  /** A collection of rows. */
  class Frame {
    /** Frame that has zero rows and is the last frame. */
    public static final Frame EMPTY =
        new Frame(0, true, Collections.emptyList());

    /** Frame that has zero rows but may have another frame. */
    public static final Frame MORE =
        new Frame(0, false, Collections.emptyList());

    /** Zero-based offset of first row. */
    public final int offset;
    /** Whether this is definitely the last frame of rows.
     * If true, there are no more rows.
     * If false, there may or may not be more rows. */
    public final boolean done;
    /** The rows. */
    public final Iterable<Object> rows;

    public Frame(int offset, boolean done, Iterable<Object> rows) {
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
  }

  /** API to put a result set into a statement, being careful to enforce
   * thread-safety and not to overwrite existing open result sets. */
  interface PrepareCallback {
    Object getMonitor();
    void clear() throws SQLException;
    void assign(Signature signature, Frame firstFrame, int updateCount)
        throws SQLException;
    void execute() throws SQLException;
  }
}

// End Meta.java
