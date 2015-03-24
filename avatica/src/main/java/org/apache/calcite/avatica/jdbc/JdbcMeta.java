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
package org.apache.calcite.avatica.jdbc;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Implementation of {@link Meta} upon an existing JDBC data source. */
public class JdbcMeta implements Meta {

  private static final Log LOG = LogFactory.getLog(JdbcMeta.class);

  /**
   * JDBC Types Mapped to Java Types
   *
   * @see <a href="https://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html#1051555">JDBC Types Mapped to Java Types</a>
   */
  protected static final Map<Integer, Type> SQL_TYPE_TO_JAVA_TYPE =
      new HashMap<>();
  static {
    SQL_TYPE_TO_JAVA_TYPE.put(Types.CHAR, String.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.VARCHAR, String.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.LONGNVARCHAR, String.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.NUMERIC, BigDecimal.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.DECIMAL, BigDecimal.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.BIT, Boolean.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.TINYINT, Byte.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.SMALLINT, Short.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.INTEGER, Integer.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.BIGINT, Long.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.REAL, Float.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.FLOAT, Double.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.DOUBLE, Double.TYPE);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.BINARY, byte[].class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.VARBINARY, byte[].class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.LONGVARBINARY, byte[].class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.DATE, java.sql.Date.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.TIME, java.sql.Time.class);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.TIMESTAMP, java.sql.Timestamp.class);
    //put(Types.CLOB, Clob);
    //put(Types.BLOB, Blob);
    SQL_TYPE_TO_JAVA_TYPE.put(Types.ARRAY, Array.class);
  }

  //
  // Constants for connection cache settings.
  //

  private static final String CONN_CACHE_KEY_BASE = "avatica.connectioncache";
  /** JDBC connection property for setting connection cache concurrency level. */
  public static final String CONN_CACHE_CONCURRENCY_KEY =
      CONN_CACHE_KEY_BASE + ".concurrency";
  public static final String DEFAULT_CONN_CACHE_CONCURRENCY_LEVEL = "10";
  /** JDBC connection property for setting connection cache initial capacity. */
  public static final String CONN_CACHE_INITIAL_CAPACITY_KEY =
      CONN_CACHE_KEY_BASE + ".initialcapacity";
  public static final String DEFAULT_CONN_CACHE_INITIAL_CAPACITY = "100";
  /** JDBC connection property for setting connection cache maximum capacity. */
  public static final String CONN_CACHE_MAX_CAPACITY_KEY =
      CONN_CACHE_KEY_BASE + ".maxcapacity";
  public static final String DEFAULT_CONN_CACHE_MAX_CAPACITY = "1000";
  /** JDBC connection property for setting connection cache expiration duration. */
  public static final String CONN_CACHE_EXPIRY_DURATION_KEY =
      CONN_CACHE_KEY_BASE + ".expirydiration";
  public static final String DEFAULT_CONN_CACHE_EXPIRY_DURATION = "10";
  /** JDBC connection property for setting connection cache expiration unit. */
  public static final String CONN_CACHE_EXPIRY_UNIT_KEY = CONN_CACHE_KEY_BASE + ".expiryunit";
  public static final String DEFAULT_CONN_CACHE_EXPIRY_UNIT = TimeUnit.MINUTES.name();

  //
  // Constants for statement cache settings.
  //

  private static final String STMT_CACHE_KEY_BASE = "avatica.statementcache";
  /** JDBC connection property for setting connection cache concurrency level. */
  public static final String STMT_CACHE_CONCURRENCY_KEY =
      STMT_CACHE_KEY_BASE + ".concurrency";
  public static final String DEFAULT_STMT_CACHE_CONCURRENCY_LEVEL = "100";
  /** JDBC connection property for setting connection cache initial capacity. */
  public static final String STMT_CACHE_INITIAL_CAPACITY_KEY =
      STMT_CACHE_KEY_BASE + ".initialcapacity";
  public static final String DEFAULT_STMT_CACHE_INITIAL_CAPACITY = "1000";
  /** JDBC connection property for setting connection cache maximum capacity. */
  public static final String STMT_CACHE_MAX_CAPACITY_KEY =
      STMT_CACHE_KEY_BASE + ".maxcapacity";
  public static final String DEFAULT_STMT_CACHE_MAX_CAPACITY = "10000";
  /** JDBC connection property for setting connection cache expiration duration. */
  public static final String STMT_CACHE_EXPIRY_DURATION_KEY =
      STMT_CACHE_KEY_BASE + ".expirydiration";
  public static final String DEFAULT_STMT_CACHE_EXPIRY_DURATION = "5";
  /** JDBC connection property for setting connection cache expiration unit. */
  public static final String STMT_CACHE_EXPIRY_UNIT_KEY = STMT_CACHE_KEY_BASE + ".expiryunit";
  public static final String DEFAULT_STMT_CACHE_EXPIRY_UNIT = TimeUnit.MINUTES.name();

  private static final String DEFAULT_CONN_ID =
      UUID.fromString("00000000-0000-0000-0000-000000000000").toString();

  private final String url;
  private final Properties info;
  private final Connection connection; // TODO: remove default connection
  private final Cache<String, Connection> connectionCache;
  private final Cache<Integer, StatementInfo> statementCache;

  /**
   * Convert from JDBC metadata to Avatica columns.
   */
  protected static List<ColumnMetaData>
  columns(ResultSetMetaData metaData) throws SQLException {
    final List<ColumnMetaData> columns = new ArrayList<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      final Type javaType =
          SQL_TYPE_TO_JAVA_TYPE.get(metaData.getColumnType(i));
      ColumnMetaData.AvaticaType t =
          ColumnMetaData.scalar(metaData.getColumnType(i),
              metaData.getColumnTypeName(i), ColumnMetaData.Rep.of(javaType));
      ColumnMetaData md =
          new ColumnMetaData(i - 1, metaData.isAutoIncrement(i),
              metaData.isCaseSensitive(i), metaData.isSearchable(i),
              metaData.isCurrency(i), metaData.isNullable(i),
              metaData.isSigned(i), metaData.getColumnDisplaySize(i),
              metaData.getColumnLabel(i), metaData.getColumnName(i),
              metaData.getSchemaName(i), metaData.getPrecision(i),
              metaData.getScale(i), metaData.getTableName(i),
              metaData.getCatalogName(i), t, metaData.isReadOnly(i),
              metaData.isWritable(i), metaData.isDefinitelyWritable(i),
              metaData.getColumnClassName(i));
      columns.add(md);
    }
    return columns;
  }

  /**
   * Converts from JDBC metadata to AvaticaParameters
   */
  protected static List<AvaticaParameter> parameters(ParameterMetaData metaData)
      throws SQLException {
    if (metaData == null) {
      return Collections.emptyList();
    }
    final List<AvaticaParameter> params = new ArrayList<>();
    for (int i = 1; i <= metaData.getParameterCount(); i++) {
      params.add(
          new AvaticaParameter(metaData.isSigned(i), metaData.getPrecision(i),
              metaData.getScale(i), metaData.getParameterType(i),
              metaData.getParameterTypeName(i),
              metaData.getParameterClassName(i), "?" + i));
    }
    return params;
  }

  protected static Signature signature(ResultSetMetaData metaData,
      ParameterMetaData parameterMetaData, String sql) throws  SQLException {
    return new Signature(columns(metaData), sql, parameters(parameterMetaData),
        null, CursorFactory.LIST /* LIST because JdbcResultSet#frame */);
  }

  protected static Signature signature(ResultSetMetaData metaData)
      throws SQLException {
    return signature(metaData, null, null);
  }

  /** Callback for {@link #connectionCache} member expiration. */
  private class ConnectionExpiryHandler
      implements RemovalListener<String, Connection> {

    public void onRemoval(RemovalNotification<String, Connection> notification) {
      String connectionId = notification.getKey();
      Connection doomed = notification.getValue();
      // is String.equals() more efficient?
      if (notification.getValue() == connection) {
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Expiring connection " + connectionId + " because "
            + notification.getCause());
      }
      try {
        if (doomed != null) {
          doomed.close();
        }
      } catch (Throwable t) {
        LOG.info("Exception thrown while expiring connection " + connectionId, t);
      }
    }
  }

  /** Callback for {@link #statementCache} member expiration. */
  private class StatementExpiryHandler
      implements RemovalListener<Integer, StatementInfo> {
    public void onRemoval(RemovalNotification<Integer, StatementInfo> notification) {
      Integer stmtId = notification.getKey();
      StatementInfo doomed = notification.getValue();
      if (doomed == null) {
        // log/throw?
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Expiring statement " + stmtId + " because "
            + notification.getCause());
      }
      try {
        if (doomed.resultSet != null) {
          doomed.resultSet.close();
        }
        if (doomed.statement != null) {
          doomed.statement.close();
        }
      } catch (Throwable t) {
        LOG.info("Exception thrown while expiring statement " + stmtId);
      }
    }
  }

  /**
   * @param url a database url of the form
   *  <code> jdbc:<em>subprotocol</em>:<em>subname</em></code>
   */
  public JdbcMeta(String url) throws SQLException {
    this(url, new Properties());
  }

  /**
   * @param url a database url of the form
   * <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
   * @param user the database user on whose behalf the connection is being
   *   made
   * @param password the user's password
   */
  public JdbcMeta(final String url, final String user, final String password)
      throws SQLException {
    this(url, new Properties() {
      {
        put("user", user);
        put("password", password);
      }
    });
  }

  /**
   * @param url a database url of the form
   * <code> jdbc:<em>subprotocol</em>:<em>subname</em></code>
   * @param info a list of arbitrary string tag/value pairs as
   * connection arguments; normally at least a "user" and
   * "password" property should be included
   */
  public JdbcMeta(String url, Properties info) throws SQLException {
    this.url = url;
    this.info = info;
    this.connection = DriverManager.getConnection(url, info);

    int concurrencyLevel = Integer.parseInt(
        info.getProperty(CONN_CACHE_CONCURRENCY_KEY, DEFAULT_CONN_CACHE_CONCURRENCY_LEVEL));
    int initialCapacity = Integer.parseInt(
        info.getProperty(CONN_CACHE_INITIAL_CAPACITY_KEY, DEFAULT_CONN_CACHE_INITIAL_CAPACITY));
    long maxCapacity = Long.parseLong(
        info.getProperty(CONN_CACHE_MAX_CAPACITY_KEY, DEFAULT_CONN_CACHE_MAX_CAPACITY));
    long connectionExpiryDuration = Long.parseLong(
        info.getProperty(CONN_CACHE_EXPIRY_DURATION_KEY, DEFAULT_CONN_CACHE_EXPIRY_DURATION));
    TimeUnit connectionExpiryUnit = TimeUnit.valueOf(
        info.getProperty(CONN_CACHE_EXPIRY_UNIT_KEY, DEFAULT_CONN_CACHE_EXPIRY_UNIT));
    this.connectionCache = CacheBuilder.newBuilder()
        .concurrencyLevel(concurrencyLevel)
        .initialCapacity(initialCapacity)
        .maximumSize(maxCapacity)
        .expireAfterAccess(connectionExpiryDuration, connectionExpiryUnit)
        .removalListener(new ConnectionExpiryHandler())
        .build();
    if (LOG.isDebugEnabled()) {
      LOG.debug("instantiated connection cache: " + connectionCache.stats());
    }

    concurrencyLevel = Integer.parseInt(
        info.getProperty(STMT_CACHE_CONCURRENCY_KEY, DEFAULT_STMT_CACHE_CONCURRENCY_LEVEL));
    initialCapacity = Integer.parseInt(
        info.getProperty(STMT_CACHE_INITIAL_CAPACITY_KEY, DEFAULT_STMT_CACHE_INITIAL_CAPACITY));
    maxCapacity = Long.parseLong(
        info.getProperty(STMT_CACHE_MAX_CAPACITY_KEY, DEFAULT_STMT_CACHE_MAX_CAPACITY));
    connectionExpiryDuration = Long.parseLong(
        info.getProperty(STMT_CACHE_EXPIRY_DURATION_KEY, DEFAULT_STMT_CACHE_EXPIRY_DURATION));
    connectionExpiryUnit = TimeUnit.valueOf(
        info.getProperty(STMT_CACHE_EXPIRY_UNIT_KEY, DEFAULT_STMT_CACHE_EXPIRY_UNIT));
    this.statementCache = CacheBuilder.newBuilder()
        .concurrencyLevel(concurrencyLevel)
        .initialCapacity(initialCapacity)
        .maximumSize(maxCapacity)
        .expireAfterAccess(connectionExpiryDuration, connectionExpiryUnit)
        .removalListener(new StatementExpiryHandler())
        .build();
    if (LOG.isDebugEnabled()) {
      LOG.debug("instantiated statement cache: " + statementCache.stats());
    }
  }

  public String getSqlKeywords() {
    try {
      return connection.getMetaData().getSQLKeywords();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public String getNumericFunctions() {
    try {
      return connection.getMetaData().getNumericFunctions();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public String getStringFunctions() {
    return null;
  }

  public String getSystemFunctions() {
    return null;
  }

  public String getTimeDateFunctions() {
    return null;
  }

  public MetaResultSet getTables(String catalog, Pat schemaPattern,
      Pat tableNamePattern, List<String> typeList) {
    try {
      String[] types = new String[typeList == null ? 0 : typeList.size()];
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getTables(catalog, schemaPattern.s,
              tableNamePattern.s,
              typeList == null ? types : typeList.toArray(types)));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getColumns(String catalog, Pat schemaPattern,
      Pat tableNamePattern, Pat columnNamePattern) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getColumns(catalog, schemaPattern.s,
              tableNamePattern.s, columnNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getSchemas(String catalog, Pat schemaPattern) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getSchemas(catalog, schemaPattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getCatalogs() {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getCatalogs());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getTableTypes() {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getTableTypes());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getProcedures(String catalog, Pat schemaPattern,
      Pat procedureNamePattern) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getProcedures(catalog, schemaPattern.s,
              procedureNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getProcedureColumns(String catalog, Pat schemaPattern,
      Pat procedureNamePattern, Pat columnNamePattern) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getProcedureColumns(catalog,
              schemaPattern.s, procedureNamePattern.s, columnNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getColumnPrivileges(String catalog, String schema,
      String table, Pat columnNamePattern) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getColumnPrivileges(catalog, schema,
              table, columnNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getTablePrivileges(String catalog, Pat schemaPattern,
      Pat tableNamePattern) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getTablePrivileges(catalog,
              schemaPattern.s, tableNamePattern.s));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getBestRowIdentifier(String catalog, String schema,
      String table, int scope, boolean nullable) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getBestRowIdentifier(catalog, schema,
              table, scope, nullable));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getVersionColumns(String catalog, String schema,
      String table) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getVersionColumns(catalog, schema, table));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getPrimaryKeys(String catalog, String schema,
      String table) {
    try {
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1,
          connection.getMetaData().getPrimaryKeys(catalog, schema, table));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getImportedKeys(String catalog, String schema,
      String table) {
    return null;
  }

  public MetaResultSet getExportedKeys(String catalog, String schema,
      String table) {
    return null;
  }

  public MetaResultSet getCrossReference(String parentCatalog,
      String parentSchema, String parentTable, String foreignCatalog,
      String foreignSchema, String foreignTable) {
    return null;
  }

  public MetaResultSet getTypeInfo() {
    return null;
  }

  public MetaResultSet getIndexInfo(String catalog, String schema, String table,
      boolean unique, boolean approximate) {
    return null;
  }

  public MetaResultSet getUDTs(String catalog, Pat schemaPattern,
      Pat typeNamePattern, int[] types) {
    return null;
  }

  public MetaResultSet getSuperTypes(String catalog, Pat schemaPattern,
      Pat typeNamePattern) {
    return null;
  }

  public MetaResultSet getSuperTables(String catalog, Pat schemaPattern,
      Pat tableNamePattern) {
    return null;
  }

  public MetaResultSet getAttributes(String catalog, Pat schemaPattern,
      Pat typeNamePattern, Pat attributeNamePattern) {
    return null;
  }

  public MetaResultSet getClientInfoProperties() {
    return null;
  }

  public MetaResultSet getFunctions(String catalog, Pat schemaPattern,
      Pat functionNamePattern) {
    return null;
  }

  public MetaResultSet getFunctionColumns(String catalog, Pat schemaPattern,
      Pat functionNamePattern, Pat columnNamePattern) {
    return null;
  }

  public MetaResultSet getPseudoColumns(String catalog, Pat schemaPattern,
      Pat tableNamePattern, Pat columnNamePattern) {
    return null;
  }

  public Iterable<Object> createIterable(StatementHandle handle,
      Signature signature, List<Object> parameterValues, Frame firstFrame) {
    return null;
  }

  protected Connection getConnection(String id) throws SQLException {
    Connection conn = connectionCache.getIfPresent(id);
    if (conn == null) {
      conn = DriverManager.getConnection(url, info);
      connectionCache.put(id, conn);
    }
    return conn;
  }

  public StatementHandle createStatement(ConnectionHandle ch) {
    try {
      final Connection conn = getConnection(ch.id);
      final Statement statement = conn.createStatement();
      final int id = System.identityHashCode(statement);
      statementCache.put(id, new StatementInfo(statement));
      StatementHandle h = new StatementHandle(ch.id, id, null);
      if (LOG.isTraceEnabled()) {
        LOG.trace("created statement " + h);
      }
      return h;
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  @Override public void closeStatement(StatementHandle h) {
    StatementInfo info = statementCache.getIfPresent(h.id);
    if (info == null || info.statement == null) {
      LOG.debug("client requested close unknown statement " + h);
      return;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("closing statement " + h);
    }
    try {
      if (info.resultSet != null) {
        info.resultSet.close();
      }
      info.statement.close();
    } catch (SQLException e) {
      throw propagate(e);
    } finally {
      statementCache.invalidate(h.id);
    }
  }

  @Override public void closeConnection(ConnectionHandle ch) {
    Connection conn = connectionCache.getIfPresent(ch.id);
    if (conn == null) {
      LOG.debug("client requested close unknown connection " + ch);
      return;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("closing connection " + ch);
    }
    try {
      conn.close();
    } catch (SQLException e) {
      throw propagate(e);
    } finally {
      connectionCache.invalidate(ch.id);
    }
  }

  private RuntimeException propagate(Throwable e) {
    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    } else if (e instanceof Error) {
      throw (Error) e;
    } else {
      throw new RuntimeException(e);
    }
  }

  public StatementHandle prepare(ConnectionHandle ch, String sql,
      int maxRowCount) {
    try {
      final Connection conn = getConnection(ch.id);
      final PreparedStatement statement = conn.prepareStatement(sql);
      final int id = System.identityHashCode(statement);
      statementCache.put(id, new StatementInfo(statement));
      StatementHandle h = new StatementHandle(ch.id, id,
          signature(statement.getMetaData(), statement.getParameterMetaData(),
              sql));
      if (LOG.isTraceEnabled()) {
        LOG.trace("prepared statement " + h);
      }
      return h;
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  public MetaResultSet prepareAndExecute(ConnectionHandle ch, String sql,
      int maxRowCount, PrepareCallback callback) {
    try {
      final Connection connection = getConnection(ch.id);
      final PreparedStatement statement = connection.prepareStatement(sql);
      final int id = System.identityHashCode(statement);
      final StatementInfo info = new StatementInfo(statement);
      statementCache.put(id, info);
      info.resultSet = statement.executeQuery();
      MetaResultSet mrs = JdbcResultSet.create(ch.id, id, info.resultSet);
      if (LOG.isTraceEnabled()) {
        StatementHandle h = new StatementHandle(ch.id, id, null);
        LOG.trace("prepAndExec statement " + h);
      }
      return mrs;
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  public Frame fetch(StatementHandle h, List<Object> parameterValues,
      int offset, int fetchMaxRowCount) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("fetching " + h + " offset:" + offset + " fetchMaxRowCount:" + fetchMaxRowCount);
    }
    try {
      final StatementInfo statementInfo = Objects.requireNonNull(
          statementCache.getIfPresent(h.id),
          "Statement not found, potentially expired. " + h);
      if (statementInfo.resultSet == null || parameterValues != null) {
        if (statementInfo.resultSet != null) {
          statementInfo.resultSet.close();
        }
        final PreparedStatement preparedStatement =
            (PreparedStatement) statementInfo.statement;
        if (parameterValues != null) {
          for (int i = 0; i < parameterValues.size(); i++) {
            Object o = parameterValues.get(i);
            preparedStatement.setObject(i + 1, o);
          }
        }
        statementInfo.resultSet = preparedStatement.executeQuery();
      }
      return JdbcResultSet.frame(statementInfo.resultSet, offset,
          fetchMaxRowCount);
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  /** All we know about a statement. */
  private static class StatementInfo {
    final Statement statement; // sometimes a PreparedStatement
    ResultSet resultSet;

    private StatementInfo(Statement statement) {
      this.statement = Objects.requireNonNull(statement);
    }
  }
}

// End JdbcMeta.java
