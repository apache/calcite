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
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.remote.TypedValue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Implementation of {@link Meta} upon an existing JDBC data source. */
public class JdbcMeta implements Meta {
  private static final Log LOG = LogFactory.getLog(JdbcMeta.class);

  private static final String CONN_CACHE_KEY_BASE = "avatica.connectioncache";

  private static final String STMT_CACHE_KEY_BASE = "avatica.statementcache";

  private static final String DEFAULT_CONN_ID =
      UUID.fromString("00000000-0000-0000-0000-000000000000").toString();

  /** Special value for {@link Statement#getLargeMaxRows()} that means fetch
   * an unlimited number of rows in a single batch.
   *
   * <p>Any other negative value will return an unlimited number of rows but
   * will do it in the default batch size, namely 100. */
  public static final long UNLIMITED_COUNT = -2L;

  // End of constants, start of member variables

  final Calendar calendar = Calendar.getInstance();

  /** Generates ids for statements. The ids are unique across all connections
   * created by this JdbcMeta. */
  private final AtomicInteger statementIdGenerator = new AtomicInteger();

  private final String url;
  private final Properties info;
  private final Cache<String, Connection> connectionCache;
  private final Cache<Integer, StatementInfo> statementCache;

  /**
   * Creates a JdbcMeta.
   *
   * @param url a database url of the form
   *  <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
   */
  public JdbcMeta(String url) throws SQLException {
    this(url, new Properties());
  }

  /**
   * Creates a JdbcMeta.
   *
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
   * Creates a JdbcMeta.
   *
   * @param url a database url of the form
   * <code> jdbc:<em>subprotocol</em>:<em>subname</em></code>
   * @param info a list of arbitrary string tag/value pairs as
   * connection arguments; normally at least a "user" and
   * "password" property should be included
   */
  public JdbcMeta(String url, Properties info) throws SQLException {
    this.url = url;
    this.info = info;

    int concurrencyLevel = Integer.parseInt(
        info.getProperty(ConnectionCacheSettings.CONCURRENCY_LEVEL.key(),
            ConnectionCacheSettings.CONCURRENCY_LEVEL.defaultValue()));
    int initialCapacity = Integer.parseInt(
        info.getProperty(ConnectionCacheSettings.INITIAL_CAPACITY.key(),
            ConnectionCacheSettings.INITIAL_CAPACITY.defaultValue()));
    long maxCapacity = Long.parseLong(
        info.getProperty(ConnectionCacheSettings.MAX_CAPACITY.key(),
            ConnectionCacheSettings.MAX_CAPACITY.defaultValue()));
    long connectionExpiryDuration = Long.parseLong(
        info.getProperty(ConnectionCacheSettings.EXPIRY_DURATION.key(),
            ConnectionCacheSettings.EXPIRY_DURATION.defaultValue()));
    TimeUnit connectionExpiryUnit = TimeUnit.valueOf(
        info.getProperty(ConnectionCacheSettings.EXPIRY_UNIT.key(),
            ConnectionCacheSettings.EXPIRY_UNIT.defaultValue()));
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
        info.getProperty(StatementCacheSettings.CONCURRENCY_LEVEL.key(),
            StatementCacheSettings.CONCURRENCY_LEVEL.defaultValue()));
    initialCapacity = Integer.parseInt(
        info.getProperty(StatementCacheSettings.INITIAL_CAPACITY.key(),
            StatementCacheSettings.INITIAL_CAPACITY.defaultValue()));
    maxCapacity = Long.parseLong(
        info.getProperty(StatementCacheSettings.MAX_CAPACITY.key(),
            StatementCacheSettings.MAX_CAPACITY.defaultValue()));
    connectionExpiryDuration = Long.parseLong(
        info.getProperty(StatementCacheSettings.EXPIRY_DURATION.key(),
            StatementCacheSettings.EXPIRY_DURATION.defaultValue()));
    connectionExpiryUnit = TimeUnit.valueOf(
        info.getProperty(StatementCacheSettings.EXPIRY_UNIT.key(),
            StatementCacheSettings.EXPIRY_UNIT.defaultValue()));
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

  /**
   * Converts from JDBC metadata to Avatica columns.
   */
  protected static List<ColumnMetaData>
  columns(ResultSetMetaData metaData) throws SQLException {
    if (metaData == null) {
      return Collections.emptyList();
    }
    final List<ColumnMetaData> columns = new ArrayList<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      final SqlType sqlType = SqlType.valueOf(metaData.getColumnType(i));
      final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(sqlType.internal);
      ColumnMetaData.AvaticaType t =
          ColumnMetaData.scalar(metaData.getColumnType(i),
              metaData.getColumnTypeName(i), rep);
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
   * Converts from JDBC metadata to Avatica parameters.
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
      ParameterMetaData parameterMetaData, String sql,
      Meta.StatementType statementType) throws  SQLException {
    final CursorFactory cf = CursorFactory.LIST;  // because JdbcResultSet#frame
    return new Signature(columns(metaData), sql, parameters(parameterMetaData),
        null, cf, statementType);
  }

  protected static Signature signature(ResultSetMetaData metaData)
      throws SQLException {
    return signature(metaData, null, null, null);
  }

  public Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle ch) {
    try {
      final Map<DatabaseProperty, Object> map = new HashMap<>();
      final DatabaseMetaData metaData = getConnection(ch.id).getMetaData();
      for (DatabaseProperty p : DatabaseProperty.values()) {
        addProperty(map, metaData, p);
      }
      return map;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static Object addProperty(Map<DatabaseProperty, Object> map,
      DatabaseMetaData metaData, DatabaseProperty p) throws SQLException {
    try {
      return map.put(p, p.method.invoke(metaData));
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getTables(ConnectionHandle ch, String catalog, Pat schemaPattern,
      Pat tableNamePattern, List<String> typeList) {
    try {
      final ResultSet rs =
          getConnection(ch.id).getMetaData().getTables(catalog, schemaPattern.s,
              tableNamePattern.s, toArray(typeList));
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
      Pat tableNamePattern, Pat columnNamePattern) {
    try {
      final ResultSet rs =
          getConnection(ch.id).getMetaData().getColumns(catalog, schemaPattern.s,
              tableNamePattern.s, columnNamePattern.s);
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs, UNLIMITED_COUNT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
    try {
      final ResultSet rs =
          getConnection(ch.id).getMetaData().getSchemas(catalog, schemaPattern.s);
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs, UNLIMITED_COUNT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getCatalogs(ConnectionHandle ch) {
    try {
      final ResultSet rs = getConnection(ch.id).getMetaData().getCatalogs();
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs, UNLIMITED_COUNT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getTableTypes(ConnectionHandle ch) {
    try {
      final ResultSet rs = getConnection(ch.id).getMetaData().getTableTypes();
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs, UNLIMITED_COUNT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getProcedures(ConnectionHandle ch, String catalog, Pat schemaPattern,
      Pat procedureNamePattern) {
    try {
      final ResultSet rs =
          getConnection(ch.id).getMetaData().getProcedures(catalog, schemaPattern.s,
              procedureNamePattern.s);
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs, UNLIMITED_COUNT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getProcedureColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
      Pat procedureNamePattern, Pat columnNamePattern) {
    try {
      final ResultSet rs =
          getConnection(ch.id).getMetaData().getProcedureColumns(catalog,
              schemaPattern.s, procedureNamePattern.s, columnNamePattern.s);
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs, UNLIMITED_COUNT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getColumnPrivileges(ConnectionHandle ch, String catalog, String schema,
      String table, Pat columnNamePattern) {
    try {
      final ResultSet rs =
          getConnection(ch.id).getMetaData().getColumnPrivileges(catalog, schema,
              table, columnNamePattern.s);
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs, UNLIMITED_COUNT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getTablePrivileges(ConnectionHandle ch, String catalog, Pat schemaPattern,
      Pat tableNamePattern) {
    try {
      final ResultSet rs =
          getConnection(ch.id).getMetaData().getTablePrivileges(catalog,
              schemaPattern.s, tableNamePattern.s);
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs, UNLIMITED_COUNT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getBestRowIdentifier(ConnectionHandle ch, String catalog, String schema,
      String table, int scope, boolean nullable) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("getBestRowIdentifier catalog:" + catalog + " schema:" + schema
          + " table:" + table + " scope:" + scope + " nullable:" + nullable);
    }
    try {
      final ResultSet rs =
          getConnection(ch.id).getMetaData().getBestRowIdentifier(catalog, schema,
              table, scope, nullable);
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs, UNLIMITED_COUNT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getVersionColumns(ConnectionHandle ch, String catalog, String schema,
      String table) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("getVersionColumns catalog:" + catalog + " schema:" + schema + " table:" + table);
    }
    try {
      final ResultSet rs =
          getConnection(ch.id).getMetaData().getVersionColumns(catalog, schema, table);
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs, UNLIMITED_COUNT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getPrimaryKeys(ConnectionHandle ch, String catalog, String schema,
      String table) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("getPrimaryKeys catalog:" + catalog + " schema:" + schema + " table:" + table);
    }
    try {
      final ResultSet rs =
          getConnection(ch.id).getMetaData().getPrimaryKeys(catalog, schema, table);
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs, UNLIMITED_COUNT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getImportedKeys(ConnectionHandle ch, String catalog, String schema,
      String table) {
    return null;
  }

  public MetaResultSet getExportedKeys(ConnectionHandle ch, String catalog, String schema,
      String table) {
    return null;
  }

  public MetaResultSet getCrossReference(ConnectionHandle ch, String parentCatalog,
      String parentSchema, String parentTable, String foreignCatalog,
      String foreignSchema, String foreignTable) {
    return null;
  }

  public MetaResultSet getTypeInfo(ConnectionHandle ch) {
    try {
      final ResultSet rs = getConnection(ch.id).getMetaData().getTypeInfo();
      return JdbcResultSet.create(DEFAULT_CONN_ID, -1, rs, UNLIMITED_COUNT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public MetaResultSet getIndexInfo(ConnectionHandle ch, String catalog, String schema,
      String table, boolean unique, boolean approximate) {
    return null;
  }

  public MetaResultSet getUDTs(ConnectionHandle ch, String catalog, Pat schemaPattern,
      Pat typeNamePattern, int[] types) {
    return null;
  }

  public MetaResultSet getSuperTypes(ConnectionHandle ch, String catalog, Pat schemaPattern,
      Pat typeNamePattern) {
    return null;
  }

  public MetaResultSet getSuperTables(ConnectionHandle ch, String catalog, Pat schemaPattern,
      Pat tableNamePattern) {
    return null;
  }

  public MetaResultSet getAttributes(ConnectionHandle ch, String catalog, Pat schemaPattern,
      Pat typeNamePattern, Pat attributeNamePattern) {
    return null;
  }

  public MetaResultSet getClientInfoProperties(ConnectionHandle ch) {
    return null;
  }

  public MetaResultSet getFunctions(ConnectionHandle ch, String catalog, Pat schemaPattern,
      Pat functionNamePattern) {
    return null;
  }

  public MetaResultSet getFunctionColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
      Pat functionNamePattern, Pat columnNamePattern) {
    return null;
  }

  public MetaResultSet getPseudoColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
      Pat tableNamePattern, Pat columnNamePattern) {
    return null;
  }

  public Iterable<Object> createIterable(StatementHandle handle,
      Signature signature, List<TypedValue> parameterValues, Frame firstFrame) {
    return null;
  }

  protected Connection getConnection(String id) throws SQLException {
    if (id == null) {
      throw new NullPointerException("Connection id is null.");
    }
    Connection conn = connectionCache.getIfPresent(id);
    if (conn == null) {
      throw new RuntimeException("Connection not found: invalid id, closed, or expired: " + id);
    }
    return conn;
  }

  public StatementHandle createStatement(ConnectionHandle ch) {
    try {
      final Connection conn = getConnection(ch.id);
      final Statement statement = conn.createStatement();
      final int id = statementIdGenerator.getAndIncrement();
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

  @Override
  public void openConnection(ConnectionHandle ch, Map<String, String> info) {
    Properties fullInfo = new Properties();
    fullInfo.putAll(this.info);
    if (info != null) {
      fullInfo.putAll(info);
    }

    synchronized (this) {
      try {
        if (connectionCache.asMap().containsKey(ch.id)) {
          throw new RuntimeException("Connection already exists: " + ch.id);
        }
        Connection conn = DriverManager.getConnection(url, fullInfo);
        connectionCache.put(ch.id, conn);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
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

  protected void apply(Connection conn, ConnectionProperties connProps)
      throws SQLException {
    if (connProps.isAutoCommit() != null) {
      conn.setAutoCommit(connProps.isAutoCommit());
    }
    if (connProps.isReadOnly() != null) {
      conn.setReadOnly(connProps.isReadOnly());
    }
    if (connProps.getTransactionIsolation() != null) {
      conn.setTransactionIsolation(connProps.getTransactionIsolation());
    }
    if (connProps.getCatalog() != null) {
      conn.setCatalog(connProps.getCatalog());
    }
    if (connProps.getSchema() != null) {
      conn.setSchema(connProps.getSchema());
    }
  }

  @Override public ConnectionProperties connectionSync(ConnectionHandle ch,
      ConnectionProperties connProps) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("syncing properties for connection " + ch);
    }
    try {
      Connection conn = getConnection(ch.id);
      ConnectionPropertiesImpl props = new ConnectionPropertiesImpl(conn).merge(connProps);
      if (props.isDirty()) {
        apply(conn, props);
        props.setDirty(false);
      }
      return props;
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  private RuntimeException propagate(Throwable e) {
    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    } else if (e instanceof Error) {
      throw (Error) e;
    } else {
      throw new RuntimeException(e.getMessage());
    }
  }

  public StatementHandle prepare(ConnectionHandle ch, String sql,
      long maxRowCount) {
    try {
      final Connection conn = getConnection(ch.id);
      final PreparedStatement statement = conn.prepareStatement(sql);
      final int id = statementIdGenerator.getAndIncrement();
      Meta.StatementType statementType = null;
      if (statement.isWrapperFor(AvaticaPreparedStatement.class)) {
        final AvaticaPreparedStatement avaticaPreparedStatement;
        avaticaPreparedStatement =
            statement.unwrap(AvaticaPreparedStatement.class);
        statementType = avaticaPreparedStatement.getStatementType();
      }
      statementCache.put(id, new StatementInfo(statement));
      StatementHandle h = new StatementHandle(ch.id, id,
          signature(statement.getMetaData(), statement.getParameterMetaData(),
              sql, statementType));
      if (LOG.isTraceEnabled()) {
        LOG.trace("prepared statement " + h);
      }
      return h;
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  public ExecuteResult prepareAndExecute(StatementHandle h, String sql,
      long maxRowCount, PrepareCallback callback) {
    try {
      final StatementInfo info = statementCache.getIfPresent(h.id);
      if (info == null) {
        throw new RuntimeException("Statement not found, potentially expired. "
            + h);
      }
      final Statement statement = info.statement;
      // Special handling of maxRowCount as JDBC 0 is unlimited, our meta 0 row
      if (maxRowCount > 0) {
        AvaticaUtils.setLargeMaxRows(statement, maxRowCount);
      } else if (maxRowCount < 0) {
        statement.setMaxRows(0);
      }
      boolean ret = statement.execute(sql);
      info.resultSet = statement.getResultSet();
      assert ret || info.resultSet == null;
      final List<MetaResultSet> resultSets = new ArrayList<>();
      if (info.resultSet == null) {
        // Create a special result set that just carries update count
        resultSets.add(
            JdbcResultSet.count(h.connectionId, h.id,
                AvaticaUtils.getLargeUpdateCount(statement)));
      } else {
        resultSets.add(
            JdbcResultSet.create(h.connectionId, h.id, info.resultSet,
                maxRowCount));
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("prepAndExec statement " + h);
      }
      // TODO: review client to ensure statementId is updated when appropriate
      return new ExecuteResult(resultSets);
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("fetching " + h + " offset:" + offset + " fetchMaxRowCount:"
          + fetchMaxRowCount);
    }
    try {
      final StatementInfo statementInfo = Objects.requireNonNull(
          statementCache.getIfPresent(h.id),
          "Statement not found, potentially expired. " + h);
      if (statementInfo.resultSet == null) {
        return Frame.EMPTY;
      } else {
        return JdbcResultSet.frame(statementInfo.resultSet, offset,
            fetchMaxRowCount, calendar);
      }
    } catch (SQLException e) {
      throw propagate(e);
    }
  }

  private static String[] toArray(List<String> typeList) {
    if (typeList == null) {
      return null;
    }
    return typeList.toArray(new String[typeList.size()]);
  }

  @Override public ExecuteResult execute(StatementHandle h,
      List<TypedValue> parameterValues, long maxRowCount) {
    try {
      if (MetaImpl.checkParameterValueHasNull(parameterValues)) {
        throw new SQLException("exception while executing query: unbound parameter");
      }

      final StatementInfo statementInfo = Objects.requireNonNull(
          statementCache.getIfPresent(h.id),
          "Statement not found, potentially expired. " + h);
      final List<MetaResultSet> resultSets = new ArrayList<>();
      final PreparedStatement preparedStatement =
          (PreparedStatement) statementInfo.statement;

      if (parameterValues != null) {
        for (int i = 0; i < parameterValues.size(); i++) {
          TypedValue o = parameterValues.get(i);
          preparedStatement.setObject(i + 1, o.toJdbc(calendar));
        }
      }

      if (preparedStatement.execute()) {
        final Meta.Frame frame;
        final Signature signature2;
        if (preparedStatement.isWrapperFor(AvaticaPreparedStatement.class)) {
          signature2 = h.signature;
        } else {
          h.signature = signature(preparedStatement.getMetaData(),
              preparedStatement.getParameterMetaData(), h.signature.sql,
              Meta.StatementType.SELECT);
          signature2 = h.signature;
        }

        statementInfo.resultSet = preparedStatement.getResultSet();
        if (statementInfo.resultSet == null) {
          frame = Frame.EMPTY;
          resultSets.add(JdbcResultSet.empty(h.connectionId, h.id, signature2));
        } else {
          resultSets.add(
              JdbcResultSet.create(h.connectionId, h.id,
                  statementInfo.resultSet, maxRowCount, signature2));
        }
      } else {
        resultSets.add(
            JdbcResultSet.count(
                h.connectionId, h.id, preparedStatement.getUpdateCount()));
      }

      return new ExecuteResult(resultSets);
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

  /** Configurable statement cache settings. */
  public enum StatementCacheSettings {
    /** JDBC connection property for setting connection cache concurrency level. */
    CONCURRENCY_LEVEL(STMT_CACHE_KEY_BASE + ".concurrency", "100"),

    /** JDBC connection property for setting connection cache initial capacity. */
    INITIAL_CAPACITY(STMT_CACHE_KEY_BASE + ".initialcapacity", "1000"),

    /** JDBC connection property for setting connection cache maximum capacity. */
    MAX_CAPACITY(STMT_CACHE_KEY_BASE + ".maxcapacity", "10000"),

    /** JDBC connection property for setting connection cache expiration duration.
     *
     * <p>Used in conjunction with {@link #EXPIRY_UNIT}.</p>
     */
    EXPIRY_DURATION(STMT_CACHE_KEY_BASE + ".expirydiration", "5"),

    /** JDBC connection property for setting connection cache expiration unit.
     *
     * <p>Used in conjunction with {@link #EXPIRY_DURATION}.</p>
     */
    EXPIRY_UNIT(STMT_CACHE_KEY_BASE + ".expiryunit", TimeUnit.MINUTES.name());

    private final String key;
    private final String defaultValue;

    StatementCacheSettings(String key, String defaultValue) {
      this.key = key;
      this.defaultValue = defaultValue;
    }

    /** The configuration key for specifying this setting. */
    public String key() {
      return key;
    }

    /** The default value for this setting. */
    public String defaultValue() {
      return defaultValue;
    }
  }

  /** Configurable connection cache settings. */
  public enum ConnectionCacheSettings {
    /** JDBC connection property for setting connection cache concurrency level. */
    CONCURRENCY_LEVEL(CONN_CACHE_KEY_BASE + ".concurrency", "10"),

    /** JDBC connection property for setting connection cache initial capacity. */
    INITIAL_CAPACITY(CONN_CACHE_KEY_BASE + ".initialcapacity", "100"),

    /** JDBC connection property for setting connection cache maximum capacity. */
    MAX_CAPACITY(CONN_CACHE_KEY_BASE + ".maxcapacity", "1000"),

    /** JDBC connection property for setting connection cache expiration duration. */
    EXPIRY_DURATION(CONN_CACHE_KEY_BASE + ".expiryduration", "10"),

    /** JDBC connection property for setting connection cache expiration unit. */
    EXPIRY_UNIT(CONN_CACHE_KEY_BASE + ".expiryunit", TimeUnit.MINUTES.name());

    private final String key;
    private final String defaultValue;

    ConnectionCacheSettings(String key, String defaultValue) {
      this.key = key;
      this.defaultValue = defaultValue;
    }

    /** The configuration key for specifying this setting. */
    public String key() {
      return key;
    }

    /** The default value for this setting. */
    public String defaultValue() {
      return defaultValue;
    }
  }

  /** Callback for {@link #connectionCache} member expiration. */
  private class ConnectionExpiryHandler
      implements RemovalListener<String, Connection> {

    public void onRemoval(RemovalNotification<String, Connection> notification) {
      String connectionId = notification.getKey();
      Connection doomed = notification.getValue();
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
}

// End JdbcMeta.java
