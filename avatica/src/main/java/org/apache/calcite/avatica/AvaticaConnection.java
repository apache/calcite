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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Implementation of JDBC connection
 * for the Avatica framework.
 *
 * <p>Abstract to allow newer versions of JDBC to add methods.
 */
public abstract class AvaticaConnection implements Connection {

  protected int statementCount;
  private boolean closed;
  private int holdability;
  private int networkTimeout;

  public final String id;
  public final Meta.ConnectionHandle handle;
  protected final UnregisteredDriver driver;
  protected final AvaticaFactory factory;
  final String url;
  protected final Properties info;
  protected final Meta meta;
  protected final AvaticaDatabaseMetaData metaData;
  public final Helper helper = Helper.INSTANCE;
  public final Map<InternalProperty, Object> properties = new HashMap<>();
  public final Map<Integer, AvaticaStatement> statementMap =
      new ConcurrentHashMap<>();

  /**
   * Creates an AvaticaConnection.
   *
   * <p>Not public; method is called only from the driver or a derived
   * class.</p>
   *
   * @param driver Driver
   * @param factory Factory for JDBC objects
   * @param url Server URL
   * @param info Other connection properties
   */
  protected AvaticaConnection(UnregisteredDriver driver,
      AvaticaFactory factory,
      String url,
      Properties info) {
    this.id = UUID.randomUUID().toString();
    this.handle = new Meta.ConnectionHandle(this.id);
    this.driver = driver;
    this.factory = factory;
    this.url = url;
    this.info = info;
    this.meta = driver.createMeta(this);
    this.metaData = factory.newDatabaseMetaData(this);
    this.holdability = metaData.getResultSetHoldability();
  }

  /** Returns a view onto this connection's configuration properties. Code
   * in Avatica and derived projects should use this view rather than calling
   * {@link java.util.Properties#getProperty(String)}. Derived projects will
   * almost certainly subclass {@link ConnectionConfig} with their own
   * properties. */
  public ConnectionConfig config() {
    return new ConnectionConfigImpl(info);
  }

  // Connection methods

  public AvaticaStatement createStatement() throws SQLException {
    //noinspection MagicConstant
    return createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        holdability);
  }

  public PreparedStatement prepareStatement(String sql) throws SQLException {
    //noinspection MagicConstant
    return prepareStatement(
        sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        holdability);
  }

  public CallableStatement prepareCall(String sql) throws SQLException {
    throw helper.unsupported();
  }

  public String nativeSQL(String sql) throws SQLException {
    throw helper.unsupported();
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    meta.connectionSync(handle, new ConnectionPropertiesImpl().setAutoCommit(autoCommit));
  }

  public boolean getAutoCommit() throws SQLException {
    return meta.connectionSync(handle, new ConnectionPropertiesImpl()).isAutoCommit();
  }

  public void commit() throws SQLException {
    throw helper.unsupported();
  }

  public void rollback() throws SQLException {
    throw helper.unsupported();
  }

  public void close() throws SQLException {
    if (!closed) {
      closed = true;

      // Per specification, if onConnectionClose throws, this method will throw
      // a SQLException, but statement will still be closed.
      try {
        meta.closeConnection(handle);
        driver.handler.onConnectionClose(this);
      } catch (RuntimeException e) {
        throw helper.createException("While closing connection", e);
      }
    }
  }

  public boolean isClosed() throws SQLException {
    return closed;
  }

  public DatabaseMetaData getMetaData() throws SQLException {
    return metaData;
  }

  public void setReadOnly(boolean readOnly) throws SQLException {
    meta.connectionSync(handle, new ConnectionPropertiesImpl().setReadOnly(readOnly));
  }

  public boolean isReadOnly() throws SQLException {
    return meta.connectionSync(handle, new ConnectionPropertiesImpl()).isReadOnly();
  }

  public void setCatalog(String catalog) throws SQLException {
    meta.connectionSync(handle, new ConnectionPropertiesImpl().setCatalog(catalog));
  }

  public String getCatalog() {
    return meta.connectionSync(handle, new ConnectionPropertiesImpl()).getCatalog();
  }

  public void setTransactionIsolation(int level) throws SQLException {
    meta.connectionSync(handle, new ConnectionPropertiesImpl().setTransactionIsolation(level));
  }

  public int getTransactionIsolation() throws SQLException {
    //noinspection MagicConstant
    return meta.connectionSync(handle, new ConnectionPropertiesImpl()).getTransactionIsolation();
  }

  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  public void clearWarnings() throws SQLException {
    // no-op since connection pooling often calls this.
  }

  public Statement createStatement(
      int resultSetType, int resultSetConcurrency) throws SQLException {
    //noinspection MagicConstant
    return createStatement(resultSetType, resultSetConcurrency, holdability);
  }

  public PreparedStatement prepareStatement(
      String sql,
      int resultSetType,
      int resultSetConcurrency) throws SQLException {
    //noinspection MagicConstant
    return prepareStatement(
        sql, resultSetType, resultSetConcurrency, holdability);
  }

  public CallableStatement prepareCall(
      String sql,
      int resultSetType,
      int resultSetConcurrency) throws SQLException {
    throw helper.unsupported();
  }

  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw helper.unsupported();
  }

  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw helper.unsupported();
  }

  public void setHoldability(int holdability) throws SQLException {
    if (!(holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT
        || holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
      throw new SQLException("invalid value");
    }
    this.holdability = holdability;
  }

  public int getHoldability() throws SQLException {
    return holdability;
  }

  public Savepoint setSavepoint() throws SQLException {
    throw helper.unsupported();
  }

  public Savepoint setSavepoint(String name) throws SQLException {
    throw helper.unsupported();
  }

  public void rollback(Savepoint savepoint) throws SQLException {
    throw helper.unsupported();
  }

  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw helper.unsupported();
  }

  public AvaticaStatement createStatement(
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    return factory.newStatement(this, null, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  public PreparedStatement prepareStatement(
      String sql,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    try {
      final Meta.ConnectionHandle ch = new Meta.ConnectionHandle(id);
      final Meta.StatementHandle h = meta.prepare(ch, sql, -1);
      return factory.newPreparedStatement(this, h, h.signature, resultSetType,
          resultSetConcurrency, resultSetHoldability);
    } catch (RuntimeException e) {
      throw helper.createException("while preparing SQL: " + sql, e);
    }
  }

  public CallableStatement prepareCall(
      String sql,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    throw helper.unsupported();
  }

  public PreparedStatement prepareStatement(
      String sql, int autoGeneratedKeys) throws SQLException {
    throw helper.unsupported();
  }

  public PreparedStatement prepareStatement(
      String sql, int[] columnIndexes) throws SQLException {
    throw helper.unsupported();
  }

  public PreparedStatement prepareStatement(
      String sql, String[] columnNames) throws SQLException {
    throw helper.unsupported();
  }

  public Clob createClob() throws SQLException {
    throw helper.unsupported();
  }

  public Blob createBlob() throws SQLException {
    throw helper.unsupported();
  }

  public NClob createNClob() throws SQLException {
    throw helper.unsupported();
  }

  public SQLXML createSQLXML() throws SQLException {
    throw helper.unsupported();
  }

  public boolean isValid(int timeout) throws SQLException {
    throw helper.unsupported();
  }

  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
    throw helper.clientInfo();
  }

  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
    throw helper.clientInfo();
  }

  public String getClientInfo(String name) throws SQLException {
    throw helper.unsupported();
  }

  public Properties getClientInfo() throws SQLException {
    throw helper.unsupported();
  }

  public Array createArrayOf(String typeName, Object[] elements)
      throws SQLException {
    throw helper.unsupported();
  }

  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    throw helper.unsupported();
  }

  public void setSchema(String schema) throws SQLException {
    meta.connectionSync(handle, new ConnectionPropertiesImpl().setSchema(schema));
  }

  public String getSchema() {
    return meta.connectionSync(handle, new ConnectionPropertiesImpl()).getSchema();
  }

  public void abort(Executor executor) throws SQLException {
    throw helper.unsupported();
  }

  public void setNetworkTimeout(
      Executor executor, int milliseconds) throws SQLException {
    this.networkTimeout = milliseconds;
  }

  public int getNetworkTimeout() throws SQLException {
    return networkTimeout;
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw helper.createException(
        "does not implement '" + iface + "'");
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }

  /** Returns the time zone of this connection. Determines the offset applied
   * when converting datetime values from the database into
   * {@link java.sql.Timestamp} values. */
  public TimeZone getTimeZone() {
    final String timeZoneName = config().timeZone();
    return timeZoneName == null
        ? TimeZone.getDefault()
        : TimeZone.getTimeZone(timeZoneName);
  }

  /**
   * Executes a prepared query, closing any previously open result set.
   *
   * @param statement     Statement
   * @param signature     Prepared query
   * @param firstFrame    First frame of rows, or null if we need to execute
   * @return Result set
   * @throws java.sql.SQLException if a database error occurs
   */
  protected ResultSet executeQueryInternal(AvaticaStatement statement,
      Meta.Signature signature, Meta.Frame firstFrame) throws SQLException {
    // Close the previous open result set, if there is one.
    synchronized (statement) {
      if (statement.openResultSet != null) {
        final AvaticaResultSet rs = statement.openResultSet;
        statement.openResultSet = null;
        try {
          rs.close();
        } catch (Exception e) {
          throw helper.createException(
              "Error while closing previous result set", e);
        }
      }

      final TimeZone timeZone = getTimeZone();
      statement.openResultSet =
          factory.newResultSet(statement, signature, timeZone, firstFrame);
    }
    // Release the monitor before executing, to give another thread the
    // opportunity to call cancel.
    try {
      statement.openResultSet.execute();
    } catch (Exception e) {
      throw helper.createException(
          "exception while executing query: " + e.getMessage(), e);
    }
    return statement.openResultSet;
  }

  protected Meta.ExecuteResult prepareAndExecuteInternal(
      final AvaticaStatement statement, String sql, int maxRowCount)
      throws SQLException {
    final Meta.PrepareCallback callback =
        new Meta.PrepareCallback() {
          public Object getMonitor() {
            return statement;
          }

          public void clear() throws SQLException {
            if (statement.openResultSet != null) {
              final AvaticaResultSet rs = statement.openResultSet;
              statement.openResultSet = null;
              try {
                rs.close();
              } catch (Exception e) {
                throw helper.createException(
                    "Error while closing previous result set", e);
              }
            }
          }

          public void assign(Meta.Signature signature, Meta.Frame firstFrame,
              int updateCount) throws SQLException {
            if (updateCount != -1) {
              statement.updateCount = updateCount;
            } else {
              final TimeZone timeZone = getTimeZone();
              statement.openResultSet = factory.newResultSet(statement,
                  signature, timeZone, firstFrame);
            }
          }

          public void execute() throws SQLException {
            if (statement.openResultSet != null) {
              statement.openResultSet.execute();
            }
          }
        };
    return meta.prepareAndExecute(statement.handle, sql, maxRowCount, callback);
  }

  protected ResultSet createResultSet(Meta.MetaResultSet metaResultSet)
      throws SQLException {
    final Meta.StatementHandle h = new Meta.StatementHandle(
        metaResultSet.connectionId, metaResultSet.statementId, null);
    final AvaticaStatement statement = lookupStatement(h);
    return executeQueryInternal(statement, metaResultSet.signature.sanitize(),
        metaResultSet.firstFrame);
  }

  /** Creates a statement wrapper around an existing handle. */
  protected AvaticaStatement lookupStatement(Meta.StatementHandle h)
      throws SQLException {
    final AvaticaStatement statement = statementMap.get(h.id);
    if (statement != null) {
      return statement;
    }
    //noinspection MagicConstant
    return factory.newStatement(this, Objects.requireNonNull(h),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, holdability);
  }

  // do not make public
  protected static Trojan createTrojan() {
    return new Trojan();
  }

  /** A way to call package-protected methods. But only a sub-class of
   * connection can create one. */
  public static class Trojan {
    // must be private
    private Trojan() {
    }

    /** A means for anyone who has a trojan to call the protected method
     * {@link org.apache.calcite.avatica.AvaticaResultSet#execute()}.
     * @throws SQLException if execute fails for some reason. */
    public ResultSet execute(AvaticaResultSet resultSet) throws SQLException {
      return resultSet.execute();
    }

    /** A means for anyone who has a trojan to call the protected method
     * {@link org.apache.calcite.avatica.AvaticaStatement#getParameterValues()}.
     */
    public List<TypedValue> getParameterValues(AvaticaStatement statement) {
      return statement.getParameterValues();
    }

    /** A means for anyone who has a trojan to get the protected field
     * {@link org.apache.calcite.avatica.AvaticaConnection#meta}. */
    public Meta getMeta(AvaticaConnection connection) {
      return connection.meta;
    }
  }
}

// End AvaticaConnection.java
