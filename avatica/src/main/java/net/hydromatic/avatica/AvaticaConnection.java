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

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * Implementation of JDBC connection
 * for the Avatica framework.
 *
 * <p>Abstract to allow newer versions of JDBC to add methods.</p>
 */
public abstract class AvaticaConnection implements Connection {
  private boolean autoCommit;
  private boolean closed;
  private boolean readOnly;
  private int transactionIsolation;
  private int holdability;
  private int networkTimeout;
  private String catalog;

  protected final UnregisteredDriver driver;
  protected final AvaticaFactory factory;
  final String url;
  protected final Properties info;
  protected final Meta meta;
  private String schema;
  protected final AvaticaDatabaseMetaData metaData;
  public final Helper helper = Helper.INSTANCE;
  public final Map<InternalProperty, Object> properties =
      new HashMap<InternalProperty, Object>();

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
    this.driver = driver;
    this.factory = factory;
    this.url = url;
    this.info = info;
    this.meta = createMeta();
    this.metaData = factory.newDatabaseMetaData(this);
    this.holdability = metaData.getResultSetHoldability();
  }

  protected Meta createMeta() {
    throw new UnsupportedOperationException();
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
    return createStatement(
        ResultSet.TYPE_FORWARD_ONLY,
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
    throw new UnsupportedOperationException();
  }

  public String nativeSQL(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    this.autoCommit = autoCommit;
  }

  public boolean getAutoCommit() throws SQLException {
    return autoCommit;
  }

  public void commit() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void rollback() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void close() throws SQLException {
    if (!closed) {
      closed = true;

      // Per specification, if onConnectionClose throws, this method will throw
      // a SQLException, but statement will still be closed.
      try {
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
    this.readOnly = readOnly;
  }

  public boolean isReadOnly() throws SQLException {
    return readOnly;
  }

  public void setCatalog(String catalog) throws SQLException {
    this.catalog = catalog;
  }

  public String getCatalog() {
    return catalog;
  }

  public void setTransactionIsolation(int level) throws SQLException {
    this.transactionIsolation = level;
  }

  public int getTransactionIsolation() throws SQLException {
    return transactionIsolation;
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
    return createStatement(
        resultSetType, resultSetConcurrency, holdability);
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
    throw new UnsupportedOperationException();
  }

  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
  }

  public Savepoint setSavepoint(String name) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void rollback(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public AvaticaStatement createStatement(
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    return factory.newStatement(
        this, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  public PreparedStatement prepareStatement(
      String sql,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException(); // TODO:
  }

  public CallableStatement prepareCall(
      String sql,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public PreparedStatement prepareStatement(
      String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public PreparedStatement prepareStatement(
      String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public PreparedStatement prepareStatement(
      String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public Clob createClob() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public Blob createBlob() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public NClob createNClob() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public SQLXML createSQLXML() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public boolean isValid(int timeout) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setClientInfo(
      String name, String value) throws SQLClientInfoException {
    throw new UnsupportedOperationException();
  }

  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
    throw new UnsupportedOperationException();
  }

  public String getClientInfo(String name) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public Properties getClientInfo() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public Array createArrayOf(
      String typeName, Object[] elements) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public Struct createStruct(
      String typeName, Object[] attributes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setSchema(String schema) throws SQLException {
    this.schema = schema;
  }

  public String getSchema() {
    return schema;
  }

  public void abort(Executor executor) throws SQLException {
    throw new UnsupportedOperationException();
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
   * Executes a parsed query, closing any previously open result set.
   *
   * @param statement     Statement
   * @param prepareResult Parsed query
   * @return Result set
   * @throws java.sql.SQLException if a database error occurs
   */
  protected ResultSet executeQueryInternal(
      AvaticaStatement statement,
      AvaticaPrepareResult prepareResult) throws SQLException {
    final TimeZone timeZone = getTimeZone();

    // Close the previous open CellSet, if there is one.
    synchronized (statement) {
      if (statement.openResultSet != null) {
        final AvaticaResultSet cs = statement.openResultSet;
        statement.openResultSet = null;
        try {
          cs.close();
        } catch (Exception e) {
          throw helper.createException(
              "Error while closing previous result set", e);
        }
      }

      statement.openResultSet =
          factory.newResultSet(
              statement, prepareResult, timeZone);
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
     * {@link net.hydromatic.avatica.AvaticaResultSet#execute()}.
     * @throws SQLException if execute fails for some reason. */
    public ResultSet execute(AvaticaResultSet resultSet) throws SQLException {
      return resultSet.execute();
    }

    /** A means for anyone who has a trojan to call the protected method
     * {@link net.hydromatic.avatica.AvaticaStatement#getParameterValues()}. */
    public List<Object> getParameterValues(AvaticaStatement statement) {
      return statement.getParameterValues();
    }
  }

}

// End AvaticaConnection.java
