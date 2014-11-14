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

/**
 * Implementation of {@link java.sql.Statement}
 * for the Avatica engine.
 */
public abstract class AvaticaStatement
    implements Statement {
  protected final AvaticaConnection connection;
  protected boolean closed;

  /**
   * Support for {@link #closeOnCompletion()} method.
   */
  protected boolean closeOnCompletion;

  /**
   * Current result set, or null if the statement is not executing anything.
   * Any method which modifies this member must synchronize
   * on the AvaticaStatement.
   */
  protected AvaticaResultSet openResultSet;

  private int queryTimeoutMillis;
  final int resultSetType;
  final int resultSetConcurrency;
  final int resultSetHoldability;
  private int fetchSize;
  private int fetchDirection;
  protected int maxRowCount;

  protected AvaticaStatement(AvaticaConnection connection,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) {
    assert connection != null;
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.resultSetHoldability = resultSetHoldability;
    this.connection = connection;
    this.closed = false;
  }

  // implement Statement

  public boolean execute(String sql) throws SQLException {
    try {
      AvaticaPrepareResult x = connection.meta.prepare(this, sql);
      return executeInternal(x);
    } catch (RuntimeException e) {
      throw connection.helper.createException("while executing SQL: " + sql, e);
    }
  }

  public ResultSet executeQuery(String sql) throws SQLException {
    try {
      AvaticaPrepareResult x = connection.meta.prepare(this, sql);
      return executeQueryInternal(x);
    } catch (RuntimeException e) {
      throw connection.helper.createException(
        "error while executing SQL \"" + sql + "\": " + e.getMessage(), e);
    }
  }

  public int executeUpdate(String sql) throws SQLException {
    ResultSet resultSet = executeQuery(sql);
    if (resultSet.getMetaData().getColumnCount() != 1) {
      throw new SQLException("expected one result column");
    }
    if (!resultSet.next()) {
      throw new SQLException("expected one row, got zero");
    }
    int result = resultSet.getInt(1);
    if (resultSet.next()) {
      throw new SQLException("expected one row, got two or more");
    }
    resultSet.close();
    return result;
  }

  public synchronized void close() throws SQLException {
    try {
      close_();
    } catch (RuntimeException e) {
      throw connection.helper.createException("While closing statement", e);
    }
  }

  protected void close_() {
    if (!closed) {
      closed = true;
      if (openResultSet != null) {
        AvaticaResultSet c = openResultSet;
        openResultSet = null;
        c.close();
      }
      // If onStatementClose throws, this method will throw an exception (later
      // converted to SQLException), but this statement still gets closed.
      connection.driver.handler.onStatementClose(this);
    }
  }

  public int getMaxFieldSize() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setMaxFieldSize(int max) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int getMaxRows() {
    return maxRowCount;
  }

  public void setMaxRows(int maxRowCount) throws SQLException {
    if (maxRowCount < 0) {
      throw connection.helper.createException(
          "illegal maxRows value: " + maxRowCount);
    }
    this.maxRowCount = maxRowCount;
  }

  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int getQueryTimeout() throws SQLException {
    long timeoutSeconds = getQueryTimeoutMillis() / 1000;
    if (timeoutSeconds > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    if (timeoutSeconds == 0 && getQueryTimeoutMillis() > 0) {
      // Don't return timeout=0 if e.g. timeoutMillis=500. 0 is special.
      return 1;
    }
    return (int) timeoutSeconds;
  }

  int getQueryTimeoutMillis() {
    return queryTimeoutMillis;
  }

  public void setQueryTimeout(int seconds) throws SQLException {
    if (seconds < 0) {
      throw connection.helper.createException(
          "illegal timeout value " + seconds);
    }
    setQueryTimeoutMillis(seconds * 1000);
  }

  void setQueryTimeoutMillis(int millis) {
    this.queryTimeoutMillis = millis;
  }

  public synchronized void cancel() throws SQLException {
    if (openResultSet != null) {
      openResultSet.cancel();
    }
  }

  public SQLWarning getWarnings() throws SQLException {
    return null; // no warnings, since warnings are not supported
  }

  public void clearWarnings() throws SQLException {
    // no-op since warnings are not supported
  }

  public void setCursorName(String name) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public ResultSet getResultSet() throws SQLException {
    // NOTE: result set becomes visible in this member while
    // executeQueryInternal is still in progress, and before it has
    // finished executing. Its internal state may not be ready for API
    // calls. JDBC never claims to be thread-safe! (Except for calls to the
    // cancel method.) It is not possible to synchronize, because it would
    // block 'cancel'.
    return openResultSet;
  }

  public int getUpdateCount() throws SQLException {
    return -1;
  }

  public boolean getMoreResults() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setFetchDirection(int direction) throws SQLException {
    this.fetchDirection = direction;
  }

  public int getFetchDirection() {
    return fetchDirection;
  }

  public void setFetchSize(int rows) throws SQLException {
    this.fetchSize = rows;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public int getResultSetConcurrency() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int getResultSetType() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void addBatch(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void clearBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int[] executeBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public AvaticaConnection getConnection() {
    return connection;
  }

  public boolean getMoreResults(int current) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public ResultSet getGeneratedKeys() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int executeUpdate(
      String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int executeUpdate(
      String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int executeUpdate(
      String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public boolean execute(
      String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public boolean execute(
      String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public boolean execute(
      String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int getResultSetHoldability() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public boolean isClosed() throws SQLException {
    return closed;
  }

  public void setPoolable(boolean poolable) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public boolean isPoolable() throws SQLException {
    throw new UnsupportedOperationException();
  }

  // implements java.sql.Statement.closeOnCompletion (added in JDK 1.7)
  public void closeOnCompletion() throws SQLException {
    closeOnCompletion = true;
  }

  // implements java.sql.Statement.isCloseOnCompletion (added in JDK 1.7)
  public boolean isCloseOnCompletion() throws SQLException {
    return closeOnCompletion;
  }

  // implement Wrapper

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw connection.helper.createException(
        "does not implement '" + iface + "'");
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }

  /**
   * Executes a parsed statement.
   *
   * @param prepareResult Parsed statement
   * @return as specified by {@link java.sql.Statement#execute(String)}
   * @throws java.sql.SQLException if a database error occurs
   */
  protected boolean executeInternal(
      AvaticaPrepareResult prepareResult) throws SQLException {
    ResultSet resultSet = executeQueryInternal(prepareResult);
    return true;
  }

  /**
   * Executes a parsed query, closing any previously open result set.
   *
   * @param prepareResult Parsed query
   * @return Result set
   * @throws java.sql.SQLException if a database error occurs
   */
  protected ResultSet executeQueryInternal(
      AvaticaPrepareResult prepareResult) throws SQLException {
    return connection.executeQueryInternal(this, prepareResult);
  }

  /**
   * Called by each child result set when it is closed.
   *
   * @param resultSet Result set or cell set
   */
  void onResultSetClose(ResultSet resultSet) {
    if (closeOnCompletion) {
      close_();
    }
  }

  /** Returns the list of values of this statement's parameters.
   *
   * <p>Called at execute time. Not a public API.</p>
   *
   * <p>The default implementation returns the empty list, because non-prepared
   * statements have no parameters.</p>
   *
   * @see net.hydromatic.avatica.AvaticaConnection.Trojan#getParameterValues(AvaticaStatement)
   */
  protected List<Object> getParameterValues() {
    return Collections.emptyList();
  }
}

// End AvaticaStatement.java
