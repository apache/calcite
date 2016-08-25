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

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link java.sql.Statement}
 * for the Avatica engine.
 */
public abstract class AvaticaStatement
    implements Statement {
  /** The default value for {@link Statement#getFetchSize()}. */
  public static final int DEFAULT_FETCH_SIZE = 100;

  public final AvaticaConnection connection;
  /** Statement id; unique within connection. */
  public Meta.StatementHandle handle;
  protected boolean closed;

  /** Support for {@link #cancel()} method. */
  protected final AtomicBoolean cancelFlag;

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

  /** Current update count. Same lifecycle as {@link #openResultSet}. */
  protected long updateCount;

  private int queryTimeoutMillis;
  final int resultSetType;
  final int resultSetConcurrency;
  final int resultSetHoldability;
  private int fetchSize = DEFAULT_FETCH_SIZE;
  private int fetchDirection;
  protected long maxRowCount = 0;

  private Meta.Signature signature;

  private final List<String> batchedSql;

  protected void setSignature(Meta.Signature signature) {
    this.signature = signature;
  }

  protected Meta.Signature getSignature() {
    return signature;
  }

  public Meta.StatementType getStatementType() {
    return signature.statementType;
  }

  /**
   * Creates an AvaticaStatement.
   *
   * @param connection Connection
   * @param h Statement handle
   * @param resultSetType Result set type
   * @param resultSetConcurrency Result set concurrency
   * @param resultSetHoldability Result set holdability
   */
  protected AvaticaStatement(AvaticaConnection connection,
      Meta.StatementHandle h, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) {
    this(connection, h, resultSetType, resultSetConcurrency, resultSetHoldability, null);
  }

  protected AvaticaStatement(AvaticaConnection connection,
      Meta.StatementHandle h, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability, Meta.Signature signature) {
    this.connection = Objects.requireNonNull(connection);
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.resultSetHoldability = resultSetHoldability;
    this.signature = signature;
    this.closed = false;
    if (h == null) {
      final Meta.ConnectionHandle ch = connection.handle;
      h = connection.meta.createStatement(ch);
    }
    connection.statementMap.put(h.id, this);
    this.handle = h;
    this.batchedSql = new ArrayList<>();
    try {
      this.cancelFlag = connection.getCancelFlag(h);
    } catch (NoSuchStatementException e) {
      throw new AssertionError("no statement", e);
    }
  }

  /** Returns the identifier of the statement, unique within its connection. */
  public int getId() {
    return handle.id;
  }

  private void checkNotPreparedOrCallable(String s) throws SQLException {
    if (this instanceof PreparedStatement
        || this instanceof CallableStatement) {
      throw connection.helper.createException("Cannot call " + s
          + " on prepared or callable statement");
    }
  }

  protected void executeInternal(String sql) throws SQLException {
    // reset previous state before moving forward.
    this.updateCount = -1;
    try {
      // In JDBC, maxRowCount = 0 means no limit; in prepare it means LIMIT 0
      final long maxRowCount1 = maxRowCount <= 0 ? -1 : maxRowCount;
      for (int i = 0; i < connection.maxRetriesPerExecute; i++) {
        try {
          Meta.ExecuteResult x =
              connection.prepareAndExecuteInternal(this, sql, maxRowCount1);
          return;
        } catch (NoSuchStatementException e) {
          resetStatement();
        }
      }
    } catch (RuntimeException e) {
      throw connection.helper.createException("Error while executing SQL \"" + sql + "\": "
          + e.getMessage(), e);
    }

    throw new RuntimeException("Failed to successfully execute query after "
        + connection.maxRetriesPerExecute + " attempts.");
  }

  /**
   * Executes a collection of updates in a single batch RPC.
   *
   * @return an array of long mapping to the update count per SQL command.
   */
  protected long[] executeBatchInternal() throws SQLException {
    for (int i = 0; i < connection.maxRetriesPerExecute; i++) {
      try {
        return connection.prepareAndUpdateBatch(this, batchedSql).updateCounts;
      } catch (NoSuchStatementException e) {
        resetStatement();
      }
    }

    throw new RuntimeException("Failed to successfully execute batch update after "
        +  connection.maxRetriesPerExecute + " attempts");
  }

  protected void resetStatement() {
    // Invalidate the old statement
    connection.statementMap.remove(handle.id);
    connection.flagMap.remove(handle.id);
    // Get a new one
    final Meta.ConnectionHandle ch = new Meta.ConnectionHandle(connection.id);
    Meta.StatementHandle h = connection.meta.createStatement(ch);
    // Cache it in the connection
    connection.statementMap.put(h.id, this);
    // Update the local state and try again
    this.handle = h;
  }

  /**
   * Re-initialize the ResultSet on the server with the given state.
   * @param state The ResultSet's state.
   * @param offset Offset into the desired ResultSet
   * @return True if the ResultSet has more results, false if there are no more results.
   */
  protected boolean syncResults(QueryState state, long offset) throws NoSuchStatementException {
    return connection.meta.syncResults(handle, state, offset);
  }

  // implement Statement

  public boolean execute(String sql) throws SQLException {
    checkNotPreparedOrCallable("execute(String)");
    executeInternal(sql);
    // Result set is null for DML or DDL.
    // Result set is closed if user cancelled the query.
    return openResultSet != null && !openResultSet.isClosed();
  }

  public ResultSet executeQuery(String sql) throws SQLException {
    checkNotPreparedOrCallable("executeQuery(String)");
    try {
      executeInternal(sql);
      if (openResultSet == null) {
        throw connection.helper.createException(
            "Statement did not return a result set");
      }
      return openResultSet;
    } catch (RuntimeException e) {
      throw connection.helper.createException("Error while executing SQL \"" + sql + "\": "
          + e.getMessage(), e);
    }
  }

  public final int executeUpdate(String sql) throws SQLException {
    return AvaticaUtils.toSaturatedInt(executeLargeUpdate(sql));
  }

  public long executeLargeUpdate(String sql) throws SQLException {
    checkNotPreparedOrCallable("executeUpdate(String)");
    executeInternal(sql);
    return updateCount;
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
      try {
        // inform the server to close the resource
        connection.meta.closeStatement(handle);
      } finally {
        // make sure we don't leak on our side
        connection.statementMap.remove(handle.id);
        connection.flagMap.remove(handle.id);
      }
      // If onStatementClose throws, this method will throw an exception (later
      // converted to SQLException), but this statement still gets closed.
      connection.driver.handler.onStatementClose(this);
    }
  }

  public int getMaxFieldSize() throws SQLException {
    throw connection.helper.unsupported();
  }

  public void setMaxFieldSize(int max) throws SQLException {
    throw connection.helper.unsupported();
  }

  public final int getMaxRows() {
    return AvaticaUtils.toSaturatedInt(getLargeMaxRows());
  }

  public long getLargeMaxRows() {
    return maxRowCount;
  }

  public final void setMaxRows(int maxRowCount) throws SQLException {
    setLargeMaxRows(maxRowCount);
  }

  public void setLargeMaxRows(long maxRowCount) throws SQLException {
    if (maxRowCount < 0) {
      throw connection.helper.createException(
          "illegal maxRows value: " + maxRowCount);
    }
    this.maxRowCount = maxRowCount;
  }

  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw connection.helper.unsupported();
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
    // If there is an open result set, it probably just set the same flag.
    cancelFlag.compareAndSet(false, true);
  }

  public SQLWarning getWarnings() throws SQLException {
    return null; // no warnings, since warnings are not supported
  }

  public void clearWarnings() throws SQLException {
    // no-op since warnings are not supported
  }

  public void setCursorName(String name) throws SQLException {
    throw connection.helper.unsupported();
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
    return AvaticaUtils.toSaturatedInt(updateCount);
  }

  public long getLargeUpdateCount() throws SQLException {
    return updateCount;
  }

  public boolean getMoreResults() throws SQLException {
    throw connection.helper.unsupported();
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
    throw connection.helper.unsupported();
  }

  public int getResultSetType() throws SQLException {
    throw connection.helper.unsupported();
  }

  public void addBatch(String sql) throws SQLException {
    this.batchedSql.add(Objects.requireNonNull(sql));
  }

  public void clearBatch() throws SQLException {
    this.batchedSql.clear();
  }

  public int[] executeBatch() throws SQLException {
    return AvaticaUtils.toSaturatedInts(executeLargeBatch());
  }

  public long[] executeLargeBatch() throws SQLException {
    try {
      return executeBatchInternal();
    } finally {
      // If we failed to send this batch, that's a problem for the user to handle, not us.
      // Make sure we always clear the statements we collected to submit in one RPC.
      clearBatch();
    }
  }

  public AvaticaConnection getConnection() {
    return connection;
  }

  public boolean getMoreResults(int current) throws SQLException {
    throw connection.helper.unsupported();
  }

  public ResultSet getGeneratedKeys() throws SQLException {
    throw connection.helper.unsupported();
  }

  public int executeUpdate(
      String sql, int autoGeneratedKeys) throws SQLException {
    throw connection.helper.unsupported();
  }

  public int executeUpdate(
      String sql, int[] columnIndexes) throws SQLException {
    throw connection.helper.unsupported();
  }

  public int executeUpdate(
      String sql, String[] columnNames) throws SQLException {
    throw connection.helper.unsupported();
  }

  public boolean execute(
      String sql, int autoGeneratedKeys) throws SQLException {
    throw connection.helper.unsupported();
  }

  public boolean execute(
      String sql, int[] columnIndexes) throws SQLException {
    throw connection.helper.unsupported();
  }

  public boolean execute(
      String sql, String[] columnNames) throws SQLException {
    throw connection.helper.unsupported();
  }

  public int getResultSetHoldability() throws SQLException {
    throw connection.helper.unsupported();
  }

  public boolean isClosed() throws SQLException {
    return closed;
  }

  public void setPoolable(boolean poolable) throws SQLException {
    throw connection.helper.unsupported();
  }

  public boolean isPoolable() throws SQLException {
    throw connection.helper.unsupported();
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
   * Executes a prepared statement.
   *
   * @param signature Parsed statement
   * @param isUpdate if the execute is for an update
   *
   * @return as specified by {@link java.sql.Statement#execute(String)}
   * @throws java.sql.SQLException if a database error occurs
   */
  protected boolean executeInternal(Meta.Signature signature, boolean isUpdate)
      throws SQLException {
    ResultSet resultSet = executeQueryInternal(signature, isUpdate);
    // user may have cancelled the query
    if (resultSet.isClosed()) {
      return false;
    }
    return true;
  }

  /**
   * Executes a prepared query, closing any previously open result set.
   *
   * @param signature Parsed query
   * @param isUpdate If the execute is for an update
   * @return Result set
   * @throws java.sql.SQLException if a database error occurs
   */
  protected ResultSet executeQueryInternal(Meta.Signature signature, boolean isUpdate)
      throws SQLException {
    return connection.executeQueryInternal(this, signature, null, null, isUpdate);
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
   * @see org.apache.calcite.avatica.AvaticaConnection.Trojan#getParameterValues(AvaticaStatement)
   */
  protected List<TypedValue> getParameterValues() {
    return Collections.emptyList();
  }

  /** Returns a list of bound parameter values.
   *
   * <p>If any of the parameters have not been bound, throws.
   * If parameters have been bound to null, the value in the list is null.
   */
  protected List<TypedValue> getBoundParameterValues() throws SQLException {
    final List<TypedValue> parameterValues = getParameterValues();
    for (Object parameterValue : parameterValues) {
      if (parameterValue == null) {
        throw new SQLException("unbound parameter");
      }
    }
    return parameterValues;
  }

}

// End AvaticaStatement.java
