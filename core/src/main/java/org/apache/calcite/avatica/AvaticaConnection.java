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

import org.apache.calcite.avatica.ColumnMetaData.AvaticaType;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.remote.KerberosConnection;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.Service.ErrorResponse;
import org.apache.calcite.avatica.remote.Service.OpenConnectionRequest;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.ArrayFactoryImpl;

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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of JDBC connection
 * for the Avatica framework.
 *
 * <p>Abstract to allow newer versions of JDBC to add methods.
 */
public abstract class AvaticaConnection implements Connection {

  /** The name of the sole column returned by DML statements, containing
   * the number of rows modified. */
  public static final String ROWCOUNT_COLUMN_NAME = "ROWCOUNT";

  public static final String NUM_EXECUTE_RETRIES_KEY = "avatica.statement.retries";
  public static final String NUM_EXECUTE_RETRIES_DEFAULT = "5";

  /** The name of the sole column returned by an EXPLAIN statement.
   *
   * <p>Actually Avatica does not care what this column is called, but here is
   * a useful place to define a suggested value. */
  public static final String PLAN_COLUMN_NAME = "PLAN";

  public static final Helper HELPER = Helper.INSTANCE;

  protected int statementCount;
  private boolean closed;
  private int holdability;
  private int networkTimeout;
  private KerberosConnection kerberosConnection;
  private Service service;

  public final String id;
  public final Meta.ConnectionHandle handle;
  protected final UnregisteredDriver driver;
  protected final AvaticaFactory factory;
  final String url;
  protected final Properties info;
  protected final Meta meta;
  protected final AvaticaSpecificDatabaseMetaData metaData;
  public final Map<InternalProperty, Object> properties = new HashMap<>();
  public final Map<Integer, AvaticaStatement> statementMap = new ConcurrentHashMap<>();
  final Map<Integer, AtomicBoolean> flagMap = new ConcurrentHashMap<>();
  protected final long maxRetriesPerExecute;

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
    try {
      this.holdability = metaData.getResultSetHoldability();
    } catch (SQLException e) {
      // We know the impl doesn't throw this.
      throw new RuntimeException(e);
    }
    this.maxRetriesPerExecute = getNumStatementRetries(info);
  }

  /** Computes the number of retries
   * {@link AvaticaStatement#executeInternal(Meta.Signature, boolean)}
   * should retry before failing. */
  long getNumStatementRetries(Properties props) {
    return Long.parseLong(Objects.requireNonNull(props)
        .getProperty(NUM_EXECUTE_RETRIES_KEY, NUM_EXECUTE_RETRIES_DEFAULT));
  }

  /** Returns a view onto this connection's configuration properties. Code
   * in Avatica and derived projects should use this view rather than calling
   * {@link java.util.Properties#getProperty(String)}. Derived projects will
   * almost certainly subclass {@link ConnectionConfig} with their own
   * properties. */
  public ConnectionConfig config() {
    return new ConnectionConfigImpl(info);
  }

  /**
   * Opens the connection on the server.
   */
  public void openConnection() {
    // Open the connection on the server
    this.meta.openConnection(handle, OpenConnectionRequest.serializeProperties(info));
  }

  protected void checkOpen() throws SQLException {
    if (isClosed()) {
      throw HELPER.closed();
    }
  }
  // Connection methods

  public AvaticaStatement createStatement() throws SQLException {
    checkOpen();
    //noinspection MagicConstant
    return createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        holdability);
  }

  public PreparedStatement prepareStatement(String sql) throws SQLException {
    checkOpen();
    //noinspection MagicConstant
    return prepareStatement(
        sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        holdability);
  }

  public CallableStatement prepareCall(String sql) throws SQLException {
    throw HELPER.unsupported();
  }

  public String nativeSQL(String sql) throws SQLException {
    throw HELPER.unsupported();
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    checkOpen();
    meta.connectionSync(handle, new ConnectionPropertiesImpl().setAutoCommit(autoCommit));
  }

  public boolean getAutoCommit() throws SQLException {
    checkOpen();
    return unbox(sync().isAutoCommit(), true);
  }

  public void commit() throws SQLException {
    checkOpen();
    meta.commit(handle);
  }

  public void rollback() throws SQLException {
    checkOpen();
    meta.rollback(handle);
  }

  public void close() throws SQLException {
    if (!closed) {
      closed = true;

      // Per specification, if onConnectionClose throws, this method will throw
      // a SQLException, but statement will still be closed.
      try {
        meta.closeConnection(handle);
        driver.handler.onConnectionClose(this);
        if (null != kerberosConnection) {
          kerberosConnection.stopRenewalThread();
        }
      } catch (RuntimeException e) {
        throw HELPER.createException("While closing connection", e);
      }
    }
  }

  public boolean isClosed() throws SQLException {
    return closed;
  }

  public DatabaseMetaData getMetaData() throws SQLException {
    checkOpen();
    return metaData;
  }

  public void setReadOnly(boolean readOnly) throws SQLException {
    checkOpen();
    meta.connectionSync(handle, new ConnectionPropertiesImpl().setReadOnly(readOnly));
  }

  public boolean isReadOnly() throws SQLException {
    checkOpen();
    return unbox(sync().isReadOnly(), true);
  }

  public void setCatalog(String catalog) throws SQLException {
    checkOpen();
    meta.connectionSync(handle, new ConnectionPropertiesImpl().setCatalog(catalog));
  }

  public String getCatalog() throws SQLException {
    checkOpen();
    return sync().getCatalog();
  }

  public void setTransactionIsolation(int level) throws SQLException {
    checkOpen();
    meta.connectionSync(handle, new ConnectionPropertiesImpl().setTransactionIsolation(level));
  }

  public int getTransactionIsolation() throws SQLException {
    checkOpen();
    //noinspection MagicConstant
    return unbox(sync().getTransactionIsolation(), TRANSACTION_NONE);
  }

  public SQLWarning getWarnings() throws SQLException {
    checkOpen();
    return null;
  }

  public void clearWarnings() throws SQLException {
    checkOpen();
    // no-op since connection pooling often calls this.
  }

  public Statement createStatement(
      int resultSetType, int resultSetConcurrency) throws SQLException {
    checkOpen();
    //noinspection MagicConstant
    return createStatement(resultSetType, resultSetConcurrency, holdability);
  }

  public PreparedStatement prepareStatement(
      String sql,
      int resultSetType,
      int resultSetConcurrency) throws SQLException {
    checkOpen();
    //noinspection MagicConstant
    return prepareStatement(
        sql, resultSetType, resultSetConcurrency, holdability);
  }

  public CallableStatement prepareCall(
      String sql,
      int resultSetType,
      int resultSetConcurrency) throws SQLException {
    throw HELPER.unsupported();
  }

  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw HELPER.unsupported();
  }

  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw HELPER.unsupported();
  }

  public void setHoldability(int holdability) throws SQLException {
    checkOpen();
    if (!(holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT
        || holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
      throw new SQLException("invalid value");
    }
    this.holdability = holdability;
  }

  public int getHoldability() throws SQLException {
    checkOpen();
    return holdability;
  }

  public Savepoint setSavepoint() throws SQLException {
    throw HELPER.unsupported();
  }

  public Savepoint setSavepoint(String name) throws SQLException {
    throw HELPER.unsupported();
  }

  public void rollback(Savepoint savepoint) throws SQLException {
    throw HELPER.unsupported();
  }

  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw HELPER.unsupported();
  }

  public AvaticaStatement createStatement(
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    checkOpen();
    return factory.newStatement(this, null, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  public PreparedStatement prepareStatement(
      String sql,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    checkOpen();
    try {
      final Meta.StatementHandle h = meta.prepare(handle, sql, -1);
      return factory.newPreparedStatement(this, h, h.signature, resultSetType,
          resultSetConcurrency, resultSetHoldability);
    } catch (RuntimeException e) {
      throw HELPER.createException("while preparing SQL: " + sql, e);
    }
  }

  public CallableStatement prepareCall(
      String sql,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    throw HELPER.unsupported();
  }

  public PreparedStatement prepareStatement(
      String sql, int autoGeneratedKeys) throws SQLException {
    throw HELPER.unsupported();
  }

  public PreparedStatement prepareStatement(
      String sql, int[] columnIndexes) throws SQLException {
    throw HELPER.unsupported();
  }

  public PreparedStatement prepareStatement(
      String sql, String[] columnNames) throws SQLException {
    throw HELPER.unsupported();
  }

  public Clob createClob() throws SQLException {
    throw HELPER.unsupported();
  }

  public Blob createBlob() throws SQLException {
    throw HELPER.unsupported();
  }

  public NClob createNClob() throws SQLException {
    throw HELPER.unsupported();
  }

  public SQLXML createSQLXML() throws SQLException {
    throw HELPER.unsupported();
  }

  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw HELPER.createException("timeout is less than 0");
    }

    // TODO check if connection is actually alive using timeout
    return !isClosed();
  }

  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
    throw HELPER.clientInfo();
  }

  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
    throw HELPER.clientInfo();
  }

  public String getClientInfo(String name) throws SQLException {
    return getClientInfo().getProperty(name);
  }

  public Properties getClientInfo() throws SQLException {
    checkOpen();
    return new Properties();
  }

  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    checkOpen();
    @SuppressWarnings("unchecked")
    List<Object> elementList = (List<Object>) AvaticaUtils.primitiveList(elements);
    SqlType type;
    try {
      type = SqlType.valueOf(typeName);
    } catch (IllegalArgumentException e) {
      throw new SQLException("Could not find JDBC type for '" + typeName + "'");
    }
    AvaticaType avaticaType = null;
    switch (type) {
    case ARRAY:
      // TODO: Nested ARRAYs
      throw HELPER.createException("Cannot create an ARRAY of ARRAY's");
    case STRUCT:
      // TODO: ARRAYs of STRUCTs
      throw HELPER.createException("Cannot create an ARRAY of STRUCT's");
    default:
      // This is an ARRAY, we need to use Objects, not primitives (nullable).
      avaticaType = ColumnMetaData.scalar(type.id, typeName, Rep.nonPrimitiveRepOf(type));
    }
    ArrayFactoryImpl arrayFactory = new ArrayFactoryImpl(getTimeZone());
    return arrayFactory.createArray(avaticaType, elementList);
  }

  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    throw HELPER.unsupported();
  }

  public void setSchema(String schema) throws SQLException {
    checkOpen();
    meta.connectionSync(handle, new ConnectionPropertiesImpl().setSchema(schema));
  }

  public String getSchema() throws SQLException {
    checkOpen();
    return sync().getSchema();
  }

  public void abort(Executor executor) throws SQLException {
    throw HELPER.unsupported();
  }

  public void setNetworkTimeout(
      Executor executor, int milliseconds) throws SQLException {
    checkOpen();
    this.networkTimeout = milliseconds;
  }

  public int getNetworkTimeout() throws SQLException {
    checkOpen();
    return networkTimeout;
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw HELPER.createException(
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
   * @param state         The state used to create the given result
   * @param isUpdate      Was the caller context via {@link PreparedStatement#executeUpdate()}.
   * @return Result set
   * @throws java.sql.SQLException if a database error occurs
   */
  protected ResultSet executeQueryInternal(AvaticaStatement statement,
      Meta.Signature signature, Meta.Frame firstFrame, QueryState state, boolean isUpdate)
      throws SQLException {
    // Close the previous open result set, if there is one.
    Meta.Frame frame = firstFrame;
    Meta.Signature signature2 = signature;

    synchronized (statement) {
      if (statement.openResultSet != null) {
        final AvaticaResultSet rs = statement.openResultSet;
        statement.openResultSet = null;
        try {
          rs.close();
        } catch (Exception e) {
          throw HELPER.createException(
              "Error while closing previous result set", e);
        }
      }

      try {
        if (statement.isWrapperFor(AvaticaPreparedStatement.class)) {
          final AvaticaPreparedStatement pstmt = (AvaticaPreparedStatement) statement;
          Meta.StatementHandle handle = pstmt.handle;
          if (isUpdate) {
            // Make a copy of the StatementHandle, nulling out the Signature.
            // CALCITE-1086 we don't need to send the Signature to the server
            // when we're only performing an update. Saves on serialization.
            handle = new Meta.StatementHandle(handle.connectionId, handle.id, null);
          }
          final Meta.ExecuteResult executeResult =
              meta.execute(handle, pstmt.getParameterValues(),
                  statement.getFetchSize());
          final MetaResultSet metaResultSet = executeResult.resultSets.get(0);
          frame = metaResultSet.firstFrame;
          statement.updateCount = metaResultSet.updateCount;
          signature2 = executeResult.resultSets.get(0).signature;
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw HELPER.createException(e.getMessage(), e);
      }

      final TimeZone timeZone = getTimeZone();
      if (frame == null && signature2 == null && statement.updateCount != -1) {
        statement.openResultSet = null;
      } else {
        // Duplicative SQL, for support non-prepared statements
        statement.openResultSet =
            factory.newResultSet(statement, state, signature2, timeZone, frame);
      }
    }
    // Release the monitor before executing, to give another thread the
    // opportunity to call cancel.
    try {
      if (statement.openResultSet != null) {
        statement.openResultSet.execute();
        isUpdateCapable(statement);
      }
    } catch (Exception e) {
      throw HELPER.createException(
          "exception while executing query: " + e.getMessage(), e);
    }
    return statement.openResultSet;
  }

  /** Executes a batch update using an {@link AvaticaPreparedStatement}.
   *
   * @param pstmt The prepared statement.
   * @return An array of update counts containing one element for each command in the batch.
   */
  protected long[] executeBatchUpdateInternal(AvaticaPreparedStatement pstmt) throws SQLException {
    try {
      // Get the handle from the statement
      Meta.StatementHandle handle = pstmt.handle;
      // Execute it against meta
      return meta.executeBatch(handle, pstmt.getParameterValueBatch()).updateCounts;
    } catch (Exception e) {
      throw HELPER.createException(e.getMessage(), e);
    }
  }

  /** Returns whether a a statement is capable of updates and if so,
   * and the statement's {@code updateCount} is still -1, proceeds to
   * get updateCount value from statement's resultSet.
   *
   * <p>Handles "ROWCOUNT" object as Number or List
   *
   * @param statement Statement
   * @throws SQLException on error
   */
  private void isUpdateCapable(final AvaticaStatement statement)
      throws SQLException {
    Meta.Signature signature = statement.getSignature();
    if (signature == null || signature.statementType == null) {
      return;
    }
    if (signature.statementType.canUpdate() && statement.updateCount == -1) {
      statement.openResultSet.next();
      Object obj = statement.openResultSet.getObject(ROWCOUNT_COLUMN_NAME);
      if (obj instanceof Number) {
        statement.updateCount = ((Number) obj).intValue();
      } else if (obj instanceof List) {
        @SuppressWarnings("unchecked")
        final List<Number> numbers = (List<Number>) obj;
        statement.updateCount = numbers.get(0).intValue();
      } else {
        throw HELPER.createException("Not a valid return result.");
      }
      statement.openResultSet = null;
    }
  }

  protected Meta.ExecuteResult prepareAndExecuteInternal(
      final AvaticaStatement statement, final String sql, long maxRowCount)
      throws SQLException, NoSuchStatementException {
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
                throw HELPER.createException(
                    "Error while closing previous result set", e);
              }
            }
          }

          public void assign(Meta.Signature signature, Meta.Frame firstFrame,
              long updateCount) throws SQLException {
            statement.setSignature(signature);

            if (updateCount != -1) {
              statement.updateCount = updateCount;
            } else {
              final TimeZone timeZone = getTimeZone();
              statement.openResultSet = factory.newResultSet(statement, new QueryState(sql),
                  signature, timeZone, firstFrame);
            }
          }

          public void execute() throws SQLException {
            if (statement.openResultSet != null) {
              statement.openResultSet.execute();
              isUpdateCapable(statement);
            }
          }
        };
    // The old semantics were that maxRowCount was also treated as the maximum number of
    // elements in the first Frame of results. A value of -1 would also preserve this, but an
    // explicit (positive) number is easier to follow, IMO.
    return meta.prepareAndExecute(statement.handle, sql, maxRowCount,
        AvaticaUtils.toSaturatedInt(maxRowCount), callback);
  }

  protected ExecuteBatchResult prepareAndUpdateBatch(final AvaticaStatement statement,
      final List<String> queries) throws NoSuchStatementException, SQLException {
    return meta.prepareAndExecuteBatch(statement.handle, queries);
  }

  protected ResultSet createResultSet(Meta.MetaResultSet metaResultSet, QueryState state)
      throws SQLException {
    final Meta.StatementHandle h = new Meta.StatementHandle(
        metaResultSet.connectionId, metaResultSet.statementId, null);
    final AvaticaStatement statement = lookupStatement(h);
    // These are all the metadata operations, no updates
    ResultSet resultSet = executeQueryInternal(statement, metaResultSet.signature.sanitize(),
        metaResultSet.firstFrame, state, false);
    if (metaResultSet.ownStatement) {
      resultSet.getStatement().closeOnCompletion();
    }
    return resultSet;
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

  /** Converts a {@link Boolean} to a {@code boolean}, with a default value. */
  private boolean unbox(Boolean b, boolean defaultValue) {
    return b == null ? defaultValue : b;
  }

  /** Converts an {@link Integer} to an {@code int}, with a default value. */
  private int unbox(Integer i, int defaultValue) {
    return i == null ? defaultValue : i;
  }

  private Meta.ConnectionProperties sync() {
    return meta.connectionSync(handle, new ConnectionPropertiesImpl());
  }

  /** Returns or creates a slot whose state can be changed to cancel a
   * statement. Statements will receive the same slot if and only if their id
   * is the same. */
  public AtomicBoolean getCancelFlag(Meta.StatementHandle h)
      throws NoSuchStatementException {
    AvaticaUtils.upgrade("after dropping JDK 1.7, use Map.computeIfAbsent");
    synchronized (flagMap) {
      AtomicBoolean b = flagMap.get(h.id);
      if (b == null) {
        b = new AtomicBoolean();
        flagMap.put(h.id, b);
      }
      return b;
    }
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

  /**
   * A Callable-like interface but without a "throws Exception".
   *
   * @param <T> The return type from {@code call}.
   */
  public interface CallableWithoutException<T> {
    T call();
  }

  /**
   * Invokes the given "callable", retrying the call when the server responds with an error
   * denoting that the connection is missing on the server.
   *
   * @param callable The function to invoke.
   * @return The value from the result of the callable.
   */
  public <T> T invokeWithRetries(CallableWithoutException<T> callable) {
    RuntimeException lastException = null;
    for (int i = 0; i < maxRetriesPerExecute; i++) {
      try {
        return callable.call();
      } catch (AvaticaClientRuntimeException e) {
        lastException = e;
        if (ErrorResponse.MISSING_CONNECTION_ERROR_CODE == e.getErrorCode()) {
          this.openConnection();
          continue;
        }
        throw e;
      }
    }
    if (null != lastException) {
      throw lastException;
    } else {
      // Shouldn't ever happen.
      throw new IllegalStateException();
    }
  }

  public void setKerberosConnection(KerberosConnection kerberosConnection) {
    this.kerberosConnection = Objects.requireNonNull(kerberosConnection);
  }

  public KerberosConnection getKerberosConnection() {
    return this.kerberosConnection;
  }

  public Service getService() {
    assert null != service;
    return service;
  }

  public void setService(Service service) {
    this.service = Objects.requireNonNull(service);
  }
}

// End AvaticaConnection.java
