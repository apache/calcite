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
package org.apache.calcite.adapter.file.execution.duckdb;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Wrapper;
import java.util.logging.Logger;

/**
 * DataSource wrapper for DuckDB connections.
 * This allows us to use ResultSetEnumerable with DuckDB.
 */
public class DuckDBDataSource implements DataSource {
  private final String schemaName;
  
  public DuckDBDataSource(String schemaName) {
    this.schemaName = schemaName;
  }
  
  @Override
  public Connection getConnection() throws SQLException {
    // Get the shared connection from the connection manager
    Connection conn = DuckDBConnectionManager.getConnection(schemaName);
    // Return a wrapper that prevents closing the shared connection
    return new NonClosingConnectionWrapper(conn);
  }
  
  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    // DuckDB doesn't use authentication for in-memory databases
    return getConnection();
  }
  
  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return null;
  }
  
  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    // Not implemented
  }
  
  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    // Not implemented
  }
  
  @Override
  public int getLoginTimeout() throws SQLException {
    return 0;
  }
  
  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }
  
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface);
  }
  
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }
  
  /**
   * Connection wrapper that prevents closing the underlying shared connection.
   */
  private static class NonClosingConnectionWrapper implements Connection {
    private final Connection delegate;
    
    NonClosingConnectionWrapper(Connection connection) {
      this.delegate = connection;
    }
    
    @Override
    public void close() throws SQLException {
      // Don't close the shared connection
      // Just clean up any resources we might have created
    }
    
    // Delegate all other methods to the underlying connection
    @Override
    public java.sql.Statement createStatement() throws SQLException {
      return delegate.createStatement();
    }
    
    @Override
    public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {
      return delegate.prepareStatement(sql);
    }
    
    @Override
    public java.sql.CallableStatement prepareCall(String sql) throws SQLException {
      return delegate.prepareCall(sql);
    }
    
    @Override
    public String nativeSQL(String sql) throws SQLException {
      return delegate.nativeSQL(sql);
    }
    
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
      delegate.setAutoCommit(autoCommit);
    }
    
    @Override
    public boolean getAutoCommit() throws SQLException {
      return delegate.getAutoCommit();
    }
    
    @Override
    public void commit() throws SQLException {
      delegate.commit();
    }
    
    @Override
    public void rollback() throws SQLException {
      delegate.rollback();
    }
    
    @Override
    public boolean isClosed() throws SQLException {
      return delegate.isClosed();
    }
    
    @Override
    public java.sql.DatabaseMetaData getMetaData() throws SQLException {
      return delegate.getMetaData();
    }
    
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
      delegate.setReadOnly(readOnly);
    }
    
    @Override
    public boolean isReadOnly() throws SQLException {
      return delegate.isReadOnly();
    }
    
    @Override
    public void setCatalog(String catalog) throws SQLException {
      delegate.setCatalog(catalog);
    }
    
    @Override
    public String getCatalog() throws SQLException {
      return delegate.getCatalog();
    }
    
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
      delegate.setTransactionIsolation(level);
    }
    
    @Override
    public int getTransactionIsolation() throws SQLException {
      return delegate.getTransactionIsolation();
    }
    
    @Override
    public java.sql.SQLWarning getWarnings() throws SQLException {
      return delegate.getWarnings();
    }
    
    @Override
    public void clearWarnings() throws SQLException {
      delegate.clearWarnings();
    }
    
    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
      return delegate.createStatement(resultSetType, resultSetConcurrency);
    }
    
    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }
    
    @Override
    public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
    }
    
    @Override
    public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {
      return delegate.getTypeMap();
    }
    
    @Override
    public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {
      delegate.setTypeMap(map);
    }
    
    @Override
    public void setHoldability(int holdability) throws SQLException {
      delegate.setHoldability(holdability);
    }
    
    @Override
    public int getHoldability() throws SQLException {
      return delegate.getHoldability();
    }
    
    @Override
    public java.sql.Savepoint setSavepoint() throws SQLException {
      return delegate.setSavepoint();
    }
    
    @Override
    public java.sql.Savepoint setSavepoint(String name) throws SQLException {
      return delegate.setSavepoint(name);
    }
    
    @Override
    public void rollback(java.sql.Savepoint savepoint) throws SQLException {
      delegate.rollback(savepoint);
    }
    
    @Override
    public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException {
      delegate.releaseSavepoint(savepoint);
    }
    
    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }
    
    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }
    
    @Override
    public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }
    
    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
      return delegate.prepareStatement(sql, autoGeneratedKeys);
    }
    
    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
      return delegate.prepareStatement(sql, columnIndexes);
    }
    
    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
      return delegate.prepareStatement(sql, columnNames);
    }
    
    @Override
    public java.sql.Clob createClob() throws SQLException {
      return delegate.createClob();
    }
    
    @Override
    public java.sql.Blob createBlob() throws SQLException {
      return delegate.createBlob();
    }
    
    @Override
    public java.sql.NClob createNClob() throws SQLException {
      return delegate.createNClob();
    }
    
    @Override
    public java.sql.SQLXML createSQLXML() throws SQLException {
      return delegate.createSQLXML();
    }
    
    @Override
    public boolean isValid(int timeout) throws SQLException {
      return delegate.isValid(timeout);
    }
    
    @Override
    public void setClientInfo(String name, String value) throws java.sql.SQLClientInfoException {
      delegate.setClientInfo(name, value);
    }
    
    @Override
    public void setClientInfo(java.util.Properties properties) throws java.sql.SQLClientInfoException {
      delegate.setClientInfo(properties);
    }
    
    @Override
    public String getClientInfo(String name) throws SQLException {
      return delegate.getClientInfo(name);
    }
    
    @Override
    public java.util.Properties getClientInfo() throws SQLException {
      return delegate.getClientInfo();
    }
    
    @Override
    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
      return delegate.createArrayOf(typeName, elements);
    }
    
    @Override
    public java.sql.Struct createStruct(String typeName, Object[] attributes) throws SQLException {
      return delegate.createStruct(typeName, attributes);
    }
    
    @Override
    public void setSchema(String schema) throws SQLException {
      delegate.setSchema(schema);
    }
    
    @Override
    public String getSchema() throws SQLException {
      return delegate.getSchema();
    }
    
    @Override
    public void abort(java.util.concurrent.Executor executor) throws SQLException {
      delegate.abort(executor);
    }
    
    @Override
    public void setNetworkTimeout(java.util.concurrent.Executor executor, int milliseconds) throws SQLException {
      delegate.setNetworkTimeout(executor, milliseconds);
    }
    
    @Override
    public int getNetworkTimeout() throws SQLException {
      return delegate.getNetworkTimeout();
    }
    
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      if (iface.isInstance(this)) {
        return iface.cast(this);
      }
      return delegate.unwrap(iface);
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return iface.isInstance(this) || delegate.isWrapperFor(iface);
    }
  }
}