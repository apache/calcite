/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.jdbc;

import net.hydromatic.linq4j.function.Function0;
import net.hydromatic.optiq.runtime.ColumnMetaData;
import net.hydromatic.optiq.runtime.Cursor;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.List;
import java.util.Properties;

/**
 * Implementation of {@link Factory} for JDBC 4.1 (corresponds to JDK 1.7).
 */
@SuppressWarnings("UnusedDeclaration")
class FactoryJdbc41 implements Factory {
  private final int major;
  private final int minor;

  /** Creates a JDBC factory. */
  public FactoryJdbc41() {
    this(4, 1);
  }

  /** Creates a JDBC factory with given major/minor version number. */
  protected FactoryJdbc41(int major, int minor) {
    this.major = major;
    this.minor = minor;
  }

  public int getJdbcMajorVersion() {
    return major;
  }

  public int getJdbcMinorVersion() {
    return minor;
  }

  public OptiqConnectionImpl newConnection(
      UnregisteredDriver driver,
      Factory factory,
      Function0<OptiqPrepare> prepareFactory,
      String url,
      Properties info) {
    return new OptiqConnectionJdbc41(
        driver, factory, prepareFactory, url, info);
  }

  public OptiqDatabaseMetaData newDatabaseMetaData(
      OptiqConnectionImpl connection) {
    return new OptiqDatabaseMetaDataJdbc41(connection);
  }

  public OptiqStatement newStatement(
      OptiqConnectionImpl connection,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) {
    return new OptiqStatementJdbc41(
        connection, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  public OptiqPreparedStatement newPreparedStatement(
      OptiqConnectionImpl connection,
      String sql,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    return new OptiqPreparedStatementJdbc41(
        connection, sql, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  public OptiqResultSet newResultSet(
      OptiqStatement statement,
      List<ColumnMetaData> columnMetaDataList,
      Function0<Cursor> cursorFactory) {
    final ResultSetMetaData metaData =
        newResultSetMetaData(statement, columnMetaDataList);
    return new OptiqResultSet(
        statement, columnMetaDataList, metaData, cursorFactory);
  }

  public ResultSetMetaData newResultSetMetaData(
      OptiqStatement statement,
      List<ColumnMetaData> columnMetaDataList) {
    return new OptiqResultSetMetaData(
        statement, null, columnMetaDataList);
  }

  private static class OptiqConnectionJdbc41 extends OptiqConnectionImpl {
    OptiqConnectionJdbc41(
        UnregisteredDriver driver,
        Factory factory,
        Function0<OptiqPrepare> prepareFactory,
        String url,
        Properties info) {
      super(driver, factory, prepareFactory, url, info);
    }
  }

  private static class OptiqStatementJdbc41 extends OptiqStatement {
    public OptiqStatementJdbc41(
        OptiqConnectionImpl connection,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability) {
      super(
          connection, resultSetType, resultSetConcurrency,
          resultSetHoldability);
    }

    public void closeOnCompletion() throws SQLException {
      this.closeOnCompletion = true;
    }

    public boolean isCloseOnCompletion() throws SQLException {
      return closeOnCompletion;
    }
  }

  private static class OptiqPreparedStatementJdbc41
      extends OptiqPreparedStatement {
    OptiqPreparedStatementJdbc41(
        OptiqConnectionImpl connection,
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability) throws SQLException {
      super(
          connection, sql, resultSetType, resultSetConcurrency,
          resultSetHoldability);
    }

    public void setRowId(
        int parameterIndex,
        RowId x) throws SQLException {
      getParameter(parameterIndex).setRowId(x);
    }

    public void setNString(
        int parameterIndex, String value) throws SQLException {
      getParameter(parameterIndex).setNString(value);
    }

    public void setNCharacterStream(
        int parameterIndex,
        Reader value,
        long length) throws SQLException {
      getParameter(parameterIndex).setNCharacterStream(value, length);
    }

    public void setNClob(
        int parameterIndex,
        NClob value) throws SQLException {
      getParameter(parameterIndex).setNClob(value);
    }

    public void setClob(
        int parameterIndex,
        Reader reader,
        long length) throws SQLException {
      getParameter(parameterIndex).setClob(reader, length);
    }

    public void setBlob(
        int parameterIndex,
        InputStream inputStream,
        long length) throws SQLException {
      getParameter(parameterIndex).setBlob(inputStream, length);
    }

    public void setNClob(
        int parameterIndex,
        Reader reader,
        long length) throws SQLException {
      getParameter(parameterIndex).setNClob(reader, length);
    }

    public void setSQLXML(
        int parameterIndex, SQLXML xmlObject) throws SQLException {
      getParameter(parameterIndex).setSQLXML(xmlObject);
    }

    public void setAsciiStream(
        int parameterIndex,
        InputStream x,
        long length) throws SQLException {
      getParameter(parameterIndex).setAsciiStream(x, length);
    }

    public void setBinaryStream(
        int parameterIndex,
        InputStream x,
        long length) throws SQLException {
      getParameter(parameterIndex).setBinaryStream(x, length);
    }

    public void setCharacterStream(
        int parameterIndex,
        Reader reader,
        long length) throws SQLException {
      getParameter(parameterIndex).setCharacterStream(reader, length);
    }

    public void setAsciiStream(
        int parameterIndex, InputStream x) throws SQLException {
      getParameter(parameterIndex).setAsciiStream(x);
    }

    public void setBinaryStream(
        int parameterIndex, InputStream x) throws SQLException {
      getParameter(parameterIndex).setBinaryStream(x);
    }

    public void setCharacterStream(
        int parameterIndex, Reader reader) throws SQLException {
      getParameter(parameterIndex).setCharacterStream(reader);
    }

    public void setNCharacterStream(
        int parameterIndex, Reader value) throws SQLException {
      getParameter(parameterIndex).setNCharacterStream(value);
    }

    public void setClob(
        int parameterIndex,
        Reader reader) throws SQLException {
      getParameter(parameterIndex).setClob(reader);
    }

    public void setBlob(
        int parameterIndex, InputStream inputStream) throws SQLException {
      getParameter(parameterIndex).setBlob(inputStream);
    }

    public void setNClob(
        int parameterIndex, Reader reader) throws SQLException {
      getParameter(parameterIndex).setNClob(reader);
    }

    public void closeOnCompletion() throws SQLException {
      closeOnCompletion = true;
    }

    public boolean isCloseOnCompletion() throws SQLException {
      return closeOnCompletion;
    }
  }

  private static class OptiqDatabaseMetaDataJdbc41
      extends OptiqDatabaseMetaData {
    OptiqDatabaseMetaDataJdbc41(OptiqConnectionImpl connection) {
      super(connection);
    }
  }
}

// End FactoryJdbc41.java
