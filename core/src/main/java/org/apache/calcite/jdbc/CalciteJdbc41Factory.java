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
package org.apache.calcite.jdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Implementation of {@link org.apache.calcite.avatica.AvaticaFactory}
 * for Calcite and JDBC 4.1 (corresponds to JDK 1.7).
 */
@SuppressWarnings("UnusedDeclaration")
public class CalciteJdbc41Factory extends CalciteFactory {
  /** Creates a factory for JDBC version 4.1. */
  public CalciteJdbc41Factory() {
    this(4, 1);
  }

  /** Creates a JDBC factory with given major/minor version number. */
  protected CalciteJdbc41Factory(int major, int minor) {
    super(major, minor);
  }

  public CalciteJdbc41Connection newConnection(UnregisteredDriver driver,
      AvaticaFactory factory, String url, Properties info,
      CalciteSchema rootSchema, JavaTypeFactory typeFactory) {
    return new CalciteJdbc41Connection(
        (Driver) driver, factory, url, info, rootSchema, typeFactory);
  }

  public CalciteJdbc41DatabaseMetaData newDatabaseMetaData(
      AvaticaConnection connection) {
    return new CalciteJdbc41DatabaseMetaData(
        (CalciteConnectionImpl) connection);
  }

  public CalciteJdbc41Statement newStatement(AvaticaConnection connection,
      Meta.StatementHandle h,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) {
    return new CalciteJdbc41Statement(
        (CalciteConnectionImpl) connection,
        h,
        resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  public AvaticaPreparedStatement newPreparedStatement(
      AvaticaConnection connection,
      Meta.StatementHandle h,
      Meta.Signature signature,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    return new CalciteJdbc41PreparedStatement(
        (CalciteConnectionImpl) connection, h,
        (CalcitePrepare.CalciteSignature) signature, resultSetType,
        resultSetConcurrency, resultSetHoldability);
  }

  public CalciteResultSet newResultSet(AvaticaStatement statement, QueryState state,
      Meta.Signature signature, TimeZone timeZone, Meta.Frame firstFrame)
      throws SQLException {
    final ResultSetMetaData metaData =
        newResultSetMetaData(statement, signature);
    final CalcitePrepare.CalciteSignature calciteSignature =
        (CalcitePrepare.CalciteSignature) signature;
    return new CalciteResultSet(statement, calciteSignature, metaData, timeZone,
        firstFrame);
  }

  public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement,
      Meta.Signature signature) {
    return new AvaticaResultSetMetaData(statement, null, signature);
  }

  /** Implementation of connection for JDBC 4.1. */
  private static class CalciteJdbc41Connection extends CalciteConnectionImpl {
    CalciteJdbc41Connection(Driver driver, AvaticaFactory factory, String url,
        Properties info, CalciteSchema rootSchema,
        JavaTypeFactory typeFactory) {
      super(driver, factory, url, info, rootSchema, typeFactory);
    }
  }

  /** Implementation of statement for JDBC 4.1. */
  private static class CalciteJdbc41Statement extends CalciteStatement {
    CalciteJdbc41Statement(CalciteConnectionImpl connection,
        Meta.StatementHandle h, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) {
      super(connection, h, resultSetType, resultSetConcurrency,
          resultSetHoldability);
    }
  }

  /** Implementation of prepared statement for JDBC 4.1. */
  private static class CalciteJdbc41PreparedStatement
      extends CalcitePreparedStatement {
    CalciteJdbc41PreparedStatement(CalciteConnectionImpl connection,
        Meta.StatementHandle h, CalcitePrepare.CalciteSignature signature,
        int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
      super(connection, h, signature, resultSetType, resultSetConcurrency,
          resultSetHoldability);
    }

    public void setRowId(
        int parameterIndex,
        RowId x) throws SQLException {
      getSite(parameterIndex).setRowId(x);
    }

    public void setNString(
        int parameterIndex, String value) throws SQLException {
      getSite(parameterIndex).setNString(value);
    }

    public void setNCharacterStream(
        int parameterIndex,
        Reader value,
        long length) throws SQLException {
      getSite(parameterIndex)
          .setNCharacterStream(value, length);
    }

    public void setNClob(
        int parameterIndex,
        NClob value) throws SQLException {
      getSite(parameterIndex).setNClob(value);
    }

    public void setClob(
        int parameterIndex,
        Reader reader,
        long length) throws SQLException {
      getSite(parameterIndex)
          .setClob(reader, length);
    }

    public void setBlob(
        int parameterIndex,
        InputStream inputStream,
        long length) throws SQLException {
      getSite(parameterIndex)
          .setBlob(inputStream, length);
    }

    public void setNClob(
        int parameterIndex,
        Reader reader,
        long length) throws SQLException {
      getSite(parameterIndex).setNClob(reader, length);
    }

    public void setSQLXML(
        int parameterIndex, SQLXML xmlObject) throws SQLException {
      getSite(parameterIndex).setSQLXML(xmlObject);
    }

    public void setAsciiStream(
        int parameterIndex,
        InputStream x,
        long length) throws SQLException {
      getSite(parameterIndex)
          .setAsciiStream(x, length);
    }

    public void setBinaryStream(
        int parameterIndex,
        InputStream x,
        long length) throws SQLException {
      getSite(parameterIndex)
          .setBinaryStream(x, length);
    }

    public void setCharacterStream(
        int parameterIndex,
        Reader reader,
        long length) throws SQLException {
      getSite(parameterIndex)
          .setCharacterStream(reader, length);
    }

    public void setAsciiStream(
        int parameterIndex, InputStream x) throws SQLException {
      getSite(parameterIndex).setAsciiStream(x);
    }

    public void setBinaryStream(
        int parameterIndex, InputStream x) throws SQLException {
      getSite(parameterIndex).setBinaryStream(x);
    }

    public void setCharacterStream(
        int parameterIndex, Reader reader) throws SQLException {
      getSite(parameterIndex)
          .setCharacterStream(reader);
    }

    public void setNCharacterStream(
        int parameterIndex, Reader value) throws SQLException {
      getSite(parameterIndex)
          .setNCharacterStream(value);
    }

    public void setClob(
        int parameterIndex,
        Reader reader) throws SQLException {
      getSite(parameterIndex).setClob(reader);
    }

    public void setBlob(
        int parameterIndex, InputStream inputStream) throws SQLException {
      getSite(parameterIndex)
          .setBlob(inputStream);
    }

    public void setNClob(
        int parameterIndex, Reader reader) throws SQLException {
      getSite(parameterIndex)
          .setNClob(reader);
    }
  }

  /** Implementation of database metadata for JDBC 4.1. */
  private static class CalciteJdbc41DatabaseMetaData
      extends AvaticaDatabaseMetaData {
    CalciteJdbc41DatabaseMetaData(CalciteConnectionImpl connection) {
      super(connection);
    }
  }
}

// End CalciteJdbc41Factory.java
