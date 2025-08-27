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
package org.apache.calcite.adapter.splunk;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * PreparedStatement wrapper that wraps ResultSets with JDBC 4.3 compliant wrappers.
 */
public class Jdbc43PreparedStatementWrapper extends Jdbc43StatementWrapper implements PreparedStatement {
  private final PreparedStatement preparedDelegate;

  public Jdbc43PreparedStatementWrapper(PreparedStatement delegate) {
    super(delegate);
    this.preparedDelegate = delegate;
  }

  @Override public ResultSet executeQuery() throws SQLException {
    ResultSet resultSet = preparedDelegate.executeQuery();
    return new Jdbc43ResultSetWrapper(resultSet);
  }

  @Override public boolean execute() throws SQLException {
    return preparedDelegate.execute();
  }

  // All other methods delegate directly to the underlying PreparedStatement

  @Override public int executeUpdate() throws SQLException {
    return preparedDelegate.executeUpdate();
  }

  @Override public void setNull(int parameterIndex, int sqlType) throws SQLException {
    preparedDelegate.setNull(parameterIndex, sqlType);
  }

  @Override public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    preparedDelegate.setNull(parameterIndex, sqlType, typeName);
  }

  @Override public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    preparedDelegate.setBoolean(parameterIndex, x);
  }

  @Override public void setByte(int parameterIndex, byte x) throws SQLException {
    preparedDelegate.setByte(parameterIndex, x);
  }

  @Override public void setShort(int parameterIndex, short x) throws SQLException {
    preparedDelegate.setShort(parameterIndex, x);
  }

  @Override public void setInt(int parameterIndex, int x) throws SQLException {
    preparedDelegate.setInt(parameterIndex, x);
  }

  @Override public void setLong(int parameterIndex, long x) throws SQLException {
    preparedDelegate.setLong(parameterIndex, x);
  }

  @Override public void setFloat(int parameterIndex, float x) throws SQLException {
    preparedDelegate.setFloat(parameterIndex, x);
  }

  @Override public void setDouble(int parameterIndex, double x) throws SQLException {
    preparedDelegate.setDouble(parameterIndex, x);
  }

  @Override public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    preparedDelegate.setBigDecimal(parameterIndex, x);
  }

  @Override public void setString(int parameterIndex, String x) throws SQLException {
    preparedDelegate.setString(parameterIndex, x);
  }

  @Override public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    preparedDelegate.setBytes(parameterIndex, x);
  }

  @Override public void setDate(int parameterIndex, Date x) throws SQLException {
    preparedDelegate.setDate(parameterIndex, x);
  }

  @Override public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    preparedDelegate.setDate(parameterIndex, x, cal);
  }

  @Override public void setTime(int parameterIndex, Time x) throws SQLException {
    preparedDelegate.setTime(parameterIndex, x);
  }

  @Override public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    preparedDelegate.setTime(parameterIndex, x, cal);
  }

  @Override public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    preparedDelegate.setTimestamp(parameterIndex, x);
  }

  @Override public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    preparedDelegate.setTimestamp(parameterIndex, x, cal);
  }

  @Override public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    preparedDelegate.setAsciiStream(parameterIndex, x, length);
  }

  @Override public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    preparedDelegate.setAsciiStream(parameterIndex, x, length);
  }

  @Override public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    preparedDelegate.setAsciiStream(parameterIndex, x);
  }

  @Override @SuppressWarnings("deprecation")
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    preparedDelegate.setUnicodeStream(parameterIndex, x, length);
  }

  @Override public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    preparedDelegate.setBinaryStream(parameterIndex, x, length);
  }

  @Override public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    preparedDelegate.setBinaryStream(parameterIndex, x, length);
  }

  @Override public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    preparedDelegate.setBinaryStream(parameterIndex, x);
  }

  @Override public void clearParameters() throws SQLException {
    preparedDelegate.clearParameters();
  }

  @Override public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    preparedDelegate.setObject(parameterIndex, x, targetSqlType);
  }

  @Override public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    preparedDelegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
  }

  @Override public void setObject(int parameterIndex, Object x) throws SQLException {
    preparedDelegate.setObject(parameterIndex, x);
  }

  @Override public void addBatch() throws SQLException {
    preparedDelegate.addBatch();
  }

  @Override public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    preparedDelegate.setCharacterStream(parameterIndex, reader, length);
  }

  @Override public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    preparedDelegate.setCharacterStream(parameterIndex, reader, length);
  }

  @Override public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    preparedDelegate.setCharacterStream(parameterIndex, reader);
  }

  @Override public void setRef(int parameterIndex, Ref x) throws SQLException {
    preparedDelegate.setRef(parameterIndex, x);
  }

  @Override public void setBlob(int parameterIndex, Blob x) throws SQLException {
    preparedDelegate.setBlob(parameterIndex, x);
  }

  @Override public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    preparedDelegate.setBlob(parameterIndex, inputStream, length);
  }

  @Override public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    preparedDelegate.setBlob(parameterIndex, inputStream);
  }

  @Override public void setClob(int parameterIndex, Clob x) throws SQLException {
    preparedDelegate.setClob(parameterIndex, x);
  }

  @Override public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    preparedDelegate.setClob(parameterIndex, reader, length);
  }

  @Override public void setClob(int parameterIndex, Reader reader) throws SQLException {
    preparedDelegate.setClob(parameterIndex, reader);
  }

  @Override public void setArray(int parameterIndex, Array x) throws SQLException {
    preparedDelegate.setArray(parameterIndex, x);
  }

  @Override public ResultSetMetaData getMetaData() throws SQLException {
    return preparedDelegate.getMetaData();
  }

  @Override public ParameterMetaData getParameterMetaData() throws SQLException {
    return preparedDelegate.getParameterMetaData();
  }

  @Override public void setRowId(int parameterIndex, RowId x) throws SQLException {
    preparedDelegate.setRowId(parameterIndex, x);
  }

  @Override public void setNString(int parameterIndex, String value) throws SQLException {
    preparedDelegate.setNString(parameterIndex, value);
  }

  @Override public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    preparedDelegate.setNCharacterStream(parameterIndex, value, length);
  }

  @Override public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    preparedDelegate.setNCharacterStream(parameterIndex, value);
  }

  @Override public void setNClob(int parameterIndex, NClob value) throws SQLException {
    preparedDelegate.setNClob(parameterIndex, value);
  }

  @Override public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    preparedDelegate.setNClob(parameterIndex, reader, length);
  }

  @Override public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    preparedDelegate.setNClob(parameterIndex, reader);
  }

  @Override public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    preparedDelegate.setSQLXML(parameterIndex, xmlObject);
  }

  @Override public void setURL(int parameterIndex, URL x) throws SQLException {
    preparedDelegate.setURL(parameterIndex, x);
  }
}
