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

import org.eigenbase.util.Util;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

/**
 * Implementation of {@link java.sql.PreparedStatement}
 * for the Optiq engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link Factory#newPreparedStatement}.</p>
 */
abstract class OptiqPreparedStatement
    extends OptiqStatement
    implements PreparedStatement, ParameterMetaData
{
  private final OptiqPrepare.PrepareResult<?> prepareResult;
  private final ResultSetMetaData resultSetMetaData;

  /**
   * Creates an OptiqPreparedStatement.
   *
   * @param connection Connection
   * @param sql MDX query string
   *
   * @throws SQLException if database error occurs
   */
  protected OptiqPreparedStatement(
      OptiqConnectionImpl connection,
      String sql,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability)
      throws SQLException {
    super(
        connection, resultSetType, resultSetConcurrency,
        resultSetHoldability);
    this.prepareResult = parseQuery(sql);
    this.resultSetMetaData =
        connection.factory.newResultSetMetaData(
            this, prepareResult.columnList);
  }

  // implement PreparedStatement

  public ResultSet executeQuery() throws SQLException {
    return executeQueryInternal(prepareResult);
  }

  public ParameterMetaData getParameterMetaData() throws SQLException {
    return this;
  }

  public int executeUpdate() throws SQLException {
    throw new UnsupportedOperationException(); // TODO:
  }

  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    getParameter(parameterIndex).setNull(sqlType);
  }

  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    getParameter(parameterIndex).setBoolean(x);
  }

  public void setByte(int parameterIndex, byte x) throws SQLException {
    getParameter(parameterIndex).setByte(x);
  }

  public void setShort(int parameterIndex, short x) throws SQLException {
    getParameter(parameterIndex).setShort(x);
  }

  public void setInt(int parameterIndex, int x) throws SQLException {
    getParameter(parameterIndex).setInt(x);
  }

  public void setLong(int parameterIndex, long x) throws SQLException {
    getParameter(parameterIndex).setValue(x);
  }

  public void setFloat(int parameterIndex, float x) throws SQLException {
    getParameter(parameterIndex).setFloat(x);
  }

  public void setDouble(int parameterIndex, double x) throws SQLException {
    getParameter(parameterIndex).setDouble(x);
  }

  public void setBigDecimal(
      int parameterIndex, BigDecimal x) throws SQLException {
    getParameter(parameterIndex).setBigDecimal(x);
  }

  public void setString(int parameterIndex, String x) throws SQLException {
    getParameter(parameterIndex).setString(x);
  }

  public void setBytes(int parameterIndex, byte x[]) throws SQLException {
    getParameter(parameterIndex).setBytes(x);
  }

  public void setDate(int parameterIndex, Date x) throws SQLException {
    getParameter(parameterIndex).setDate(x);
  }

  public void setTime(int parameterIndex, Time x) throws SQLException {
    getParameter(parameterIndex).setTime(x);
  }

  public void setTimestamp(
      int parameterIndex, Timestamp x) throws SQLException {
    getParameter(parameterIndex).setTimestamp(x);
  }

  public void setAsciiStream(
      int parameterIndex, InputStream x, int length) throws SQLException {
    getParameter(parameterIndex).setAsciiStream(x, length);
  }

  public void setUnicodeStream(
      int parameterIndex, InputStream x, int length) throws SQLException {
    getParameter(parameterIndex).setUnicodeStream(x, length);
  }

  public void setBinaryStream(
      int parameterIndex, InputStream x, int length) throws SQLException {
    getParameter(parameterIndex).setBinaryStream(x, length);
  }

  public void clearParameters() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setObject(
      int parameterIndex, Object x, int targetSqlType) throws SQLException {
    getParameter(parameterIndex).setObject(x, targetSqlType);
  }

  public void setObject(int parameterIndex, Object x) throws SQLException {
    getParameter(parameterIndex).setObject(x);
  }

  public boolean execute() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void addBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setCharacterStream(
      int parameterIndex, Reader reader, int length) throws SQLException {
    getParameter(parameterIndex).setCharacterStream(reader, length);
  }

  public void setRef(int parameterIndex, Ref x) throws SQLException {
    getParameter(parameterIndex).setRef(x);
  }

  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    getParameter(parameterIndex).setBlob(x);
  }

  public void setClob(int parameterIndex, Clob x) throws SQLException {
    getParameter(parameterIndex).setClob(x);
  }

  public void setArray(int parameterIndex, Array x) throws SQLException {
    getParameter(parameterIndex).setArray(x);
  }

  public ResultSetMetaData getMetaData() {
    return resultSetMetaData;
  }

  public void setDate(
      int parameterIndex, Date x, Calendar cal) throws SQLException {
    getParameter(parameterIndex).setDate(x, cal);
  }

  public void setTime(
      int parameterIndex, Time x, Calendar cal) throws SQLException {
    getParameter(parameterIndex).setTime(x, cal);
  }

  public void setTimestamp(
      int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    getParameter(parameterIndex).setTimestamp(x, cal);
  }

  public void setNull(
      int parameterIndex, int sqlType, String typeName) throws SQLException {
    getParameter(parameterIndex).setNull(sqlType, typeName);
  }

  public void setURL(int parameterIndex, URL x) throws SQLException {
    getParameter(parameterIndex).setURL(x);
  }

  public void setObject(
      int parameterIndex,
      Object x,
      int targetSqlType,
      int scaleOrLength) throws SQLException {
    getParameter(parameterIndex).setObject(x, targetSqlType, scaleOrLength);
  }

  // implement ParameterMetaData

  // not JDBC
  public String getParameterName(int index) throws SQLException {
    return getParameter(index).name;
  }

  // not JDBC
  public boolean isSet(int index) throws SQLException {
    return getParameter(index).isSet();
  }

  protected OptiqPrepare.Parameter getParameter(int param) throws SQLException {
    try {
      return prepareResult.parameterList.get(param - 1);
    } catch (IndexOutOfBoundsException e) {
      //noinspection ThrowableResultOfMethodCallIgnored
      throw connection.helper.toSQLException(
          connection.helper.createException(
              "parameter ordinal " + param + " out of range"));
    }
  }

  public int getParameterCount() {
    return prepareResult.parameterList.size();
  }

  public int isNullable(int param) throws SQLException {
    return ParameterMetaData.parameterNullableUnknown;
  }

  public boolean isSigned(int index) throws SQLException {
    return getParameter(index).signed;
  }

  public int getPrecision(int index) throws SQLException {
    return getParameter(index).precision;
  }

  public int getScale(int index) throws SQLException {
    return getParameter(index).scale;
  }

  public int getParameterType(int index) throws SQLException {
    return getParameter(index).parameterType;
  }

  public String getParameterTypeName(int index) throws SQLException {
    return getParameter(index).typeName;
  }

  public String getParameterClassName(int index) throws SQLException {
    return getParameter(index).className;
  }

  public int getParameterMode(int param) throws SQLException {
    OptiqPrepare.Parameter paramDef =
        getParameter(param); // forces param range check
    Util.discard(paramDef);
    return ParameterMetaData.parameterModeIn;
  }

  // Helper classes

  static class Type {}
}

// End OptiqPreparedStatement.java
