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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

/**
 * Implementation of {@link java.sql.PreparedStatement}
 * for the Avatica engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link AvaticaFactory#newPreparedStatement}.</p>
 */
public abstract class AvaticaPreparedStatement
    extends AvaticaStatement
    implements PreparedStatement, ParameterMetaData {
  private final Meta.Signature signature;
  private final ResultSetMetaData resultSetMetaData;
  protected final Object[] slots;

  /**
   * Creates an AvaticaPreparedStatement.
   *
   * @param connection Connection
   * @param h Statement handle
   * @param signature Result of preparing statement
   * @param resultSetType Result set type
   * @param resultSetConcurrency Result set concurrency
   * @param resultSetHoldability Result set holdability
   * @throws SQLException If fails due to underlying implementation reasons.
   */
  protected AvaticaPreparedStatement(AvaticaConnection connection,
      Meta.StatementHandle h,
      Meta.Signature signature,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    super(connection, h, resultSetType, resultSetConcurrency,
        resultSetHoldability);
    this.signature = signature;
    this.slots = new Object[signature.parameters.size()];
    this.resultSetMetaData =
        connection.factory.newResultSetMetaData(this, signature);
  }

  @Override protected List<Object> getParameterValues() {
    return Arrays.asList(slots);
  }

  // implement PreparedStatement

  public ResultSet executeQuery() throws SQLException {
    return getConnection().executeQueryInternal(this, signature, null);
  }

  public ParameterMetaData getParameterMetaData() throws SQLException {
    return this;
  }

  public int executeUpdate() throws SQLException {
    getConnection().executeQueryInternal(this, signature, null);
    return updateCount;
  }

  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    getParameter(parameterIndex).setNull(slots, parameterIndex - 1, sqlType);
  }

  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    getParameter(parameterIndex).setBoolean(slots, parameterIndex - 1, x);
  }

  public void setByte(int parameterIndex, byte x) throws SQLException {
    getParameter(parameterIndex).setByte(slots, parameterIndex - 1, x);
  }

  public void setShort(int parameterIndex, short x) throws SQLException {
    getParameter(parameterIndex).setShort(slots, parameterIndex - 1, x);
  }

  public void setInt(int parameterIndex, int x) throws SQLException {
    getParameter(parameterIndex).setInt(slots, parameterIndex - 1, x);
  }

  public void setLong(int parameterIndex, long x) throws SQLException {
    getParameter(parameterIndex).setLong(slots, parameterIndex - 1, x);
  }

  public void setFloat(int parameterIndex, float x) throws SQLException {
    getParameter(parameterIndex).setFloat(slots, parameterIndex - 1, x);
  }

  public void setDouble(int parameterIndex, double x) throws SQLException {
    getParameter(parameterIndex).setDouble(slots, parameterIndex - 1, x);
  }

  public void setBigDecimal(int parameterIndex, BigDecimal x)
      throws SQLException {
    getParameter(parameterIndex).setBigDecimal(slots, parameterIndex - 1, x);
  }

  public void setString(int parameterIndex, String x) throws SQLException {
    getParameter(parameterIndex).setString(slots, parameterIndex - 1, x);
  }

  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    getParameter(parameterIndex).setBytes(slots, parameterIndex - 1, x);
  }

  public void setDate(int parameterIndex, Date x) throws SQLException {
    getParameter(parameterIndex).setDate(slots, parameterIndex - 1, x);
  }

  public void setTime(int parameterIndex, Time x) throws SQLException {
    getParameter(parameterIndex).setTime(slots, parameterIndex - 1, x);
  }

  public void setTimestamp(int parameterIndex, Timestamp x)
      throws SQLException {
    getParameter(parameterIndex).setTimestamp(slots, parameterIndex - 1, x);
  }

  public void setAsciiStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    getParameter(parameterIndex)
        .setAsciiStream(slots, parameterIndex - 1, x, length);
  }

  public void setUnicodeStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    getParameter(parameterIndex)
        .setUnicodeStream(slots, parameterIndex - 1, x, length);
  }

  public void setBinaryStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    getParameter(parameterIndex)
        .setBinaryStream(slots, parameterIndex - 1, x, length);
  }

  public void clearParameters() throws SQLException {
    for (int i = 0; i < slots.length; i++) {
      slots[i] = null;
    }
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType)
      throws SQLException {
    getParameter(parameterIndex)
        .setObject(slots, parameterIndex - 1, x, targetSqlType);
  }

  public void setObject(int parameterIndex, Object x) throws SQLException {
    getParameter(parameterIndex).setObject(slots, parameterIndex - 1, x);
  }

  public boolean execute() throws SQLException {
    getConnection().executeQueryInternal(this, signature, null);
    // Result set is null for DML or DDL.
    // Result set is closed if user cancelled the query.
    return openResultSet != null && !openResultSet.isClosed();
  }

  public void addBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    getParameter(parameterIndex)
        .setCharacterStream(slots, parameterIndex - 1, reader, length);
  }

  public void setRef(int parameterIndex, Ref x) throws SQLException {
    getParameter(parameterIndex).setRef(slots, parameterIndex - 1, x);
  }

  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    getParameter(parameterIndex).setBlob(slots, parameterIndex - 1, x);
  }

  public void setClob(int parameterIndex, Clob x) throws SQLException {
    getParameter(parameterIndex).setClob(slots, parameterIndex - 1, x);
  }

  public void setArray(int parameterIndex, Array x) throws SQLException {
    getParameter(parameterIndex).setArray(slots, parameterIndex - 1, x);
  }

  public ResultSetMetaData getMetaData() {
    return resultSetMetaData;
  }

  public void setDate(int parameterIndex, Date x, Calendar cal)
      throws SQLException {
    getParameter(parameterIndex).setDate(slots, parameterIndex - 1, x, cal);
  }

  public void setTime(int parameterIndex, Time x, Calendar cal)
      throws SQLException {
    getParameter(parameterIndex).setTime(slots, parameterIndex - 1, x, cal);
  }

  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
      throws SQLException {
    getParameter(parameterIndex)
        .setTimestamp(slots, parameterIndex - 1, x, cal);
  }

  public void setNull(int parameterIndex, int sqlType, String typeName)
      throws SQLException {
    getParameter(parameterIndex)
        .setNull(slots, parameterIndex - 1, sqlType, typeName);
  }

  public void setURL(int parameterIndex, URL x) throws SQLException {
    getParameter(parameterIndex).setURL(slots, parameterIndex - 1, x);
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType,
      int scaleOrLength) throws SQLException {
    getParameter(parameterIndex)
        .setObject(slots, parameterIndex - 1, x, targetSqlType, scaleOrLength);
  }

  // implement ParameterMetaData

  protected AvaticaParameter getParameter(int param) throws SQLException {
    try {
      return signature.parameters.get(param - 1);
    } catch (IndexOutOfBoundsException e) {
      //noinspection ThrowableResultOfMethodCallIgnored
      throw connection.helper.toSQLException(
          connection.helper.createException(
              "parameter ordinal " + param + " out of range"));
    }
  }

  public int getParameterCount() {
    return signature.parameters.size();
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
    //noinspection UnusedDeclaration
    AvaticaParameter paramDef = getParameter(param); // forces param range check
    return ParameterMetaData.parameterModeIn;
  }
}

// End AvaticaPreparedStatement.java
