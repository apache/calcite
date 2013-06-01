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
import net.hydromatic.optiq.runtime.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Implementation of {@link ResultSet}
 * for the Optiq engine.
 */
public class OptiqResultSet implements ResultSet {
  private final OptiqStatement statement;
  private final boolean[] wasNull = {false};
  private final List<ColumnMetaData> columnMetaDataList;
  private final Function0<Cursor> cursorFactory;
  private final ResultSetMetaData resultSetMetaData;

  private Cursor cursor;
  private List<Cursor.Accessor> accessorList;
  private Map<String, Cursor.Accessor> accessorMap =
      new HashMap<String, Cursor.Accessor>();
  private final Map<String, Integer> columnNameMap =
      new HashMap<String, Integer>();
  private int row = -1;
  private boolean afterLast;
  private int fetchDirection;
  private int fetchSize;
  private int type;
  private int concurrency;
  private int holdability;
  private boolean closed;
  private long timeoutMillis;
  private Cursor timeoutCursor;

  OptiqResultSet(
      OptiqStatement statement,
      List<ColumnMetaData> columnMetaDataList,
      ResultSetMetaData resultSetMetaData,
      Function0<Cursor> cursorFactory) {
    this.statement = statement;
    this.columnMetaDataList = columnMetaDataList;
    this.cursorFactory = cursorFactory;
    this.type = statement.resultSetType;
    this.concurrency = statement.resultSetConcurrency;
    this.holdability = statement.resultSetHoldability;
    this.fetchSize = statement.getFetchSize();
    this.fetchDirection = statement.getFetchDirection();
    this.resultSetMetaData = resultSetMetaData;
    for (ColumnMetaData column : columnMetaDataList) {
      columnNameMap.put(column.label, columnNameMap.size());
    }
  }

  private Cursor.Accessor getAccessor(int columnIndex) {
    return accessorList.get(columnIndex);
  }

  private Cursor.Accessor getAccessor(String columnLabel) {
    return accessorMap.get(columnLabel);
  }

  public void close() {
    closed = true;
    // TODO: for timeout, see IteratorResultSet.close
/*
        if (timeoutCursor != null) {
            final long noTimeout = 0;
            timeoutCursor.close(noTimeout);
            timeoutCursor = null;
        }
*/
  }

  /**
   * Sets the timeout that this result set will wait for a row from the
   * underlying iterator.
   *
   * <p>Not a JDBC method.</p>
   *
   * @param timeoutMillis Timeout in milliseconds. Must be greater than zero.
   */
  void setTimeout(long timeoutMillis) {
    assert timeoutMillis > 0;
    assert this.timeoutMillis == 0;
    this.timeoutMillis = timeoutMillis;
    assert timeoutCursor == null;
    timeoutCursor = cursor;

    // TODO: for timeout, see IteratorResultSet.setTimeout
/*
        timeoutCursor = new TimeoutCursor(timeoutMillis);
        timeoutCursor.start();
*/
  }

  // not JDBC
  void cancel() {
    // TODO:
  }

  /**
   * Executes this result set. (Not a JDBC method.)
   *
   * <p>Note that execute cannot occur in the constructor, because the
   * constructor occurs while the statement is locked, to make sure that
   * execute/cancel don't happen at the same time.</p>
   */
  OptiqResultSet execute() {
    // Call driver's callback. It is permitted to throw a RuntimeException.
    final boolean autoTemp =
        ConnectionProperty.AUTO_TEMP.getBoolean(
            statement.connection.getProperties());
    Handler.ResultSink resultSink = null;
    if (autoTemp) {
      resultSink = new Handler.ResultSink() {
        public void toBeCompleted() {
        }
      };
    }
    statement.connection.driver.handler.onStatementExecute(
        statement, resultSink);

    this.cursor = cursorFactory.apply();
    this.accessorList = cursor.createAccessors(columnMetaDataList);
    accessorMap.clear();
    for (Map.Entry<String, Integer> entry : columnNameMap.entrySet()) {
      accessorMap.put(entry.getKey(), accessorList.get(entry.getValue()));
    }
    return this;
  }

  public boolean next() throws SQLException {
    // TODO: for timeout, see IteratorResultSet.next
    if (cursor.next()) {
      ++row;
      return true;
    } else {
      afterLast = true;
      return false;
    }
  }

  public boolean wasNull() throws SQLException {
    return wasNull[0];
  }

  public String getString(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getString();
  }

  public boolean getBoolean(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getBoolean();
  }

  public byte getByte(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getByte();
  }

  public short getShort(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getShort();
  }

  public int getInt(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getInt();
  }

  public long getLong(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getLong();
  }

  public float getFloat(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getFloat();
  }

  public double getDouble(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getDouble();
  }

  public BigDecimal getBigDecimal(
      int columnIndex, int scale) throws SQLException {
    return getAccessor(columnIndex - 1).getBigDecimal(scale);
  }

  public byte[] getBytes(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getBytes();
  }

  public Date getDate(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getDate();
  }

  public Time getTime(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getTime();
  }

  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getTimestamp();
  }

  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getAsciiStream();
  }

  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getUnicodeStream();
  }

  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getBinaryStream();
  }

  public String getString(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getString();
  }

  public boolean getBoolean(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getBoolean();
  }

  public byte getByte(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getByte();
  }

  public short getShort(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getShort();
  }

  public int getInt(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getInt();
  }

  public long getLong(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getLong();
  }

  public float getFloat(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getFloat();
  }

  public double getDouble(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getDouble();
  }

  public BigDecimal getBigDecimal(
      String columnLabel, int scale) throws SQLException {
    return getAccessor(columnLabel).getBigDecimal(scale);
  }

  public byte[] getBytes(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getBytes();
  }

  public Date getDate(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getDate();
  }

  public Time getTime(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getTime();
  }

  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getTimestamp();
  }

  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getAsciiStream();
  }

  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getUnicodeStream();
  }

  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getBinaryStream();
  }

  public SQLWarning getWarnings() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void clearWarnings() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public String getCursorName() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    return resultSetMetaData;
  }

  public Object getObject(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getObject();
  }

  public Object getObject(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getObject();
  }

  public int findColumn(String columnLabel) throws SQLException {
    final Integer integer = columnNameMap.get(columnLabel);
    if (integer == null) {
      throw new SQLException("column '" + columnLabel + "' not found");
    }
    return integer;
  }

  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getCharacterStream();
  }

  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getCharacterStream();
  }

  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getBigDecimal();
  }

  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getBigDecimal();
  }

  public boolean isBeforeFirst() throws SQLException {
    return row < 0;
  }

  public boolean isAfterLast() throws SQLException {
    return afterLast;
  }

  public boolean isFirst() throws SQLException {
    return row == 0;
  }

  public boolean isLast() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void beforeFirst() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void afterLast() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public boolean first() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public boolean last() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int getRow() throws SQLException {
    return row;
  }

  public boolean absolute(int row) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public boolean relative(int rows) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public boolean previous() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setFetchDirection(int direction) throws SQLException {
    this.fetchDirection = direction;
  }

  public int getFetchDirection() throws SQLException {
    return fetchDirection;
  }

  public void setFetchSize(int fetchSize) throws SQLException {
    this.fetchSize = fetchSize;
  }

  public int getFetchSize() throws SQLException {
    return fetchSize;
  }

  public int getType() throws SQLException {
    return type;
  }

  public int getConcurrency() throws SQLException {
    return concurrency;
  }

  public boolean rowUpdated() throws SQLException {
    return false;
  }

  public boolean rowInserted() throws SQLException {
    return false;
  }

  public boolean rowDeleted() throws SQLException {
    return false;
  }

  public void updateNull(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBigDecimal(
      int columnIndex, BigDecimal x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateString(int columnIndex, String x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateTimestamp(
      int columnIndex, Timestamp x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateAsciiStream(
      int columnIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBinaryStream(
      int columnIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateCharacterStream(
      int columnIndex, Reader x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateObject(
      int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateNull(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBoolean(
      String columnLabel, boolean x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBigDecimal(
      String columnLabel, BigDecimal x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateString(String columnLabel, String x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateTimestamp(
      String columnLabel, Timestamp x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateAsciiStream(
      String columnLabel, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBinaryStream(
      String columnLabel, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateCharacterStream(
      String columnLabel, Reader reader, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateObject(
      String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void insertRow() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateRow() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void deleteRow() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void refreshRow() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void cancelRowUpdates() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void moveToInsertRow() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void moveToCurrentRow() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public Statement getStatement() throws SQLException {
    return statement;
  }

  public Object getObject(
      int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return getAccessor(columnIndex - 1).getObject(map);
  }

  public Ref getRef(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getRef();
  }

  public Blob getBlob(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getBlob();
  }

  public Clob getClob(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getClob();
  }

  public Array getArray(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getArray();
  }

  public Object getObject(
      String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return getAccessor(columnLabel).getObject(map);
  }

  public Ref getRef(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getRef();
  }

  public Blob getBlob(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getBlob();
  }

  public Clob getClob(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getClob();
  }

  public Array getArray(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getArray();
  }

  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return getAccessor(columnIndex - 1).getDate(cal);
  }

  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return getAccessor(columnLabel).getDate(cal);
  }

  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return getAccessor(columnIndex - 1).getTime(cal);
  }

  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return getAccessor(columnLabel).getTime(cal);
  }

  public Timestamp getTimestamp(
      int columnIndex, Calendar cal) throws SQLException {
    return getAccessor(columnIndex - 1).getTimestamp(cal);
  }

  public Timestamp getTimestamp(
      String columnLabel, Calendar cal) throws SQLException {
    return getAccessor(columnLabel).getTimestamp(cal);
  }

  public URL getURL(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getURL();
  }

  public URL getURL(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getURL();
  }

  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public RowId getRowId(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public RowId getRowId(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int getHoldability() throws SQLException {
    return holdability;
  }

  public boolean isClosed() throws SQLException {
    return closed;
  }

  public void updateNString(
      int columnIndex, String nString) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateNString(
      String columnLabel, String nString) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateNClob(
      String columnLabel, NClob nClob) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public NClob getNClob(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getNClob();
  }

  public NClob getNClob(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getNClob();
  }

  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getSQLXML();
  }

  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getSQLXML();
  }

  public void updateSQLXML(
      int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateSQLXML(
      String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public String getNString(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getNString();
  }

  public String getNString(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getNString();
  }

  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return getAccessor(columnIndex - 1).getNCharacterStream();
  }

  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return getAccessor(columnLabel).getNCharacterStream();
  }

  public void updateNCharacterStream(
      int columnIndex, Reader x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateNCharacterStream(
      String columnLabel, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateAsciiStream(
      int columnIndex, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBinaryStream(
      int columnIndex, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateCharacterStream(
      int columnIndex, Reader x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateAsciiStream(
      String columnLabel, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBinaryStream(
      String columnLabel, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateCharacterStream(
      String columnLabel, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBlob(
      int columnIndex,
      InputStream inputStream,
      long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBlob(
      String columnLabel,
      InputStream inputStream,
      long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateClob(
      int columnIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateClob(
      String columnLabel, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateNClob(
      int columnIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateNClob(
      String columnLabel, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateNCharacterStream(
      int columnIndex, Reader x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateNCharacterStream(
      String columnLabel, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateAsciiStream(
      int columnIndex, InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBinaryStream(
      int columnIndex, InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateCharacterStream(
      int columnIndex, Reader x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateAsciiStream(
      String columnLabel, InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBinaryStream(
      String columnLabel, InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateCharacterStream(
      String columnLabel, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBlob(
      int columnIndex, InputStream inputStream) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateBlob(
      String columnLabel, InputStream inputStream) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateClob(
      String columnLabel, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateNClob(
      int columnIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void updateNClob(
      String columnLabel, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return getAccessor(columnIndex - 1).getObject(type);
  }

  public <T> T getObject(
      String columnLabel, Class<T> type) throws SQLException {
    return getAccessor(columnLabel).getObject(type);
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw statement.connection.helper.createException(
        "does not implement '" + iface + "'");
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }
}

// End OptiqResultSet.java
