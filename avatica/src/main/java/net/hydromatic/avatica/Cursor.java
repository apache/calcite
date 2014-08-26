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

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * Interface to an iteration that is similar to, and can easily support,
 * a JDBC {@link ResultSet}, but is simpler to implement.
 */
public interface Cursor extends Closeable {
  /**
   * Creates a list of accessors, one per column.
   *
   *
   * @param types List of column types, per {@link java.sql.Types}.
   * @param localCalendar Calendar in local timezone
   * @param factory Factory that creates sub-ResultSets when needed
   * @return List of column accessors
   */
  List<Accessor> createAccessors(List<ColumnMetaData> types,
      Calendar localCalendar, ArrayImpl.Factory factory);

  /**
   * Moves to the next row.
   *
   * @return Whether moved
   *
   * @throws SQLException on database error
   */
  boolean next() throws SQLException;

  /**
   * Closes this cursor and releases resources.
   */
  void close();

  /**
   * Returns whether the last value returned was null.
   *
   * @throws SQLException on database error
   */
  boolean wasNull() throws SQLException;

  /**
   * Accessor of a column value.
   */
  public interface Accessor {
    boolean wasNull() throws SQLException;

    String getString() throws SQLException;

    boolean getBoolean() throws SQLException;

    byte getByte() throws SQLException;

    short getShort() throws SQLException;

    int getInt() throws SQLException;

    long getLong() throws SQLException;

    float getFloat() throws SQLException;

    double getDouble() throws SQLException;

    BigDecimal getBigDecimal() throws SQLException;

    BigDecimal getBigDecimal(int scale) throws SQLException;

    byte[] getBytes() throws SQLException;

    InputStream getAsciiStream() throws SQLException;

    InputStream getUnicodeStream() throws SQLException;

    InputStream getBinaryStream() throws SQLException;

    Object getObject() throws SQLException;

    Reader getCharacterStream() throws SQLException;

    Object getObject(Map<String, Class<?>> map) throws SQLException;

    Ref getRef() throws SQLException;

    Blob getBlob() throws SQLException;

    Clob getClob() throws SQLException;

    Array getArray() throws SQLException;

    Date getDate(Calendar calendar) throws SQLException;

    Time getTime(Calendar calendar) throws SQLException;

    Timestamp getTimestamp(Calendar calendar) throws SQLException;

    URL getURL() throws SQLException;

    NClob getNClob() throws SQLException;

    SQLXML getSQLXML() throws SQLException;

    String getNString() throws SQLException;

    Reader getNCharacterStream() throws SQLException;

    <T> T getObject(Class<T> type) throws SQLException;
  }
}

// End Cursor.java
