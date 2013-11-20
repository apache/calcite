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
   * @return List of column accessors
   */
  List<Accessor> createAccessors(List<ColumnMetaData> types,
      Calendar localCalendar);

  /**
   * Moves to the next row.
   *
   * @return Whether moved
   */
  boolean next();

  /**
   * Closes this cursor and releases resources.
   */
  void close();

  /**
   * Returns whether the last value returned was null.
   */
  boolean wasNull();

  /**
   * Accessor of a column value.
   */
  interface Accessor {
    boolean wasNull();

    String getString();

    boolean getBoolean();

    byte getByte();

    short getShort();

    int getInt();

    long getLong();

    float getFloat();

    double getDouble();

    BigDecimal getBigDecimal();

    BigDecimal getBigDecimal(int scale);

    byte[] getBytes();

    InputStream getAsciiStream();

    InputStream getUnicodeStream();

    InputStream getBinaryStream();

    Object getObject();

    Reader getCharacterStream();

    Object getObject(Map<String, Class<?>> map);

    Ref getRef();

    Blob getBlob();

    Clob getClob();

    Array getArray();

    Date getDate(Calendar calendar);

    Time getTime(Calendar calendar);

    Timestamp getTimestamp(Calendar calendar);

    URL getURL();

    NClob getNClob();

    SQLXML getSQLXML();

    String getNString();

    Reader getNCharacterStream();

    <T> T getObject(Class<T> type);
  }
}

// End Cursor.java
