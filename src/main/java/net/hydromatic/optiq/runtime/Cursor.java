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
package net.hydromatic.optiq.runtime;

import java.io.InputStream;
import java.io.Reader;
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
public interface Cursor {
  /**
   * Creates a list of accessors, one per column.
   *
   * @param types List of column types, per {@link java.sql.Types}.
   * @return List of column accessors
   */
  List<Accessor> createAccessors(List<ColumnMetaData> types);

  /**
   * Moves to the next row.
   *
   * @return Whether moved
   */
  boolean next();

  /**
   * Accessor of a column value.
   */
  interface Accessor {
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

    Date getDate();

    Time getTime();

    Timestamp getTimestamp();

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

    Date getDate(Calendar cal);

    Time getTime(Calendar cal);

    Timestamp getTimestamp(Calendar cal);

    URL getURL();

    NClob getNClob();

    SQLXML getSQLXML();

    String getNString();

    Reader getNCharacterStream();

    <T> T getObject(Class<T> type);
  }
}

// End Cursor.java
