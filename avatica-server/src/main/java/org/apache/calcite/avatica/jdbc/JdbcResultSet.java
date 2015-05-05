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
package org.apache.calcite.avatica.jdbc;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.util.DateTimeUtils;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/** Implementation of {@link org.apache.calcite.avatica.Meta.MetaResultSet}
 *  upon a JDBC {@link java.sql.ResultSet}.
 *
 *  @see org.apache.calcite.avatica.jdbc.JdbcMeta */
class JdbcResultSet extends Meta.MetaResultSet {
  protected JdbcResultSet(String connectionId, int statementId,
      boolean ownStatement, Meta.Signature signature, Meta.Frame firstFrame) {
    super(connectionId, statementId, ownStatement, signature, firstFrame, -1);
  }

  /** Creates a result set. */
  public static JdbcResultSet create(String connectionId, int statementId,
      ResultSet resultSet) {
    return create(connectionId, statementId, resultSet, -1);
  }

  /** Creates a result set with maxRowCount. */
  public static JdbcResultSet create(String connectionId, int statementId,
      ResultSet resultSet, int maxRowCount) {
    try {
      Meta.Signature sig = JdbcMeta.signature(resultSet.getMetaData());
      final Calendar calendar = Calendar.getInstance(DateTimeUtils.GMT_ZONE);
      final Meta.Frame firstFrame = frame(resultSet, 0, maxRowCount, calendar);
      resultSet.close();
      return new JdbcResultSet(connectionId, statementId, true, sig,
          firstFrame);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** Creates a frame containing a given number or unlimited number of rows
   * from a result set. */
  static Meta.Frame frame(ResultSet resultSet, int offset,
      int fetchMaxRowCount, Calendar calendar) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    final int[] types = new int[columnCount];
    for (int i = 0; i < types.length; i++) {
      types[i] = metaData.getColumnType(i + 1);
    }
    final List<Object> rows = new ArrayList<>();
    boolean done = false;
    for (int i = 0; fetchMaxRowCount < 0 || i < fetchMaxRowCount; i++) {
      if (!resultSet.next()) {
        done = true;
        break;
      }
      Object[] columns = new Object[columnCount];
      for (int j = 0; j < columnCount; j++) {
        columns[j] = getValue(resultSet, types[j], j, calendar);
      }
      rows.add(columns);
    }
    return new Meta.Frame(offset, done, rows);
  }

  private static Object getValue(ResultSet resultSet, int type, int j,
      Calendar calendar) throws SQLException {
    switch (type) {
    case Types.BIGINT:
      final long aLong = resultSet.getLong(j + 1);
      return aLong == 0 && resultSet.wasNull() ? null : aLong;
    case Types.INTEGER:
      final int anInt = resultSet.getInt(j + 1);
      return anInt == 0 && resultSet.wasNull() ? null : anInt;
    case Types.SMALLINT:
      final short aShort = resultSet.getShort(j + 1);
      return aShort == 0 && resultSet.wasNull() ? null : aShort;
    case Types.TINYINT:
      final byte aByte = resultSet.getByte(j + 1);
      return aByte == 0 && resultSet.wasNull() ? null : aByte;
    case Types.DOUBLE:
    case Types.FLOAT:
      final double aDouble = resultSet.getDouble(j + 1);
      return aDouble == 0D && resultSet.wasNull() ? null : aDouble;
    case Types.REAL:
      final float aFloat = resultSet.getFloat(j + 1);
      return aFloat == 0D && resultSet.wasNull() ? null : aFloat;
    case Types.DATE:
      final Date aDate = resultSet.getDate(j + 1, calendar);
      return aDate == null
          ? null
          : (int) (aDate.getTime() / DateTimeUtils.MILLIS_PER_DAY);
    case Types.TIME:
      final Time aTime = resultSet.getTime(j + 1, calendar);
      return aTime == null
          ? null
          : (int) (aTime.getTime() % DateTimeUtils.MILLIS_PER_DAY);
    case Types.TIMESTAMP:
      final Timestamp aTimestamp = resultSet.getTimestamp(j + 1, calendar);
      return aTimestamp == null ? null : aTimestamp.getTime();
    default:
      return resultSet.getObject(j + 1);
    }
  }
}

// End JdbcResultSet.java
