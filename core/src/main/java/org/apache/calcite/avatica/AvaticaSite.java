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

import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.Cursor;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

/**
 * A location that a value can be written to or read from.
 */
public class AvaticaSite {
  final AvaticaParameter parameter;

  /** Calendar is not thread-safe. But calendar is only used from within one
   * thread, and we have to trust that clients are not modifying calendars
   * that they pass to us in a method such as
   * {@link java.sql.PreparedStatement#setTime(int, Time, Calendar)}, so we do
   * not need to synchronize access. */
  final Calendar calendar;
  private final int index;
  final TypedValue[] slots;

  /** Value that means the parameter has been set to null.
   * If value is null, parameter has not been set. */
  public static final Object DUMMY_VALUE = Dummy.INSTANCE;

  public AvaticaSite(AvaticaParameter parameter, Calendar calendar, int index,
      TypedValue[] slots) {
    assert calendar != null;
    assert parameter != null;
    assert slots != null;
    this.parameter = parameter;
    this.calendar = calendar;
    this.index = index;
    this.slots = slots;
  }

  private TypedValue wrap(ColumnMetaData.Rep rep, Object o,
      Calendar calendar) {
    return TypedValue.ofJdbc(rep, o, calendar);
  }

  private TypedValue wrap(ColumnMetaData.Rep rep, Object o) {
    return TypedValue.ofJdbc(rep, o, calendar);
  }

  public boolean isSet(int index) {
    return slots[index] != null;
  }

  public void setByte(byte o) {
    slots[index] = wrap(ColumnMetaData.Rep.BYTE, o);
  }

  public void setChar(char o) {
    slots[index] = wrap(ColumnMetaData.Rep.CHARACTER, o);
  }

  public void setShort(short o) {
    slots[index] = wrap(ColumnMetaData.Rep.SHORT, o);
  }

  public void setInt(int o) {
    slots[index] = wrap(ColumnMetaData.Rep.INTEGER, o);
  }

  public void setLong(long o) {
    slots[index] = wrap(ColumnMetaData.Rep.LONG, o);
  }

  public void setBoolean(boolean o) {
    slots[index] = wrap(ColumnMetaData.Rep.BOOLEAN, o);
  }

  public void setRowId(RowId x) {
    slots[index] = wrap(ColumnMetaData.Rep.OBJECT, x);
  }

  public void setNString(String o) {
    slots[index] = wrap(ColumnMetaData.Rep.STRING, o);
  }

  public void setNCharacterStream(Reader value, long length) {
  }

  public void setNClob(NClob value) {
    slots[index] = wrap(ColumnMetaData.Rep.OBJECT, value);
  }

  public void setClob(Reader reader, long length) {
  }

  public void setBlob(InputStream inputStream, long length) {
  }

  public void setNClob(Reader reader, long length) {
  }

  public void setSQLXML(SQLXML xmlObject) {
    slots[index] = wrap(ColumnMetaData.Rep.OBJECT, xmlObject);
  }

  public void setAsciiStream(InputStream x, long length) {
  }

  public void setBinaryStream(InputStream x, long length) {
  }

  public void setCharacterStream(Reader reader, long length) {
  }

  public void setAsciiStream(InputStream x) {
  }

  public void setBinaryStream(InputStream x) {
  }

  public void setCharacterStream(Reader reader) {
  }

  public void setNCharacterStream(Reader value) {
  }

  public void setClob(Reader reader) {
  }

  public void setBlob(InputStream inputStream) {
  }

  public void setNClob(Reader reader) {
  }

  public void setUnicodeStream(InputStream x, int length) {
  }

  public void setFloat(float x) {
    slots[index] = wrap(ColumnMetaData.Rep.FLOAT, x);
  }

  public void setDouble(double x) {
    slots[index] = wrap(ColumnMetaData.Rep.DOUBLE, x);
  }

  public void setBigDecimal(BigDecimal x) {
    slots[index] = wrap(ColumnMetaData.Rep.NUMBER, x);
  }

  public void setString(String x) {
    slots[index] = wrap(ColumnMetaData.Rep.STRING, x);
  }

  public void setBytes(byte[] x) {
    slots[index] = wrap(ColumnMetaData.Rep.BYTE_STRING, x);
  }

  public void setTimestamp(Timestamp x, Calendar calendar) {
    slots[index] = wrap(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, x, calendar);
  }

  public void setTime(Time x, Calendar calendar) {
    slots[index] = wrap(ColumnMetaData.Rep.JAVA_SQL_TIME, x, calendar);
  }

  public void setDate(Date x, Calendar calendar) {
    slots[index] = wrap(ColumnMetaData.Rep.JAVA_SQL_DATE, x, calendar);
  }

  public void setObject(Object x, int targetSqlType) {
    if (x == null || Types.NULL == targetSqlType) {
      setNull(targetSqlType);
      return;
    }
    switch (targetSqlType) {
    case Types.CLOB:
    case Types.DATALINK:
    case Types.NCLOB:
    case Types.OTHER:
    case Types.REF:
    case Types.SQLXML:
    case Types.STRUCT:
      throw notImplemented();
    case Types.ARRAY:
      setArray(toArray(x));
      break;
    case Types.BIGINT:
      setLong(toLong(x));
      break;
    case Types.BINARY:
    case Types.LONGVARBINARY:
    case Types.VARBINARY:
      setBytes(toBytes(x));
      break;
    case Types.BIT:
    case Types.BOOLEAN:
      setBoolean(toBoolean(x));
      break;
    case Types.BLOB:
      if (x instanceof Blob) {
        setBlob((Blob) x);
        break;
      } else if (x instanceof InputStream) {
        setBlob((InputStream) x);
      }
      throw unsupportedCast(x.getClass(), Blob.class);
    case Types.DATE:
      setDate(toDate(x), calendar);
      break;
    case Types.DECIMAL:
    case Types.NUMERIC:
      setBigDecimal(toBigDecimal(x));
      break;
    case Types.DISTINCT:
      throw notImplemented();
    case Types.DOUBLE:
    case Types.FLOAT: // yes really; SQL FLOAT is up to 8 bytes
      setDouble(toDouble(x));
      break;
    case Types.INTEGER:
      setInt(toInt(x));
      break;
    case Types.JAVA_OBJECT:
      setObject(x);
      break;
    case Types.LONGNVARCHAR:
    case Types.LONGVARCHAR:
    case Types.NVARCHAR:
    case Types.VARCHAR:
    case Types.CHAR:
    case Types.NCHAR:
      setString(toString(x));
      break;
    case Types.REAL:
      setFloat(toFloat(x));
      break;
    case Types.ROWID:
      if (x instanceof RowId) {
        setRowId((RowId) x);
        break;
      }
      throw unsupportedCast(x.getClass(), RowId.class);
    case Types.SMALLINT:
      setShort(toShort(x));
      break;
    case Types.TIME:
      setTime(toTime(x), calendar);
      break;
    case Types.TIMESTAMP:
      setTimestamp(toTimestamp(x), calendar);
      break;
    case Types.TINYINT:
      setByte(toByte(x));
      break;
    default:
      throw notImplemented();
    }
  }

  /** Similar logic to {@link #setObject}. */
  public static Object get(Cursor.Accessor accessor, int targetSqlType,
      Calendar localCalendar) throws SQLException {
    switch (targetSqlType) {
    case Types.CLOB:
    case Types.DATALINK:
    case Types.NCLOB:
    case Types.REF:
    case Types.SQLXML:
    case Types.STRUCT:
      throw notImplemented();
    case Types.ARRAY:
      return accessor.getArray();
    case Types.BIGINT:
      final long aLong = accessor.getLong();
      if (aLong == 0 && accessor.wasNull()) {
        return null;
      }
      return aLong;
    case Types.BINARY:
    case Types.LONGVARBINARY:
    case Types.VARBINARY:
      return accessor.getBytes();
    case Types.BIT:
    case Types.BOOLEAN:
      final boolean aBoolean = accessor.getBoolean();
      if (!aBoolean && accessor.wasNull()) {
        return null;
      }
      return aBoolean;
    case Types.BLOB:
      return accessor.getBlob();
    case Types.DATE:
      return accessor.getDate(localCalendar);
    case Types.DECIMAL:
    case Types.NUMERIC:
      return accessor.getBigDecimal();
    case Types.DISTINCT:
      throw notImplemented();
    case Types.DOUBLE:
    case Types.FLOAT: // yes really; SQL FLOAT is up to 8 bytes
      final double aDouble = accessor.getDouble();
      if (aDouble == 0 && accessor.wasNull()) {
        return null;
      }
      return aDouble;
    case Types.INTEGER:
      final int anInt = accessor.getInt();
      if (anInt == 0 && accessor.wasNull()) {
        return null;
      }
      return anInt;
    case Types.JAVA_OBJECT:
    case Types.OTHER:
      return accessor.getObject();
    case Types.LONGNVARCHAR:
    case Types.LONGVARCHAR:
    case Types.NVARCHAR:
    case Types.VARCHAR:
    case Types.CHAR:
    case Types.NCHAR:
      return accessor.getString();
    case Types.REAL:
      final float aFloat = accessor.getFloat();
      if (aFloat == 0 && accessor.wasNull()) {
        return null;
      }
      return aFloat;
    case Types.ROWID:
      throw notImplemented();
    case Types.SMALLINT:
      final short aShort = accessor.getShort();
      if (aShort == 0 && accessor.wasNull()) {
        return null;
      }
      return aShort;
    case Types.TIME:
      return accessor.getTime(localCalendar);
    case Types.TIMESTAMP:
      return accessor.getTimestamp(localCalendar);
    case Types.TINYINT:
      final byte aByte = accessor.getByte();
      if (aByte == 0 && accessor.wasNull()) {
        return null;
      }
      return aByte;
    default:
      throw notImplemented();
    }
  }

  public void setObject(Object x) {
    slots[index] = TypedValue.ofJdbc(x, calendar);
  }

  public void setNull(int sqlType) {
    slots[index] = wrap(ColumnMetaData.Rep.OBJECT, null);
  }

  public void setRef(Ref x) {
  }

  public void setBlob(Blob x) {
  }

  public void setClob(Clob x) {
  }

  public void setArray(Array x) {
  }

  public void setNull(int sqlType, String typeName) {
  }

  public void setURL(URL x) {
  }

  public void setObject(Object x, int targetSqlType,
      int scaleOrLength) {
  }

  private static RuntimeException unsupportedCast(Class<?> from, Class<?> to) {
    return new UnsupportedOperationException("Cannot convert from "
        + from.getCanonicalName() + " to " + to.getCanonicalName());
  }

  private static RuntimeException notImplemented() {
    return new RuntimeException("not implemented");
  }

  private static Array toArray(Object x) {
    if (x instanceof Array) {
      return (Array) x;
    }
    throw unsupportedCast(x.getClass(), Array.class);
  }

  public static BigDecimal toBigDecimal(Object x) {
    if (x instanceof BigDecimal) {
      return (BigDecimal) x;
    } else if (x instanceof BigInteger) {
      return new BigDecimal((BigInteger) x);
    } else if (x instanceof Number) {
      if (x instanceof Double || x instanceof Float) {
        return new BigDecimal(((Number) x).doubleValue());
      } else {
        return new BigDecimal(((Number) x).longValue());
      }
    } else if (x instanceof Boolean) {
      return (Boolean) x ? BigDecimal.ONE : BigDecimal.ZERO;
    } else if (x instanceof String) {
      return new BigDecimal((String) x);
    }
    throw unsupportedCast(x.getClass(), BigDecimal.class);
  }

  private static boolean toBoolean(Object x) {
    if (x instanceof Boolean) {
      return (Boolean) x;
    } else if (x instanceof Number) {
      return ((Number) x).intValue() != 0;
    } else if (x instanceof String) {
      String s = (String) x;
      if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("yes")) {
        return true;
      } else if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("no")) {
        return false;
      }
    }
    throw unsupportedCast(x.getClass(), Boolean.TYPE);
  }

  private static byte toByte(Object x) {
    if (x instanceof Number) {
      return ((Number) x).byteValue();
    } else if (x instanceof Boolean) {
      return (Boolean) x ? (byte) 1 : (byte) 0;
    } else if (x instanceof String) {
      return Byte.parseByte((String) x);
    } else {
      throw unsupportedCast(x.getClass(), Byte.TYPE);
    }
  }

  private static byte[] toBytes(Object x) {
    if (x instanceof byte[]) {
      return (byte[]) x;
    }
    if (x instanceof String) {
      return ((String) x).getBytes(StandardCharsets.UTF_8);
    }
    throw unsupportedCast(x.getClass(), byte[].class);
  }

  private static Date toDate(Object x) {
    if (x instanceof String) {
      return Date.valueOf((String) x);
    }
    return new Date(toLong(x));
  }

  private static Time toTime(Object x) {
    if (x instanceof String) {
      return Time.valueOf((String) x);
    }
    return new Time(toLong(x));
  }

  private static Timestamp toTimestamp(Object x) {
    if (x instanceof String) {
      return Timestamp.valueOf((String) x);
    }
    return new Timestamp(toLong(x));
  }

  private static double toDouble(Object x) {
    if (x instanceof Number) {
      return ((Number) x).doubleValue();
    } else if (x instanceof Boolean) {
      return (Boolean) x ? 1D : 0D;
    } else if (x instanceof String) {
      return Double.parseDouble((String) x);
    } else {
      throw unsupportedCast(x.getClass(), Double.TYPE);
    }
  }

  private static float toFloat(Object x) {
    if (x instanceof Number) {
      return ((Number) x).floatValue();
    } else if (x instanceof Boolean) {
      return (Boolean) x ? 1F : 0F;
    } else if (x instanceof String) {
      return Float.parseFloat((String) x);
    } else {
      throw unsupportedCast(x.getClass(), Float.TYPE);
    }
  }

  private static int toInt(Object x) {
    if (x instanceof Number) {
      return ((Number) x).intValue();
    } else if (x instanceof Boolean) {
      return (Boolean) x ? 1 : 0;
    } else if (x instanceof String) {
      return Integer.parseInt((String) x);
    } else {
      throw unsupportedCast(x.getClass(), Integer.TYPE);
    }
  }

  private static long toLong(Object x) {
    if (x instanceof Number) {
      return ((Number) x).longValue();
    } else if (x instanceof Boolean) {
      return (Boolean) x ? 1L : 0L;
    } else if (x instanceof String) {
      return Long.parseLong((String) x);
    } else {
      throw unsupportedCast(x.getClass(), Long.TYPE);
    }
  }

  private static short toShort(Object x) {
    if (x instanceof Number) {
      return ((Number) x).shortValue();
    } else if (x instanceof Boolean) {
      return (Boolean) x ? (short) 1 : (short) 0;
    } else if (x instanceof String) {
      return Short.parseShort((String) x);
    } else {
      throw unsupportedCast(x.getClass(), Short.TYPE);
    }
  }

  private static String toString(Object x) {
    if (x instanceof String) {
      return (String) x;
    } else if (x instanceof Character
        || x instanceof Boolean) {
      return x.toString();
    }
    throw unsupportedCast(x.getClass(), String.class);
  }

  /** Singleton value to denote parameters that have been set to null (as
   * opposed to not set).
   *
   * <p>Not a valid value for a parameter.
   *
   * <p>As an enum, it is serializable by Jackson. */
  private enum Dummy {
    INSTANCE
  }
}

// End AvaticaSite.java
