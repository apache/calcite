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

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.Cursor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
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
 * Metadata for a parameter.
 */
public class AvaticaParameter {
  public final boolean signed;
  public final int precision;
  public final int scale;
  public final int parameterType;
  public final String typeName;
  public final String className;
  public final String name;

  /** Value that means the parameter has been set to null.
   * If value is null, parameter has not been set. */
  public static final Object DUMMY_VALUE = Dummy.INSTANCE;

  @JsonCreator
  public AvaticaParameter(
      @JsonProperty("signed") boolean signed,
      @JsonProperty("precision") int precision,
      @JsonProperty("scale") int scale,
      @JsonProperty("parameterType") int parameterType,
      @JsonProperty("typeName") String typeName,
      @JsonProperty("className") String className,
      @JsonProperty("name") String name) {
    this.signed = signed;
    this.precision = precision;
    this.scale = scale;
    this.parameterType = parameterType;
    this.typeName = typeName;
    this.className = className;
    this.name = name;
  }

  public void setByte(Object[] slots, int index, byte o) {
    slots[index] = o;
  }

  public void setChar(Object[] slots, int index, char o) {
    slots[index] = o;
  }

  public void setShort(Object[] slots, int index, short o) {
    slots[index] = o;
  }

  public void setInt(Object[] slots, int index, int o) {
    slots[index] = o;
  }

  public void setLong(Object[] slots, int index, long o) {
    slots[index] = o;
  }

  public void setBoolean(Object[] slots, int index, boolean o) {
    slots[index] = o;
  }

  private static Object wrap(Object o) {
    if (o == null) {
      return DUMMY_VALUE;
    }
    return o;
  }

  public boolean isSet(Object[] slots, int index) {
    return slots[index] != null;
  }

  public void setRowId(Object[] slots, int index, RowId x) {
    slots[index] = wrap(x);
  }

  public void setNString(Object[] slots, int index, String o) {
    slots[index] = wrap(o);
  }

  public void setNCharacterStream(Object[] slots, int index, Reader value,
      long length) {
  }

  public void setNClob(Object[] slots, int index, NClob value) {
    slots[index] = wrap(value);
  }

  public void setClob(Object[] slots, int index, Reader reader, long length) {
  }

  public void setBlob(Object[] slots, int index, InputStream inputStream,
      long length) {
  }

  public void setNClob(Object[] slots, int index, Reader reader, long length) {
  }

  public void setSQLXML(Object[] slots, int index, SQLXML xmlObject) {
    slots[index] = wrap(xmlObject);
  }

  public void setAsciiStream(Object[] slots, int index, InputStream x,
      long length) {
  }

  public void setBinaryStream(Object[] slots, int index, InputStream x,
      long length) {
  }

  public void setCharacterStream(Object[] slots, int index, Reader reader,
      long length) {
  }

  public void setAsciiStream(Object[] slots, int index, InputStream x) {
  }

  public void setBinaryStream(Object[] slots, int index, InputStream x) {
  }

  public void setCharacterStream(Object[] slots, int index, Reader reader) {
  }

  public void setNCharacterStream(Object[] slots, int index, Reader value) {
  }

  public void setClob(Object[] slots, int index, Reader reader) {
  }

  public void setBlob(Object[] slots, int index, InputStream inputStream) {
  }

  public void setNClob(Object[] slots, int index, Reader reader) {
  }

  public void setUnicodeStream(Object[] slots, int index, InputStream x,
      int length) {
  }

  public void setTimestamp(Object[] slots, int index, Timestamp x) {
    slots[index] = wrap(x);
  }

  public void setTime(Object[] slots, int index, Time x) {
    slots[index] = wrap(x);
  }

  public void setFloat(Object[] slots, int index, float x) {
    slots[index] = wrap(x);
  }

  public void setDouble(Object[] slots, int index, double x) {
    slots[index] = wrap(x);
  }

  public void setBigDecimal(Object[] slots, int index, BigDecimal x) {
    slots[index] = wrap(x);
  }

  public void setString(Object[] slots, int index, String x) {
    slots[index] = wrap(x);
  }

  public void setBytes(Object[] slots, int index, byte[] x) {
    slots[index] = x == null ? DUMMY_VALUE : new ByteString(x);
  }

  public void setDate(Object[] slots, int index, Date x, Calendar cal) {
  }

  public void setDate(Object[] slots, int index, Date x) {
    slots[index] = wrap(x);
  }

  public void setObject(Object[] slots, int index, Object x,
      int targetSqlType) {
    if (x == null || Types.NULL == targetSqlType) {
      setNull(slots, index, targetSqlType);
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
      setArray(slots, index, toArray(x));
      break;
    case Types.BIGINT:
      setLong(slots, index, toLong(x));
      break;
    case Types.BINARY:
    case Types.LONGVARBINARY:
    case Types.VARBINARY:
      setBytes(slots, index, toBytes(x));
      break;
    case Types.BIT:
    case Types.BOOLEAN:
      setBoolean(slots, index, toBoolean(x));
      break;
    case Types.BLOB:
      if (x instanceof Blob) {
        setBlob(slots, index, (Blob) x);
        break;
      } else if (x instanceof InputStream) {
        setBlob(slots, index, (InputStream) x);
      }
      throw unsupportedCast(x.getClass(), Blob.class);
    case Types.DATE:
      setDate(slots, index, toDate(x));
      break;
    case Types.DECIMAL:
    case Types.NUMERIC:
      setBigDecimal(slots, index, toBigDecimal(x));
      break;
    case Types.DISTINCT:
      throw notImplemented();
    case Types.DOUBLE:
    case Types.FLOAT: // yes really; SQL FLOAT is up to 8 bytes
      setDouble(slots, index, toDouble(x));
      break;
    case Types.INTEGER:
      setInt(slots, index, toInt(x));
      break;
    case Types.JAVA_OBJECT:
      setObject(slots, index, x);
      break;
    case Types.LONGNVARCHAR:
    case Types.LONGVARCHAR:
    case Types.NVARCHAR:
    case Types.VARCHAR:
    case Types.CHAR:
    case Types.NCHAR:
      setString(slots, index, toString(x));
      break;
    case Types.REAL:
      setFloat(slots, index, toFloat(x));
      break;
    case Types.ROWID:
      if (x instanceof RowId) {
        setRowId(slots, index, (RowId) x);
        break;
      }
      throw unsupportedCast(x.getClass(), RowId.class);
    case Types.SMALLINT:
      setShort(slots, index, toShort(x));
      break;
    case Types.TIME:
      setTime(slots, index, toTime(x));
      break;
    case Types.TIMESTAMP:
      setTimestamp(slots, index, toTimestamp(x));
      break;
    case Types.TINYINT:
      setByte(slots, index, toByte(x));
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

  public void setObject(Object[] slots, int index, Object x) {
    slots[index] = wrap(x);
  }

  public void setNull(Object[] slots, int index, int sqlType) {
    slots[index] = DUMMY_VALUE;
  }

  public void setTime(Object[] slots, int index, Time x, Calendar cal) {
  }

  public void setRef(Object[] slots, int index, Ref x) {
  }

  public void setBlob(Object[] slots, int index, Blob x) {
  }

  public void setClob(Object[] slots, int index, Clob x) {
  }

  public void setArray(Object[] slots, int index, Array x) {
  }

  public void setTimestamp(Object[] slots, int index, Timestamp x,
      Calendar cal) {
  }

  public void setNull(Object[] slots, int index, int sqlType, String typeName) {
  }

  public void setURL(Object[] slots, int index, URL x) {
  }

  public void setObject(Object[] slots, int index, Object x, int targetSqlType,
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
      return ((String) x).getBytes();
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

// End AvaticaParameter.java
