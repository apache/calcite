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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/** Value and type.
 *
 * <p>There are 3 representations:
 * <ul>
 *   <li>JDBC - the representation used by JDBC get and set methods
 *   <li>Serial - suitable for serializing using JSON
 *   <li>Local - used by Calcite for efficient computation
 * </ul>
 *
 * <p>The following table shows the Java type(s) that may represent each SQL
 * type in each representation.
 *
 * <table>
 *   <caption>SQL types and their representations</caption>
 *   <tr>
 *     <th>Type</th> <th>JDBC</th> <th>Serial</th> <th>Local</th>
 *   </tr>
 *   <tr>
 *     <td>BOOLEAN</td> <td>boolean</td> <td>boolean</td> <td>boolean</td>
 *   </tr>
 *   <tr>
 *     <td>BINARY, VARBINARY</td> <td>byte[]</td>
 *                    <td>String (base64)</td> <td>{@link ByteString}</td>
 *   </tr>
 *   <tr>
 *     <td>DATE</td> <td>{@link java.sql.Date}</td>
 *                                   <td>int</td> <td>int</td>
 *   </tr>
 *   <tr>
 *     <td>TIME</td> <td>{@link java.sql.Time}</td>
 *                                   <td>int</td> <td>int</td>
 *   </tr>
 *   <tr>
 *     <td>DATE</td> <td>{@link java.sql.Timestamp}</td>
 *                                   <td>long</td> <td>long</td>
 *   </tr>
 *   <tr>
 *     <td>CHAR, VARCHAR</td>
 *                   <td>String</td> <td>String</td> <td>String</td>
 *   </tr>
 *   <tr>
 *     <td>TINYINT</td> <td>byte</td> <td>Number</td> <td>byte</td>
 *   </tr>
 *   <tr>
 *     <td>SMALLINT</td> <td>short</td> <td>Number</td> <td>short</td>
 *   </tr>
 *   <tr>
 *     <td>INTEGER</td> <td>int</td> <td>Number</td> <td>int</td>
 *   </tr>
 *   <tr>
 *     <td>BIGINT</td> <td>long</td> <td>Number</td> <td>long</td>
 *   </tr>
 *   <tr>
 *     <td>REAL</td> <td>float</td> <td>Number</td> <td>float</td>
 *   </tr>
 *   <tr>
 *     <td>FLOAT, DOUBLE</td>
 *                   <td>double</td> <td>Number</td> <td>double</td>
 *   </tr>
 *   <tr>
 *     <td>DECIMAL</td>
 *                   <td>BigDecimal</td> <td>Number</td> <td>BigDecimal</td>
 *   </tr>
 * </table>
 *
 * Note:
 * <ul>
 *   <li>The various numeric types (TINYINT, SMALLINT, INTEGER, BIGINT, REAL,
 *   FLOAT, DOUBLE) are represented by {@link Number} in serial format because
 *   JSON numbers are not strongly typed. A {@code float} value {@code 3.0} is
 *   transmitted as {@code 3}, and is therefore decoded as an {@code int}.
 *
 *   <li>The date-time types (DATE, TIME, TIMESTAMP) are represented in JDBC as
 *   {@link java.sql.Date}, {@link java.sql.Time}, {@link java.sql.Timestamp},
 *   all sub-classes of {@link java.util.Date}. When they are passed to and
 *   from the server, they are interpreted in terms of a time zone, by default
 *   the current connection's time zone. Their serial and local representations
 *   as {@code int} (days since 1970-01-01 for DATE, milliseconds since
 *   00:00:00.000 for TIME), and long (milliseconds since 1970-01-01
 *   00:00:00.000 for TIMESTAMP) are easier to work with, because it is clear
 *   that time zone is not involved.
 *
 *   <li>BINARY and VARBINARY values are represented as base64-encoded strings
 *   for serialization over JSON.
 * </ul>
 */
public class TypedValue {
  public static final TypedValue NULL =
      new TypedValue(ColumnMetaData.Rep.OBJECT, null);

  /** Type of the value. */
  public final ColumnMetaData.Rep type;

  /** Value.
   *
   * <p>Always in a form that can be serialized to JSON by Jackson.
   * For example, byte arrays are represented as String. */
  public final Object value;

  private TypedValue(ColumnMetaData.Rep rep, Object value) {
    this.type = rep;
    this.value = value;
    assert isSerial(rep, value) : "rep: " + rep + ", value: " + value;
  }

  private boolean isSerial(ColumnMetaData.Rep rep, Object value) {
    if (value == null) {
      return true;
    }
    switch (rep) {
    case BYTE_STRING:
      return value instanceof String;
    case JAVA_SQL_DATE:
    case JAVA_SQL_TIME:
      return value instanceof Integer;
    case JAVA_SQL_TIMESTAMP:
    case JAVA_UTIL_DATE:
      return value instanceof Long;
    default:
      return true;
    }
  }

  @JsonCreator
  public static TypedValue create(@JsonProperty("type") String type,
      @JsonProperty("value") Object value) {
    if (value == null) {
      return NULL;
    }
    ColumnMetaData.Rep rep = ColumnMetaData.Rep.valueOf(type);
    return ofLocal(rep, serialToLocal(rep, value));
  }

  /** Creates a TypedValue from a value in local representation. */
  public static TypedValue ofLocal(ColumnMetaData.Rep rep, Object value) {
    return new TypedValue(rep, localToSerial(rep, value));
  }

  /** Creates a TypedValue from a value in serial representation. */
  public static TypedValue ofSerial(ColumnMetaData.Rep rep, Object value) {
    return new TypedValue(rep, value);
  }

  /** Creates a TypedValue from a value in JDBC representation. */
  public static TypedValue ofJdbc(ColumnMetaData.Rep rep, Object value,
      Calendar calendar) {
    if (value == null) {
      return NULL;
    }
    return new TypedValue(rep, jdbcToSerial(rep, value, calendar));
  }

  /** Creates a TypedValue from a value in JDBC representation,
   * deducing its type. */
  public static TypedValue ofJdbc(Object value, Calendar calendar) {
    if (value == null) {
      return NULL;
    }
    final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(value.getClass());
    return new TypedValue(rep, jdbcToSerial(rep, value, calendar));
  }

  /** Converts the value into the local representation.
   *
   * <p>For example, a byte string is represented as a {@link ByteString};
   * a long is represented as a {@link Long} (not just some {@link Number}).
   */
  public Object toLocal() {
    if (value == null) {
      return null;
    }
    return serialToLocal(type, value);
  }

  /** Converts a value to the exact type required for the given
   * representation. */
  private static Object serialToLocal(ColumnMetaData.Rep rep, Object value) {
    assert value != null;
    if (value.getClass() == rep.clazz) {
      return value;
    }
    switch (rep) {
    case BYTE:
      return ((Number) value).byteValue();
    case SHORT:
      return ((Number) value).shortValue();
    case INTEGER:
    case JAVA_SQL_DATE:
    case JAVA_SQL_TIME:
      return ((Number) value).intValue();
    case LONG:
    case JAVA_UTIL_DATE:
    case JAVA_SQL_TIMESTAMP:
      return ((Number) value).longValue();
    case FLOAT:
      return ((Number) value).floatValue();
    case DOUBLE:
      return ((Number) value).doubleValue();
    case NUMBER:
      return value instanceof BigDecimal ? value
          : value instanceof BigInteger ? new BigDecimal((BigInteger) value)
          : value instanceof Double ? new BigDecimal((Double) value)
          : value instanceof Float ? new BigDecimal((Float) value)
          : new BigDecimal(((Number) value).longValue());
    case BYTE_STRING:
      return ByteString.ofBase64((String) value);
    default:
      throw new IllegalArgumentException("cannot convert " + value + " ("
          + value.getClass() + ") to " + rep);
    }
  }

  /** Converts the value into the JDBC representation.
   *
   * <p>For example, a byte string is represented as a {@link ByteString};
   * a long is represented as a {@link Long} (not just some {@link Number}).
   */
  public Object toJdbc(Calendar calendar) {
    if (value == null) {
      return null;
    }
    return serialToJdbc(type, value, calendar);
  }

  private static Object serialToJdbc(ColumnMetaData.Rep type, Object value,
      Calendar calendar) {
    switch (type) {
    case BYTE_STRING:
      return ByteString.ofBase64((String) value).getBytes();
    case JAVA_UTIL_DATE:
      return new java.util.Date(adjust((Number) value, calendar));
    case JAVA_SQL_DATE:
      return new java.sql.Date(
          adjust(((Number) value).longValue() * DateTimeUtils.MILLIS_PER_DAY,
              calendar));
    case JAVA_SQL_TIME:
      return new java.sql.Time(adjust((Number) value, calendar));
    case JAVA_SQL_TIMESTAMP:
      return new java.sql.Timestamp(adjust((Number) value, calendar));
    default:
      return serialToLocal(type, value);
    }
  }

  private static long adjust(Number number, Calendar calendar) {
    long t = number.longValue();
    if (calendar != null) {
      t -= calendar.getTimeZone().getOffset(t);
    }
    return t;
  }

  /** Converts a value from JDBC format to a type that can be serialized as
   * JSON. */
  private static Object jdbcToSerial(ColumnMetaData.Rep rep, Object value,
      Calendar calendar) {
    switch (rep) {
    case BYTE_STRING:
      return new ByteString((byte[]) value).toBase64String();
    case JAVA_UTIL_DATE:
    case JAVA_SQL_TIMESTAMP:
    case JAVA_SQL_DATE:
    case JAVA_SQL_TIME:
      long t = ((Date) value).getTime();
      if (calendar != null) {
        t += calendar.getTimeZone().getOffset(t);
      }
      switch (rep) {
      case JAVA_SQL_DATE:
        return (int) DateTimeUtils.floorDiv(t, DateTimeUtils.MILLIS_PER_DAY);
      case JAVA_SQL_TIME:
        return (int) DateTimeUtils.floorMod(t, DateTimeUtils.MILLIS_PER_DAY);
      default:
        return t;
      }
    default:
      return value;
    }
  }

  /** Converts a value from internal format to a type that can be serialized
   * as JSON. */
  private static Object localToSerial(ColumnMetaData.Rep rep, Object value) {
    switch (rep) {
    case BYTE_STRING:
      return ((ByteString) value).toBase64String();
    default:
      return value;
    }
  }

  /** Converts a list of {@code TypedValue} to a list of values. */
  public static List<Object> values(List<TypedValue> typedValues) {
    final List<Object> list = new ArrayList<>();
    for (TypedValue typedValue : typedValues) {
      list.add(typedValue.toLocal());
    }
    return list;
  }

  public Common.TypedValue toProto() {
    final Common.TypedValue.Builder builder = Common.TypedValue.newBuilder();

    Common.Rep protoRep = type.toProto();
    builder.setType(protoRep);

    // Serialize the type into the protobuf
    switch (protoRep) {
    case BOOLEAN:
    case PRIMITIVE_BOOLEAN:
      builder.setBoolValue((boolean) value);
      break;
    case BYTE_STRING:
    case STRING:
      builder.setStringValue((String) value);
      break;
    case PRIMITIVE_CHAR:
    case CHARACTER:
      builder.setStringValue(Character.toString((char) value));
      break;
    case BYTE:
    case PRIMITIVE_BYTE:
      builder.setNumberValue(Byte.valueOf((byte) value).longValue());
      break;
    case DOUBLE:
    case PRIMITIVE_DOUBLE:
      builder.setDoubleValue((double) value);
      break;
    case FLOAT:
    case PRIMITIVE_FLOAT:
      builder.setNumberValue(Float.floatToIntBits((float) value));
      break;
    case INTEGER:
    case PRIMITIVE_INT:
      builder.setNumberValue(Integer.valueOf((int) value).longValue());
      break;
    case PRIMITIVE_SHORT:
    case SHORT:
      builder.setNumberValue(Short.valueOf((short) value).longValue());
      break;
    case LONG:
    case PRIMITIVE_LONG:
      builder.setNumberValue((long) value);
      break;
    case JAVA_SQL_DATE:
    case JAVA_SQL_TIME:
      // Persisted as integers
      builder.setNumberValue(Integer.valueOf((int) value).longValue());
      break;
    case JAVA_SQL_TIMESTAMP:
    case JAVA_UTIL_DATE:
      // Persisted as longs
      builder.setNumberValue((long) value);
      break;
    case BIG_INTEGER:
      byte[] bytes = ((BigInteger) value).toByteArray();
      builder.setBytesValues(com.google.protobuf.ByteString.copyFrom(bytes));
      break;
    case BIG_DECIMAL:
      final BigDecimal bigDecimal = (BigDecimal) value;
      final int scale = bigDecimal.scale();
      final BigInteger bigInt = bigDecimal.toBigInteger();
      builder.setBytesValues(com.google.protobuf.ByteString.copyFrom(bigInt.toByteArray()))
        .setNumberValue(scale);
      break;
    case NUMBER:
      builder.setNumberValue(((Number) value).longValue());
      break;
    case OBJECT:
      if (null == value) {
        // We can persist a null value through easily
        builder.setNull(true);
        break;
      }
      // Intentional fall-through to RTE because we can't serialize something we have no type
      // insight into.
    case UNRECOGNIZED:
      // Fail?
      throw new RuntimeException("Unhandled value: " + protoRep + " " + value.getClass());
    default:
      // Fail?
      throw new RuntimeException("Unknown serialized type: " + protoRep);
    }

    return builder.build();
  }

  public static TypedValue fromProto(Common.TypedValue proto) {
    ColumnMetaData.Rep rep = ColumnMetaData.Rep.fromProto(proto.getType());

    Object value = null;

    // Deserialize the value again
    switch (proto.getType()) {
    case BOOLEAN:
    case PRIMITIVE_BOOLEAN:
      value = proto.getBoolValue();
      break;
    case BYTE_STRING:
    case STRING:
      value = proto.getStringValue();
      break;
    case PRIMITIVE_CHAR:
    case CHARACTER:
      value = proto.getStringValue().charAt(0);
      break;
    case BYTE:
    case PRIMITIVE_BYTE:
      value = Long.valueOf(proto.getNumberValue()).byteValue();
      break;
    case DOUBLE:
    case PRIMITIVE_DOUBLE:
      value = proto.getDoubleValue();
      break;
    case FLOAT:
    case PRIMITIVE_FLOAT:
      value = Float.intBitsToFloat((int) proto.getNumberValue());
      break;
    case INTEGER:
    case PRIMITIVE_INT:
      value = Long.valueOf(proto.getNumberValue()).intValue();
      break;
    case PRIMITIVE_SHORT:
    case SHORT:
      value = Long.valueOf(proto.getNumberValue()).shortValue();
      break;
    case LONG:
    case PRIMITIVE_LONG:
      value = Long.valueOf(proto.getNumberValue());
      break;
    case JAVA_SQL_DATE:
    case JAVA_SQL_TIME:
      value = Long.valueOf(proto.getNumberValue()).intValue();
      break;
    case JAVA_SQL_TIMESTAMP:
    case JAVA_UTIL_DATE:
      value = proto.getNumberValue();
      break;
    case BIG_INTEGER:
      value = new BigInteger(proto.getBytesValues().toByteArray());
      break;
    case BIG_DECIMAL:
      BigInteger bigInt = new BigInteger(proto.getBytesValues().toByteArray());
      value = new BigDecimal(bigInt, (int) proto.getNumberValue());
      break;
    case NUMBER:
      value = Long.valueOf(proto.getNumberValue());
      break;
    case OBJECT:
      if (proto.getNull()) {
        value = null;
        break;
      }
      // Intentional fall through to RTE. If we sent an object over the wire, it could only
      // possibly be null (at this point). Anything else has to be an error.
    case UNRECOGNIZED:
      // Fail?
      throw new RuntimeException("Unhandled type: " + proto.getType());
    default:
      // Fail?
      throw new RuntimeException("Unknown type: " + proto.getType());
    }

    return new TypedValue(rep, value);
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }

  @Override public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TypedValue) {
      TypedValue other = (TypedValue) o;

      if (type != other.type) {
        return false;
      }

      if (null == value) {
        if (null != other.value) {
          return false;
        }
      }

      return value.equals(other.value);
    }

    return false;
  }
}

// End TypedValue.java
