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
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.util.Base64;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.UnsafeByteOperations;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

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
  private static final FieldDescriptor NUMBER_DESCRIPTOR = Common.TypedValue.getDescriptor()
      .findFieldByNumber(Common.TypedValue.NUMBER_VALUE_FIELD_NUMBER);
  private static final FieldDescriptor STRING_DESCRIPTOR = Common.TypedValue.getDescriptor()
      .findFieldByNumber(Common.TypedValue.STRING_VALUE_FIELD_NUMBER);
  private static final FieldDescriptor BYTES_DESCRIPTOR = Common.TypedValue.getDescriptor()
      .findFieldByNumber(Common.TypedValue.BYTES_VALUE_FIELD_NUMBER);

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

  /**
   * Converts the given value from serial form to JDBC form.
   *
   * @param type The type of the value
   * @param value The value
   * @param calendar A calendar instance
   * @return The JDBC representation of the value.
   */
  private static Object serialToJdbc(ColumnMetaData.Rep type, Object value, Calendar calendar) {
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

  /**
   * Creates a protocol buffer equivalent object for <code>this</code>.
   * @return A protobuf TypedValue equivalent for <code>this</code>
   */
  public Common.TypedValue toProto() {
    final Common.TypedValue.Builder builder = Common.TypedValue.newBuilder();

    Common.Rep protoRep = type.toProto();
    // Protobuf has an explicit BIG_DECIMAL representation enum value.
    if (Common.Rep.NUMBER == protoRep && value instanceof BigDecimal) {
      protoRep = Common.Rep.BIG_DECIMAL;
    }

    // Serialize the type into the protobuf
    writeToProtoWithType(builder, value, protoRep);

    return builder.build();
  }

  private static void writeToProtoWithType(Common.TypedValue.Builder builder, Object o,
      Common.Rep type) {
    builder.setType(type);

    switch (type) {
    case BOOLEAN:
    case PRIMITIVE_BOOLEAN:
      builder.setBoolValue((boolean) o);
      return;
    case BYTE_STRING:
      byte[] bytes;
      // Serial representation is b64. We don't need to do that for protobuf
      if (o instanceof String) {
        // Backwards compatibility for client CALCITE-1209
        builder.setStringValue((String) o);
        // Assume strings are already b64 encoded
        bytes = ByteString.parseBase64((String) o);
      } else {
        // Backwards compatibility for client CALCITE-1209
        builder.setStringValue(Base64.encodeBytes((byte[]) o));
        // Use the byte array
        bytes = (byte[]) o;
      }
      builder.setBytesValue(UnsafeByteOperations.unsafeWrap(bytes));
      return;
    case STRING:
      builder.setStringValueBytes(UnsafeByteOperations.unsafeWrap(((String) o).getBytes(UTF_8)));
      return;
    case PRIMITIVE_CHAR:
    case CHARACTER:
      builder.setStringValue(Character.toString((char) o));
      return;
    case BYTE:
    case PRIMITIVE_BYTE:
      builder.setNumberValue(Byte.valueOf((byte) o).longValue());
      return;
    case DOUBLE:
    case PRIMITIVE_DOUBLE:
      builder.setDoubleValue((double) o);
      return;
    case FLOAT:
    case PRIMITIVE_FLOAT:
      builder.setNumberValue(Float.floatToIntBits((float) o));
      return;
    case INTEGER:
    case PRIMITIVE_INT:
      builder.setNumberValue(Integer.valueOf((int) o).longValue());
      return;
    case PRIMITIVE_SHORT:
    case SHORT:
      builder.setNumberValue(Short.valueOf((short) o).longValue());
      return;
    case LONG:
    case PRIMITIVE_LONG:
      builder.setNumberValue((long) o);
      return;
    case JAVA_SQL_DATE:
    case JAVA_SQL_TIME:
      // Persisted as integers
      builder.setNumberValue(Integer.valueOf((int) o).longValue());
      return;
    case JAVA_SQL_TIMESTAMP:
    case JAVA_UTIL_DATE:
      // Persisted as longs
      builder.setNumberValue((long) o);
      return;
    case BIG_INTEGER:
      byte[] byteRep = ((BigInteger) o).toByteArray();
      builder.setBytesValue(com.google.protobuf.ByteString.copyFrom(byteRep));
      return;
    case BIG_DECIMAL:
      final BigDecimal bigDecimal = (BigDecimal) o;
      builder.setStringValue(bigDecimal.toString());
      return;
    case NUMBER:
      builder.setNumberValue(((Number) o).longValue());
      return;
    case NULL:
      builder.setNull(true);
      return;
    case OBJECT:
      if (null == o) {
        // We can persist a null value through easily
        builder.setNull(true);
        return;
      }
      // Intentional fall-through to RTE because we can't serialize something we have no type
      // insight into.
    case UNRECOGNIZED:
      // Fail?
      throw new RuntimeException("Unhandled value: " + type + " " + o.getClass());
    default:
      // Fail?
      throw new RuntimeException("Unknown serialized type: " + type);
    }
  }

  /**
   * Constructs a {@link TypedValue} from the protocol buffer representation.
   *
   * @param proto The protobuf Typedvalue
   * @return A {@link TypedValue} instance
   */
  public static TypedValue fromProto(Common.TypedValue proto) {
    ColumnMetaData.Rep rep = ColumnMetaData.Rep.fromProto(proto.getType());
    Object value = getSerialFromProto(proto);

    return new TypedValue(rep, value);
  }

  /**
   * Converts the serialized value into the appropriate primitive/object.
   *
   * @param protoValue The serialized TypedValue.
   * @return The appropriate concrete type for the parameter value (as an Object).
   */
  public static Object getSerialFromProto(Common.TypedValue protoValue) {
    // Deserialize the value again
    switch (protoValue.getType()) {
    case BOOLEAN:
    case PRIMITIVE_BOOLEAN:
      return protoValue.getBoolValue();
    case BYTE_STRING:
      if (protoValue.hasField(STRING_DESCRIPTOR) && !protoValue.hasField(BYTES_DESCRIPTOR)) {
        // Prior to CALCITE-1103, clients would send b64 strings for bytes instead of using the
        // native byte format. The value we need to provide as the local format for TypedValue
        // is directly accessible via the Protobuf representation. Both fields are sent by the
        // server to support older clients, so only parse the string value when it is alone.
        return protoValue.getStringValue();
      }
      // TypedValue is still going to expect a b64string for BYTE_STRING even though we sent it
      // across the wire natively as bytes. Return it as b64.
      return (new ByteString(protoValue.getBytesValue().toByteArray())).toBase64String();
    case STRING:
      return protoValue.getStringValue();
    case PRIMITIVE_CHAR:
    case CHARACTER:
      return protoValue.getStringValue().charAt(0);
    case BYTE:
    case PRIMITIVE_BYTE:
      return Long.valueOf(protoValue.getNumberValue()).byteValue();
    case DOUBLE:
    case PRIMITIVE_DOUBLE:
      return protoValue.getDoubleValue();
    case FLOAT:
    case PRIMITIVE_FLOAT:
      return Float.intBitsToFloat((int) protoValue.getNumberValue());
    case INTEGER:
    case PRIMITIVE_INT:
      return Long.valueOf(protoValue.getNumberValue()).intValue();
    case PRIMITIVE_SHORT:
    case SHORT:
      return Long.valueOf(protoValue.getNumberValue()).shortValue();
    case LONG:
    case PRIMITIVE_LONG:
      return Long.valueOf(protoValue.getNumberValue());
    case JAVA_SQL_DATE:
    case JAVA_SQL_TIME:
      return Long.valueOf(protoValue.getNumberValue()).intValue();
    case JAVA_SQL_TIMESTAMP:
    case JAVA_UTIL_DATE:
      return protoValue.getNumberValue();
    case BIG_INTEGER:
      return new BigInteger(protoValue.getBytesValue().toByteArray());
    case BIG_DECIMAL:
      // CALCITE-1103 shifts BigDecimals to be serialized as strings.
      if (protoValue.hasField(NUMBER_DESCRIPTOR)) {
        // This is the old (broken) style.
        BigInteger bigInt = new BigInteger(protoValue.getBytesValue().toByteArray());
        return new BigDecimal(bigInt, (int) protoValue.getNumberValue());
      }
      return new BigDecimal(protoValue.getStringValueBytes().toStringUtf8());
    case NUMBER:
      return Long.valueOf(protoValue.getNumberValue());
    case NULL:
      return null;
    case OBJECT:
      if (protoValue.getNull()) {
        return null;
      }
      // Intentional fall through to RTE. If we sent an object over the wire, it could only
      // possibly be null (at this point). Anything else has to be an error.
    case UNRECOGNIZED:
      // Fail?
      throw new RuntimeException("Unhandled type: " + protoValue.getType());
    default:
      // Fail?
      throw new RuntimeException("Unknown type: " + protoValue.getType());
    }
  }

  /**
   * Writes the given object into the Protobuf representation of a TypedValue. The object is
   * serialized given the type of that object, mapping it to the appropriate representation.
   *
   * @param builder The TypedValue protobuf builder
   * @param o The object (value)
   */
  public static void toProto(Common.TypedValue.Builder builder, Object o) {
    // Numbers
    if (o instanceof Byte) {
      writeToProtoWithType(builder, o, Common.Rep.BYTE);
    } else if (o instanceof Short) {
      writeToProtoWithType(builder, o, Common.Rep.SHORT);
    } else if (o instanceof Integer) {
      writeToProtoWithType(builder, o, Common.Rep.INTEGER);
    } else if (o instanceof Long) {
      writeToProtoWithType(builder, o, Common.Rep.LONG);
    } else if (o instanceof Double) {
      writeToProtoWithType(builder, o, Common.Rep.DOUBLE);
    } else if (o instanceof Float) {
      writeToProtoWithType(builder, ((Float) o).longValue(), Common.Rep.FLOAT);
    } else if (o instanceof BigDecimal) {
      writeToProtoWithType(builder, o, Common.Rep.BIG_DECIMAL);
    // Strings
    } else if (o instanceof String) {
      writeToProtoWithType(builder, o, Common.Rep.STRING);
    } else if (o instanceof Character) {
      writeToProtoWithType(builder, o.toString(), Common.Rep.CHARACTER);
    // Bytes
    } else if (o instanceof byte[]) {
      writeToProtoWithType(builder, o, Common.Rep.BYTE_STRING);
    // Boolean
    } else if (o instanceof Boolean) {
      writeToProtoWithType(builder, o, Common.Rep.BOOLEAN);
    } else if (null == o) {
      writeToProtoWithType(builder, o, Common.Rep.NULL);
    // Unhandled
    } else {
      throw new RuntimeException("Unhandled type in Frame: " + o.getClass());
    }
  }

  /**
   * Extracts the JDBC value from protobuf-TypedValue representation.
   *
   * @param protoValue Protobuf TypedValue
   * @param calendar Instance of a calendar
   * @return The JDBC representation of this TypedValue
   */
  public static Object protoToJdbc(Common.TypedValue protoValue, Calendar calendar) {
    Object o = getSerialFromProto(Objects.requireNonNull(protoValue));
    // Shortcircuit the null
    if (null == o) {
      return o;
    }
    return serialToJdbc(Rep.fromProto(protoValue.getType()), o, calendar);
    //return protoSerialToJdbc(protoValue.getType(), o, Objects.requireNonNull(calendar));
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
