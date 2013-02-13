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
package net.hydromatic.linq4j.expressions;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Enumeration of Java's primitive types.
 *
 * <p>There are fields for the native class (e.g. <code>int</code>, also
 * known as {@link Integer#TYPE}) and the boxing class
 * (e.g. {@link Integer}).</p>
 */
public enum Primitive {
  BOOLEAN(Boolean.TYPE, Boolean.class, 1, true, Boolean.FALSE, Boolean.TRUE),
  BYTE(Byte.TYPE, Byte.class, 2, true, Byte.MIN_VALUE, Byte.MAX_VALUE),
  CHAR(Character.TYPE, Character.class, 2, true, Character.MIN_VALUE,
      Character.MAX_VALUE),
  SHORT(Short.TYPE, Short.class, 2, true, Short.MIN_VALUE, Short.MAX_VALUE),
  INT(Integer.TYPE, Integer.class, 2, true, Short.MIN_VALUE, Short.MAX_VALUE),
  LONG(Long.TYPE, Long.class, 2, true, Long.MIN_VALUE, Long.MAX_VALUE),
  FLOAT(Float.TYPE, Float.class, 3, false, Float.MIN_VALUE, Float.MAX_VALUE),
  DOUBLE(Double.TYPE, Double.class, 3, false, Double.MIN_VALUE,
      Double.MAX_VALUE),
  VOID(Void.TYPE, Void.class, 4, false, null, null),
  OTHER(null, null, 5, false, null, null);

  public final Class primitiveClass;
  public final Class boxClass;
  public final String primitiveName; // e.g. "int"
  private final int family;
  public final boolean fixed;
  public final Object min;
  public final Object max;

  private static final Map<Class, Primitive> PRIMITIVE_MAP =
      new HashMap<Class, Primitive>();
  private static final Map<Class, Primitive> BOX_MAP =
      new HashMap<Class, Primitive>();

  static {
    Primitive[] values = Primitive.values();
    for (Primitive value : values) {
      if (value.primitiveClass != null) {
        PRIMITIVE_MAP.put(value.primitiveClass, value);
      }
      if (value.boxClass != null) {
        BOX_MAP.put(value.boxClass, value);
      }
    }
  }

  Primitive(Class primitiveClass, Class boxClass, int family, boolean fixed,
      Object min, Object max) {
    this.primitiveClass = primitiveClass;
    this.family = family;
    this.primitiveName =
        primitiveClass != null ? primitiveClass.getSimpleName() : null;
    this.boxClass = boxClass;
    this.fixed = fixed;
    this.min = min;
    this.max = max;
  }

  /**
   * Returns the Primitive object for a given primitive class.
   *
   * <p>For example, <code>of(Long.TYPE)</code> or <code>of(long.class)</code>
   * returns {@link #LONG}.
   */
  public static Primitive of(Type type) {
    //noinspection SuspiciousMethodCalls
    return PRIMITIVE_MAP.get(type);
  }

  /**
   * Returns the Primitive object for a given boxing class.
   *
   * <p>For example, <code>ofBox(java.util.Long.class)</code>
   * returns {@link #LONG}.
   */
  public static Primitive ofBox(Type type) {
    //noinspection SuspiciousMethodCalls
    return BOX_MAP.get(type);
  }

  /**
   * Converts a primitive type to a boxed type; returns other types
   * unchanged.
   */
  public static Type box(Type type) {
    Primitive primitive = of(type);
    return primitive == null ? type : primitive.boxClass;
  }

  /**
   * Converts a primitive class to a boxed class; returns other classes
   * unchanged.
   */
  public static Class box(Class type) {
    Primitive primitive = of(type);
    return primitive == null ? type : primitive.boxClass;
  }

  /**
   * Converts a boxed type to a primitive type; returns other types
   * unchanged.
   */
  public static Type unbox(Type type) {
    Primitive primitive = ofBox(type);
    return primitive == null ? type : primitive.primitiveClass;
  }

  /**
   * Converts a boxed class to a primitive class; returns other types
   * unchanged.
   */
  public static Class unbox(Class type) {
    Primitive primitive = ofBox(type);
    return primitive == null ? type : primitive.primitiveClass;
  }

  /**
   * Adapts a primitive array into a {@link List}. For example,
   * {@code asList(new double[2])} returns a {@code List&lt;Double&gt;}.
   */
  public static List<?> asList(final Object array) {
    // REVIEW: A per-type list might be more efficient. (Or might not.)
    return new AbstractList() {
      public Object get(int index) {
        return Array.get(array, index);
      }

      public int size() {
        return Array.getLength(array);
      }
    };
  }

  /**
   * Adapts an array of {@code boolean} into a {@link List} of
   * {@link Boolean}.
   */
  public static List<Boolean> asList(boolean[] elements) {
    //noinspection unchecked
    return (List<Boolean>) asList((Object) elements);
  }

  /**
   * Adapts an array of {@code byte} into a {@link List} of
   * {@link Byte}.
   */
  public static List<Byte> asList(byte[] elements) {
    //noinspection unchecked
    return (List<Byte>) asList((Object) elements);
  }

  /**
   * Adapts an array of {@code char} into a {@link List} of
   * {@link Character}.
   */
  public static List<Character> asList(char[] elements) {
    //noinspection unchecked
    return (List<Character>) asList((Object) elements);
  }

  /**
   * Adapts an array of {@code short} into a {@link List} of
   * {@link Short}.
   */
  public static List<Short> asList(short[] elements) {
    //noinspection unchecked
    return (List<Short>) asList((Object) elements);
  }

  /**
   * Adapts an array of {@code int} into a {@link List} of
   * {@link Integer}.
   */
  public static List<Integer> asList(int[] elements) {
    //noinspection unchecked
    return (List<Integer>) asList((Object) elements);
  }

  /**
   * Adapts an array of {@code long} into a {@link List} of
   * {@link Long}.
   */
  public static List<Long> asList(long[] elements) {
    //noinspection unchecked
    return (List<Long>) asList((Object) elements);
  }

  /**
   * Adapts an array of {@code float} into a {@link List} of
   * {@link Float}.
   */
  public static List<Float> asList(float[] elements) {
    //noinspection unchecked
    return (List<Float>) asList((Object) elements);
  }

  /**
   * Adapts an array of {@code double} into a {@link List} of
   * {@link Double}.
   */
  public static List<Double> asList(double[] elements) {
    //noinspection unchecked
    return (List<Double>) asList((Object) elements);
  }

  /**
   * Converts a collection of boxed primitives into an array of primitives.
   *
   * @param collection Collection of boxed primitives
   *
   * @return array of primitives
   * @throws ClassCastException   if any element is not of the box type
   * @throws NullPointerException if any element is null
   */
  @SuppressWarnings("unchecked")
  public Object toArray(Collection collection) {
    int i = 0;
    switch (this) {
    case DOUBLE:
      double[] doubles = new double[collection.size()];
      for (double _double : (Collection<Double>) collection) {
        doubles[i++] = _double;
      }
      return doubles;
    case FLOAT:
      float[] floats = new float[collection.size()];
      for (float _float : (Collection<Float>) collection) {
        floats[i++] = _float;
      }
      return floats;
    case INT:
      int[] ints = new int[collection.size()];
      for (int _int : (Collection<Integer>) collection) {
        ints[i++] = _int;
      }
      return ints;
    case LONG:
      long[] longs = new long[collection.size()];
      for (long _long : (Collection<Long>) collection) {
        longs[i++] = _long;
      }
      return longs;
    case SHORT:
      short[] shorts = new short[collection.size()];
      for (short _short : (Collection<Short>) collection) {
        shorts[i++] = _short;
      }
      return shorts;
    case BOOLEAN:
      boolean[] booleans = new boolean[collection.size()];
      for (boolean _boolean : (Collection<Boolean>) collection) {
        booleans[i++] = _boolean;
      }
      return booleans;
    case BYTE:
      byte[] bytes = new byte[collection.size()];
      for (byte _byte : (Collection<Byte>) collection) {
        bytes[i++] = _byte;
      }
      return bytes;
    case CHAR:
      char[] chars = new char[collection.size()];
      for (char _char : (Collection<Character>) collection) {
        chars[i++] = _char;
      }
      return chars;
    default:
      throw new RuntimeException("unexpected: " + this);
    }
  }

  /**
   * Converts a collection of {@link Number} to a primitive array.
   */
  public Object toArray2(Collection<Number> collection) {
    int i = 0;
    switch (this) {
    case DOUBLE:
      double[] doubles = new double[collection.size()];
      for (Number number : collection) {
        doubles[i++] = number.doubleValue();
      }
      return doubles;
    case FLOAT:
      float[] floats = new float[collection.size()];
      for (Number number : collection) {
        floats[i++] = number.floatValue();
      }
      return floats;
    case INT:
      int[] ints = new int[collection.size()];
      for (Number number : collection) {
        ints[i++] = number.intValue();
      }
      return ints;
    case LONG:
      long[] longs = new long[collection.size()];
      for (Number number : collection) {
        longs[i++] = number.longValue();
      }
      return longs;
    case SHORT:
      short[] shorts = new short[collection.size()];
      for (Number number : collection) {
        shorts[i++] = number.shortValue();
      }
      return shorts;
    case BOOLEAN:
      boolean[] booleans = new boolean[collection.size()];
      for (Number number : collection) {
        booleans[i++] = number.byteValue() != 0;
      }
      return booleans;
    case BYTE:
      byte[] bytes = new byte[collection.size()];
      for (Number number : collection) {
        bytes[i++] = number.byteValue();
      }
      return bytes;
    case CHAR:
      char[] chars = new char[collection.size()];
      for (Number number : collection) {
        chars[i++] = (char) number.shortValue();
      }
      return chars;
    default:
      throw new RuntimeException("unexpected: " + this);
    }
  }

  /**
   * Sends a field value to a sink.
   */
  public void send(Field field, Object o, Sink sink)
      throws IllegalAccessException {
    switch (this) {
    case BOOLEAN:
      sink.set(field.getBoolean(o));
      break;
    case BYTE:
      sink.set(field.getByte(o));
      break;
    case CHAR:
      sink.set(field.getChar(o));
      break;
    case SHORT:
      sink.set(field.getShort(o));
      break;
    case INT:
      sink.set(field.getInt(o));
      break;
    case LONG:
      sink.set(field.getLong(o));
      break;
    case FLOAT:
      sink.set(field.getFloat(o));
      break;
    case DOUBLE:
      sink.set(field.getDouble(o));
      break;
    default:
      sink.set(field.get(o));
      break;
    }
  }

  /**
   * Gets an item from an array.
   */
  public Object arrayItem(Object dataSet, int ordinal) {
    // Plain old Array.get doesn't cut it when you have an array of
    // Integer values but you want to read Short values. Array.getShort
    // does the right thing.
    switch (this) {
    case DOUBLE:
      return Array.getDouble(dataSet, ordinal);
    case FLOAT:
      return Array.getFloat(dataSet, ordinal);
    case BOOLEAN:
      return Array.getBoolean(dataSet, ordinal);
    case BYTE:
      return Array.getByte(dataSet, ordinal);
    case CHAR:
      return Array.getChar(dataSet, ordinal);
    case SHORT:
      return Array.getShort(dataSet, ordinal);
    case INT:
      return Array.getInt(dataSet, ordinal);
    case LONG:
      return Array.getLong(dataSet, ordinal);
    case OTHER:
      return Array.get(dataSet, ordinal);
    default:
      throw new AssertionError("unexpected " + this);
    }
  }

  /**
   * Reads value from a source into an array.
   */
  public void arrayItem(Source source, Object dataSet, int ordinal) {
    switch (this) {
    case DOUBLE:
      Array.setDouble(dataSet, ordinal, source.getDouble());
      return;
    case FLOAT:
      Array.setFloat(dataSet, ordinal, source.getFloat());
      return;
    case BOOLEAN:
      Array.setBoolean(dataSet, ordinal, source.getBoolean());
      return;
    case BYTE:
      Array.setByte(dataSet, ordinal, source.getByte());
      return;
    case CHAR:
      Array.setChar(dataSet, ordinal, source.getChar());
      return;
    case SHORT:
      Array.setShort(dataSet, ordinal, source.getShort());
      return;
    case INT:
      Array.setInt(dataSet, ordinal, source.getInt());
      return;
    case LONG:
      Array.setLong(dataSet, ordinal, source.getLong());
      return;
    case OTHER:
      Array.set(dataSet, ordinal, source.getObject());
      return;
    default:
      throw new AssertionError("unexpected " + this);
    }
  }

  /**
   * Sends to a sink an from an array.
   */
  public void arrayItem(Object dataSet, int ordinal, Sink sink) {
    switch (this) {
    case DOUBLE:
      sink.set(Array.getDouble(dataSet, ordinal));
      return;
    case FLOAT:
      sink.set(Array.getFloat(dataSet, ordinal));
      return;
    case BOOLEAN:
      sink.set(Array.getBoolean(dataSet, ordinal));
      return;
    case BYTE:
      sink.set(Array.getByte(dataSet, ordinal));
      return;
    case CHAR:
      sink.set(Array.getChar(dataSet, ordinal));
      return;
    case SHORT:
      sink.set(Array.getShort(dataSet, ordinal));
      return;
    case INT:
      sink.set(Array.getInt(dataSet, ordinal));
      return;
    case LONG:
      sink.set(Array.getLong(dataSet, ordinal));
      return;
    case OTHER:
      sink.set(Array.get(dataSet, ordinal));
      return;
    default:
      throw new AssertionError("unexpected " + this);
    }
  }

  /**
   * Gets a value from a given column in a JDBC result set.
   *
   * @param resultSet Result set
   * @param i Ordinal of column (1-based, per JDBC)
   */
  public Object jdbcGet(ResultSet resultSet, int i) throws SQLException {
    switch (this) {
    case BOOLEAN:
      return resultSet.getBoolean(i);
    case BYTE:
      return resultSet.getByte(i);
    case CHAR:
      return (char) resultSet.getShort(i);
    case DOUBLE:
      return resultSet.getDouble(i);
    case FLOAT:
      return resultSet.getFloat(i);
    case INT:
      return resultSet.getInt(i);
    case LONG:
      return resultSet.getLong(i);
    case SHORT:
      return resultSet.getShort(i);
    default:
      return resultSet.getObject(i);
    }
  }

  /**
   * Sends to a sink a value from a given column in a JDBC result set.
   *
   * @param resultSet Result set
   * @param i Ordinal of column (1-based, per JDBC)
   * @param sink Sink
   */
  public void jdbc(ResultSet resultSet, int i, Sink sink) throws SQLException {
    switch (this) {
    case BOOLEAN:
      sink.set(resultSet.getBoolean(i));
      break;
    case BYTE:
      sink.set(resultSet.getByte(i));
      break;
    case CHAR:
      sink.set((char) resultSet.getShort(i));
      break;
    case DOUBLE:
      sink.set(resultSet.getDouble(i));
      break;
    case FLOAT:
      sink.set(resultSet.getFloat(i));
      break;
    case INT:
      sink.set(resultSet.getInt(i));
      break;
    case LONG:
      sink.set(resultSet.getLong(i));
      break;
    case SHORT:
      sink.set(resultSet.getShort(i));
      break;
    default:
      sink.set(resultSet.getObject(i));
      break;
    }
  }

  /**
   * Sends a value from a source to a sink.
   */
  public void send(Source source, Sink sink) {
    switch (this) {
    case BOOLEAN:
      sink.set(source.getBoolean());
      break;
    case BYTE:
      sink.set(source.getByte());
      break;
    case CHAR:
      sink.set(source.getChar());
      break;
    case DOUBLE:
      sink.set(source.getDouble());
      break;
    case FLOAT:
      sink.set(source.getFloat());
      break;
    case INT:
      sink.set(source.getInt());
      break;
    case LONG:
      sink.set(source.getLong());
      break;
    case SHORT:
      sink.set(source.getShort());
      break;
    default:
      sink.set(source.getObject());
      break;
    }
  }

  /**
   * Calls the appropriate {@link Integer#valueOf(String) valueOf(String)}
   * method.
   */
  public Object parse(String stringValue) {
    switch (this) {
    case BOOLEAN:
      return Boolean.valueOf(stringValue);
    case BYTE:
      return Byte.valueOf(stringValue);
    case CHAR:
      return Character.valueOf(stringValue.charAt(0));
    case DOUBLE:
      return Double.valueOf(stringValue);
    case FLOAT:
      return Float.valueOf(stringValue);
    case INT:
      return Integer.valueOf(stringValue);
    case LONG:
      return Long.valueOf(stringValue);
    case SHORT:
      return Short.valueOf(stringValue);
    default:
      throw new AssertionError(stringValue);
    }
  }

  public boolean assignableFrom(Primitive primitive) {
    return family == primitive.family && ordinal() >= primitive.ordinal() && !(
        this == SHORT
        && primitive == CHAR) && !(this == CHAR && primitive == BYTE);
  }

  /** Creates a number value of this primitive's box type. For example,
   * {@code SHORT.number(Integer(0))} will return {@code Short(0)}. */
  public Number number(Number value) {
    switch (this) {
    case BYTE:
      return Byte.valueOf(value.byteValue());
    case DOUBLE:
      return Double.valueOf(value.doubleValue());
    case FLOAT:
      return Float.valueOf(value.floatValue());
    case INT:
      return Integer.valueOf(value.intValue());
    case LONG:
      return Long.valueOf(value.longValue());
    case SHORT:
      return Short.valueOf(value.shortValue());
    default:
      throw new AssertionError(this + ": " + value);
    }
  }

  /**
   * A place to send a value.
   */
  interface Sink {
    void set(boolean v);

    void set(byte v);

    void set(char v);

    void set(short v);

    void set(int v);

    void set(long v);

    void set(float v);

    void set(double v);

    void set(Object v);
  }

  /**
   * A place from which to read a value.
   */
  interface Source {
    boolean getBoolean();

    byte getByte();

    char getChar();

    short getShort();

    int getInt();

    long getLong();

    float getFloat();

    double getDouble();

    Object getObject();
  }
}

// End Primitive.java
