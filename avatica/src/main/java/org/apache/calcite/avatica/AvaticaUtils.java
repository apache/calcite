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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Avatica utilities. */
public class AvaticaUtils {
  private static final Map<Class, Class> BOX;

  private static final MethodHandle SET_LARGE_MAX_ROWS =
      method(void.class, Statement.class, "setLargeMaxRows", long.class);
  private static final MethodHandle GET_LARGE_MAX_ROWS =
      method(long.class, Statement.class, "getLargeMaxRows");
  private static final MethodHandle GET_LARGE_UPDATE_COUNT =
      method(void.class, Statement.class, "getLargeUpdateCount");

  private AvaticaUtils() {}

  static {
    BOX = new HashMap<>();
    BOX.put(boolean.class, Boolean.class);
    BOX.put(byte.class, Byte.class);
    BOX.put(char.class, Character.class);
    BOX.put(short.class, Short.class);
    BOX.put(int.class, Integer.class);
    BOX.put(long.class, Long.class);
    BOX.put(float.class, Float.class);
    BOX.put(double.class, Double.class);
  }

  private static MethodHandle method(Class returnType, Class targetType,
      String name, Class... argTypes) {
    final MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      return lookup.findVirtual(targetType, name,
          MethodType.methodType(returnType, targetType, argTypes));
    } catch (NoSuchMethodException e) {
      return null;
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Does nothing with its argument. Call this method when you have a value
   * you are not interested in, but you don't want the compiler to warn that
   * you are not using it.
   */
  public static void discard(Object o) {
    if (false) {
      discard(o);
    }
  }

  /**
   * Adapts a primitive array into a {@link List}. For example,
   * {@code asList(new double[2])} returns a {@code List&lt;Double&gt;}.
   */
  public static List<?> primitiveList(final Object array) {
    // REVIEW: A per-type list might be more efficient. (Or might not.)
    return new AbstractList() {
      public Object get(int index) {
        return java.lang.reflect.Array.get(array, index);
      }

      public int size() {
        return java.lang.reflect.Array.getLength(array);
      }
    };
  }

  /**
   * Converts a camelCase name into an upper-case underscore-separated name.
   * For example, {@code camelToUpper("myJdbcDriver")} returns
   * "MY_JDBC_DRIVER".
   */
  public static String camelToUpper(String name) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (Character.isUpperCase(c)) {
        buf.append('_');
      } else {
        c = Character.toUpperCase(c);
      }
      buf.append(c);
    }
    return buf.toString();
  }

  /**
   * Converts an underscore-separated name into a camelCase name.
   * For example, {@code uncamel("MY_JDBC_DRIVER")} returns "myJdbcDriver".
   */
  public static String toCamelCase(String name) {
    StringBuilder buf = new StringBuilder();
    int nextUpper = -1;
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (c == '_') {
        nextUpper = i + 1;
        continue;
      }
      if (nextUpper == i) {
        c = Character.toUpperCase(c);
      } else {
        c = Character.toLowerCase(c);
      }
      buf.append(c);
    }
    return buf.toString();
  }

  /** Returns the boxed class. For example, {@code box(int.class)}
   * returns {@code java.lang.Integer}. */
  public static Class box(Class clazz) {
    if (clazz.isPrimitive()) {
      return BOX.get(clazz);
    }
    return clazz;
  }

  /** Creates an instance of a plugin class. First looks for a static
   * member called INSTANCE, then calls a public default constructor.
   *
   * <p>If className contains a "#" instead looks for a static field.
   *
   * @param pluginClass Class (or interface) to instantiate
   * @param className Name of implementing class
   * @param <T> Class
   * @return Plugin instance
   */
  public static <T> T instantiatePlugin(Class<T> pluginClass,
      String className) {
    try {
      // Given a static field, say "com.example.MyClass#FOO_INSTANCE", return
      // the value of that static field.
      if (className.contains("#")) {
        try {
          int i = className.indexOf('#');
          String left = className.substring(0, i);
          String right = className.substring(i + 1);
          //noinspection unchecked
          final Class<T> clazz = (Class) Class.forName(left);
          final Field field;
          field = clazz.getField(right);
          return pluginClass.cast(field.get(null));
        } catch (NoSuchFieldException e) {
          // ignore
        }
      }
      //noinspection unchecked
      final Class<T> clazz = (Class) Class.forName(className);
      assert pluginClass.isAssignableFrom(clazz);
      try {
        // We assume that if there is an INSTANCE field it is static and
        // has the right type.
        final Field field = clazz.getField("INSTANCE");
        return pluginClass.cast(field.get(null));
      } catch (NoSuchFieldException e) {
        // ignore
      }
      return clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Property '" + className
          + "' not valid for plugin type " + pluginClass.getName(), e);
    }
  }

  /** Reads the contents of an input stream and returns as a string. */
  public static String readFully(InputStream inputStream) throws IOException {
    return _readFully(inputStream).toString();
  }

  public static byte[] readFullyToBytes(InputStream inputStream) throws IOException {
    return _readFully(inputStream).toByteArray();
  }

  /** Reads the contents of an input stream and returns a ByteArrayOutputStrema. */
  static ByteArrayOutputStream _readFully(InputStream inputStream) throws IOException {
    final byte[] bytes = new byte[4096];
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (;;) {
      int count = inputStream.read(bytes, 0, bytes.length);
      if (count < 0) {
        break;
      }
      baos.write(bytes, 0, count);
    }
    return baos;
  }

  /** Invokes {@code Statement#setLargeMaxRows}, falling back on
   * {@link Statement#setMaxRows(int)} if the method does not exist (before
   * JDK 1.8) or throws {@link UnsupportedOperationException}. */
  public static void setLargeMaxRows(Statement statement, long n)
      throws SQLException {
    if (SET_LARGE_MAX_ROWS != null) {
      try {
        // Call Statement.setLargeMaxRows
        SET_LARGE_MAX_ROWS.invokeExact(n);
        return;
      } catch (UnsupportedOperationException e) {
        // ignore, and fall through to call Statement.setMaxRows
      } catch (Error | RuntimeException | SQLException e) {
        throw e;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
    int i = (int) Math.max(Math.min(n, Integer.MAX_VALUE), Integer.MIN_VALUE);
    statement.setMaxRows(i);
  }

  /** Invokes {@code Statement#getLargeMaxRows}, falling back on
   * {@link Statement#getMaxRows()} if the method does not exist (before
   * JDK 1.8) or throws {@link UnsupportedOperationException}. */
  public static long getLargeMaxRows(Statement statement) throws SQLException {
    if (GET_LARGE_MAX_ROWS != null) {
      try {
        // Call Statement.getLargeMaxRows
        return (long) GET_LARGE_MAX_ROWS.invokeExact();
      } catch (UnsupportedOperationException e) {
        // ignore, and fall through to call Statement.getMaxRows
      } catch (Error | RuntimeException | SQLException e) {
        throw e;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
    return statement.getMaxRows();
  }

  /** Invokes {@code Statement#getLargeUpdateCount}, falling back on
   * {@link Statement#getUpdateCount()} if the method does not exist (before
   * JDK 1.8) or throws {@link UnsupportedOperationException}. */
  public static long getLargeUpdateCount(Statement statement)
      throws SQLException {
    if (GET_LARGE_UPDATE_COUNT != null) {
      try {
        // Call Statement.getLargeUpdateCount
        return (long) GET_LARGE_UPDATE_COUNT.invokeExact();
      } catch (UnsupportedOperationException e) {
        // ignore, and fall through to call Statement.getUpdateCount
      } catch (Error | RuntimeException | SQLException e) {
        throw e;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
    return statement.getUpdateCount();
  }
}

// End AvaticaUtils.java
