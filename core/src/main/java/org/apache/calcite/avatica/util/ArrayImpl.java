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
package org.apache.calcite.avatica.util;

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Implementation of JDBC {@link Array}. */
public class ArrayImpl implements Array {
  private final List<Object> list;
  private final AbstractCursor.ArrayAccessor accessor;

  public ArrayImpl(List<Object> list, AbstractCursor.ArrayAccessor accessor) {
    this.list = list;
    this.accessor = accessor;
  }

  public String getBaseTypeName() throws SQLException {
    return accessor.componentType.getName();
  }

  public int getBaseType() throws SQLException {
    return accessor.componentType.id;
  }

  public Object getArray() throws SQLException {
    return getArray(list, accessor);
  }

  @Override public String toString() {
    final Iterator<?> iterator = list.iterator();
    if (!iterator.hasNext()) {
      return "[]";
    }
    final StringBuilder buf = new StringBuilder("[");
    for (;;) {
      accessor.componentSlotGetter.slot = iterator.next();
      try {
        append(buf, accessor.componentAccessor.getString());
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      accessor.componentSlotGetter.slot = null;
      if (!iterator.hasNext()) {
        return buf.append("]").toString();
      }
      buf.append(", ");
    }
  }

  private void append(StringBuilder buf, Object o) {
    if (o == null) {
      buf.append("null");
    } else if (o.getClass().isArray()) {
      append(buf, AvaticaUtils.primitiveList(o));
    } else {
      buf.append(o);
    }
  }

  /**
   * Converts a list into an array.
   *
   * <p>If the elements of the list are primitives, converts to an array of
   * primitives (e.g. {@code boolean[]}.</p>
   *
   * @param list List of objects
   *
   * @return array
   * @throws ClassCastException   if any element is not of the box type
   * @throws NullPointerException if any element is null
   */
  @SuppressWarnings("unchecked")
  protected Object getArray(List<?> list, AbstractCursor.ArrayAccessor arrayAccessor)
      throws SQLException {
    int i = 0;
    switch (arrayAccessor.componentType.rep) {
    case PRIMITIVE_DOUBLE:
      final double[] doubles = new double[list.size()];
      for (double v : (List<Double>) list) {
        doubles[i++] = v;
      }
      return doubles;
    case PRIMITIVE_FLOAT:
      final float[] floats = new float[list.size()];
      for (float v : (List<Float>) list) {
        floats[i++] = v;
      }
      return floats;
    case PRIMITIVE_INT:
      final int[] ints = new int[list.size()];
      for (int v : (List<Integer>) list) {
        ints[i++] = v;
      }
      return ints;
    case PRIMITIVE_LONG:
      final long[] longs = new long[list.size()];
      for (long v : (List<Long>) list) {
        longs[i++] = v;
      }
      return longs;
    case PRIMITIVE_SHORT:
      final short[] shorts = new short[list.size()];
      for (short v : (List<Short>) list) {
        shorts[i++] = v;
      }
      return shorts;
    case PRIMITIVE_BOOLEAN:
      final boolean[] booleans = new boolean[list.size()];
      for (boolean v : (List<Boolean>) list) {
        booleans[i++] = v;
      }
      return booleans;
    case PRIMITIVE_BYTE:
      final byte[] bytes = new byte[list.size()];
      for (byte v : (List<Byte>) list) {
        bytes[i++] = v;
      }
      return bytes;
    case PRIMITIVE_CHAR:
      final char[] chars = new char[list.size()];
      for (char v : (List<Character>) list) {
        chars[i++] = v;
      }
      return chars;
    default:
      // fall through
    }
    final Object[] objects = list.toArray();
    switch (arrayAccessor.componentType.id) {
    case Types.ARRAY:
      final AbstractCursor.ArrayAccessor componentAccessor =
          (AbstractCursor.ArrayAccessor) arrayAccessor.componentAccessor;
      for (i = 0; i < objects.length; i++) {
        // Convert the element into a Object[] or primitive array, recurse!
        objects[i] = getArrayData(objects[i], componentAccessor);
      }
    }
    return objects;
  }

  Object getArrayData(Object o, AbstractCursor.ArrayAccessor componentAccessor)
      throws SQLException {
    if (o instanceof List) {
      return getArray((List<?>) o, componentAccessor);
    } else if (o instanceof ArrayImpl) {
      return (ArrayImpl) o;
    }
    throw new RuntimeException("Unhandled");
  }

  @Override public Object getArray(Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException(); // TODO
  }

  @Override public Object getArray(long index, int count) throws SQLException {
    if (index > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Arrays cannot be longer than " + Integer.MAX_VALUE);
    }
    // Convert from one-index to zero-index
    int startIndex = ((int) index) - 1;
    if (startIndex < 0 || startIndex > list.size()) {
      throw new IllegalArgumentException("Invalid index: " + index + ". Size = " + list.size());
    }
    int endIndex = startIndex + count;
    if (endIndex > list.size()) {
      throw new IllegalArgumentException("Invalid count provided. Size = " + list.size()
          + ", count = " + count);
    }
    // End index is non-inclusive
    return getArray(list.subList(startIndex, endIndex), accessor);
  }

  @Override public Object getArray(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    throw new UnsupportedOperationException(); // TODO
  }

  @Override public ResultSet getResultSet() throws SQLException {
    return accessor.factory.create(accessor.componentType, list);
  }

  @Override public ResultSet getResultSet(Map<String, Class<?>> map)
      throws SQLException {
    throw new UnsupportedOperationException(); // TODO
  }

  @Override public ResultSet getResultSet(long index, int count) throws SQLException {
    throw new UnsupportedOperationException(); // TODO
  }

  @Override public ResultSet getResultSet(long index, int count,
      Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException(); // TODO
  }

  @Override public void free() throws SQLException {
    // nothing to do
  }

  /** Returns whether two arrays have the same contents.
   *
   * <p>Arrays must have the same size, and elements in the same order.
   * Elements are compared using {@link Object#equals(Object)}, and null
   * values are equal to each other. */
  public static boolean equalContents(Array left, Array right) throws SQLException {
    ResultSet leftResultSet = left.getResultSet();
    ResultSet rightResultSet = right.getResultSet();
    while (leftResultSet.next() && rightResultSet.next()) {
      if (!Objects.equals(leftResultSet.getObject(1), rightResultSet.getObject(1))) {
        return false;
      }
    }
    return !leftResultSet.next() && !rightResultSet.next();
  }

  /** Factory that can create a ResultSet or Array based on a stream of values. */
  public interface Factory {

    /**
     * Creates a {@link ResultSet} from the given list of values per {@link Array#getResultSet()}.
     *
     * @param elementType The type of the elements
     * @param iterable The elements
     * @throws SQLException on error
     */
    ResultSet create(ColumnMetaData.AvaticaType elementType, Iterable<Object> iterable)
        throws SQLException;

    /**
     * Creates an {@link Array} from the given list of values, converting any primitive values
     * into the corresponding objects.
     *
     * @param elementType The type of the elements
     * @param elements The elements
     */
    Array createArray(ColumnMetaData.AvaticaType elementType, Iterable<Object> elements);
  }
}

// End ArrayImpl.java
