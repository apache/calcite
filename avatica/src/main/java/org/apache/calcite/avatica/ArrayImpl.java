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
package net.hydromatic.avatica;

import java.sql.*;
import java.util.*;

/** Implementation of JDBC {@link Array}. */
public class ArrayImpl implements Array {
  private final ColumnMetaData.AvaticaType elementType;
  private final Factory factory;
  private final List list;

  public ArrayImpl(List list, ColumnMetaData.AvaticaType elementType,
      Factory factory) {
    this.list = list;
    this.elementType = elementType;
    this.factory = factory;
  }

  public String getBaseTypeName() throws SQLException {
    return elementType.typeName;
  }

  public int getBaseType() throws SQLException {
    return elementType.type;
  }

  public Object getArray() throws SQLException {
    return getArray(list);
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
  protected Object getArray(List list) throws SQLException {
    int i = 0;
    switch (elementType.representation) {
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
    switch (elementType.type) {
    case Types.ARRAY:
      final ColumnMetaData.ArrayType arrayType =
          (ColumnMetaData.ArrayType) elementType;
      for (i = 0; i < objects.length; i++) {
        objects[i] =
            new ArrayImpl((List) objects[i], arrayType.component, factory);
      }
    }
    return objects;
  }

  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException(); // TODO
  }

  public Object getArray(long index, int count) throws SQLException {
    return getArray(list.subList((int) index, count));
  }

  public Object getArray(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    throw new UnsupportedOperationException(); // TODO
  }

  public ResultSet getResultSet() throws SQLException {
    return factory.create(elementType, list);
  }

  public ResultSet getResultSet(Map<String, Class<?>> map)
      throws SQLException {
    throw new UnsupportedOperationException(); // TODO
  }

  public ResultSet getResultSet(long index, int count) throws SQLException {
    throw new UnsupportedOperationException(); // TODO
  }

  public ResultSet getResultSet(long index, int count,
      Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException(); // TODO
  }

  public void free() throws SQLException {
    // nothing to do
  }

  /** Factory that can create a result set based on a list of values. */
  public interface Factory {
    ResultSet create(ColumnMetaData.AvaticaType elementType, Iterable iterable);
  }
}

// End ArrayImpl.java
