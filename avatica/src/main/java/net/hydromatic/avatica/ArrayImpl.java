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
package net.hydromatic.avatica;

import java.sql.*;
import java.util.*;

/** Implementation of JDBC {@link Array}. */
public class ArrayImpl implements Array {
  private final List list;
  private final ColumnMetaData component;

  public ArrayImpl(List list, ColumnMetaData component) {
    this.list = list;
    this.component = component;
  }

  public String getBaseTypeName() throws SQLException {
    return component.typeName;
  }

  public int getBaseType() throws SQLException {
    return component.type;
  }

  public Object getArray() throws SQLException {
    return getArray(list);
  }

  /**
   * Converts a collection of boxed primitives into an array of primitives.
   *
   * @param list Collection of boxed primitives
   *
   * @return array of primitives
   * @throws ClassCastException   if any element is not of the box type
   * @throws NullPointerException if any element is null
   */
  @SuppressWarnings("unchecked")
  protected Object getArray(List list) throws SQLException {
    int i = 0;
    switch (component.representation) {
    case PRIMITIVE_DOUBLE:
      double[] doubles = new double[list.size()];
      for (double v : (Collection<Double>) list) {
        doubles[i++] = v;
      }
      return doubles;
    case PRIMITIVE_FLOAT:
      float[] floats = new float[list.size()];
      for (float v : (Collection<Float>) list) {
        floats[i++] = v;
      }
      return floats;
    case PRIMITIVE_INT:
      int[] ints = new int[list.size()];
      for (int v : (Collection<Integer>) list) {
        ints[i++] = v;
      }
      return ints;
    case PRIMITIVE_LONG:
      long[] longs = new long[list.size()];
      for (long v : (Collection<Long>) list) {
        longs[i++] = v;
      }
      return longs;
    case PRIMITIVE_SHORT:
      short[] shorts = new short[list.size()];
      for (short v : (Collection<Short>) list) {
        shorts[i++] = v;
      }
      return shorts;
    case PRIMITIVE_BOOLEAN:
      boolean[] booleans = new boolean[list.size()];
      for (boolean v : (Collection<Boolean>) list) {
        booleans[i++] = v;
      }
      return booleans;
    case PRIMITIVE_BYTE:
      byte[] bytes = new byte[list.size()];
      for (byte v : (Collection<Byte>) list) {
        bytes[i++] = v;
      }
      return bytes;
    case PRIMITIVE_CHAR:
      char[] chars = new char[list.size()];
      for (char v : (Collection<Character>) list) {
        chars[i++] = v;
      }
      return chars;
    default:
      // fall through
    }
    final Object[] objects = list.toArray();
    switch (component.type) {
    case Types.ARRAY:
      for (i = 0; i < objects.length; i++) {
        objects[i] = new ArrayImpl((List) objects[i], component.component);
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
    throw new UnsupportedOperationException(); // TODO
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
}

// End ArrayImpl.java
