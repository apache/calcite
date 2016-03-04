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
package org.apache.calcite.interpreter;

import java.util.Arrays;

/**
 * Row.
 */
public class Row {
  private final Object[] values;

  /** Creates a Row. */
  // must stay package-protected, because does not copy
  Row(Object[] values) {
    this.values = values;
  }

  /** Creates a Row.
   *
   * <p>Makes a defensive copy of the array, so the Row is immutable.
   * (If you're worried about the extra copy, call {@link #of(Object)}.
   * But the JIT probably avoids the copy.)
   */
  public static Row asCopy(Object... values) {
    return new Row(values.clone());
  }

  /** Creates a Row with one column value. */
  public static Row of(Object value0) {
    return new Row(new Object[] {value0});
  }

  /** Creates a Row with two column values. */
  public static Row of(Object value0, Object value1) {
    return new Row(new Object[] {value0, value1});
  }

  /** Creates a Row with three column values. */
  public static Row of(Object value0, Object value1, Object value2) {
    return new Row(new Object[] {value0, value1, value2});
  }

  /** Creates a Row with variable number of values. */
  public static Row of(Object...values) {
    return new Row(values);
  }

  @Override public int hashCode() {
    return Arrays.hashCode(values);
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof Row
        && Arrays.equals(values, ((Row) obj).values);
  }

  @Override public String toString() {
    return Arrays.toString(values);
  }

  public Object getObject(int index) {
    return values[index];
  }

  // must stay package-protected
  Object[] getValues() {
    return values;
  }

  /** Returns a copy of the values. */
  public Object[] copyValues() {
    return values.clone();
  }

  public int size() {
    return values.length;
  }

  /**
   * Create a RowBuilder object that eases creation of a new row.
   *
   * @param size Number of columns in output data.
   * @return New RowBuilder object.
   */
  public static RowBuilder newBuilder(int size) {
    return new RowBuilder(size);
  }

  /**
   * Utility class to build row objects.
   */
  public static class RowBuilder {
    Object[] values;

    private RowBuilder(int size) {
      values = new Object[size];
    }

    /**
     * Set the value of a particular column.
     * @param index Zero-indexed position of value.
     * @param value Desired column value.
     */
    public void set(int index, Object value) {
      values[index] = value;
    }

    /** Return a Row object **/
    public Row build() {
      return new Row(values);
    }

    /** Allocates a new internal array. */
    public void reset() {
      values = new Object[values.length];
    }

    public int size() {
      return values.length;
    }
  }


}

// End Row.java
