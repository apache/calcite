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
package net.hydromatic.optiq.impl.interpreter;

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
}

// End Row.java
