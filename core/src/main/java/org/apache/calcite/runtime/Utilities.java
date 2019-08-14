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
package org.apache.calcite.runtime;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Utility methods called by generated code.
 */
public class Utilities {
  // Even though this is a utility class (all methods are static), we cannot
  // make the constructor private. Because Janino doesn't do static import,
  // generated code is placed in sub-classes.
  protected Utilities() {
  }

  /** @deprecated Use {@link java.util.Objects#equals}. */
  @Deprecated // to be removed before 2.0
  public static boolean equal(Object o0, Object o1) {
    // Same as java.lang.Objects.equals (JDK 1.7 and later)
    // and com.google.common.base.Objects.equal
    return Objects.equals(o0, o1);
  }

  public static int hash(Object v) {
    return v == null ? 0 : v.hashCode();
  }

  /** Computes the hash code of a {@code double} value. Equivalent to
   * {@link Double}{@code .hashCode(double)}, but that method was only
   * introduced in JDK 1.8.
   *
   * @param v Value
   * @return Hash code
   * @deprecated Use {@link Double#hashCode(double)}
   */
  @Deprecated // to be removed before 2.0
  public static int hashCode(double v) {
    return Double.hashCode(v);
  }

  /** Computes the hash code of a {@code float} value. Equivalent to
   * {@link Float}{@code .hashCode(float)}, but that method was only
   * introduced in JDK 1.8.
   *
   * @param v Value
   * @return Hash code
   * @deprecated Use {@link Float#hashCode(float)}
   */
  @Deprecated // to be removed before 2.0
  public static int hashCode(float v) {
    return Float.hashCode(v);
  }

  /** Computes the hash code of a {@code long} value. Equivalent to
   * {@link Long}{@code .hashCode(long)}, but that method was only
   * introduced in JDK 1.8.
   *
   * @param v Value
   * @return Hash code
   * @deprecated Use {@link Long#hashCode(long)}
   */
  @Deprecated // to be removed before 2.0
  public static int hashCode(long v) {
    return Long.hashCode(v);
  }

  /** Computes the hash code of a {@code boolean} value. Equivalent to
   * {@link Boolean}{@code .hashCode(boolean)}, but that method was only
   * introduced in JDK 1.8.
   *
   * @param v Value
   * @return Hash code
   * @deprecated Use {@link Boolean#hashCode(boolean)}
   */
  @Deprecated // to be removed before 2.0
  public static int hashCode(boolean v) {
    return Boolean.hashCode(v);
  }

  public static int hash(int h, boolean v) {
    return h * 31 + hashCode(v);
  }

  public static int hash(int h, byte v) {
    return h * 31 + v;
  }

  public static int hash(int h, char v) {
    return h * 31 + v;
  }

  public static int hash(int h, short v) {
    return h * 31 + v;
  }

  public static int hash(int h, int v) {
    return h * 31 + v;
  }

  public static int hash(int h, long v) {
    return h * 31 + Long.hashCode(v);
  }

  public static int hash(int h, float v) {
    return hash(h, Float.hashCode(v));
  }

  public static int hash(int h, double v) {
    return hash(h, Double.hashCode(v));
  }

  public static int hash(int h, Object v) {
    return h * 31 + (v == null ? 1 : v.hashCode());
  }

  public static int compare(boolean v0, boolean v1) {
    return Boolean.compare(v0, v1);
  }

  public static int compare(byte v0, byte v1) {
    return Byte.compare(v0, v1);
  }

  public static int compare(char v0, char v1) {
    return Character.compare(v0, v1);
  }

  public static int compare(short v0, short v1) {
    return Short.compare(v0, v1);
  }

  public static int compare(int v0, int v1) {
    return Integer.compare(v0, v1);
  }

  public static int compare(long v0, long v1) {
    return Long.compare(v0, v1);
  }

  public static int compare(float v0, float v1) {
    return Float.compare(v0, v1);
  }

  public static int compare(double v0, double v1) {
    return Double.compare(v0, v1);
  }

  public static int compare(List v0, List v1) {
    //noinspection unchecked
    final Iterator iterator0 = v0.iterator();
    final Iterator iterator1 = v1.iterator();
    for (;;) {
      if (!iterator0.hasNext()) {
        return !iterator1.hasNext()
            ? 0
            : -1;
      }
      if (!iterator1.hasNext()) {
        return 1;
      }
      final Object o0 = iterator0.next();
      final Object o1 = iterator1.next();
      int c = compare_(o0, o1);
      if (c != 0) {
        return c;
      }
    }
  }

  private static int compare_(Object o0, Object o1) {
    if (o0 instanceof Comparable) {
      return compare((Comparable) o0, (Comparable) o1);
    }
    return compare((List) o0, (List) o1);
  }

  public static int compare(Comparable v0, Comparable v1) {
    //noinspection unchecked
    return v0.compareTo(v1);
  }

  public static int compareNullsFirst(Comparable v0, Comparable v1) {
    //noinspection unchecked
    return v0 == v1 ? 0
        : v0 == null ? -1
            : v1 == null ? 1
                : v0.compareTo(v1);
  }

  public static int compareNullsLast(Comparable v0, Comparable v1) {
    //noinspection unchecked
    return v0 == v1 ? 0
        : v0 == null ? 1
            : v1 == null ? -1
                : v0.compareTo(v1);
  }

  public static int compareNullsLast(List v0, List v1) {
    //noinspection unchecked
    return v0 == v1 ? 0
        : v0 == null ? 1
            : v1 == null ? -1
                : FlatLists.ComparableListImpl.compare(v0, v1);
  }

  /** Creates a pattern builder. */
  public static Pattern.PatternBuilder patternBuilder() {
    return Pattern.builder();
  }
}

// End Utilities.java
