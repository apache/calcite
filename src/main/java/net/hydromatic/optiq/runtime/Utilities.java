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
package net.hydromatic.optiq.runtime;

import java.util.Iterator;
import java.util.List;

/**
 * Utility methods called by generated code.
 */
public class Utilities {
  public static boolean equal(Object o0, Object o1) {
    return o0 == o1 || o0 != null && o0.equals(o1);
  }

  public static int hash(int h, boolean v) {
    return h * 37 + (v ? 2 : 1);
  }

  public static int hash(int h, byte v) {
    return h * 37 + v;
  }

  public static int hash(int h, char v) {
    return h * 37 + v;
  }

  public static int hash(int h, short v) {
    return h * 37 + v;
  }

  public static int hash(int h, int v) {
    return h * 37 + v;
  }

  public static int hash(int h, long v) {
    return h * 37 + (int)(v ^ (v >>> 32));
  }

  public static int hash(int h, float v) {
    return hash(h, Float.floatToIntBits(v));
  }

  public static int hash(int h, double v) {
    return hash(h, Double.doubleToLongBits(v));
  }

  public static int hash(int h, Object v) {
    return h * 37 + (v == null ? 1 : v.hashCode());
  }

  public static int compare(boolean v0, boolean v1) {
    // Same as Boolean.compare (introduced in JDK 1.7)
    return (v0 == v1) ? 0 : (v0 ? 1 : -1);
  }

  public static int compare(byte v0, byte v1) {
    // Same as Byte.compare (introduced in JDK 1.7)
    return v0 - v1;
  }

  public static int compare(char v0, char v1) {
    // Same as Character.compare (introduced in JDK 1.7)
    return v0 - v1;
  }

  public static int compare(short v0, short v1) {
    // Same as Short.compare (introduced in JDK 1.7)
    return v0 - v1;
  }

  public static int compare(int v0, int v1) {
    // Same as Integer.compare (introduced in JDK 1.7)
    return (v0 < v1) ? -1 : ((v0 == v1) ? 0 : 1);
  }

  public static int compare(long v0, long v1) {
    // Same as Long.compare (introduced in JDK 1.7)
    return (v0 < v1) ? -1 : ((v0 == v1) ? 0 : 1);
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
                : FlatLists.ComparableList.compare(v0, v1);
  }
}

// End Utilities.java
