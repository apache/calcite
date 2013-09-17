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

import java.util.Collections;
import java.util.Comparator;

/**
 * Compares arrays.
 */
public class ArrayComparator implements Comparator<Object[]> {
  private final Comparator[] comparators;

  public ArrayComparator(Comparator... comparators) {
    this.comparators = comparators;
  }

  public ArrayComparator(boolean... descendings) {
    this.comparators = comparators(descendings);
  }

  private static Comparator[] comparators(boolean[] descendings) {
    Comparator[] comparators = new Comparator[descendings.length];
    for (int i = 0; i < descendings.length; i++) {
      boolean descending = descendings[i];
      comparators[i] =
          descending
              ? Collections.reverseOrder()
              : ComparableComparator.instance();
    }
    return comparators;
  }

  public int compare(Object[] o1, Object[] o2) {
    for (int i = 0; i < comparators.length; i++) {
      Comparator comparator = comparators[i];
      int c = comparator.compare(o1[i], o2[i]);
      if (c != 0) {
        return c;
      }
    }
    return 0;
  }
}

// End ArrayComparator.java
