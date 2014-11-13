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
package net.hydromatic.linq4j;

import java.util.Arrays;
import java.util.List;

/**
 * Enumerator over the cartesian product of enumerators.
 *
 * @param <T> Element type
 */
class CartesianProductEnumerator<T> implements Enumerator<List<T>> {
  private final List<Enumerator<T>> enumerators;
  private final T[] elements;
  private boolean first = true;

  public CartesianProductEnumerator(List<Enumerator<T>> enumerators) {
    this.enumerators = enumerators;
    //noinspection unchecked
    this.elements = (T[]) new Object[enumerators.size()];
  }

  public List<T> current() {
    return Arrays.asList(elements.clone());
  }

  public boolean moveNext() {
    if (first) {
      if (enumerators.isEmpty()) {
        return false;
      }
      int i = 0;
      for (Enumerator<T> enumerator : enumerators) {
        if (!enumerator.moveNext()) {
          return false;
        }
        elements[i++] = enumerator.current();
      }
      first = false;
      return true;
    }
    int ordinal = enumerators.size() - 1;
    for (;;) {
      final Enumerator<T> enumerator = enumerators.get(ordinal);
      if (enumerator.moveNext()) {
        elements[ordinal] = enumerator.current();
        return true;
      }

      // Move back to first element.
      enumerator.reset();
      if (!enumerator.moveNext()) {
        // Very strange... this was empty all along.
        return false;
      }
      elements[ordinal] = enumerator.current();

      // Advance higher rank enumerator.
      if (ordinal == 0) {
        return false;
      }
      --ordinal;
    }
  }

  public void reset() {
    first = true;
  }

  public void close() {
    // If there is one or more exceptions, carry on and close all enumerators,
    // then throw the first.
    Throwable rte = null;
    for (Enumerator<T> enumerator : enumerators) {
      try {
        enumerator.close();
      } catch (Throwable e) {
        rte = e;
      }
    }
    if (rte != null) {
      if (rte instanceof Error) {
        throw (Error) rte;
      } else {
        throw (RuntimeException) rte;
      }
    }
  }
}

// End CartesianProductEnumerator.java
