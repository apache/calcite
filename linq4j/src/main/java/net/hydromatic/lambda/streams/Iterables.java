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
package net.hydromatic.lambda.streams;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Utilities for {@link Iterable}.
 *
 * <p>Based on API of {@code java.util.Iterables} in OpenJDK 8.</p>
 */
public final class Iterables {
  private Iterables() {
    throw new AssertionError();
  }

  /**
   * Returns the only element of an iterable.
   *
   * @throws NoSuchElementException if this Iterable contains no elements.
   * @throws IllegalStateException  if this Iterable contains more than one
   *                                element.
   */
  public static <T> T only(Iterable<T> iterable) {
    Iterator<T> iterator = iterable.iterator();
    if (!iterator.hasNext()) {
      throw new NoSuchElementException();
    }
    T t = iterator.next();
    if (iterator.hasNext()) {
      throw new IllegalStateException();
    }
    return t;
  }
}

// End Iterables.java
