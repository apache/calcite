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

import net.hydromatic.linq4j.function.Function1;

import java.util.Comparator;

/**
 * Extension methods for {@link OrderedEnumerable}.
 *
 * @param <T> Element type
 */
public interface ExtendedOrderedEnumerable<T> extends Enumerable<T> {
  /**
   * Performs a subsequent ordering of the elements in an
   * {@link OrderedEnumerable} according to a key, using a specified
   * comparator.
   *
   * <p>The functionality provided by this method is like that provided by
   * {@link #thenBy(net.hydromatic.linq4j.function.Function1, java.util.Comparator) thenBy}
   * or {@link #thenByDescending(net.hydromatic.linq4j.function.Function1, java.util.Comparator) thenByDescending},
   * depending on whether descending is true or false. They both perform a
   * subordinate ordering of an already sorted sequence of type
   * {@link OrderedEnumerable}.</p>
   */
  <TKey> OrderedEnumerable<T> createOrderedEnumerable(
      Function1<T, TKey> keySelector, Comparator<TKey> comparator,
      boolean descending);

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * ascending order according to a key.
   */
  <TKey extends Comparable<TKey>> OrderedEnumerable<T> thenBy(
      Function1<T, TKey> keySelector);

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * ascending order according to a key, using a specified comparator.
   */
  <TKey> OrderedEnumerable<T> thenBy(Function1<T, TKey> keySelector,
      Comparator<TKey> comparator);

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * descending order according to a key.
   */
  <TKey extends Comparable<TKey>> OrderedEnumerable<T> thenByDescending(
      Function1<T, TKey> keySelector);

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * descending order according to a key, using a specified comparator.
   */
  <TKey> OrderedEnumerable<T> thenByDescending(Function1<T, TKey> keySelector,
      Comparator<TKey> comparator);
}

// End ExtendedOrderedEnumerable.java
