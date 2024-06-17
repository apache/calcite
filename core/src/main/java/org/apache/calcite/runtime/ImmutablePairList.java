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

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/** Immutable list of pairs.
 *
 * @param <T> First type
 * @param <U> Second type
 */
public interface ImmutablePairList<T, U> extends PairList<T, U> {

  /** Creates an empty ImmutablePairList. */
  @SuppressWarnings("unchecked")
  static <T, U> ImmutablePairList<T, U> of() {
    return (ImmutablePairList<T, U>) PairLists.EMPTY;
  }

  /** Creates a singleton ImmutablePairList. */
  static <T, U> ImmutablePairList<T, U> of(T t, U u) {
    return new PairLists.SingletonImmutablePairList<>(t, u);
  }

  /** Creates an ImmutablePairList with one or more entries. */
  static <T, U> PairList<T, U> copyOf(T t, U u, Object... rest) {
    checkArgument(rest.length % 2 == 0, "even number");
    if (rest.length == 0) {
      return new PairLists.SingletonImmutablePairList<>(t, u);
    }
    Object[] elements = new Object[rest.length + 2];
    elements[0] = requireNonNull(t, "t");
    elements[1] = requireNonNull(u, "u");
    System.arraycopy(rest, 0, elements, 2, rest.length);
    return new PairLists.ArrayImmutablePairList<>(elements);
  }

  /** Creates an ImmutablePairList whose contents are a copy of a given
   * collection. */
  @SuppressWarnings("unchecked")
  static <@NonNull T, @NonNull U> ImmutablePairList<T, U> copyOf(
      Iterable<? extends Map.Entry<T, U>> iterable) {
    // Every PairList - mutable and immutable - knows how to quickly make
    // itself immutable.
    if (iterable instanceof PairList) {
      return ((PairList<T, U>) iterable).immutable();
    }

    // If it's a collection, we know its size, and therefore can create an
    // array directly, without an intermediate ArrayList.
    if (iterable instanceof Collection) {
      final Collection<? extends Map.Entry<T, U>> collection =
          (Collection<? extends Map.Entry<T, U>>) iterable;
      switch (collection.size()) {
      case 0:
        return of();

      case 1:
        // Use of iterator is suboptimal. If we knew this was a list we could
        // call get(0), but the special case doesn't seem worth the effort.
        final Map.Entry<T, U> entry = iterable.iterator().next();
        return of(entry.getKey(), entry.getValue());

      default:
        Object[] elements = new Object[2 * collection.size()];
        int i = 0;
        for (Map.Entry<T, U> entry2 : iterable) {
          elements[i++] = castNonNull(entry2.getKey());
          elements[i++] = castNonNull(entry2.getValue());
        }
        return new PairLists.ArrayImmutablePairList<>(elements);
      }
    }

    // Not a collection, so we don't know its size in advance.
    final List<Object> list = new ArrayList<>();
    iterable.forEach(entry -> {
      list.add(castNonNull(entry.getKey()));
      list.add(castNonNull(entry.getValue()));
    });
    return PairLists.immutableBackedBy(list);
  }

  @Override default ImmutablePairList<T, U> immutable() {
    return this;
  }

  @Override ImmutablePairList<T, U> subList(int fromIndex, int toIndex);
}
