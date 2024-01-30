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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Base class for lists whose contents are constant after creation.
 *
 * @param <E> Element type
 */
abstract class AbstractImmutableList<E> implements List<E> {
  protected abstract List<E> toList();

  @Override public Iterator<E> iterator() {
    return toList().iterator();
  }

  @Override public ListIterator<E> listIterator() {
    return toList().listIterator();
  }

  @Override public boolean isEmpty() {
    return false;
  }

  @Override public boolean add(E t) {
    throw new UnsupportedOperationException();
  }

  @Override public boolean addAll(Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  @Override public boolean addAll(int index, Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  @Override public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override public E set(int index, E element) {
    throw new UnsupportedOperationException();
  }

  @Override public void add(int index, E element) {
    throw new UnsupportedOperationException();
  }

  @Override public E remove(int index) {
    throw new UnsupportedOperationException();
  }

  @Override public ListIterator<E> listIterator(int index) {
    return toList().listIterator(index);
  }

  @Override public List<E> subList(int fromIndex, int toIndex) {
    return toList().subList(fromIndex, toIndex);
  }

  @Override public boolean contains(@Nullable Object o) {
    return indexOf(castNonNull(o)) >= 0;
  }

  @Override public boolean containsAll(Collection<?> c) {
    for (Object o : c) {
      if (!contains(o)) {
        return false;
      }
    }
    return true;
  }

  @Override public boolean remove(@Nullable Object o) {
    throw new UnsupportedOperationException();
  }
}
