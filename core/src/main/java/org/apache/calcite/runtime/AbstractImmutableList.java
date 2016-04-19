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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import javax.annotation.Nonnull;

/**
 * Base class for lists whose contents are constant after creation.
 *
 * @param <E> Element type
 */
abstract class AbstractImmutableList<E> implements List<E> {
  protected abstract List<E> toList();

  @Nonnull public Iterator<E> iterator() {
    return toList().iterator();
  }

  @Nonnull public ListIterator<E> listIterator() {
    return toList().listIterator();
  }

  public boolean isEmpty() {
    return false;
  }

  public boolean add(E t) {
    throw new UnsupportedOperationException();
  }

  public boolean addAll(@Nonnull Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  public boolean addAll(int index, @Nonnull Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  public boolean removeAll(@Nonnull Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  public boolean retainAll(@Nonnull Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  public void clear() {
    throw new UnsupportedOperationException();
  }

  public E set(int index, E element) {
    throw new UnsupportedOperationException();
  }

  public void add(int index, E element) {
    throw new UnsupportedOperationException();
  }

  public E remove(int index) {
    throw new UnsupportedOperationException();
  }

  @Nonnull public ListIterator<E> listIterator(int index) {
    return toList().listIterator(index);
  }

  @Nonnull public List<E> subList(int fromIndex, int toIndex) {
    return toList().subList(fromIndex, toIndex);
  }

  public boolean contains(Object o) {
    return indexOf(o) >= 0;
  }

  public boolean containsAll(@Nonnull Collection<?> c) {
    for (Object o : c) {
      if (!contains(o)) {
        return false;
      }
    }
    return true;
  }

  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }
}

// End AbstractImmutableList.java
