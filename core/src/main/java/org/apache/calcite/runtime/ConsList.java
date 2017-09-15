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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import javax.annotation.Nonnull;

/**
 * List that consists of a head element and an immutable non-empty list.
 *
 * @param <E> Element type
 */
public class ConsList<E> extends AbstractImmutableList<E> {
  private final E first;
  private final List<E> rest;

  /** Creates a ConsList.
   * It consists of an element pre-pended to another list.
   * If the other list is mutable, creates an immutable copy. */
  public static <E> List<E> of(E first, List<? extends E> rest) {
    if (rest instanceof ConsList
        || rest instanceof ImmutableList
        && !rest.isEmpty()) {
      //noinspection unchecked
      return new ConsList<>(first, (List<E>) rest);
    } else {
      return ImmutableList.<E>builder().add(first).addAll(rest).build();
    }
  }

  private ConsList(E first, List<E> rest) {
    this.first = first;
    this.rest = rest;
  }

  public E get(int index) {
    for (ConsList<E> c = this;; c = (ConsList<E>) c.rest) {
      if (index == 0) {
        return c.first;
      }
      --index;
      if (!(c.rest instanceof ConsList)) {
        return c.rest.get(index);
      }
    }
  }

  public int size() {
    int s = 1;
    for (ConsList c = this;; c = (ConsList) c.rest, ++s) {
      if (!(c.rest instanceof ConsList)) {
        return s + c.rest.size();
      }
    }
  }

  @Override public int hashCode() {
    return toList().hashCode();
  }

  @Override public boolean equals(Object o) {
    return o == this
        || o instanceof List
        && toList().equals(o);
  }

  @Override public String toString() {
    return toList().toString();
  }

  protected final List<E> toList() {
    final List<E> list = new ArrayList<>();
    for (ConsList<E> c = this;; c = (ConsList<E>) c.rest) {
      list.add(c.first);
      if (!(c.rest instanceof ConsList)) {
        list.addAll(c.rest);
        return list;
      }
    }
  }

  @Override @Nonnull public ListIterator<E> listIterator() {
    return toList().listIterator();
  }

  @Override @Nonnull public Iterator<E> iterator() {
    return toList().iterator();
  }

  @Override @Nonnull public ListIterator<E> listIterator(int index) {
    return toList().listIterator(index);
  }

  @Nonnull public Object[] toArray() {
    return toList().toArray();
  }

  @Nonnull public <T> T[] toArray(@Nonnull T[] a) {
    final int s = size();
    if (s > a.length) {
      a = Arrays.copyOf(a, s);
    } else if (s < a.length) {
      a[s] = null;
    }
    int i = 0;
    for (ConsList c = this;; c = (ConsList) c.rest) {
      //noinspection unchecked
      a[i++] = (T) c.first;
      if (!(c.rest instanceof ConsList)) {
        Object[] a2 = c.rest.toArray();
        //noinspection SuspiciousSystemArraycopy
        System.arraycopy(a2, 0, a, i, a2.length);
        return a;
      }
    }
  }

  public int indexOf(Object o) {
    return toList().indexOf(o);
  }

  public int lastIndexOf(Object o) {
    return toList().lastIndexOf(o);
  }
}

// End ConsList.java
