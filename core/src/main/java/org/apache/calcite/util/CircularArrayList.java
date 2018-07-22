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
package org.apache.calcite.util;

import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

/**
 * Similar to {@link ArrayDeque}, but can be used as a list.
 *
 * @param <E> Element type
 */
public class CircularArrayList<E> extends AbstractList<E>
    implements RandomAccess {
  private E[] es; // length must be multiple of 2
  private int first;
  private int last;

  public CircularArrayList() {
    this(16);
  }

  public CircularArrayList(Collection<E> nodes) {
    this(nextPowerOf2(nodes.size()));
    addAll(nodes);
  }

  private CircularArrayList(int capacity) {
    first = last = 0;
    //noinspection unchecked
    es = (E[]) new Object[capacity];
  }

  private static int nextPowerOf2(int v) {
    // Algorithm from
    // http://graphics.stanford.edu/~seander/bithacks.html
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
  }

  @SuppressWarnings({"SuspiciousSystemArraycopy", "unchecked" })
  private void expand() {
    Object[] olds = es;
    es = (E[]) new Object[es.length * 2];
    System.arraycopy(olds, 0, es, 0, olds.length);
    if (last <= first) {
      final int x = last & (olds.length - 1);
      System.arraycopy(olds, 0, es, olds.length, x);
      last += olds.length;
    }
  }

  public E get(int index) {
    if (index < 0 || index >= size()) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: "
          + size());
    }
    return es[(first + index) & (es.length - 1)];
  }

  public int size() {
    assert last >= 0 && last < es.length;
    assert first >= 0 && first < es.length;
    return (last - first) & (es.length - 1);
  }

  @Override public boolean isEmpty() {
    return last == first;
  }

  @Override public boolean add(E e) {
    es[last] = e;
    last = (last + 1) & (es.length - 1);
    if (last == first) {
      expand();
    }
    return true;
  }

  @Override public void add(int index, E element) {
    if (index == size()) {
      add(element);
      return;
    }
    int i;
    if (first < last) {
      // list is not "wrapped around"
      i = first + index;
      if (index <= (last - first) / 2 && first > 0) {
        // before half way; shift earlier elements down
        System.arraycopy(es, first + 1, es, first, i - first);
        --first;
        --i;
      } else {
        // after half way; shift later elements up
        System.arraycopy(es, i, es, i + 1, last - i);
        last = (last + 1) & (es.length - 1);
      }
    } else {
      // list is "wrapped around"
      i = (first + index) & (es.length - 1);
      if (i >= first) {
        // The element is in the first part of the logical list, which means at
        // the end of the buffer. Move everything before it down.
        System.arraycopy(es, first, es, first - 1, i - first);
        first = (first - 1) & (es.length - 1);
        i = (i - 1) & (es.length - 1);
      } else {
        // The element is in the last part of the logical list, which means at
        // the beginning of the buffer. Move everything after it up.
        System.arraycopy(es, i, es, i + 1, last - i - 1);
        last = (last + 1) & (es.length - 1);
      }
    }
    es[i] = element;
    if (last == first) {
      expand();
    }
  }

  /** As {@link ArrayDeque#addFirst(Object)}, adds the specified element to the
   * front of this list. */
  public void addFirst(E e) {
    add(0, e);
  }

  /** As {@link ArrayDeque#addLast(Object)}, adds the specified element to the
   * end of this list. */
  public void addLast(E e) {
    add(e);
  }

  public void clear() {
    if (first < last) {
      for (int i = first; i <= last; i++) {
        es[i] = null;
      }
    } else {
      for (int i = 0; i < last; i++) {
        es[i] = null;
      }
      for (int i = first; i < es.length; i++) {
        es[i] = null;
      }
    }
    first = last = 0;
  }

  @Override public E remove(int index) {
    if (index == 0) {
      return removeFirst();
    }
    if (index < 0 || index >= size()) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: "
          + size());
    }
    if (index == size() - 1) {
      return removeLast();
    }
    final E e;
    if (first < last) {
      // list is not "wrapped around"
      final int i = first + index;
      e = es[i];
      if (i <= (last - first) / 2) {
        // before half way; shift earlier elements up
        System.arraycopy(es, first, es, first + 1, i - first);
        es[first++] = null; // clear to allow gc
      } else {
        // after half way; shift later elements down
        System.arraycopy(es, i + 1, es, i, last - i - 1);
        es[--last] = null; // clear to allow gc
      }
    } else {
      // list is "wrapped around"
      final int i = (first + index) & (es.length - 1);
      e = es[i];
      if (i >= first) {
        // The element is in the first part of the logical list, which means at
        // the end of the buffer. Move everything before it up.
        System.arraycopy(es, first, es, first + 1, i - first);
        es[first] = null; // clear to allow gc
        first = (first + 1) & (es.length - 1);
      } else {
        // The element is in the last part of the logical list, which means at
        // the beginning of the buffer. Move everything after it down.
        System.arraycopy(es, i + 1, es, i, last - i - 1);
        last = (last - 1) & (es.length - 1);
        es[last] = null; // clear to allow gc
      }
    }
    return e;
  }

  /** As {@link java.util.Deque#pop()}, removes an returns an element or
   * throws {@link NoSuchElementException} if the list is empty. */
  public E pop() {
    return removeFirst();
  }

  /** As {@link Deque#removeFirst()}, removes an returns an element or
   * throws {@link NoSuchElementException} if the list is empty. */
  public E removeFirst() {
    if (last == first) {
      throw new NoSuchElementException();
    }
    E e = es[first];
    es[first] = null; // allow gc
    first = (first + 1) & (es.length - 1);
    return e;
  }

  public E removeLast() {
    if (last == first) {
      throw new NoSuchElementException();
    }
    last = (last - 1) & (es.length - 1);
    E e = es[last];
    es[last] = null; // allow gc
    return e;
  }
}

// End CircularArrayList.java
