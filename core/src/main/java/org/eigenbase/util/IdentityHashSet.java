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
package org.eigenbase.util;

import java.util.*;

/**
 * Set based on object identity, like {@link IdentityHashMap}.
 */
public class IdentityHashSet<E> extends AbstractSet<E> implements Set<E> {
  private final Map<E, Object> map;

  // Dummy value to associate with an Object in the backing Map
  private static final Object PRESENT = new Object();

  /**
   * Creates an empty IdentityHashSet.
   */
  public IdentityHashSet() {
    map = new IdentityHashMap<E, Object>();
  }

  /**
   * Creates an IdentityHashSet containing the elements of the specified
   * collection.
   *
   * @param c the collection whose elements are to be placed into this set
   */
  public IdentityHashSet(Collection<? extends E> c) {
    map = new IdentityHashMap<E, Object>(Math.max(c.size() * 2 + 1, 16));
    addAll(c);
  }

  public int size() {
    return map.size();
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public boolean contains(Object o) {
    //noinspection SuspiciousMethodCalls
    return map.containsKey(o);
  }

  public Iterator<E> iterator() {
    return map.keySet().iterator();
  }

  public Object[] toArray() {
    return map.keySet().toArray();
  }

  public <T> T[] toArray(T[] a) {
    //noinspection SuspiciousToArrayCall
    return map.keySet().toArray(a);
  }

  public boolean add(E e) {
    return map.put(e, PRESENT) != null;
  }

  public boolean remove(Object o) {
    return map.remove(o) != null;
  }

  public void clear() {
    map.clear();
  }
}

// End IdentityHashSet.java
