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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * An implementation of {@code java.util.Map} that ignores any {@code put}
 * operation.
 *
 * <p>The implementation does not fully conform to {@code java.util.Map} API, as
 * any write operation would succeed, but any read operation would not return
 * any value.
 *
 * @param <K> the type of the keys for the map
 * @param <V> the type of the values for the map
 */
final class BlackholeMap<K, V> extends AbstractMap<K, V> {
  /**
   * Blackhole implementation of {@code Iterator}. Always empty.
   *
   * @param <E> type of the entries for the iterator
   */
  private static final class BHIterator<E> implements Iterator<E> {
    @SuppressWarnings("rawtypes")
    private static final Iterator INSTANCE = new BHIterator<>();
    private BHIterator() {}

    @Override public boolean hasNext() {
      return false;
    }

    @Override public E next() {
      throw new NoSuchElementException();
    }

    @SuppressWarnings("unchecked")
    public static <T> Iterator<T> of() {
      return (Iterator<T>) INSTANCE;
    }
  }

  /**
   * Blackhole implementation of {@code Set}. Always ignores add.
   *
   * @param <E> type of the entries for the set
   */
  private static class BHSet<E> extends AbstractSet<E> {
    @SuppressWarnings("rawtypes")
    private static final Set INSTANCE = new BHSet<>();

    @Override public boolean add(E e) {
      return true;
    }

    @Override public Iterator<E> iterator() {
      return BHIterator.of();
    }

    @Override public int size() {
      return 0;
    }

    @SuppressWarnings("unchecked")
    public static <T> Set<T> of() {
      return (Set<T>) INSTANCE;
    }

  }

  @SuppressWarnings("rawtypes")
  private static final Map INSTANCE = new BlackholeMap<>();

  private BlackholeMap() {}

  @Override public V put(K key, V value) {
    return null;
  }

  @Override public Set<Entry<K, V>> entrySet() {
    return BHSet.of();
  }

  /**
   * Gets an instance of {@code BlackholeMap}
   *
   * @param <K> type of the keys for the map
   * @param <V> type of the values for the map
   * @return a blackhole map
   */
  @SuppressWarnings("unchecked")
  public static <K, V> Map<K, V> of() {
    return (Map<K, V>) INSTANCE;
  }
}

// End BlackholeMap.java
