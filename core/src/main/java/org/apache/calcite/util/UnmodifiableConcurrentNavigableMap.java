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

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;

class UnmodifiableConcurrentNavigableMap<K, V> extends ConcurrentSkipListMap<K, V> {
  private final ConcurrentNavigableMap<K, V> delegate;

  UnmodifiableConcurrentNavigableMap(ConcurrentNavigableMap<K, V> delegate) {
    this.delegate = delegate;
  }

  // Provide unmodifiable wrappers for all methods of ConcurrentNavigableMap
  @Override public V put(K key, V value) {
    throw new UnsupportedOperationException();
  }

  @Override public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  private V doGet(Object key) {
    throw new UnsupportedOperationException();
  }

  private V doPut(K key, V value, boolean onlyIfAbsent) {
    throw new UnsupportedOperationException();
  }

  final V doRemove(Object key, Object value) {
    throw new UnsupportedOperationException();
  }

  public V putIfAbsent(K key, V value) {
    throw new UnsupportedOperationException();
  }

  public boolean remove(Object key, Object value) {
    throw new UnsupportedOperationException();
  }

  public boolean replace(K key, V oldValue, V newValue) {
    throw new UnsupportedOperationException();
  }

  public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
    throw new UnsupportedOperationException();
  }


  public V replace(K key, V value) {
    throw new UnsupportedOperationException();
  }

  public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
    throw new UnsupportedOperationException();
  }

  boolean removeEntryIf(Predicate<? super Entry<K, V>> function) {
    throw new UnsupportedOperationException();
  }

  boolean removeValueIf(Predicate<? super V> function) {
    throw new UnsupportedOperationException();
  }

  @Override public V get(Object key) {
    return delegate.get(key);
  }

  @Override public boolean containsKey(Object key) {
    return delegate.containsKey(key);
  }

  @Override public Set<Entry<K, V>> entrySet() {
    return Collections.unmodifiableSet(delegate.entrySet());
  }

  @Override public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey,
      boolean toInclusive) {
    return new UnmodifiableConcurrentNavigableMap<>(
        delegate.subMap(fromKey, fromInclusive, toKey
        , toInclusive));
  }
}
