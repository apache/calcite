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
package org.apache.calcite.linq4j;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of {@link Grouping}.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
class GroupingImpl<K, V> extends AbstractEnumerable<V>
    implements Grouping<K, V>, Map.Entry<K, Enumerable<V>> {
  private final K key;
  private final Collection<V> values;

  GroupingImpl(K key, Collection<V> values) {
    this.key = Objects.requireNonNull(key);
    this.values = Objects.requireNonNull(values);
  }

  @Override public String toString() {
    return key + ": " + values;
  }

  /** {@inheritDoc}
   *
   * <p>Computes hash code consistent with
   * {@link java.util.Map.Entry#hashCode()}. */
  @Override public int hashCode() {
    return key.hashCode() ^ values.hashCode();
  }

  @Override public boolean equals(Object obj) {
    return obj instanceof GroupingImpl
           && key.equals(((GroupingImpl) obj).key)
           && values.equals(((GroupingImpl) obj).values);
  }

  // implement Map.Entry
  public Enumerable<V> getValue() {
    return Linq4j.asEnumerable(values);
  }

  // implement Map.Entry
  public Enumerable<V> setValue(Enumerable<V> value) {
    // immutable
    throw new UnsupportedOperationException();
  }

  // implement Map.Entry
  // implement Grouping
  public K getKey() {
    return key;
  }

  public Enumerator<V> enumerator() {
    return Linq4j.enumerator(values);
  }
}

// End GroupingImpl.java
