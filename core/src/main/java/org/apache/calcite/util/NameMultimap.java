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

import org.apache.calcite.linq4j.function.Experimental;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.stream.Collectors;

import static org.apache.calcite.util.CaseInsensitiveComparator.COMPARATOR;

/** Multimap whose keys are names and can be accessed with and without case
 * sensitivity.
 *
 * @param <V> Value type */
public class NameMultimap<V> {
  private final NameMap<List<V>> map;

  /** Creates a NameMultimap based on an existing map. */
  private NameMultimap(NameMap<List<V>> map) {
    this.map = map;
    assert map.map().comparator() == COMPARATOR;
  }

  /** Creates a NameMultimap, initially empty. */
  public NameMultimap() {
    this(new NameMap<>());
  }

  @Override public String toString() {
    return map.toString();
  }

  @Override public int hashCode() {
    return map.hashCode();
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof NameMultimap
        && map.equals(((NameMultimap) obj).map);
  }

  /** Adds an entry to this multimap. */
  public void put(String name, V v) {
    List<V> list = map().computeIfAbsent(name, k -> new ArrayList<>());
    list.add(v);
  }

  /** Removes all entries that have the given case-sensitive key.
   *
   * @return Whether a value was removed */
  @Experimental
  public boolean remove(String key, V value) {
    final List<V> list = map().get(key);
    if (list == null) {
      return false;
    }
    return list.remove(value);
  }

  /** Returns a map containing all the entries in this multimap that match the
   * given name. */
  public Collection<Map.Entry<String, V>> range(String name,
      boolean caseSensitive) {
    NavigableMap<String, List<V>> range = map.range(name, caseSensitive);
    List<Pair<String, V>> result = range.entrySet().stream()
        .flatMap(e -> e.getValue().stream().map(v -> Pair.of(e.getKey(), v)))
        .collect(Collectors.toList());
    return Collections.unmodifiableList(result);
  }

  /** Returns whether this map contains a given key, with a given
   * case-sensitivity. */
  public boolean containsKey(String name, boolean caseSensitive) {
    return map.containsKey(name, caseSensitive);
  }

  /** Returns the underlying map.
   * Its size is the number of keys, not the number of values. */
  public NavigableMap<String, List<V>> map() {
    return map.map();
  }
}

// End NameMultimap.java
