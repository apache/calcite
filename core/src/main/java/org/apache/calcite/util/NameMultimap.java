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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.calcite.util.NameSet.COMPARATOR;

/** Multimap whose keys are names and can be accessed with and without case
 * sensitivity.
 *
 * @param <V> Value type */
public class NameMultimap<V> {
  private final NavigableMap<String, List<V>> map;

  /** Creates a NameMultimap based on an existing map. */
  private NameMultimap(NavigableMap<String, List<V>> map) {
    this.map = map;
    assert this.map.comparator() == COMPARATOR;
  }

  /** Creates a NameMultimap, initially empty. */
  public NameMultimap() {
    this(new TreeMap<String, List<V>>(COMPARATOR));
  }

  /** Adds an entry to this multimap. */
  public void put(String name, V v) {
    List<V> list = map.computeIfAbsent(name, k -> new ArrayList<>());
    list.add(v);
  }

  /** Removes all entries that have the given case-sensitive key.
   *
   * @return Whether a value was removed */
  @Experimental
  public boolean remove(String key, V value) {
    final List<V> list = map.get(key);
    if (list == null) {
      return false;
    }
    return list.remove(value);
  }

  /** Returns a map containing all the entries in this multimap that match the
   * given name. */
  public Collection<Map.Entry<String, V>> range(String name,
      boolean caseSensitive) {
    if (caseSensitive) {
      final List<V> list = map.get(name);
      if (list != null && !list.isEmpty()) {
        final ImmutableList.Builder<Map.Entry<String, V>> builder =
            ImmutableList.builder();
        for (V v : list) {
          builder.add(Pair.of(name, v));
        }
        return builder.build();
      } else {
        return ImmutableList.of();
      }
    } else {
      final ImmutableList.Builder<Map.Entry<String, V>> builder =
          ImmutableList.builder();
      NavigableMap<String, List<V>> m =
          map.subMap(name.toUpperCase(Locale.ROOT), true,
              name.toLowerCase(Locale.ROOT), true);
      for (Map.Entry<String, List<V>> entry : m.entrySet()) {
        for (V v : entry.getValue()) {
          builder.add(Pair.of(entry.getKey(), v));
        }
      }
      return builder.build();
    }
  }

  /** Returns whether this map contains a given key, with a given
   * case-sensitivity. */
  public boolean containsKey(String name, boolean caseSensitive) {
    return !range(name, caseSensitive).isEmpty();
  }

  /** Returns the underlying map.
   * Its size is the number of keys, not the number of values. */
  public NavigableMap<String, List<V>> map() {
    return map;
  }
}

// End NameMultimap.java
