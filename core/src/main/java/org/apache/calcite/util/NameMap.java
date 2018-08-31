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

import com.google.common.collect.ImmutableSortedMap;

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.calcite.util.CaseInsensitiveComparator.COMPARATOR;

/** Map whose keys are names and can be accessed with and without case
 * sensitivity.
 *
 * @param <V> Value type */
public class NameMap<V> {
  private final NavigableMap<String, V> map;

  /** Creates a NameSet based on an existing set. */
  private NameMap(NavigableMap<String, V> map) {
    this.map = map;
    assert this.map.comparator() == COMPARATOR;
  }

  /** Creates a NameMap, initially empty. */
  public NameMap() {
    this(new TreeMap<String, V>(COMPARATOR));
  }

  @Override public String toString() {
    return map.toString();
  }

  @Override public int hashCode() {
    return map.hashCode();
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof NameMap
        && map.equals(((NameMap) obj).map);
  }

  /** Creates a NameMap that is an immutable copy of a given map. */
  public static <V> NameMap immutableCopyOf(Map<String, V> names) {
    return new NameMap<>(ImmutableSortedMap.copyOf(names, COMPARATOR));
  }

  public void put(String name, V v) {
    map.put(name, v);
  }

  /** Returns a map containing all the entries in the map that match the given
   * name. If case-sensitive, that map will have 0 or 1 elements; if
   * case-insensitive, it may have 0 or more. */
  public NavigableMap<String, V> range(String name, boolean caseSensitive) {
    Object floorKey;
    Object ceilingKey;
    if (caseSensitive) {
      floorKey = name;
      ceilingKey = name;
    } else {
      floorKey = COMPARATOR.floorKey(name);
      ceilingKey = COMPARATOR.ceilingKey(name);
    }
    NavigableMap subMap = ((NavigableMap) map).subMap(floorKey, true, ceilingKey, true);
    return Collections.unmodifiableNavigableMap((NavigableMap<String, V>) subMap);
  }

  /** Returns whether this map contains a given key, with a given
   * case-sensitivity. */
  public boolean containsKey(String name, boolean caseSensitive) {
    return !range(name, caseSensitive).isEmpty();
  }

  /** Returns the underlying map. */
  public NavigableMap<String, V> map() {
    return map;
  }

  @Experimental
  public V remove(String key) {
    return map.remove(key);
  }
}

// End NameMap.java
