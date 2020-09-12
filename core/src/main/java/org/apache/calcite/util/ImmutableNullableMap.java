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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * An immutable map that may contain null values.
 *
 * <p>If the map cannot contain null values, use {@link ImmutableMap}.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public abstract class ImmutableNullableMap<K, V> extends AbstractMap<K, V> {

  private static final Map<Integer, Integer> SINGLETON_MAP =
      Collections.singletonMap(0, 0);

  private ImmutableNullableMap() {
  }

  /**
   * Returns an immutable map containing the given elements.
   *
   * <p>Behavior is as {@link ImmutableMap#copyOf(Iterable)}
   * except that this map allows nulls.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <K, V> Map<K, V> copyOf(Map<? extends K, ? extends V> map) {
    if (map instanceof ImmutableNullableMap
        || map instanceof ImmutableMap
        || map == Collections.emptyMap()
        || map == Collections.emptyNavigableMap()
        || map.getClass() == SINGLETON_MAP.getClass()) {
      return (Map<K, V>) map;
    }
    if (map instanceof SortedMap) {
      final SortedMap<K, V> sortedMap = (SortedMap) map;
      try {
        return ImmutableSortedMap.copyOf(sortedMap, sortedMap.comparator());
      } catch (NullPointerException e) {
        // Make an effectively immutable map by creating a mutable copy
        // and wrapping it to prevent modification. Unfortunately, if we see
        // it again we will not recognize that it is immutable and we will make
        // another copy.
        return Collections.unmodifiableNavigableMap(new TreeMap<>(sortedMap));
      }
    } else {
      try {
        return ImmutableMap.copyOf(map);
      } catch (NullPointerException e) {
        // Make an effectively immutable map by creating a mutable copy
        // and wrapping it to prevent modification. Unfortunately, if we see
        // it again we will not recognize that it is immutable and we will make
        // another copy.
        return Collections.unmodifiableMap(new HashMap<>(map));
      }
    }
  }

  /**
   * Returns an immutable navigable map containing the given entries.
   *
   * <p>Behavior is as {@link ImmutableSortedMap#copyOf(Map)}
   * except that this map allows nulls.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <K, V> Map<K, V> copyOf(
      SortedMap<? extends K, ? extends V> map) {
    if (map instanceof ImmutableNullableMap
        || map instanceof ImmutableMap
        || map == Collections.emptyMap()
        || map == Collections.emptyNavigableMap()) {
      return (Map<K, V>) map;
    }
    final SortedMap<K, V> sortedMap = (SortedMap) map;
    try {
      return ImmutableSortedMap.copyOf(sortedMap, sortedMap.comparator());
    } catch (NullPointerException e) {
      // Make an effectively immutable map by creating a mutable copy
      // and wrapping it to prevent modification. Unfortunately, if we see
      // it again we will not recognize that it is immutable and we will make
      // another copy.
      return Collections.unmodifiableNavigableMap(new TreeMap<>(sortedMap));
    }
  }
}
