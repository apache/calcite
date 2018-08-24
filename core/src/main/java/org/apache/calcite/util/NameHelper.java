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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.calcite.util.NameSet.COMPARATOR;

/** Helps construct case-insensitive ranges of {@link NameSet},
 * {@link NameMap}, {@link NameMultimap}.
 *
 * <p>Not thread-safe. */
class NameHelper {
  /** Characters whose floor/ceiling are not the same as their upper/lower
   * case. Out of 64k unicode characters, there are 289 weird characters. */
  private static final ImmutableMap<Character, Pair<Character, Character>>
      WEIRD_CHARACTERS = weirdCharacters();

  /** Workspace for computing the floor key. */
  private final StringBuilder floorBuilder = new StringBuilder();

  /** Workspace for computing the ceiling key. */
  private final StringBuilder ceilingBuilder = new StringBuilder();

  /** Given a string, computes the smallest and largest strings that are
   * case-insensitive equal to that string,
   * calls the given function,
   * and returns its result.
   *
   * <p>For latin strings such as "bAz" computing the smallest and largest
   * strings is straightforward:
   * the floor is the upper-case string ("BAZ"), and
   * the ceiling is the lower-case string ("baz").
   *
   * <p>It's more complicated for non-Latin strings that have characters
   * whose lower-case value is less than their upper-case value.
   *
   * <p>This method is not thread-safe.
   */
  private <R> R applyFloorCeiling(String name,
      BiFunction<String, String, R> f) {
    name.chars()
        .forEachOrdered(i -> {
          final char c = (char) i;
          final Pair<Character, Character> pair = WEIRD_CHARACTERS.get(c);
          if (pair == null) {
            floorBuilder.append(Character.toUpperCase(c));
            ceilingBuilder.append(Character.toLowerCase(c));
          } else {
            floorBuilder.append(pair.left);
            ceilingBuilder.append(pair.right);
          }
        });
    final String floor = bufValue(floorBuilder, name);
    final String ceiling = bufValue(ceilingBuilder, name);
    assert floor.compareTo(ceiling) <= 0;
    return f.apply(floor, ceiling);
  }

  /** Returns the value of a {@link StringBuilder} as a string,
   * and clears the builder.
   *
   * <p>If the value is the same the given string, returns that string,
   * thereby saving the effort of building a new string. */
  private static String bufValue(StringBuilder b, String s) {
    if (b.length() != s.length() || b.indexOf(s) != 0) {
      s = b.toString();
    }
    b.setLength(0);
    return s;
  }

  /** Used by {@link NameSet#range(String, boolean)}. */
  Collection<String> set(NavigableSet<String> names, String name) {
    return applyFloorCeiling(name,
        (floor, ceiling) -> {
          final NavigableSet<String> subSet =
              names.subSet(floor, true, ceiling, true);
          return subSet
              .stream()
              .filter(s -> s.equalsIgnoreCase(name))
              .collect(Collectors.toList());
        });
  }

  /** Used by {@link NameMap#range(String, boolean)}. */
  <V> ImmutableSortedMap<String, V> map(NavigableMap<String, V> map,
      String name) {
    return applyFloorCeiling(name,
        (floor, ceiling) -> {
          final ImmutableSortedMap.Builder<String, V> builder =
              new ImmutableSortedMap.Builder<>(COMPARATOR);
          final NavigableMap<String, V> subMap =
              map.subMap(floor, true, ceiling, true);
          for (Map.Entry<String, V> e : subMap.entrySet()) {
            if (e.getKey().equalsIgnoreCase(name)) {
              builder.put(e.getKey(), e.getValue());
            }
          }
          return builder.build();
        });
  }

  /** Used by {@link NameMultimap#range(String, boolean)}. */
  <V> Collection<Map.Entry<String, V>> multimap(
      NavigableMap<String, List<V>> map, String name) {
    return applyFloorCeiling(name,
        (floor, ceiling) -> {
          final NavigableMap<String, List<V>> subMap =
              map.subMap(floor, true, ceiling, true);
          final ImmutableList.Builder<Map.Entry<String, V>> builder =
              ImmutableList.builder();
          for (Map.Entry<String, List<V>> e : subMap.entrySet()) {
            if (e.getKey().equalsIgnoreCase(name)) {
              for (V v : e.getValue()) {
                builder.add(Pair.of(e.getKey(), v));
              }
            }
          }
          return builder.build();
        });
  }

  /** Returns whether an equivalence class of characters is simple.
   *
   * <p>It is simple if
   * the floor of the class is the upper-case value of every character, and
   * the ceiling of the class is the lower-case value of every character. */
  private static boolean isSimple(Collection<Character> characters,
      Character floor, Character ceiling) {
    for (Character character : characters) {
      if (!floor.equals(Character.toUpperCase(character))) {
        return false;
      }
      if (!ceiling.equals(Character.toLowerCase(character))) {
        return false;
      }
    }
    return true;
  }

  private static ImmutableMap<Character, Pair<Character, Character>>
      weirdCharacters() {
    final EquivalenceSet<Character> strange = new EquivalenceSet<>();
    for (int i = 0; i < 0xffff; i++) {
      char c = (char) i;
      strange.add(c);
      strange.equiv(c, Character.toLowerCase(c));
      strange.equiv(c, Character.toUpperCase(c));
    }
    final SortedMap<Character, SortedSet<Character>> map = strange.map();
    final ImmutableMap.Builder<Character, Pair<Character, Character>> builder =
        ImmutableMap.builder();
    for (Map.Entry<Character, SortedSet<Character>> entry : map.entrySet()) {
      final Collection<Character> characters = entry.getValue();
      final Character floor = Ordering.natural().min(characters);
      final Character ceiling = Ordering.natural().max(characters);
      if (isSimple(characters, floor, ceiling)) {
        continue;
      }
      final Pair<Character, Character> pair = Pair.of(floor, ceiling);
      for (Character character : characters) {
        builder.put(character, pair);
      }
    }
    return builder.build();
  }
}

// End NameHelper.java
