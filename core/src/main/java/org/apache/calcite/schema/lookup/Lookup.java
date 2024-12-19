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
package org.apache.calcite.schema.lookup;

import org.apache.calcite.util.NameMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;
import java.util.function.BiFunction;

/**
 * A casesensitive/insensitive lookup for tables, schems, functions ...
 *
 * @param <T> Element type
 */
public interface Lookup<T> {
  /**
   * Returns a named entity with a given name, or null if not found.
   *
   * @param name Name
   * @return Entity, or null
   */
  @Nullable T get(String name);

  /**
   * Returns a named entity with a given name ignoring the case, or null if not found.
   *
   * @param name Name
   * @return Entity, or null
   */
  @Nullable Named<T> getIgnoreCase(String name);

  /**
   * Returns the names of the entities in matching pattern.
   *
   * @return Names of the entities
   */
  Set<String> getNames(LikePattern pattern);

  default <S> Lookup<S> map(BiFunction<T, String, S> mapper) {
    return new MappedLookup<>(this, mapper);
  }

  /**
   * Helper method to call Lookup.get(String) or Lookup.getIgnoreCase(String)
   * depending on the parameter caseSensitive.
   *
   * @return Entity, or null
   */
  static <T> @Nullable T get(Lookup<T> lookup, String name, boolean caseSensitive) {
    if (caseSensitive) {
      T entry = lookup.get(name);
      if (entry == null) {
        return null;
      }
      return entry;
    }
    return Named.entityOrNull(lookup.getIgnoreCase(name));
  }

  /**
   * Returns an empty lookup.
   */
  static <T> Lookup<T> empty() {
    return (Lookup<T>) EmptyLookup.INSTANCE;
  }

  /**
   * Creates a new lookup object based on a NameMap.
   */
  static <T> Lookup<T> of(NameMap<T> map) {
    return new NameMapLookup<>(map);
  }

  /**
   * Concat a list of lookups.
   */
  static <T> Lookup<T> concat(Lookup<T>... lookups) {
    return new ConcatLookup<>(lookups);
  }
}
