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

import org.apache.calcite.util.LazyReference;
import org.apache.calcite.util.NameMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.Set;

/**
 * An abstract base class for lookups implementing case insensitive lookup.
 *
 * @param <T> Element type
 */
public abstract class IgnoreCaseLookup<T> implements Lookup<T> {

  /**
   * This member is used to lazily load the list of all names into memory.
   *
   * <p>A {@link NameMap} is used, which is capable to lookup names in a
   * case insensitive way.
   */
  private LazyReference<NameMap<String>> nameMap = new LazyReference<>();

  /**
   * Returns a named entity with a given name, or null if not found.
   *
   * @return Entity with the specified name, or null when the entity is not found.
   */
  @Override public abstract @Nullable T get(String name);

  /**
   * Returns a named entity with a given name ignoring the case, or null if not found.
   *
   * @return Entity with the specified name, or null when the entity is not found.
   */
  @Override @Nullable public Named<T> getIgnoreCase(String name) {
    int retryCounter = 0;
    while (true) {
      Map.Entry<String, String> entry = nameMap.getOrCompute(this::loadNames)
          .range(name, false)
          .firstEntry();
      if (entry != null) {
        T result = get(entry.getValue());
        return result == null ? null : new Named<>(entry.getKey(), result);
      }
      // if the name was not found in the cached list of names,
      // we try to reload the cache once because the table/schema
      // might have been created in the meantime.
      retryCounter++;
      if (retryCounter > 1) {
        return null;
      }
      nameMap.reset();
    }
  }

  @Override public abstract Set<String> getNames(LikePattern pattern);

  private NameMap<String> loadNames() {
    NameMap<String> result = new NameMap<>();
    for (String name : getNames(LikePattern.any())) {
      result.put(name, name);
    }
    return result;
  }
}
