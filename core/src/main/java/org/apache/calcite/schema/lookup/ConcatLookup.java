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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class can be used to concat a list of lookups.
 *
 * @param <T> Element type
 */
class ConcatLookup<T> implements Lookup<T> {
  private final Lookup<T>[] lookups;

  ConcatLookup(Lookup<T>[] lookups) {
    this.lookups = lookups;
  }

  @Override public @Nullable T get(String name) {
    for (Lookup<T> lookup : lookups) {
      T t = lookup.get(name);
      if (t != null) {
        return t;
      }
    }
    return null;
  }

  @Override public @Nullable Named<T> getIgnoreCase(String name) {
    for (Lookup<T> lookup : lookups) {
      Named<T> t = lookup.getIgnoreCase(name);
      if (t != null) {
        return t;
      }
    }
    return null;
  }

  @Override public Set<String> getNames(LikePattern pattern) {
    return Stream.of(lookups)
        .flatMap(lookup -> lookup.getNames(pattern).stream())
        .collect(Collectors.toSet());
  }
}