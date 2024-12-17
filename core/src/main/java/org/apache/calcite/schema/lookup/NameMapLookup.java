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

import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.util.NameMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Lookup class which is based on a NameMap.
 *
 * @param <T> Element type
 */
class NameMapLookup<T> implements Lookup<T> {
  private final NameMap<T> map;

  NameMapLookup(NameMap<T> map) {
    this.map = map;
  }

  @Override public @Nullable T get(String name) {
    Map.Entry<String, T> entry = map.range(name, true).firstEntry();
    if (entry != null) {
      return entry.getValue();
    }
    return null;
  }

  @Override public @Nullable Named<T> getIgnoreCase(String name) {
    Map.Entry<String, T> entry = map.range(name, false).firstEntry();
    if (entry != null) {
      return new Named<>(entry.getKey(), entry.getValue());
    }
    return null;
  }

  @Override public Set<String> getNames(LikePattern pattern) {
    final Predicate1<String> matcher = pattern.matcher();
    return map.map().keySet().stream()
        .filter(name -> matcher.apply(name))
        .collect(Collectors.toSet());
  }
}
