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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Simple utility class to test other implementations of Lookup.
 */
class FakeLookup implements Lookup<String> {
  private final Map<String, String> map;
  private final Map<String, Named<String>> ignoreCaseMap;

  FakeLookup(String... keyAndValues) {
    this.map = new HashMap<>();
    for (int i = 0; i < keyAndValues.length - 1; i += 2) {
      map.put(keyAndValues[i], keyAndValues[i + 1]);
    }
    this.ignoreCaseMap = this.map.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().toLowerCase(Locale.ROOT),
                entry -> new Named<>(entry.getKey(), entry.getValue())));
  }

  @Override public @Nullable String get(final String name) {
    return map.get(name);
  }

  @Override public @Nullable Named<String> getIgnoreCase(final String name) {
    return ignoreCaseMap.get(name.toLowerCase(Locale.ROOT));
  }

  @Override public Set<String> getNames(final LikePattern pattern) {
    Predicate1<String> predicate = pattern.matcher();
    return map.keySet().stream().filter(predicate::apply).collect(Collectors.toSet());
  }
}
