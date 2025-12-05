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
import java.util.function.BiFunction;

/**
 * This class implements a {@code Lookup} on base of another {@code Lookup}
 * transforming all entities using a given function.
 *
 * @param <S> Source element type
 * @param <T> Target element type
 */
class TransformingLookup<S, T> implements Lookup<T> {
  private final Lookup<S> lookup;
  private final BiFunction<S, String, T> transform;

  TransformingLookup(Lookup<S> lookup, BiFunction<S, String, T> transform) {
    this.lookup = lookup;
    this.transform = transform;
  }

  @Override public @Nullable T get(String name) {
    S entity = lookup.get(name);
    return entity == null ? null : transform.apply(entity, name);
  }

  @Override public @Nullable Named<T> getIgnoreCase(String name) {
    Named<S> named = lookup.getIgnoreCase(name);
    return named == null
        ? null
        : new Named<>(named.name(), transform.apply(named.entity(), named.name()));
  }

  @Override public Set<String> getNames(LikePattern pattern) {
    return lookup.getNames(pattern);
  }
}
