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

import java.util.Set;

/**
 * This class can be used to make a snapshot of a lookups.
 *
 * @param <T> Element Type
 */
public class SnapshotLookup<T> implements Lookup<T> {

  private final Lookup<T> delegate;
  private LazyReference<Lookup<T>> cachedDelegate = new LazyReference<>();
  private boolean enabled = true;

  public SnapshotLookup(Lookup<T> delegate) {
    this.delegate = delegate;
  }

  @Override public @Nullable T get(final String name) {
    return delegate().get(name);
  }

  @Override public @Nullable Named<T> getIgnoreCase(final String name) {
    return delegate().getIgnoreCase(name);
  }

  @Override public Set<String> getNames(final LikePattern pattern) {
    return delegate().getNames(pattern);
  }

  private Lookup<T> delegate() {
    if (!enabled) {
      return delegate;
    }
    return cachedDelegate.getOrCompute(() -> new NameMapLookup<>(loadNameMap()));
  }

  private NameMap<T> loadNameMap() {
    NameMap<T> map = new NameMap<>();
    for (String name : delegate.getNames(LikePattern.any())) {
      T entry = delegate.get(name);
      if (entry != null) {
        map.put(name, entry);
      }
    }
    return map;
  }

  public void enable(boolean enabled) {
    if (!enabled) {
      cachedDelegate.reset();
    }
    this.enabled = enabled;
  }

}
