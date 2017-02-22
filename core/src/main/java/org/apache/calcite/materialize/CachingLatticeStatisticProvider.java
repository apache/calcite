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
package org.apache.calcite.materialize;

import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.concurrent.ExecutionException;

/**
 * Implementation of {@link LatticeStatisticProvider} that gets statistics by
 * executing "SELECT COUNT(DISTINCT ...) ..." SQL queries.
 */
class CachingLatticeStatisticProvider implements LatticeStatisticProvider {
  private final LoadingCache<Pair<Lattice, Lattice.Column>, Integer> cache;

  /** Creates a CachingStatisticProvider. */
  public CachingLatticeStatisticProvider(
      final LatticeStatisticProvider provider) {
    cache = CacheBuilder.<Pair<Lattice, Lattice.Column>>newBuilder()
        .build(
            new CacheLoader<Pair<Lattice, Lattice.Column>, Integer>() {
              public Integer load(Pair<Lattice, Lattice.Column> key)
                  throws Exception {
                return provider.cardinality(key.left, key.right);
              }
            });
  }

  public int cardinality(Lattice lattice, Lattice.Column column) {
    try {
      return cache.get(Pair.of(lattice, column));
    } catch (UncheckedExecutionException | ExecutionException e) {
      Util.throwIfUnchecked(e.getCause());
      throw new RuntimeException(e.getCause());
    }
  }
}

// End CachingLatticeStatisticProvider.java
