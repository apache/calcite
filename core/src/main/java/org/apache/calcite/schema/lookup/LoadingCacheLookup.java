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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * This class can be used to cache lookups.
 *
 * @param <T> Element Type
 */
public class LoadingCacheLookup<T> implements Lookup<T> {
  private final Lookup<T> delegate;

  private final LoadingCache<String, T> cache;
  private final LoadingCache<String, Named<T>> cacheIgnoreCase;

  public LoadingCacheLookup(Lookup<T> delegate) {
    this.delegate = delegate;
    this.cache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build(CacheLoader.from(name -> requireNonNull(delegate.get(name))));
    this.cacheIgnoreCase = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build(CacheLoader.from(name -> requireNonNull(delegate.getIgnoreCase(name))));
  }

  @Override public @Nullable T get(String name) {
    try {
      return cache.get(name);
    } catch (UncheckedExecutionException e) {
      if (e.getCause() instanceof NullPointerException) {
        return null;
      }
      throw e;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public @Nullable Named<T> getIgnoreCase(String name) {
    try {
      return cacheIgnoreCase.get(name);
    } catch (UncheckedExecutionException e) {
      if (e.getCause() instanceof NullPointerException) {
        return null;
      }
      throw e;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public Set<String> getNames(LikePattern pattern) {
    return delegate.getNames(pattern);
  }
}
