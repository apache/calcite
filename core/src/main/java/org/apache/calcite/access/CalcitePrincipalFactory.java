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
package org.apache.calcite.access;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Factory for {@link CalcitePrincipal}.
 */
public class CalcitePrincipalFactory {

  public static final CalcitePrincipalFactory INSTANCE = new CalcitePrincipalFactory();

  private final LoadingCache<String, CalcitePrincipal> cache =
      CacheBuilder.newBuilder().build(CacheLoader.from(CalcitePrincipalFactory::createInternal));

  private CalcitePrincipalFactory() {
  }

  private static CalcitePrincipal createInternal(String name) {
    return new CalcitePrincipal(name);
  }

  public static @Nullable CalcitePrincipal create(@Nullable String name) {
    if (name == null) {
      return null;
    }
    return INSTANCE.cache.getUnchecked(name);
  }
}
