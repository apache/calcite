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
package org.eigenbase.rel.metadata;

import java.util.concurrent.ExecutionException;

import org.eigenbase.rel.RelNode;
import org.eigenbase.util.Pair;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/** Implementation of {@link MetadataFactory} that gets providers from a
 * {@link RelMetadataProvider} and stores them in a cache.
 *
 * <p>The cache does not store metadata. It remembers which providers can
 * provide which kinds of metadata, for which kinds of relational
 * expressions.</p>
 */
public class MetadataFactoryImpl implements MetadataFactory {
  @SuppressWarnings("unchecked")
  public static final Function<RelNode, Metadata> DUMMY =
      (Function) Functions.<Metadata>constant(null);

  private final LoadingCache<
      Pair<Class<RelNode>, Class<Metadata>>,
      Function<RelNode, Metadata>> cache;

  public MetadataFactoryImpl(RelMetadataProvider provider) {
    this.cache = CacheBuilder.newBuilder().build(loader(provider));
  }

  static CacheLoader<Pair<Class<RelNode>, Class<Metadata>>,
      Function<RelNode, Metadata>> loader(final RelMetadataProvider provider) {
    return new CacheLoader<Pair<Class<RelNode>, Class<Metadata>>,
        Function<RelNode, Metadata>>() {
      @Override
      public Function<RelNode, Metadata> load(
          Pair<Class<RelNode>, Class<Metadata>> key) throws Exception {
        final Function<RelNode, Metadata> function =
            provider.apply(key.left, key.right);
        // Return DUMMY, not null, so the cache knows to not ask again.
        return function != null ? function : DUMMY;
      }
    };
  }

  public <T extends Metadata> T query(RelNode rel, Class<T> clazz) {
    try {
      //noinspection unchecked
      final Pair<Class<RelNode>, Class<Metadata>> key =
          (Pair) Pair.of(rel.getClass(), clazz);
      final Metadata apply = cache.get(key).apply(rel);
      //noinspection unchecked
      return (T) apply;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      } else {
        throw (Error) e.getCause();
      }
    }
  }
}

// End MetadataFactoryImpl.java
