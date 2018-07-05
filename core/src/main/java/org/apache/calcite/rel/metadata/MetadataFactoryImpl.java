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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.concurrent.ExecutionException;

/** Implementation of {@link MetadataFactory} that gets providers from a
 * {@link RelMetadataProvider} and stores them in a cache.
 *
 * <p>The cache does not store metadata. It remembers which providers can
 * provide which kinds of metadata, for which kinds of relational
 * expressions.</p>
 */
public class MetadataFactoryImpl implements MetadataFactory {
  @SuppressWarnings("unchecked")
  public static final UnboundMetadata<Metadata> DUMMY = (rel, mq) -> null;

  private final LoadingCache<
      Pair<Class<RelNode>, Class<Metadata>>, UnboundMetadata<Metadata>> cache;

  public MetadataFactoryImpl(RelMetadataProvider provider) {
    this.cache = CacheBuilder.newBuilder().build(loader(provider));
  }

  private static CacheLoader<Pair<Class<RelNode>, Class<Metadata>>,
      UnboundMetadata<Metadata>> loader(final RelMetadataProvider provider) {
    return CacheLoader.from(key -> {
      final UnboundMetadata<Metadata> function =
          provider.apply(key.left, key.right);
      // Return DUMMY, not null, so the cache knows to not ask again.
      return function != null ? function : DUMMY;
    });
  }

  public <M extends Metadata> M query(RelNode rel, RelMetadataQuery mq,
      Class<M> metadataClazz) {
    try {
      //noinspection unchecked
      final Pair<Class<RelNode>, Class<Metadata>> key =
          (Pair) Pair.of(rel.getClass(), metadataClazz);
      final Metadata apply = cache.get(key).bind(rel, mq);
      return metadataClazz.cast(apply);
    } catch (UncheckedExecutionException | ExecutionException e) {
      Util.throwIfUnchecked(e.getCause());
      throw new RuntimeException(e.getCause());
    }
  }
}

// End MetadataFactoryImpl.java
