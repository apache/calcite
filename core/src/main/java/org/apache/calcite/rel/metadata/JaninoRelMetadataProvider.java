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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.janino.JaninoMetadataHandlerCreator;
import org.apache.calcite.util.Util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

/**
 * Implementation of the {@link RelMetadataProvider} interface that generates
 * a class that dispatches to the underlying providers.
 */
public class JaninoRelMetadataProvider implements RelMetadataProvider {
  private final RelMetadataProvider provider;

  // Constants and static fields

  public static final JaninoRelMetadataProvider DEFAULT =
      JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE);


  /** Cache of pre-generated handlers by provider and kind of metadata.
   * For the cache to be effective, providers should implement identity
   * correctly. */
  @SuppressWarnings("unchecked")
  private static final LoadingCache<Key, MetadataHandler> HANDLERS =
      maxSize(CacheBuilder.newBuilder(),
          CalciteSystemProperty.METADATA_HANDLER_CACHE_MAXIMUM_SIZE.value())
          .build(cacheLoader());

  // Pre-register the most common relational operators, to reduce the number of
  // times we re-generate.

  /** Private constructor; use {@link #of}. */
  private JaninoRelMetadataProvider(RelMetadataProvider provider) {
    this.provider = provider;
  }

  /** Creates a JaninoRelMetadataProvider.
   *
   * @param provider Underlying provider
   */
  public static JaninoRelMetadataProvider of(RelMetadataProvider provider) {
    if (provider instanceof JaninoRelMetadataProvider) {
      return (JaninoRelMetadataProvider) provider;
    }
    return new JaninoRelMetadataProvider(provider);
  }

  // helper for initialization
  private static <K, V> CacheBuilder<K, V> maxSize(CacheBuilder<K, V> builder,
      int size) {
    if (size >= 0) {
      builder.maximumSize(size);
    }
    return builder;
  }

  @Override public boolean equals(@Nullable Object obj) {
    return obj == this
        || obj instanceof JaninoRelMetadataProvider
        && ((JaninoRelMetadataProvider) obj).provider.equals(provider);
  }

  @Override public int hashCode() {
    return 109 + provider.hashCode();
  }

  @Deprecated // to be removed before 2.0
  @Override public <@Nullable M extends @Nullable Metadata> UnboundMetadata<M> apply(
      Class<? extends RelNode> relClass, Class<? extends M> metadataClass) {
    throw new UnsupportedOperationException();
  }

  @Override public <M extends Metadata> Multimap<Method, MetadataHandler<M>>
      handlers(MetadataDef<M> def) {
    return provider.handlers(def);
  }


  @Override public <M extends Metadata> ImmutableSet<? extends MetadataHandler<M>> handlers(
      Class<? extends MetadataHandler<? extends M>> handlerClass) {
    return provider.handlers(handlerClass);
  }

  static synchronized <M extends Metadata, H extends MetadataHandler<M>> H create(
      RelMetadataProvider provider, Class<H> handlerClass) {
    try {
      final Key key = new Key(handlerClass, provider);
      //noinspection unchecked
      return (H) HANDLERS.get(key);
    } catch (UncheckedExecutionException | ExecutionException e) {
      throw Util.throwAsRuntime(Util.causeOrSelf(e));
    }
  }

  static synchronized <M extends Metadata, H extends MetadataHandler<M>> H revise(
      RelMetadataProvider provider, Class<H> handlerClass) {
    //noinspection unchecked
    return create(provider, handlerClass);
  }

  /** Registers some classes. Does not flush the providers, but next time we
   * need to generate a provider, it will handle all of these classes. So,
   * calling this method reduces the number of times we need to re-generate. */
  @Deprecated
  public void register(Iterable<Class<? extends RelNode>> classes) {
  }

  private static <M extends Metadata, MH extends MetadataHandler<M>>
      CacheLoader<Key, MetadataHandler> cacheLoader() {
    CacheLoader cacheLoader = CacheLoader.<Key<M, MH>, MH>from(key -> {
      ImmutableSet<? extends MetadataHandler<Metadata>> handlers =
          key.provider.handlers(key.handlerClass);
      return JaninoMetadataHandlerCreator.newInstance(key.handlerClass, handlers);
    });
    return cacheLoader;
  }

  /** Exception that indicates there there should be a handler for
   * this class but there is not. The action is probably to
   * re-generate the handler class.
   *
   * Please use MetadataHandlerProvider.NoHandler.*/
  @Deprecated
  public static class NoHandler extends MetadataHandlerProvider.NoHandler {

    public NoHandler(Class<? extends RelNode> relClass) {
      super(relClass);
    }
  }

  /**
   * Key for the cache.
   * @param <M> Metadata type
   * @param <MH> Handler type
   */
  private static class Key<M extends Metadata, MH extends MetadataHandler<M>> {
    public final Class<MH> handlerClass;
    public final RelMetadataProvider provider;

    private Key(Class<MH> handlerClass,
        RelMetadataProvider provider) {
      this.handlerClass = handlerClass;
      this.provider = provider;
    }

    @Override public int hashCode() {
      return (handlerClass.hashCode() * 37
          + provider.hashCode()) * 37;
    }

    @Override public boolean equals(@Nullable Object obj) {
      return this == obj
          || obj instanceof Key
          && ((Key) obj).handlerClass.equals(handlerClass)
          && ((Key) obj).provider.equals(provider);
    }
  }
}
