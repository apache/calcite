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

import java.lang.reflect.Proxy;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Provides metadata handlers generated via Janino.
 */
public class JaninoMetadataHandlerProvider implements MetadataHandlerProvider {
  private final RelMetadataProvider relMetadataProvider;
  private final Supplier<MetadataCache> metadataCacheSupplier;

  private JaninoMetadataHandlerProvider(
      RelMetadataProvider relMetadataProvider,
      Supplier<MetadataCache> metadataCacheSupplier
  ) {
    this.relMetadataProvider = relMetadataProvider;
    this.metadataCacheSupplier = metadataCacheSupplier;
  }

  @Override public <H> H initialHandler(Class<H> handlerClass) {
    return handlerClass.cast(
        Proxy.newProxyInstance(RelMetadataQuery.class.getClassLoader(),
            new Class[] {handlerClass}, (proxy, method, args) -> {
              final RelNode r = requireNonNull((RelNode) args[0], "(RelNode) args[0]");
              throw new NoHandler(r.getClass());
            }));
  }

  @Override public <H extends MetadataHandler<M>, M extends Metadata> H revise(
      Class<H> handlerClass) {
    return JaninoRelMetadataProvider.revise(
        relMetadataProvider,
        handlerClass);
  }

  @Override public MetadataCache buildCache() {
    return metadataCacheSupplier.get();
  }

  public static Builder builder() {
    return Builder.INSTANCE;
  }

  /**
   * Builds instances of {@link JaninoMetadataHandlerProvider}.
   */
  public static class Builder {
    private static final Builder INSTANCE = new Builder(
        DefaultRelMetadataProvider.INSTANCE, TableMetadataCache::new);
    private RelMetadataProvider relMetadataProvider;
    private Supplier<MetadataCache> metadataCacheSupplier;

    public Builder(RelMetadataProvider relMetadataProvider,
        Supplier<MetadataCache> metadataCacheSupplier) {
      this.relMetadataProvider = relMetadataProvider;
      this.metadataCacheSupplier = metadataCacheSupplier;
    }

    public Builder metadataCacheSupplier(Supplier<MetadataCache> metadataCacheSupplier) {
      return new Builder(relMetadataProvider, metadataCacheSupplier);
    }

    public Builder relMetadataProvider(RelMetadataProvider relMetadataProvider) {
      return new Builder(relMetadataProvider, metadataCacheSupplier);
    }

    public JaninoMetadataHandlerProvider build() {
      return new JaninoMetadataHandlerProvider(relMetadataProvider, metadataCacheSupplier);
    }
  }
}
