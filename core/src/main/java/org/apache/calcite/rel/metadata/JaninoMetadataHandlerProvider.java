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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Proxy;

import static java.util.Objects.requireNonNull;

/**
 * Provides metadata handlers generated via Janino.
 */
public class JaninoMetadataHandlerProvider implements MetadataHandlerProvider {

  public static final JaninoMetadataHandlerProvider INSTANCE = new JaninoMetadataHandlerProvider();
  private static final ThreadLocal<@Nullable RelMetadataProvider> METADATA_PROVIDER_THREAD_LOCAL =
      new ThreadLocal<>();

  protected JaninoMetadataHandlerProvider() {
  }

  @Override public <H extends MetadataHandler<M>, M extends Metadata>
  H initialHandler(Class<H> handlerClass) {
    return handlerClass.cast(
        Proxy.newProxyInstance(RelMetadataQuery.class.getClassLoader(),
            new Class[] {handlerClass}, (proxy, method, args) -> {
              final RelNode r = requireNonNull((RelNode) args[0], "(RelNode) args[0]");
              METADATA_PROVIDER_THREAD_LOCAL.set(r.getCluster().getMetadataProvider());
              throw new NoHandler(r.getClass());
            }));
  }

  @Override public <H extends MetadataHandler<M>, M extends Metadata> H revise(
      Class<H> handlerClass) {
    return JaninoRelMetadataProvider.revise(
        requireNonNull(METADATA_PROVIDER_THREAD_LOCAL.get(), "relMetadataProvider"),
        handlerClass);
  }

  @Override public MetadataCache buildCache() {
    return new TableMetadataCache();
  }
}
