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

import java.lang.reflect.*;
import java.util.*;

import org.eigenbase.rel.*;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Implementation of the {@link RelMetadataProvider}
 * interface via the
 * {@link org.eigenbase.util.Glossary#CHAIN_OF_RESPONSIBILITY_PATTERN}.
 *
 * <p>When a consumer calls the {@link #apply} method to ask for a provider
 * for a particular type of {@link RelNode} and {@link Metadata}, scans the list
 * of underlying providers.</p>
 */
public class ChainedRelMetadataProvider implements RelMetadataProvider {
  //~ Instance fields --------------------------------------------------------

  private final ImmutableList<RelMetadataProvider> providers;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a chain.
   */
  protected ChainedRelMetadataProvider(
      ImmutableList<RelMetadataProvider> providers) {
    this.providers = providers;
  }

  //~ Methods ----------------------------------------------------------------

  public Function<RelNode, Metadata> apply(Class<? extends RelNode> relClass,
      final Class<? extends Metadata> metadataClass) {
    final List<Function<RelNode, Metadata>> functions = Lists.newArrayList();
    for (RelMetadataProvider provider : providers) {
      final Function<RelNode, Metadata> function =
          provider.apply(relClass, metadataClass);
      if (function == null) {
        continue;
      }
      functions.add(function);
    }
    switch (functions.size()) {
    case 0:
      return null;
    case 1:
      return functions.get(0);
    default:
      return new Function<RelNode, Metadata>() {
        public Metadata apply(RelNode input) {
          final List<Metadata> metadataList = Lists.newArrayList();
          for (Function<RelNode, Metadata> function : functions) {
            final Metadata metadata = function.apply(input);
            if (metadata != null) {
              metadataList.add(metadata);
            }
          }
          return (Metadata) Proxy.newProxyInstance(
              metadataClass.getClassLoader(),
              new Class[]{metadataClass},
              new ChainedInvocationHandler(metadataList));
        }
      };

    }
  }

  /** Creates a chain. */
  public static RelMetadataProvider of(List<RelMetadataProvider> list) {
    return new ChainedRelMetadataProvider(ImmutableList.copyOf(list));
  }

  /** Invocation handler that calls a list of {@link Metadata} objects,
   * returning the first non-null value. */
  private static class ChainedInvocationHandler implements InvocationHandler {
    private final List<Metadata> metadataList;

    public ChainedInvocationHandler(List<Metadata> metadataList) {
      this.metadataList = ImmutableList.copyOf(metadataList);
    }

    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      for (Metadata metadata : metadataList) {
        final Object o = method.invoke(metadata, args);
        if (o != null) {
          return o;
        }
      }
      return null;
    }
  }
}

// End ChainedRelMetadataProvider.java
