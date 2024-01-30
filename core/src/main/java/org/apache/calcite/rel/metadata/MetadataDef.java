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

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Definition of metadata.
 *
 * @param <M> Kind of metadata
 */
public class MetadataDef<M extends Metadata> {
  public final Class<M> metadataClass;
  public final Class<? extends MetadataHandler<M>> handlerClass;
  public final ImmutableList<Method> methods;

  private MetadataDef(Class<M> metadataClass,
      Class<? extends MetadataHandler<M>> handlerClass, Method... methods) {
    this.metadataClass = metadataClass;
    this.handlerClass = handlerClass;
    this.methods =
        Arrays.stream(methods)
            .sorted(Comparator.comparing(Method::getName))
            .collect(toImmutableList());
    final SortedMap<String, Method> handlerMethods =
        MetadataHandler.handlerMethods(handlerClass);

    // Handler must have the same methods as Metadata, each method having
    // additional "subclass-of-RelNode, RelMetadataQuery" parameters.
    checkArgument(this.methods.size() == handlerMethods.size(),
        "handlerMethods.length = methods.length", this.methods, handlerMethods);
    Pair.forEach(this.methods, handlerMethods.values(),
        (method, handlerMethod) -> {
          final List<Class<?>> methodTypes =
              Arrays.asList(method.getParameterTypes());
          final List<Class<?>> handlerTypes =
              Arrays.asList(handlerMethod.getParameterTypes());
          checkArgument(methodTypes.size() + 2 == handlerTypes.size(),
              "methodTypes.size + 2 == handlerTypes.size", handlerMethod,
              methodTypes, handlerTypes);
          checkArgument(RelNode.class.isAssignableFrom(handlerTypes.get(0)),
              "RelNode.assignableFrom(handlerType[0])", handlerMethod,
              handlerTypes);
          checkArgument(RelMetadataQuery.class == handlerTypes.get(1),
              "handlerTypes[1] == RelMetadataQuery", handlerMethod,
              handlerTypes);
          checkArgument(methodTypes.equals(Util.skip(handlerTypes, 2)),
              "methodTypes == handlerTypes.skip(2)", handlerMethod, methodTypes,
              handlerTypes);
        });
  }

  /** Creates a {@link org.apache.calcite.rel.metadata.MetadataDef}. */
  public static <M extends Metadata> MetadataDef<M> of(Class<M> metadataClass,
      Class<? extends MetadataHandler<M>> handlerClass, Method... methods) {
    return new MetadataDef<>(metadataClass, handlerClass, methods);
  }
}
