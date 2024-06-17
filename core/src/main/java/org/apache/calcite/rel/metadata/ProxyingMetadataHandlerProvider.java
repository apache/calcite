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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * A MetadataHandlerProvider built on a RelMetadataProvider.
 *
 * <p>Uses proxies to call the underlying metadata provider.
 */
public class ProxyingMetadataHandlerProvider implements MetadataHandlerProvider {

  private final RelMetadataProvider provider;

  /**
   * Create a proxying handler provider.
   *
   * @param provider The provider this will operate against.
   */
  public ProxyingMetadataHandlerProvider(RelMetadataProvider provider) {
    this.provider = provider;
  }

  @SuppressWarnings("deprecation")
  @Override public <MH extends MetadataHandler<?>> MH handler(Class<MH> handlerClass) {

    Type[] types = handlerClass.getGenericInterfaces();
    if (types.length != 1 || !(types[0] instanceof ParameterizedType)) {
      throw new UnsupportedOperationException("Unexpected failure. " + handlerClass);
    }

    ParameterizedType pType = (ParameterizedType) types[0];
    if (pType.getRawType() != MetadataHandler.class) {
      throw new UnsupportedOperationException("Unexpected failure. " + handlerClass);
    }
    Class<?> metadataType = (Class<?>) pType.getActualTypeArguments()[0];
    final Field field;
    final MetadataDef<?> def;
    try {
      field = metadataType.getField("DEF");
      def =
          requireNonNull((MetadataDef<?>) field.get(null),
              () -> "Unexpected failure. " + handlerClass);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }

    List<Method> methods = def.methods;
    Map<String, Method> methodMap = methods.stream()
        .collect(Collectors.toMap(Method::getName, f -> f));
    InvocationHandler handler = (proxy, method, args) -> {
      Method metadataMethod =
          requireNonNull(methodMap.get(method.getName()),
              () -> "Not supported: " + method);
      RelNode rel = requireNonNull((RelNode) args[0], "rel must be non null");
      RelMetadataQuery mq =
          requireNonNull((RelMetadataQuery) args[1], "mq must be non null");

      // using deprecated RelMetadataProvider method here as the non-deprecated methods completely
      // sidestep the purpose of RelMetadataProvider reflection-based functionality.
      @SuppressWarnings({"unchecked", "rawtypes"})
      UnboundMetadata metadata =
          provider.apply(rel.getClass(),
              (Class<? extends Metadata>) metadataType);

      if (metadata == null) {
        Method handlerMethod =
            Arrays.stream(handlerClass.getMethods())
                .filter(m -> m.getName().equals(metadataMethod.getName()))
                .findFirst()
                .orElseThrow(()
                    -> new IllegalArgumentException("Unable to find method."));
        throw new IllegalArgumentException(
            String.format(Locale.ROOT, "No handler for method [%s] applied to "
                + "argument of type [%s]; we recommend you create a catch-all "
                + "(RelNode) handler", handlerMethod, rel.getClass()));
      }
      Metadata bound =
          requireNonNull(metadata, "expected defined metadata")
              .bind(rel, mq);

      Object[] abbreviatedArgs = new Object[args.length - 2];
      System.arraycopy(args, 2, abbreviatedArgs, 0, abbreviatedArgs.length);
      try {
        return metadataMethod.invoke(bound, abbreviatedArgs);
      } catch (InvocationTargetException ex) {
        if (ex.getCause() instanceof CyclicMetadataException) {
          throw (CyclicMetadataException) ex.getCause();
        }

        throw new RuntimeException(ex.getCause());
      }
    };

    return (MH) Proxy.newProxyInstance(
        handlerClass.getClassLoader(),
        new Class[]{handlerClass},
        handler);
  }

}
