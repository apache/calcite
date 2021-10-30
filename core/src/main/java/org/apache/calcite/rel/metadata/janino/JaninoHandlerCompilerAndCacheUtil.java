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
package org.apache.calcite.rel.metadata.janino;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.util.Util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.codehaus.commons.compiler.ISimpleCompiler;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Util for compiling, instantating, caching of {@link RelMetadataHandler}.
 */
public class JaninoHandlerCompilerAndCacheUtil {

  private JaninoHandlerCompilerAndCacheUtil() {
  }

  /**
   * Cache of pre-generated handlers by provider and kind of metadata.
   * For the cache to be effective, providers should implement identity
   * correctly.
   */
  private static final LoadingCache<Key, MetadataHandler<?>> HANDLERS =
      maxSize(CacheBuilder.newBuilder(),
          CalciteSystemProperty.METADATA_HANDLER_CACHE_MAXIMUM_SIZE.value())
          .build(
              CacheLoader.from(key ->
                  generateCompileAndInstantiate(key.handlerClass,
                      key.provider.handlers(key.handlerClass))));

  public static <MH extends MetadataHandler<?>> MH getHandler(
      RelMetadataProvider provider, Class<MH> handlerClass) {
    try {
      final Key key =
          new Key(handlerClass, provider);
      //noinspection unchecked
      return handlerClass.cast(HANDLERS.get(key));
    } catch (UncheckedExecutionException | ExecutionException e) {
      throw Util.throwAsRuntime(Util.causeOrSelf(e));
    }
  }

  private static <MH extends MetadataHandler<?>> MH generateCompileAndInstantiate(
      Class<MH> handlerClass,
      List<? extends MetadataHandler<? extends Metadata>> handlers) {
    final List<? extends MetadataHandler<? extends Metadata>> uniqueHandlers = handlers.stream()
        .distinct()
        .collect(Collectors.toList());
    RelMetadataHandlerGeneratorUtil.HandlerNameAndGeneratedCode handlerNameAndGeneratedCode =
        RelMetadataHandlerGeneratorUtil.generateHandler(handlerClass, uniqueHandlers);
    Class<? extends MH> mhImplClazz = compile(handlerNameAndGeneratedCode.getHandlerName(),
        handlerNameAndGeneratedCode.getGeneratedCode(), handlerClass);
    return instantiate(mhImplClazz, uniqueHandlers);
  }


  private static <MH extends MetadataHandler<?>> Class<? extends MH> compile(String className,
      String generatedCode, Class<MH> handlerClass) {
    final ICompilerFactory compilerFactory;
    ClassLoader classLoader = Objects.requireNonNull(
        JaninoHandlerCompilerAndCacheUtil.class.getClassLoader(), "classLoader");
    try {
      compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory(classLoader);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to instantiate java compiler", e);
    }

    final ISimpleCompiler compiler = compilerFactory.newSimpleCompiler();
    compiler.setParentClassLoader(JaninoRexCompiler.class.getClassLoader());

    if (CalciteSystemProperty.DEBUG.value()) {
      // Add line numbers to the generated janino class
      compiler.setDebuggingInformation(true, true, true);
      System.out.println(generatedCode);
    }
    try {
      compiler.cook(generatedCode);
      Class mhImplClass = compiler.getClassLoader().loadClass(className);
      assert handlerClass.isAssignableFrom(mhImplClass);
      //noinspection unchecked
      return mhImplClass;
    } catch (CompileException | ClassNotFoundException e) {
      throw new RuntimeException("Error compiling:\n"
          + generatedCode, e);
    }
  }

  private static <MH extends MetadataHandler<?>> MH instantiate(
      Class<? extends MH> metadataImpl, List<? extends Object> argList) {
    try {
      Constructor<?> constructor = metadataImpl.getConstructors()[0];
      Object instant = constructor.newInstance(argList.toArray());
      return metadataImpl.cast(instant);
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  // helper for initialization
  private static <K, V> CacheBuilder<K, V> maxSize(CacheBuilder<K, V> builder,
      int size) {
    if (size >= 0) {
      builder.maximumSize(size);
    }
    return builder;
  }

  /** Key for the cache. */
  private static class Key {
    final Class<? extends MetadataHandler<? extends Metadata>> handlerClass;
    final RelMetadataProvider provider;

    private Key(Class<? extends MetadataHandler<?>> handlerClass,
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
