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
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.janino.CacheGenerator;
import org.apache.calcite.rel.metadata.janino.DispatchGenerator;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.codehaus.commons.compiler.ISimpleCompiler;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private static <M extends Metadata, MH extends MetadataHandler<M>> MH load3(
      Class<MH> handlerClass,
      ImmutableSet<? extends MetadataHandler<? extends Metadata>> handlerSet) {
    final ImmutableList<? extends MetadataHandler<?>> handlers =
        ImmutableList.copyOf(handlerSet);
    final StringBuilder buff = new StringBuilder();
    final String name =
        "GeneratedMetadata_" + simpleNameForHandler(handlerClass);
    final Set<MetadataHandler<?>> providerSet = new HashSet<>();

    final Map<MetadataHandler<?>, String> handlerToName = new LinkedHashMap<>();
    for (MetadataHandler<?> provider : handlers) {
      if (providerSet.add(provider)) {
        handlerToName.put(provider,
            "provider" + (providerSet.size() - 1));
      }
    }

    //PROPERTIES
    for (Ord<Method> method : Ord.zip(handlerClass.getDeclaredMethods())) {
      CacheGenerator.generateCacheProperty(buff, method.e, method.i);
    }
    for (Map.Entry<MetadataHandler<?>, String> handlerAndName : handlerToName.entrySet()) {
      buff.append("  public final ").append(handlerAndName.getKey().getClass().getName())
          .append(' ').append(handlerAndName.getValue()).append(";\n");
    }

    //CONSTRUCTOR
    buff.append("  public ").append(name).append("(\n");
    for (Map.Entry<MetadataHandler<?>, String> handlerAndName : handlerToName.entrySet()) {
      buff.append("      ")
          .append(handlerAndName.getKey().getClass().getName())
          .append(' ')
          .append(handlerAndName.getValue())
          .append(",\n");
    }
    if (!handlerToName.isEmpty()) {
      buff.setLength(buff.length() - 2);
    }
    buff.append(") {\n");
    for (String handlerName : handlerToName.values()) {
      buff.append("    this.").append(handlerName).append(" = ").append(handlerName)
          .append(";\n");
    }
    buff.append("  }\n");

    //METHODS
    generateGetDefMethod(buff,
        handlerToName.values()
            .stream()
            .findFirst()
            .orElse(null));

    DispatchGenerator dispatchGenerator = new DispatchGenerator(handlerToName);
    for (Ord<Method> method : Ord.zip(handlerClass.getDeclaredMethods())) {
      CacheGenerator.generateCachedMethod(buff, method.e, method.i);
      dispatchGenerator.generateDispatchMethod(buff, method.e, handlers);
    }

    final List<Object> argList = new ArrayList<>(handlerToName.keySet());
    try {
      return compile(name, buff.toString(), handlerClass, argList);
    } catch (CompileException | IOException e) {
      throw new RuntimeException("Error compiling:\n"
          + buff, e);
    }
  }

  private static void generateGetDefMethod(StringBuilder buff, @Nullable String handlerName) {
    buff.append("  public ")
        .append(MetadataDef.class.getName())
        .append(" getDef() {\n");

    if (handlerName == null) {
      buff.append("    return null;");
    } else {
      buff.append("    return ")
          .append(handlerName)
          .append(".getDef();\n");
    }
    buff.append("  }\n");
  }

  private static String simpleNameForHandler(Class<? extends MetadataHandler<?>> clazz) {
    String simpleName = clazz.getSimpleName();
    //Previously the pattern was to have a nested in class named Handler
    //So we need to add the parents class to get a unique name
    if (simpleName.equals("Handler")) {
      String[] parts = clazz.getName().split("\\.|\\$");
      return parts[parts.length - 2] + parts[parts.length - 1];
    } else {
      return simpleName;
    }
  }


  static <MH extends MetadataHandler<?>> MH compile(String className,
      String classBody, Class<MH> handlerClass,
      List<Object> argList) throws CompileException, IOException {
    final ICompilerFactory compilerFactory;
    try {
      compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to instantiate java compiler", e);
    }

    final ISimpleCompiler compiler = compilerFactory.newSimpleCompiler();
    compiler.setParentClassLoader(JaninoRexCompiler.class.getClassLoader());

    final String s = "public final class " + className
        + " implements " + handlerClass.getCanonicalName() + " {\n"
        + classBody
        + "\n"
        + "}";

    if (CalciteSystemProperty.DEBUG.value()) {
      // Add line numbers to the generated janino class
      compiler.setDebuggingInformation(true, true, true);
      System.out.println(s);
    }

    compiler.cook(s);
    final Constructor constructor;
    final Object o;
    try {
      constructor = compiler.getClassLoader().loadClass(className)
              .getDeclaredConstructors()[0];
      o = constructor.newInstance(argList.toArray());
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return handlerClass.cast(o);
  }

  synchronized <M extends Metadata, H extends MetadataHandler<M>> H create(
      MetadataDef<M> def) {
    try {
      final Key key = new Key(def.handlerClass, provider);
      //noinspection unchecked
      return (H) HANDLERS.get(key);
    } catch (UncheckedExecutionException | ExecutionException e) {
      throw Util.throwAsRuntime(Util.causeOrSelf(e));
    }
  }

  synchronized <M extends Metadata, H extends MetadataHandler<M>> H revise(
      Class<? extends RelNode> rClass, MetadataDef<M> def) {
    //noinspection unchecked
    return (H) create(def);
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
      return JaninoRelMetadataProvider.load3(
          key.handlerClass, handlers);
    });
    return cacheLoader;
  }

  /** Exception that indicates there there should be a handler for
   * this class but there is not. The action is probably to
   * re-generate the handler class. */
  public static class NoHandler extends ControlFlowException {
    public final Class<? extends RelNode> relClass;

    public NoHandler(Class<? extends RelNode> relClass) {
      this.relClass = relClass;
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
