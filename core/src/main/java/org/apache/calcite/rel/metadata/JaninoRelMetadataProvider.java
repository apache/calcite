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
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.janino.DispatchGenerator;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
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
          .build(
              CacheLoader.from(key ->
                  load3(key.def, key.provider.handlers(key.def))));

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

  private static <M extends Metadata> MetadataHandler<M> load3(
      MetadataDef<M> def, Multimap<Method, ? extends MetadataHandler<?>> map) {
    final StringBuilder buff = new StringBuilder();
    final String name =
        "GeneratedMetadata_" + simpleNameForHandler(def.handlerClass);
    final Set<MetadataHandler<?>> providerSet = new HashSet<>();

    final Map<MetadataHandler<?>, String> handlerToName = new LinkedHashMap<>();
    for (MetadataHandler<?> provider : map.values()) {
      if (providerSet.add(provider)) {
        handlerToName.put(provider,
            "provider" + (providerSet.size() - 1));
      }
    }

    buff.append("  private final org.apache.calcite.rel.metadata.MetadataDef def;\n");
    for (Map.Entry<MetadataHandler<?>, String> handlerAndName : handlerToName.entrySet()) {
      buff.append("  public final ").append(handlerAndName.getKey().getClass().getName())
          .append(' ').append(handlerAndName.getValue()).append(";\n");
    }
    buff.append("  public ").append(name).append("(\n")
        .append("      org.apache.calcite.rel.metadata.MetadataDef def");
    for (Map.Entry<MetadataHandler<?>, String> handlerAndName : handlerToName.entrySet()) {
      buff.append(",\n")
          .append("      ")
          .append(handlerAndName.getKey().getClass().getName())
          .append(' ')
          .append(handlerAndName.getValue());
    }
    buff.append(") {\n")
        .append("    this.def = def;\n");

    for (String handlerName : handlerToName.values()) {
      buff.append("    this.").append(handlerName).append(" = ").append(handlerName)
          .append(";\n");
    }
    buff.append("  }\n")
        .append("  public ")
        .append(MetadataDef.class.getName())
        .append(" getDef() {\n")
        .append("    return def;")
        .append("  }\n");
    DispatchGenerator dispatchGenerator = new DispatchGenerator(handlerToName);
    for (Ord<Method> method : Ord.zip(map.keySet())) {
      generateCachedMethod(buff, method.e, method.i);
      dispatchGenerator.dispatchMethod(buff, method.e, map.get(method.e));
    }
    //buff.append("}");
    final List<Object> argList = new ArrayList<>();
    argList.add(def);
    argList.addAll(handlerToName.keySet());
    try {
      return compile(name, buff.toString(), def, argList);
    } catch (CompileException | IOException e) {
      throw new RuntimeException("Error compiling:\n"
          + buff, e);
    }
  }

  private static void generateCachedMethod(StringBuilder buff, Method method, int methodIndex) {
    buff.append("  public ")
        .append(method.getReturnType().getName())
        .append(" ")
        .append(method.getName())
        .append("(\n")
        .append("      ")
        .append(RelNode.class.getName())
        .append(" r,\n")
        .append("      ")
        .append(RelMetadataQuery.class.getName())
        .append(" mq");
    paramList(buff, method)
        .append(") {\n");
    buff.append("    final java.util.List key = ")
        .append(
            (method.getParameterTypes().length < 4
                ? org.apache.calcite.runtime.FlatLists.class
                : ImmutableList.class).getName())
        .append(".of(");
    if (methodIndex == 0) {
      buff.append("def");
    } else {
      buff.append("def.methods.get(")
          .append(methodIndex)
          .append(")");
    }
    safeArgList(buff, method)
        .append(");\n")
        .append("    final Object v = mq.map.get(r, key);\n")
        .append("    if (v != null) {\n")
        .append("      if (v == ")
        .append(NullSentinel.class.getName())
        .append(".ACTIVE) {\n")
        .append("        throw new ")
        .append(CyclicMetadataException.class.getName())
        .append("();\n")
        .append("      }\n")
        .append("      if (v == ")
        .append(NullSentinel.class.getName())
        .append(".INSTANCE) {\n")
        .append("        return null;\n")
        .append("      }\n")
        .append("      return (")
        .append(method.getReturnType().getName())
        .append(") v;\n")
        .append("    }\n")
        .append("    mq.map.put(r, key,")
        .append(NullSentinel.class.getName())
        .append(".ACTIVE);\n")
        .append("    try {\n")
        .append("      final ")
        .append(method.getReturnType().getName())
        .append(" x = ")
        .append(method.getName())
        .append("_(r, mq");
    argList(buff, method)
        .append(");\n")
        .append("      mq.map.put(r, key, ")
        .append(NullSentinel.class.getName())
        .append(".mask(x));\n")
        .append("      return x;\n")
        .append("    } catch (")
        .append(Exception.class.getName())
        .append(" e) {\n")
        .append("      mq.map.row(r).clear();\n")
        .append("      throw e;\n")
        .append("    }\n")
        .append("  }\n")
        .append("\n");
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


  /** Returns e.g. ", ignoreNulls". */
  private static StringBuilder argList(StringBuilder buff, Method method) {
    for (Ord<Class<?>> t : Ord.zip(method.getParameterTypes())) {
      buff.append(", a").append(t.i);
    }
    return buff;
  }

  /** Returns e.g. ", ignoreNulls". */
  private static StringBuilder safeArgList(StringBuilder buff, Method method) {
    for (Ord<Class<?>> t : Ord.zip(method.getParameterTypes())) {
      if (Primitive.is(t.e) || RexNode.class.isAssignableFrom(t.e)) {
        buff.append(", a").append(t.i);
      } else {
        buff.append(", ") .append(NullSentinel.class.getName())
            .append(".mask(a").append(t.i).append(")");
      }
    }
    return buff;
  }

  /** Returns e.g. ",\n boolean ignoreNulls". */
  private static StringBuilder paramList(StringBuilder buff, Method method) {
    for (Ord<Class<?>> t : Ord.zip(method.getParameterTypes())) {
      buff.append(",\n      ").append(t.e.getName()).append(" a").append(t.i);
    }
    return buff;
  }

  static <M extends Metadata> MetadataHandler<M> compile(String className,
      String classBody, MetadataDef<M> def,
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
        + " implements " + def.handlerClass.getCanonicalName() + " {\n"
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
    return def.handlerClass.cast(o);
  }

  synchronized <M extends Metadata, H extends MetadataHandler<M>> H create(
      MetadataDef<M> def) {
    try {
      final Key key = new Key((MetadataDef) def, provider);
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

  /** Exception that indicates there there should be a handler for
   * this class but there is not. The action is probably to
   * re-generate the handler class. */
  public static class NoHandler extends ControlFlowException {
    public final Class<? extends RelNode> relClass;

    public NoHandler(Class<? extends RelNode> relClass) {
      this.relClass = relClass;
    }
  }

  /** Key for the cache. */
  private static class Key {
    public final MetadataDef def;
    public final RelMetadataProvider provider;

    private Key(MetadataDef def, RelMetadataProvider provider) {
      this.def = def;
      this.provider = provider;
    }

    @Override public int hashCode() {
      return (def.hashCode() * 37
          + provider.hashCode()) * 37;
    }

    @Override public boolean equals(@Nullable Object obj) {
      return this == obj
          || obj instanceof Key
          && ((Key) obj).def.equals(def)
          && ((Key) obj).provider.equals(provider);
    }
  }
}
