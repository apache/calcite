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
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

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

/**
 * Generates the {@link MetadataHandler} code, then compiles the code using janino.
 */
public class JaninoMetadataHandlerCreator {

  private JaninoMetadataHandlerCreator() {
  }

  public static <M extends Metadata, MH extends MetadataHandler<M>> MH newInstance(
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
      CacheGenerator.cacheProperties(buff, method.e, method.i);
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
    getDefMethod(buff,
        handlerToName.values()
            .stream()
            .findFirst()
            .orElse(null));

    DispatchGenerator dispatchGenerator = new DispatchGenerator(handlerToName);
    for (Ord<Method> method : Ord.zip(handlerClass.getDeclaredMethods())) {
      CacheGenerator.cachedMethod(buff, method.e, method.i);
      dispatchGenerator.dispatchMethod(buff, method.e, handlers);
    }

    final List<Object> argList = new ArrayList<>(handlerToName.keySet());
    try {
      return compile(name, buff.toString(), handlerClass, argList);
    } catch (CompileException | IOException e) {
      throw new RuntimeException("Error compiling:\n"
          + buff, e);
    }
  }

  private static void getDefMethod(StringBuilder buff, @Nullable String handlerName) {
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


  private static <MH extends MetadataHandler<?>> MH compile(String className,
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
}
