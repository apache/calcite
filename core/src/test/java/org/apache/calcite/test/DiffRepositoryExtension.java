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
package org.apache.calcite.test;

import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver;

import java.io.File;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A JUnit extension to manage access to {@code DiffRepository}
 *
 * The extension injects a {@code DiffRepository} instance to constructor or test method parameter.
 * By default the repository is based on the test class name, but a different repository can be
 * used thanks to the {@code DiffRepositoryName} annotation.
 *
 * At the end of the test run, all instances are dumped on disk.
 */
public class DiffRepositoryExtension extends TypeBasedParameterResolver<DiffRepository>
    implements BeforeAllCallback, AfterAllCallback {
  private static final String DIFF_REPOSITORIES = "diff repositories";

  public DiffRepositoryExtension() {
  }

  @Override public DiffRepository resolveParameter(ParameterContext parameterContext,
      ExtensionContext extensionContext) throws ParameterResolutionException {
    // Get the enclosing class
    final Class<?> clazz = parameterContext.findAnnotation(DiffRepositoryName.class)
        .<Class<?>>map(DiffRepositoryName::value)
        .orElseGet(() -> getDefaultClass(parameterContext));

    @SuppressWarnings("unchecked")
    final Map<Class<?>, DiffRepository> repositories = (Map<Class<?>, DiffRepository>) getStore(
        extensionContext).get(DIFF_REPOSITORIES);
    DiffRepository repository = repositories.computeIfAbsent(clazz, this::toRepo);
    return repository;
  }

  private Class<?> getDefaultClass(ParameterContext parameterContext) {
    return parameterContext.getTarget().<Class<?>>map(Object::getClass)
        .orElse(parameterContext.getDeclaringExecutable().getDeclaringClass());
  }

  @Override public void beforeAll(ExtensionContext context) throws Exception {
    final Store store = getStore(context);
    store.put(DIFF_REPOSITORIES, new ConcurrentHashMap<String, DiffRepository>());
  }

  @Override public void afterAll(ExtensionContext context) throws Exception {
    @SuppressWarnings("unchecked")
    Map<Class<?>, DiffRepository> repositories = (Map<Class<?>, DiffRepository>) getStore(context)
        .get(DIFF_REPOSITORIES);
    for (Map.Entry<Class<?>, DiffRepository> entry : repositories.entrySet()) {
      final Class<?> clazz = entry.getKey();
      final DiffRepository repository = entry.getValue();

      final URL refFile = findFile(clazz, ".xml");
      final String refFilePath = Sources.of(refFile).file().getAbsolutePath();
      final String logFilePath = refFilePath.replace(".xml", "_actual.xml");
      final File logFile = new File(logFilePath);
      assert !refFilePath.equals(logFile.getAbsolutePath());

      int indent = logFile.getPath().contains("RelOptRulesTest")
          || logFile.getPath().contains("SqlToRelConverterTest")
          || logFile.getPath().contains("SqlLimitsTest") ? 4 : 2;

      repository.writeToFile(logFile, indent);
    }
  }

  private Store getStore(ExtensionContext context) {
    return context.getStore(Namespace.create(getClass(), context.getRoot()));
  }

  private static URL findFile(Class<?> clazz, final String suffix) {
    // The reference file for class "com.foo.Bar" is "com/foo/Bar.xml"
    String rest = "/" + clazz.getName().replace('.', File.separatorChar) + suffix;
    return clazz.getResource(rest);
  }

  private DiffRepository toRepo(Class<?> clazz) {
    final URL refFile = findFile(clazz, ".xml");

    return new DiffRepository(refFile);
  }
}
