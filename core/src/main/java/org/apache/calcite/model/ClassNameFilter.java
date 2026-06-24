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
package org.apache.calcite.model;

import org.apache.calcite.config.CalciteSystemProperty;

import com.google.common.collect.ImmutableList;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Filters class names that may be loaded by reflection from a Calcite
 * model: user-defined functions, custom schemas, custom tables, JDBC
 * drivers, dialect factories, and lattice statistic providers.
 *
 * <p>The behavior of the filter is determined by the allowlist and the denylist.
 * Both lists are comma separated patterns determining a package or class name.
 * A pattern ending in {@code "."} matches any class in that package or its sub-packages;
 * otherwise the pattern matches a class name exactly. Whitespace around
 * commas is ignored.
 */
@API(since = "1.43.0", status = API.Status.EXPERIMENTAL)
public final class ClassNameFilter {
  /** Built-in denylist: class-name patterns known to enable RCE when
   * registered as UDFs, schema/table factories, JDBC drivers, dialect
   * factories, or lattice statistic providers. */
  static final String DEFAULT_DENYLIST = ""
      + "javax.naming.,"
      + "com.sun.jndi.,"
      + "java.lang.Runtime,"
      + "java.lang.ProcessBuilder,"
      + "java.lang.ProcessImpl,"
      + "java.lang.System,"
      + "java.lang.Class,"
      + "java.lang.reflect.,"
      + "java.lang.invoke.,"
      + "javax.script.,"
      + "bsh.,"
      + "groovy.,"
      + "org.codehaus.groovy.,"
      + "org.python.util.PythonInterpreter,"
      + "org.springframework.expression.,"
      + "org.apache.commons.collections.functors.,"
      + "org.apache.commons.collections4.functors.,"
      + "org.apache.commons.beanutils.,"
      + "com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl,"
      + "sun.misc.Unsafe,"
      + "jdk.internal.";

  /** Cache shared by all factory calls; filters are immutable and small,
   * so identical (denylist, allowlist) inputs need only be parsed once. */
  private static final ConcurrentMap<String, ClassNameFilter> CACHE =
      new ConcurrentHashMap<>();

  /** The standard filter, built once from the built-in denylist plus
   * the {@link CalciteSystemProperty} pair. Initialized via {@link #of}
   * so it shares the same cache. */
  private static final ClassNameFilter STANDARD =
      of(
          append(DEFAULT_DENYLIST,
              CalciteSystemProperty.MODEL_CLASSES_DENIED.value()),
          CalciteSystemProperty.MODEL_CLASSES_ALLOWED.value());

  private final ImmutableList<String> denylist;
  private final ImmutableList<String> allowlist;

  private ClassNameFilter(String denylist, String allowlist) {
    this.denylist = parse(denylist);
    this.allowlist = parse(allowlist);
  }

  /** Returns the standard filter used by {@link ModelHandler}. The
   * built-in {@link #DEFAULT_DENYLIST} (extended by
   * {@link CalciteSystemProperty#MODEL_CLASSES_DENIED}) plus the
   * allowlist from
   * {@link CalciteSystemProperty#MODEL_CLASSES_ALLOWED}. */
  static ClassNameFilter standard() {
    return STANDARD;
  }

  /** Returns a filter parsed from comma-separated {@code denylist} and
   * {@code allowlist} pattern strings; either may be empty. Filters are
   * cached, so repeated calls with the same arguments return the same
   * instance. */
  public static ClassNameFilter of(String denylist, String allowlist) {
    // NUL is forbidden in JVM class names, so concatenating with NUL is
    // an injection-proof cache key.
    String key = denylist + '\0' + allowlist;
    return CACHE.computeIfAbsent(key,
        k -> new ClassNameFilter(denylist, allowlist));
  }

  /** Throws {@link SecurityException} if {@code classRef} is not allowed
   * by this filter. A null reference is a no-op. */
  void check(@Nullable String classRef) {
    if (classRef == null) {
      return;
    }
    String className = stripFieldRef(classRef);
    for (String pattern : denylist) {
      if (matches(pattern, className)) {
        throw new SecurityException(
            "Class '" + className + "' rejected by the denylist (pattern '" + pattern + "').");
      }
    }

    for (String pattern : allowlist) {
      if (matches(pattern, className)) {
        return;
      }
    }
    throw new SecurityException("Class '" + className + "' rejected by the allowlist.");
  }

  private static String stripFieldRef(String classRef) {
    int hash = classRef.indexOf('#');
    return hash >= 0 ? classRef.substring(0, hash) : classRef;
  }

  private static boolean matches(String pattern, String className) {
    if (pattern.endsWith(".")) {
      return className.startsWith(pattern);
    }
    return className.equals(pattern);
  }

  /** Returns the concatenation of two comma-separated pattern strings,
   * inserting a comma if needed and tolerating empty inputs. */
  static String append(String first, String second) {
    if (first.isEmpty()) {
      return second;
    }
    if (second.isEmpty()) {
      return first;
    }
    return first + "," + second;
  }

  private static ImmutableList<String> parse(String list) {
    if (list.isEmpty()) {
      return ImmutableList.of();
    }
    ImmutableList.Builder<String> b = ImmutableList.builder();
    for (String s : list.split(",")) {
      String trimmed = s.trim();
      if (!trimmed.isEmpty()) {
        b.add(trimmed);
      }
    }
    return b.build();
  }
}
