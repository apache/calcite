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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

/**
 * Filters class names that may be loaded by reflection from a Calcite
 * model: user-defined functions, custom schemas, custom tables, JDBC
 * drivers, dialect factories, and lattice statistic providers.
 *
 * <p>{@link #standard()} returns the filter applied by
 * {@link ModelHandler}: the built-in {@link #DEFAULT_DENYLIST} together
 * with any patterns from
 * {@link CalciteSystemProperty#MODEL_CLASSES_DENIED} (which
 * <em>extends</em> the denylist).
 *
 * <p>The denylist is a comma-separated pattern string. A pattern ending
 * in {@code "."} matches any class in that package or its sub-packages;
 * otherwise the pattern matches a class name exactly. Whitespace around
 * commas is ignored.
 *
 * <p>The denylist is not a sandbox. Any string passed to a
 * {@code className}, {@code factory}, {@code jdbcDriver},
 * {@code sqlDialectFactory}, or {@code statisticProvider} field is
 * classpath-equivalent; only accept models from trusted sources.
 */
class ClassNameFilter implements Predicate<String> {
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
   * so identical denylist inputs need only be parsed once. */
  private static final ConcurrentMap<String, ClassNameFilter> CACHE =
      new ConcurrentHashMap<>();

  /** The standard filter, built once from the built-in denylist plus
   * the {@link CalciteSystemProperty#MODEL_CLASSES_DENIED} extension.
   * Initialized via {@link #of} so it shares the same cache. */
  private static final ClassNameFilter STANDARD =
      of(
          append(DEFAULT_DENYLIST,
          CalciteSystemProperty.MODEL_CLASSES_DENIED.value()));

  private final ImmutableList<String> denylist;

  private ClassNameFilter(String denylist) {
    this.denylist = parse(denylist);
  }

  /** Returns the standard filter used by {@link ModelHandler}: the
   * built-in {@link #DEFAULT_DENYLIST} (extended by
   * {@link CalciteSystemProperty#MODEL_CLASSES_DENIED}). */
  static ClassNameFilter standard() {
    return STANDARD;
  }

  /** Returns a filter parsed from a comma-separated denylist pattern
   * string; may be empty. Filters are cached, so repeated calls with
   * the same argument return the same instance. */
  static ClassNameFilter of(String denylist) {
    return CACHE.computeIfAbsent(denylist, ClassNameFilter::new);
  }

  /** Returns whether {@code classRef} is allowed (not on the denylist).
   * A null reference is allowed.
   *
   * <p>{@code classRef} may be a plain class name or the
   * {@code "ClassName#STATIC_FIELD"} form accepted by
   * {@link org.apache.calcite.avatica.AvaticaUtils#instantiatePlugin};
   * the field portion is stripped before matching. */
  @Override public boolean test(@Nullable String classRef) {
    if (classRef == null) {
      return true;
    }
    String className = stripFieldRef(classRef);
    for (String pattern : denylist) {
      if (matches(pattern, className)) {
        return false;
      }
    }
    return true;
  }

  /** Throws {@link SecurityException} if {@code classRef} is on the
   * denylist. A null reference is a no-op. */
  void check(@Nullable String classRef) {
    if (classRef == null) {
      return;
    }
    String className = stripFieldRef(classRef);
    for (String pattern : denylist) {
      if (matches(pattern, className)) {
        throw new SecurityException("Class '" + className
            + "' is rejected by the Calcite class-name filter "
            + "(matches denylist pattern '" + pattern + "'). "
            + "If this load is unintended, adjust the model; the "
            + "denylist cannot be loosened at runtime.");
      }
    }
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
