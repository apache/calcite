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

import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Guards against introducing new dispatch gaps in {@link RelShuttle}. */
class RelShuttleCoverageTest {

  private static final Set<String> SCANNED_PACKAGES =
      ImmutableSet.of("org.apache.calcite.rel.core",
      "org.apache.calcite.rel.logical");

  /** Every concrete RelNode in the scanned packages must be covered by a non-{@code visit(RelNode)}
   * overload on {@link RelShuttle}. Without this, a {@code RelShuttleImpl} subclass that customizes
   * the type-specific visitor would silently not be called for the rel. */
  @Test void everyRelNodeHasMatchingVisitOverload() throws IOException {
    final Set<Class<?>> visitParameters = collectVisitParameterTypes();
    final Set<Class<? extends RelNode>> relClasses = findConcreteRelNodesInPackages();

    final Set<Class<? extends RelNode>> uncovered = relClasses.stream()
        .filter(c -> visitParameters.stream().noneMatch(vp -> vp.isAssignableFrom(c)))
        .collect(
            Collectors.toCollection(() ->
            new TreeSet<>(Comparator.comparing(Class::getName))));

    assertTrue(uncovered.isEmpty(),
        () -> "RelNodes with no RelShuttle.visit(...) overload covering them "
            + "(only the generic visit(RelNode) catch-all matches): " + uncovered);
  }

  /** Every {@link RelShuttle#visit(...)} parameter type (other than {@code RelNode}) must declare
   * its own {@code accept(RelShuttle)} so dispatch routes through the type-specific overload. */
  @Test void everyVisitParameterTypeDeclaresAccept() {
    final Set<Class<?>> missingAccept = collectVisitParameterTypes().stream()
        .filter(c -> !declaresAcceptRelShuttle(c))
        .collect(
            Collectors.toCollection(() ->
            new TreeSet<>(Comparator.comparing(Class::getName))));

    assertTrue(missingAccept.isEmpty(),
        () -> "RelShuttle.visit(X) parameter types whose X does not declare accept(RelShuttle): "
            + missingAccept);
  }

  /** Visit parameter types other than {@link RelNode} (the catch-all fallback). */
  private static Set<Class<?>> collectVisitParameterTypes() {
    final Set<Class<?>> params = new HashSet<>();
    for (Method method : RelShuttle.class.getMethods()) {
      if ("visit".equals(method.getName()) && method.getParameterCount() == 1) {
        Class<?> paramType = method.getParameterTypes()[0];
        if (paramType != RelNode.class) {
          params.add(paramType);
        }
      }
    }
    return params;
  }

  private static boolean declaresAcceptRelShuttle(Class<?> clazz) {
    try {
      clazz.getDeclaredMethod("accept", RelShuttle.class);
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  private static Set<Class<? extends RelNode>> findConcreteRelNodesInPackages() throws IOException {
    final Set<Class<? extends RelNode>> classes = new HashSet<>();
    final ClassPath classPath = ClassPath.from(RelShuttleCoverageTest.class.getClassLoader());
    for (String packageName : SCANNED_PACKAGES) {
      for (ClassPath.ClassInfo info : classPath.getTopLevelClasses(packageName)) {
        final Class<?> clazz = info.load();
        if (RelNode.class.isAssignableFrom(clazz)
            && !Modifier.isAbstract(clazz.getModifiers())
            && clazz != AbstractRelNode.class) {
          @SuppressWarnings("unchecked")
          Class<? extends RelNode> relClass = (Class<? extends RelNode>) clazz;
          classes.add(relClass);
        }
      }
    }
    return classes;
  }
}
