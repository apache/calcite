package org.apache.calcite.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Discovers every concrete {@link RelNode} implementation on the classpath and
 * verifies {@link RelShuttle} declares a compatible {@code visit} method.
 */
class RelNodeVisitCoverageTest {

  @Test void relShuttleCoversAllRelNodes() {
    Set<Class<? extends RelNode>> relNodeClasses = findRelNodeClasses();
    Set<Class<?>> visitParameters = visitParameterTypes();

    Set<Class<? extends RelNode>> missing = relNodeClasses.stream()
        .filter(relClass -> visitParameters.stream()
            .noneMatch(relClass::isAssignableFrom))
        .collect(Collectors.toCollection(() ->
            new TreeSet<>(Comparator.comparing(Class::getName))));

    assertTrue(missing.isEmpty(),
        () -> "RelShuttle.visit(...) is missing for: " + missing);
  }

  private Set<Class<?>> visitParameterTypes() {
    Set<Class<?>> visitParameters = new HashSet<>();
    for (Method method : RelShuttle.class.getMethods()) {
      if ("visit".equals(method.getName()) && method.getParameterCount() == 1) {
        visitParameters.add(method.getParameterTypes()[0]);
      }
    }
    return visitParameters;
  }

  private Set<Class<? extends RelNode>> findRelNodeClasses() {
    Set<Class<? extends RelNode>> result =
        new TreeSet<>(Comparator.comparing(Class::getName));
    try (ScanResult scan = new ClassGraph()
        .enableClassInfo()
        .ignoreClassVisibility()
        .acceptPackages(RelNode.class.getPackageName())
        .scan()) {
      for (ClassInfo classInfo : scan.getClassesImplementing(RelNode.class.getName())) {
        Class<? extends RelNode> clazz = classInfo.loadClass(RelNode.class);
        if (isConcreteRelNode(clazz)) {
          result.add(clazz);
        }
      }
    }
    return result;
  }

  private boolean isConcreteRelNode(Class<?> clazz) {
    return RelNode.class.isAssignableFrom(clazz)
        && !clazz.isInterface()
        && !Modifier.isAbstract(clazz.getModifiers());
  }
}
