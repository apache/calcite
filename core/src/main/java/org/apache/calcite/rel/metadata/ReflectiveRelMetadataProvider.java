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
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Implementation of the {@link RelMetadataProvider} interface that dispatches
 * metadata methods to methods on a given object via reflection.
 *
 * <p>The methods on the target object must be public and non-static, and have
 * the same signature as the implemented metadata method except for an
 * additional first parameter of type {@link RelNode} or a sub-class. That
 * parameter gives this provider an indication of that relational expressions it
 * can handle.</p>
 *
 * <p>For an example, see {@link RelMdColumnOrigins#SOURCE}.
 */
public class ReflectiveRelMetadataProvider
    implements RelMetadataProvider, ReflectiveVisitor {

  /** Comparator that sorts derived classes before their base classes. */
  private static final Comparator<Class<RelNode>> SUPERCLASS_COMPARATOR =
      new Comparator<Class<RelNode>>() {
        public int compare(Class<RelNode> c1, Class<RelNode> c2) {
          return c1 == c2 ? 0 : c2.isAssignableFrom(c1) ? -1 : 1;
        }
      };

  //~ Instance fields --------------------------------------------------------
  private final ImmutableMap<Class<RelNode>, Function<RelNode, Metadata>> map;
  private final Class<?> metadataClass0;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ReflectiveRelMetadataProvider.
   *
   * @param map Map
   * @param metadataClass0 Metadata class
   */
  protected ReflectiveRelMetadataProvider(
      ImmutableMap<Class<RelNode>, Function<RelNode, Metadata>> map,
      Class<?> metadataClass0) {
    assert !map.isEmpty() : "are your methods named wrong?";
    this.map = map;
    this.metadataClass0 = metadataClass0;
  }

  /** Returns an implementation of {@link RelMetadataProvider} that scans for
   * methods with a preceding argument.
   *
   * <p>For example, {@link BuiltInMetadata.Selectivity} has a method
   * {@link BuiltInMetadata.Selectivity#getSelectivity(org.apache.calcite.rex.RexNode)}.
   * A class</p>
   *
   * <blockquote><pre><code>
   * class RelMdSelectivity {
   *   public Double getSelectivity(Union rel, RexNode predicate) { }
   *   public Double getSelectivity(Filter rel, RexNode predicate) { }
   * </code></pre></blockquote>
   *
   * <p>provides implementations of selectivity for relational expressions
   * that extend {@link org.apache.calcite.rel.core.Union}
   * or {@link org.apache.calcite.rel.core.Filter}.</p>
   */
  public static RelMetadataProvider reflectiveSource(Method method,
      Object target) {
    return reflectiveSource(target, ImmutableList.of(method));
  }

  /** Returns a reflective metadata provider that implements several
   * methods. */
  public static RelMetadataProvider reflectiveSource(Object target,
      Method... methods) {
    return reflectiveSource(target, ImmutableList.copyOf(methods));
  }

  private static RelMetadataProvider reflectiveSource(final Object target,
      final ImmutableList<Method> methods) {
    assert methods.size() > 0;
    final Method method0 = methods.get(0);
    final Class<?> metadataClass0 = method0.getDeclaringClass();
    assert Metadata.class.isAssignableFrom(metadataClass0);
    for (Method method : methods) {
      assert method.getDeclaringClass() == metadataClass0;
    }

    // Find the distinct set of RelNode classes handled by this provider,
    // ordered base-class first.
    final TreeSet<Class<RelNode>> classes =
        Sets.newTreeSet(SUPERCLASS_COMPARATOR);
    final Map<Pair<Class<RelNode>, Method>, Method> handlerMap =
        Maps.newHashMap();
    for (final Method handlerMethod : target.getClass().getMethods()) {
      for (Method method : methods) {
        if (couldImplement(handlerMethod, method)) {
          @SuppressWarnings("unchecked") final Class<RelNode> relNodeClass =
              (Class<RelNode>) handlerMethod.getParameterTypes()[0];
          classes.add(relNodeClass);
          handlerMap.put(Pair.of(relNodeClass, method), handlerMethod);
        }
      }
    }

    final Map<Class<RelNode>, Function<RelNode, Metadata>> treeMap =
        Maps.newTreeMap(SUPERCLASS_COMPARATOR);

    for (Class<RelNode> key : classes) {
      ImmutableNullableList.Builder<Method> builder =
          ImmutableNullableList.builder();
      for (final Method method : methods) {
        builder.add(find(classes, handlerMap, key, method));
      }
      final List<Method> handlerMethods = builder.build();
      final Function<RelNode, Metadata> function =
          new Function<RelNode, Metadata>() {
            public Metadata apply(final RelNode rel) {
              return (Metadata) Proxy.newProxyInstance(
                  metadataClass0.getClassLoader(),
                  new Class[]{metadataClass0},
                  new InvocationHandler() {
                    public Object invoke(Object proxy, Method method,
                        Object[] args) throws Throwable {
                      // Suppose we are an implementation of Selectivity
                      // that wraps "filter", a LogicalFilter. Then we
                      // implement
                      //   Selectivity.selectivity(rex)
                      // by calling method
                      //   new SelectivityImpl().selectivity(filter, rex)
                      if (method.equals(
                          BuiltInMethod.METADATA_REL.method)) {
                        return rel;
                      }
                      if (method.equals(
                          BuiltInMethod.OBJECT_TO_STRING.method)) {
                        return metadataClass0.getSimpleName() + "(" + rel
                            + ")";
                      }
                      int i = methods.indexOf(method);
                      if (i < 0) {
                        throw new AssertionError("not handled: " + method
                            + " for " + rel);
                      }
                      final Object[] args1;
                      if (args == null) {
                        args1 = new Object[]{rel};
                      } else {
                        args1 = new Object[args.length + 1];
                        args1[0] = rel;
                        System.arraycopy(args, 0, args1, 1, args.length);
                      }
                      final Method handlerMethod = handlerMethods.get(i);
                      if (handlerMethod == null) {
                        throw new AssertionError("not handled: " + method
                            + " for " + rel);
                      }
                      return handlerMethod.invoke(target, args1);
                    }
                  });
            }
          };
      treeMap.put(key, function);
    }
    // Due to the comparator, the TreeMap is sorted such that any derived class
    // will occur before its base class. The immutable map is not a sorted map,
    // but it retains the traversal order, and that is sufficient.
    final ImmutableMap<Class<RelNode>, Function<RelNode, Metadata>> map =
        ImmutableMap.copyOf(treeMap);
    return new ReflectiveRelMetadataProvider(map, metadataClass0);
  }

  /** Finds an implementation of a method for {@code relNodeClass} or its
   * nearest base class. Assumes that base classes have already been added to
   * {@code map}. */
  private static Method find(TreeSet<Class<RelNode>> classes,
      Map<Pair<Class<RelNode>, Method>, Method> handlerMap,
      Class<RelNode> relNodeClass, Method method) {
    final Iterator<Class<RelNode>> iterator = classes.descendingIterator();
    for (;;) {
      final Method implementingMethod =
          handlerMap.get(Pair.of(relNodeClass, method));
      if (implementingMethod != null) {
        return implementingMethod;
      }
      if (!iterator.hasNext()) {
        return null;
      }
      relNodeClass = iterator.next();
    }
  }

  private static boolean couldImplement(Method handlerMethod, Method method) {
    if (!handlerMethod.getName().equals(method.getName())
        || (handlerMethod.getModifiers() & Modifier.STATIC) != 0
        || (handlerMethod.getModifiers() & Modifier.PUBLIC) == 0) {
      return false;
    }
    final Class<?>[] parameterTypes1 = handlerMethod.getParameterTypes();
    final Class<?>[] parameterTypes = method.getParameterTypes();
    if (parameterTypes1.length != parameterTypes.length + 1
        || !RelNode.class.isAssignableFrom(parameterTypes1[0])) {
      return false;
    }
    return Util.skip(Arrays.asList(parameterTypes1))
        .equals(Arrays.asList(parameterTypes));
  }

  //~ Methods ----------------------------------------------------------------

  public Function<RelNode, Metadata> apply(
      Class<? extends RelNode> relClass,
      Class<? extends Metadata> metadataClass) {
    if (metadataClass == metadataClass0) {
      //noinspection SuspiciousMethodCalls
      final Function<RelNode, Metadata> function = map.get(relClass);
      if (function != null) {
        return function;
      }
      for (Map.Entry<Class<RelNode>, Function<RelNode, Metadata>> entry
          : map.entrySet()) {
        if (entry.getKey().isAssignableFrom(relClass)) {
          // REVIEW: We are assuming that the first we find is the "best".
          return entry.getValue();
        }
      }
    }
    return null;
  }
}

// End ReflectiveRelMetadataProvider.java
