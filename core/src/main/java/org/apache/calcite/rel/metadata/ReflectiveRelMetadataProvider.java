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
package org.eigenbase.rel.metadata;

import java.lang.reflect.*;
import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.util.*;

import net.hydromatic.optiq.BuiltinMethod;

import com.google.common.base.Function;
import com.google.common.collect.*;

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
   * {@link BuiltInMetadata.Selectivity#getSelectivity(org.eigenbase.rex.RexNode)}.
   * A class</p>
   *
   * <blockquote><pre><code>
   * class RelMdSelectivity {
   *   public Double getSelectivity(UnionRel rel, RexNode predicate) { ... }
   *   public Double getSelectivity(FilterRel rel, RexNode predicate) { ... }
   * </code></pre></blockquote>
   *
   * <p>provides implementations of selectivity for relational expressions
   * that extend {@link UnionRel} or {@link FilterRel}.</p>
   */
  public static RelMetadataProvider reflectiveSource(Method method,
      final Object target) {
    final Class<?> metadataClass0 = method.getDeclaringClass();
    assert Metadata.class.isAssignableFrom(metadataClass0);
    final Map<Class<RelNode>, Function<RelNode, Metadata>> treeMap =
        Maps.<Class<RelNode>, Class<RelNode>, Function<RelNode, Metadata>>
            newTreeMap(SUPERCLASS_COMPARATOR);
    for (final Method method1 : target.getClass().getMethods()) {
      if (method1.getName().equals(method.getName())
          && (method1.getModifiers() & Modifier.STATIC) == 0
          && (method1.getModifiers() & Modifier.PUBLIC) != 0) {
        final Class<?>[] parameterTypes1 = method1.getParameterTypes();
        final Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes1.length == parameterTypes.length + 1
            && RelNode.class.isAssignableFrom(parameterTypes1[0])
            && Util.skip(Arrays.asList(parameterTypes1))
                .equals(Arrays.asList(parameterTypes))) {
          //noinspection unchecked
          final Class<RelNode> key = (Class) parameterTypes1[0];
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
                          // that wraps "filter", a FilterRel, Then we implement
                          //   Selectivity.selectivity(rex)
                          // by calling method
                          //   new SelectivityImpl().selectivity(filter, rex)
                          if (method.equals(
                              BuiltinMethod.METADATA_REL.method)) {
                            return rel;
                          }
                          if (method.equals(
                              BuiltinMethod.OBJECT_TO_STRING.method)) {
                            return metadataClass0.getSimpleName() + "(" + rel
                                + ")";
                          }
                          final Object[] args1;
                          if (args == null) {
                            args1 = new Object[]{rel};
                          } else {
                            args1 = new Object[args.length + 1];
                            args1[0] = rel;
                            System.arraycopy(args, 0, args1, 1, args.length);
                          }
                          return method1.invoke(target, args1);
                        }
                      });
                }
              };
          treeMap.put(key, function);
        }
      }
    }
    // Due to the comparator, the TreeMap is sorted such that any derived class
    // will occur before its base class. The immutable map is not a sorted map,
    // but it retains the traversal order, and that is sufficient.
    final ImmutableMap<Class<RelNode>, Function<RelNode, Metadata>> map =
        ImmutableMap.copyOf(treeMap);
    return new ReflectiveRelMetadataProvider(map, metadataClass0);
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
