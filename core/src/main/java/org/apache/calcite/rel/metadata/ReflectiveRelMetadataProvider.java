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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  //~ Instance fields --------------------------------------------------------
  private final Map<Class<RelNode>, UnboundMetadata> map;
  private final Class<? extends Metadata> metadataClass0;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ReflectiveRelMetadataProvider.
   *
   * @param map Map
   * @param metadataClass0 Metadata class
   */
  protected ReflectiveRelMetadataProvider(
      Map<Class<RelNode>, UnboundMetadata> map,
      Class<? extends Metadata> metadataClass0) {
    assert !map.isEmpty() : "are your methods named wrong?";
    this.map = map;
    this.metadataClass0 = metadataClass0;
  }

  /** Returns an implementation of {@link RelMetadataProvider} that scans for
   * methods with a preceding argument.
   *
   * <p>For example, {@link BuiltInMetadata.Selectivity} has a method
   * {@link BuiltInMetadata.Selectivity#getSelectivity(RexNode)}.
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
    @SuppressWarnings("unchecked")
    final Class<Metadata> metadataClass0 = (Class) method0.getDeclaringClass();
    assert Metadata.class.isAssignableFrom(metadataClass0);
    for (Method method : methods) {
      assert method.getDeclaringClass() == metadataClass0;
    }

    // Find the distinct set of RelNode classes handled by this provider,
    // ordered base-class first.
    final Set<Class<RelNode>> classes = Sets.newHashSet();
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

    final Map<Class<RelNode>, UnboundMetadata> methodsMap = new HashMap<>();
    for (Class<RelNode> key : classes) {
      ImmutableNullableList.Builder<Method> builder =
          ImmutableNullableList.builder();
      for (final Method method : methods) {
        builder.add(find(handlerMap, key, method));
      }
      final List<Method> handlerMethods = builder.build();
      final UnboundMetadata function =
          new UnboundMetadata() {
            public Metadata bind(final RelNode rel,
                final RelMetadataQuery mq) {
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
                      final Method handlerMethod = handlerMethods.get(i);
                      if (handlerMethod == null) {
                        throw new AssertionError("not handled: " + method
                            + " for " + rel);
                      }
                      final Object[] args1;
                      final List key;
                      if (args == null) {
                        args1 = new Object[]{rel, mq};
                        key = FlatLists.of(rel, method);
                      } else {
                        args1 = new Object[args.length + 2];
                        args1[0] = rel;
                        args1[1] = mq;
                        System.arraycopy(args, 0, args1, 2, args.length);

                        final Object[] args2 = args1.clone();
                        args2[1] = method; // replace RelMetadataQuery with method
                        for (int j = 0; j < args2.length; j++) {
                          if (args2[j] == null) {
                            args2[j] = NullSentinel.INSTANCE;
                          } else if (args2[j] instanceof RexNode) {
                            // Can't use RexNode.equals - it is not deep
                            args2[j] = args2[j].toString();
                          }
                        }
                        key = FlatLists.copyOf(args2);
                      }
                      if (!mq.set.add(key)) {
                        throw CyclicMetadataException.INSTANCE;
                      }
                      try {
                        return handlerMethod.invoke(target, args1);
                      } catch (InvocationTargetException
                          | UndeclaredThrowableException e) {
                        Throwables.propagateIfPossible(e.getCause());
                        throw e;
                      } finally {
                        mq.set.remove(key);
                      }
                    }
                  });
            }
          };
      methodsMap.put(key, function);
    }
    return new ReflectiveRelMetadataProvider(methodsMap, metadataClass0);
  }

  /** Finds an implementation of a method for {@code relNodeClass} or its
   * nearest base class. Assumes that base classes have already been added to
   * {@code map}. */
  @SuppressWarnings({ "unchecked", "SuspiciousMethodCalls" })
  private static Method find(Map<Pair<Class<RelNode>, Method>,
      Method> handlerMap, Class<RelNode> relNodeClass, Method method) {
    List<Class<RelNode>> newSources = Lists.newArrayList();
    Method implementingMethod;
    while (relNodeClass != null) {
      implementingMethod = handlerMap.get(Pair.of(relNodeClass, method));
      if (implementingMethod != null) {
        return implementingMethod;
      } else {
        newSources.add(relNodeClass);
      }
      for (Class<?> clazz : relNodeClass.getInterfaces()) {
        if (RelNode.class.isAssignableFrom(clazz)) {
          implementingMethod = handlerMap.get(Pair.of(clazz, method));
          if (implementingMethod != null) {
            return implementingMethod;
          }
        }
      }
      if (RelNode.class.isAssignableFrom(relNodeClass.getSuperclass())) {
        relNodeClass = (Class<RelNode>) relNodeClass.getSuperclass();
      } else {
        relNodeClass = null;
      }
    }
    return null;
  }

  private static boolean couldImplement(Method handlerMethod, Method method) {
    if (!handlerMethod.getName().equals(method.getName())
        || (handlerMethod.getModifiers() & Modifier.STATIC) != 0
        || (handlerMethod.getModifiers() & Modifier.PUBLIC) == 0) {
      return false;
    }
    final Class<?>[] parameterTypes1 = handlerMethod.getParameterTypes();
    final Class<?>[] parameterTypes = method.getParameterTypes();
    return parameterTypes1.length == parameterTypes.length + 2
        && RelNode.class.isAssignableFrom(parameterTypes1[0])
        && RelMetadataQuery.class == parameterTypes1[1]
        && Arrays.asList(parameterTypes)
            .equals(Util.skip(Arrays.asList(parameterTypes1), 2));
  }

  //~ Methods ----------------------------------------------------------------

  public <M extends Metadata> UnboundMetadata<M>
  apply(Class<? extends RelNode> relClass,
      Class<? extends M> metadataClass) {
    if (metadataClass == metadataClass0) {
      return apply(relClass);
    } else {
      return null;
    }
  }

  @SuppressWarnings({ "unchecked", "SuspiciousMethodCalls" })
  public <M extends Metadata> UnboundMetadata<M>
  apply(Class<? extends RelNode> relClass) {
    List<Class<? extends RelNode>> newSources = new ArrayList<>();
    for (;;) {
      UnboundMetadata<M> function = map.get(relClass);
      if (function != null) {
        for (@SuppressWarnings("rawtypes") Class clazz : newSources) {
          map.put(clazz, function);
        }
        return function;
      } else {
        newSources.add(relClass);
      }
      for (Class<?> interfaceClass : relClass.getInterfaces()) {
        if (RelNode.class.isAssignableFrom(interfaceClass)) {
          final UnboundMetadata<M> function2 = map.get(interfaceClass);
          if (function2 != null) {
            for (@SuppressWarnings("rawtypes") Class clazz : newSources) {
              map.put(clazz, function2);
            }
            return function2;
          }
        }
      }
      if (RelNode.class.isAssignableFrom(relClass.getSuperclass())) {
        relClass = (Class<RelNode>) relClass.getSuperclass();
      } else {
        return null;
      }
    }
  }
}

// End ReflectiveRelMetadataProvider.java
