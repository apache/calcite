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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
  private final ConcurrentMap<Class<RelNode>, UnboundMetadata> map;
  private final Class<? extends Metadata> metadataClass0;
  private final ImmutableMultimap<Method, MetadataHandler> handlerMap;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ReflectiveRelMetadataProvider.
   *
   * @param map Map
   * @param metadataClass0 Metadata class
   * @param handlerMap Methods handled and the objects to call them on
   */
  protected ReflectiveRelMetadataProvider(
      ConcurrentMap<Class<RelNode>, UnboundMetadata> map,
      Class<? extends Metadata> metadataClass0,
      Multimap<Method, MetadataHandler> handlerMap) {
    assert !map.isEmpty() : "are your methods named wrong?";
    this.map = map;
    this.metadataClass0 = metadataClass0;
    this.handlerMap = ImmutableMultimap.copyOf(handlerMap);
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
      MetadataHandler target) {
    return reflectiveSource(target, ImmutableList.of(method));
  }

  /** Returns a reflective metadata provider that implements several
   * methods. */
  public static RelMetadataProvider reflectiveSource(MetadataHandler target,
      Method... methods) {
    return reflectiveSource(target, ImmutableList.copyOf(methods));
  }

  private static RelMetadataProvider reflectiveSource(
      final MetadataHandler target, final ImmutableList<Method> methods) {
    final Space2 space = Space2.create(target, methods);

    // This needs to be a concurrent map since RelMetadataProvider are cached in static
    // fields, thus the map is subject to concurrent modifications later.
    // See map.put in org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider.apply(
    // java.lang.Class<? extends org.apache.calcite.rel.RelNode>)
    final ConcurrentMap<Class<RelNode>, UnboundMetadata> methodsMap = new ConcurrentHashMap<>();
    for (Class<RelNode> key : space.classes) {
      ImmutableNullableList.Builder<Method> builder =
          ImmutableNullableList.builder();
      for (final Method method : methods) {
        builder.add(space.find(key, method));
      }
      final List<Method> handlerMethods = builder.build();
      final UnboundMetadata function = (rel, mq) ->
          (Metadata) Proxy.newProxyInstance(
              space.metadataClass0.getClassLoader(),
              new Class[]{space.metadataClass0}, (proxy, method, args) -> {
                // Suppose we are an implementation of Selectivity
                // that wraps "filter", a LogicalFilter. Then we
                // implement
                //   Selectivity.selectivity(rex)
                // by calling method
                //   new SelectivityImpl().selectivity(filter, rex)
                if (method.equals(BuiltInMethod.METADATA_REL.method)) {
                  return rel;
                }
                if (method.equals(BuiltInMethod.OBJECT_TO_STRING.method)) {
                  return space.metadataClass0.getSimpleName() + "(" + rel + ")";
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
                final List key1;
                if (args == null) {
                  args1 = new Object[]{rel, mq};
                  key1 = FlatLists.of(rel, method);
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
                  key1 = FlatLists.copyOf(args2);
                }
                if (mq.map.put(rel, key1, NullSentinel.INSTANCE) != null) {
                  throw CyclicMetadataException.INSTANCE;
                }
                try {
                  return handlerMethod.invoke(target, args1);
                } catch (InvocationTargetException
                    | UndeclaredThrowableException e) {
                  Util.throwIfUnchecked(e.getCause());
                  throw new RuntimeException(e.getCause());
                } finally {
                  mq.map.remove(rel, key1);
                }
              });
      methodsMap.put(key, function);
    }
    return new ReflectiveRelMetadataProvider(methodsMap, space.metadataClass0,
        space.providerMap);
  }

  public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers(
      MetadataDef<M> def) {
    final ImmutableMultimap.Builder<Method, MetadataHandler<M>> builder =
        ImmutableMultimap.builder();
    for (Map.Entry<Method, MetadataHandler> entry : handlerMap.entries()) {
      if (def.methods.contains(entry.getKey())) {
        //noinspection unchecked
        builder.put(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
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

  public <M extends Metadata> UnboundMetadata<M> apply(
      Class<? extends RelNode> relClass, Class<? extends M> metadataClass) {
    if (metadataClass == metadataClass0) {
      return apply(relClass);
    } else {
      return null;
    }
  }

  @SuppressWarnings({ "unchecked", "SuspiciousMethodCalls" })
  public <M extends Metadata> UnboundMetadata<M> apply(
      Class<? extends RelNode> relClass) {
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

  /** Workspace for computing which methods can act as handlers for
   * given metadata methods. */
  static class Space {
    final Set<Class<RelNode>> classes = new HashSet<>();
    final Map<Pair<Class<RelNode>, Method>, Method> handlerMap = new HashMap<>();
    final ImmutableMultimap<Method, MetadataHandler> providerMap;

    Space(Multimap<Method, MetadataHandler> providerMap) {
      this.providerMap = ImmutableMultimap.copyOf(providerMap);

      // Find the distinct set of RelNode classes handled by this provider,
      // ordered base-class first.
      for (Map.Entry<Method, MetadataHandler> entry : providerMap.entries()) {
        final Method method = entry.getKey();
        final MetadataHandler provider = entry.getValue();
        for (final Method handlerMethod : provider.getClass().getMethods()) {
          if (couldImplement(handlerMethod, method)) {
            @SuppressWarnings("unchecked") final Class<RelNode> relNodeClass =
                (Class<RelNode>) handlerMethod.getParameterTypes()[0];
            classes.add(relNodeClass);
            handlerMap.put(Pair.of(relNodeClass, method), handlerMethod);
          }
        }
      }
    }

    /** Finds an implementation of a method for {@code relNodeClass} or its
     * nearest base class. Assumes that base classes have already been added to
     * {@code map}. */
    @SuppressWarnings({ "unchecked", "SuspiciousMethodCalls" })
    Method find(final Class<? extends RelNode> relNodeClass, Method method) {
      Objects.requireNonNull(relNodeClass);
      for (Class r = relNodeClass;;) {
        Method implementingMethod = handlerMap.get(Pair.of(r, method));
        if (implementingMethod != null) {
          return implementingMethod;
        }
        for (Class<?> clazz : r.getInterfaces()) {
          if (RelNode.class.isAssignableFrom(clazz)) {
            implementingMethod = handlerMap.get(Pair.of(clazz, method));
            if (implementingMethod != null) {
              return implementingMethod;
            }
          }
        }
        r = r.getSuperclass();
        if (r == null || !RelNode.class.isAssignableFrom(r)) {
          throw new IllegalArgumentException("No handler for method [" + method
              + "] applied to argument of type [" + relNodeClass
              + "]; we recommend you create a catch-all (RelNode) handler");
        }
      }
    }
  }

  /** Extended work space. */
  static class Space2 extends Space {
    private Class<Metadata> metadataClass0;

    Space2(Class<Metadata> metadataClass0,
        ImmutableMultimap<Method, MetadataHandler> providerMap) {
      super(providerMap);
      this.metadataClass0 = metadataClass0;
    }

    public static Space2 create(MetadataHandler target,
        ImmutableList<Method> methods) {
      assert methods.size() > 0;
      final Method method0 = methods.get(0);
      //noinspection unchecked
      Class<Metadata> metadataClass0 = (Class) method0.getDeclaringClass();
      assert Metadata.class.isAssignableFrom(metadataClass0);
      for (Method method : methods) {
        assert method.getDeclaringClass() == metadataClass0;
      }

      final ImmutableMultimap.Builder<Method, MetadataHandler> providerBuilder =
          ImmutableMultimap.builder();
      for (final Method method : methods) {
        providerBuilder.put(method, target);
      }
      return new Space2(metadataClass0, providerBuilder.build());
    }
  }
}

// End ReflectiveRelMetadataProvider.java
