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
package net.hydromatic.optiq.util;

import com.google.common.base.Function;
import com.google.common.collect.*;

import java.lang.reflect.*;
import java.util.*;

/** Compatibility layer.
 *
 * <p>Allows to use advanced functionality if the latest JDK or Guava version
 * is present.</p>
 */
public interface Compatible {
  Compatible INSTANCE = new Factory().create();

  /** Same as Guava {@code Maps.asMap(set, function)}. */
  <K, V> Map<K, V> asMap(Set<K> set, Function<? super K, V> function);

  /** Converts a {@link com.google.common.collect.ImmutableSortedSet} to a
   * {@link java.util.NavigableSet}. */
  <E> NavigableSet<E> navigableSet(ImmutableSortedSet<E> set);

  /** Converts a {@link com.google.common.collect.ImmutableSortedMap} to a
   * {@link java.util.NavigableMap}. */
  <K, V> NavigableMap<K, V> navigableMap(ImmutableSortedMap<K, V> map);

  /** Converts a {@link Map} to a {@link java.util.NavigableMap} that is
   * immutable. */
  <K, V> NavigableMap<K, V> immutableNavigableMap(NavigableMap<K, V> map);

  /** Creates the implementation of Compatible suitable for the
   * current environment. */
  class Factory {
    Compatible create() {
      return (Compatible) Proxy.newProxyInstance(
          Compatible.class.getClassLoader(),
          new Class<?>[] {Compatible.class},
          new InvocationHandler() {
            public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
              if (method.getName().equals("asMap")) {
                // Use the Guava implementation Maps.asMap if it is available
                try {
                  final Method guavaMethod = Maps.class.getMethod(
                      method.getName(), method.getParameterTypes());
                  return guavaMethod.invoke(null, args);
                } catch (NoSuchMethodException e) {
                  Set set = (Set) args[0];
                  Function function = (Function) args[1];
                  return CompatibleGuava11.asMap(set, function);
                }
              }
              if (method.getName().equals("navigableSet")) {
                ImmutableSortedSet set = (ImmutableSortedSet) args[0];
                return CompatibleGuava11.navigableSet(set);
              }
              if (method.getName().equals("navigableMap")) {
                ImmutableSortedMap map = (ImmutableSortedMap) args[0];
                return CompatibleGuava11.navigableMap(map);
              }
              if (method.getName().equals("immutableNavigableMap")) {
                Map map = (Map) args[0];
                ImmutableSortedMap sortedMap = ImmutableSortedMap.copyOf(map);
                return CompatibleGuava11.navigableMap(sortedMap);
              }
              return null;
            }
          });
    }
  }
}

// End Compatible.java
