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
package org.apache.calcite.util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

/** Compatibility layer.
 *
 * <p>Allows to use advanced functionality if the latest JDK or Guava version
 * is present.</p>
 */
public interface Compatible {
  Compatible INSTANCE = new Factory().create();

  /** Same as Guava {@code Maps.asMap(set, function)} (introduced in
   * Guava 14.0). */
  <K, V> Map<K, V> asMap(Set<K> set, Function<? super K, V> function);

  /** Converts a {@link com.google.common.collect.ImmutableSortedSet} to a
   * {@link java.util.NavigableSet}.  (In Guava 12 and later, ImmutableSortedSet
   * implements NavigableSet.) */
  <E> NavigableSet<E> navigableSet(ImmutableSortedSet<E> set);

  /** Converts a {@link com.google.common.collect.ImmutableSortedMap} to a
   * {@link java.util.NavigableMap}. (In Guava 12 and later, ImmutableSortedMap
   * implements NavigableMap.) */
  <K, V> NavigableMap<K, V> navigableMap(ImmutableSortedMap<K, V> map);

  /** Converts a {@link Map} to a {@link java.util.NavigableMap} that is
   * immutable. */
  <K, V> NavigableMap<K, V> immutableNavigableMap(NavigableMap<K, V> map);

  /** Calls {@link java.sql.Connection}{@code .setSchema(String)}.
   *
   * <p>This method is available in JDK 1.7 and above, and in
   * {@link org.apache.calcite.jdbc.CalciteConnection} in all JDK versions. */
  @SuppressWarnings("unused") @Deprecated // to be removed before 2.0
  void setSchema(Connection connection, String schema);

  /** Calls the {@link Method}.{@code getParameters()[i].getName()} method
   * (introduced in JDK 1.8). */
  String getParameterName(Method method, int i);

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
                  //noinspection ConfusingArgumentToVarargsMethod
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
              if (method.getName().equals("setSchema")) {
                Connection connection = (Connection) args[0];
                String schema = (String) args[1];
                final Method method1 =
                    connection.getClass().getMethod("setSchema", String.class);
                return method1.invoke(connection, schema);
              }
              if (method.getName().equals("getParameterName")) {
                final Method m = (Method) args[0];
                final int i = (Integer) args[1];
                try {
                  final Method method1 =
                      m.getClass().getMethod("getParameters");
                  Object parameters = method1.invoke(m);
                  final Object parameter = Array.get(parameters, i);
                  final Method method3 = parameter.getClass().getMethod("getName");
                  return method3.invoke(parameter);
                } catch (NoSuchMethodException e) {
                  return "arg" + i;
                }
              }
              return null;
            }
          });
    }
  }
}

// End Compatible.java
