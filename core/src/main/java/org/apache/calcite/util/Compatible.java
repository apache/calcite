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

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/** Compatibility layer.
 *
 * <p>Allows to use advanced functionality if the latest JDK or Guava version
 * is present.
 */
public interface Compatible {
  Compatible INSTANCE = new Factory().create();

  /** Same as {@code MethodHandles#privateLookupIn()}.
   * (On JDK 8, only {@link MethodHandles#lookup()} is available. */
  <T> MethodHandles.Lookup lookupPrivate(Class<T> clazz);

  /** Creates the implementation of Compatible suitable for the
   * current environment. */
  class Factory {
    Compatible create() {
      return (Compatible) Proxy.newProxyInstance(
          Compatible.class.getClassLoader(),
          new Class<?>[] {Compatible.class}, (proxy, method, args) -> {
            if (method.getName().equals("lookupPrivate")) {
              // Use MethodHandles.privateLookupIn if it is available (JDK 9
              // and above)
              @SuppressWarnings("rawtypes")
              final Class<?> clazz = (Class) args[0];
              try {
                final Method privateLookupMethod =
                    MethodHandles.class.getMethod("privateLookupIn",
                        Class.class, MethodHandles.Lookup.class);
                final MethodHandles.Lookup lookup = MethodHandles.lookup();
                return privateLookupMethod.invoke(null, clazz, lookup);
              } catch (NoSuchMethodException e) {
                return privateLookupJdk8(clazz);
              }
            }
            return null;
          });
    }

    /** Emulates MethodHandles.privateLookupIn on JDK 8;
     * in later JDK versions, throws. */
    @SuppressWarnings("deprecation")
    static <T> MethodHandles.Lookup privateLookupJdk8(Class<T> clazz) {
      try {
        final Constructor<MethodHandles.Lookup> constructor =
            MethodHandles.Lookup.class.getDeclaredConstructor(Class.class,
                int.class);
        if (!constructor.isAccessible()) {
          constructor.setAccessible(true);
        }
        return constructor.newInstance(clazz, MethodHandles.Lookup.PRIVATE);
      } catch (InstantiationException | IllegalAccessException
          | InvocationTargetException | NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
