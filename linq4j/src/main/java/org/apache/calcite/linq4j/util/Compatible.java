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
package org.apache.calcite.linq4j.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Proxy;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/** Compatibility layer.
 *
 * <p>Allows to use advanced functionality if the latest JDK or Guava version
 * is present.
 */
public interface Compatible {
  Compatible INSTANCE = new Compatible.Factory().create();

  <T> boolean isRecord(Class<T> clazz);

  /**
   * Creates the implementation of Compatible suitable for the
   * current environment.
   */
  class Factory {
    final IsRecordCache IS_RECORD_METHOD_CACHE = initCache(MethodHandles.lookup());
    Compatible create() {
      return (Compatible) Proxy.newProxyInstance(
          Compatible.class.getClassLoader(),
          new Class<?>[]{Compatible.class}, (proxy, method, args) -> {

            if (method.getName().equals("isRecord")) {
              // Use Class.isRecord if it is available (JDK 16 and above)
              @SuppressWarnings("rawtypes")
              final Class<?> clazz = (Class) requireNonNull(args[0], "args[0]");

              return IS_RECORD_METHOD_CACHE.getIsRecordMethod()
                  .map(isRecordMethod -> {
                    try {
                      return isRecordMethod.invoke(clazz);
                    } catch (Throwable e) {
                      throw new RuntimeException(e);
                    }
                  })
                  .orElse(false);

            }
            return null;
          });
    }

    class IsRecordCache {

      /** A cache of {@code isRecord} method. If empty, the JDK doesn't support records. */
      private final Optional<MethodHandle> isRecordMethod;

      IsRecordCache(Optional<MethodHandle> isRecordMethod) {
        this.isRecordMethod = isRecordMethod;
      }

      public Optional<MethodHandle> getIsRecordMethod() {
        return isRecordMethod;
      }
    }

    IsRecordCache initCache(MethodHandles.Lookup lookup) {
      try {
        MethodType methodType = MethodType.methodType(boolean.class);
        MethodHandle isRecordMethod = lookup.findVirtual(Class.class, "isRecord", methodType);
        return new IsRecordCache(Optional.of(isRecordMethod));
      } catch (NoSuchMethodException | IllegalAccessException e) {
        return new IsRecordCache(Optional.empty());
      }
    }
  }
}
