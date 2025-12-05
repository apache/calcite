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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Proxy;
import java.util.Locale;

import static java.util.Objects.requireNonNull;

/**
 * Compatibility layer.
 *
 * <p>Allows to use advanced functionality if the latest JDK version is present.
 */
public interface Compatible {
  Compatible INSTANCE = new Compatible.Factory().create();

  /** Tells whether the given class is a JDK 16+ record. */
  <T> boolean isRecord(Class<T> clazz);

  /** Creates an implementation of {@link Compatible} suitable for the current environment. */
  class Factory {
    private static final @Nullable MethodHandle IS_RECORD =
        tryGetIsRecordMethod(MethodHandles.lookup());

    Compatible create() {
      return (Compatible) Proxy.newProxyInstance(
          Compatible.class.getClassLoader(),
          new Class<?>[]{Compatible.class},
          (proxy, method, args) -> {
            if ("isRecord".equals(method.getName())) {
              return isRecord(requireNonNull(args[0], "args[0]"));
            }
            return null;
          });
    }

    private static boolean isRecord(Object clazz) {
      if (IS_RECORD == null) {
        return false;
      }

      try {
        return (boolean) IS_RECORD.invoke(clazz);
      } catch (Throwable e) {
        throw new RuntimeException(
            String.format(Locale.ROOT, "Failed to invoke %s on %s", IS_RECORD, clazz), e);
      }
    }

    private static @Nullable MethodHandle tryGetIsRecordMethod(MethodHandles.Lookup lookup) {
      try {
        MethodType methodType = MethodType.methodType(boolean.class);
        return lookup.findVirtual(Class.class, "isRecord", methodType);
      } catch (NoSuchMethodException | IllegalAccessException e) {
        return null;
      }
    }
  }
}
