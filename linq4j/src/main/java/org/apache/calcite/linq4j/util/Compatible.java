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

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

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
    Compatible create() {
      return (Compatible) Proxy.newProxyInstance(
          Compatible.class.getClassLoader(),
          new Class<?>[]{Compatible.class}, (proxy, method, args) -> {

            if (method.getName().equals("isRecord")) {
              // Use MethodHandles.privateLookupIn if it is available (JDK 16
              // and above)
              @SuppressWarnings("rawtypes")
              final Class<?> clazz = (Class) requireNonNull(args[0], "args[0]");
              try {
                final Method isRecordMethod = Class.class.getMethod("isRecord");
                return isRecordMethod.invoke(clazz);
              } catch (NoSuchMethodException e) {
                return isRecordJdk8(clazz);
              }
            }
            return null;
          });
    }


    static boolean isRecordJdk8(Class<?> clazz) {
      return false;
    }
  }
}
