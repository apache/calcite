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
package org.apache.calcite.piglet;

import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Utility class to find the implementation method object for a given Pig UDF
 * class.
 */
class PigUdfFinder {
  /**
   * For Pig UDF classes where the "exec" method is declared in parent class,
   * the Calcite enumerable engine will generate incorrect Java code that
   * instantiates an object of the parent class, not object of the actual UDF
   * class. If the parent class is an abstract class, the auto-generated code
   * failed to compile (we can not instantiate an object of an abstract class).
   *
   * <p>Workaround is to write a wrapper for such UDFs to instantiate the
   * correct UDF object. See method {@link PigUdfs#bigdecimalsum} as an example
   * and add others if needed.
   */
  private final ImmutableMap<String, Method> udfWrapper;

  PigUdfFinder() {
    final Map<String, Method> map = new HashMap<>();
    for (Method method : PigUdfs.class.getMethods()) {
      if (Modifier.isPublic(method.getModifiers())
          && method.getReturnType() != Method.class) {
        map.put(method.getName(), method);
      }
    }
    udfWrapper = ImmutableMap.copyOf(map);
  }

  /**
   * Finds the implementation method object for a given Pig UDF class.
   *
   * @param clazz The Pig UDF class
   *
   * @throws IllegalArgumentException if not found
   */
  @Nonnull Method findPigUdfImplementationMethod(Class clazz) {
    // Find implementation method in the wrapper map
    Method returnedMethod =
        udfWrapper.get(clazz.getSimpleName().toLowerCase(Locale.US));
    if (returnedMethod != null) {
      return returnedMethod;
    }

    // Find exec method in the declaring class
    returnedMethod = findExecMethod(clazz.getDeclaredMethods());
    if (returnedMethod != null) {
      return returnedMethod;
    }

    // Find exec method in all parent classes.
    returnedMethod = findExecMethod(clazz.getMethods());
    if (returnedMethod != null) {
      return returnedMethod;
    }

    throw new IllegalArgumentException(
        "Could not find 'exec' method for PigUDF class of " + clazz.getName());
  }

  /**
   * Finds "exec" method from a given array of methods.
   */
  private static Method findExecMethod(Method[] methods) {
    if (methods == null) {
      return null;
    }

    Method returnedMethod = null;
    for (Method method : methods) {
      if (method.getName().equals("exec")) {
        // There may be two methods named "exec", one of them just returns a
        // Java object. We will need to look for the other one if existing.
        if (method.getReturnType() != Object.class) {
          return method;
        } else {
          returnedMethod = method;
        }
      }
    }
    return returnedMethod;
  }
}

// End PigUdfFinder.java
