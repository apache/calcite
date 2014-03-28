/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl;

import net.hydromatic.optiq.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.*;
import java.util.AbstractList;
import java.util.List;

/**
* Implementation of {@link net.hydromatic.optiq.ScalarFunction}.
*/
public class ScalarFunctionImpl implements ScalarFunction {
  public final Method method;
  private final List<String> path;

  /** Private constructor. */
  private ScalarFunctionImpl(List<String> path, Method method) {
    this.path = path == null ? null : ImmutableList.copyOf(path);
    this.method = method;
  }

  /** Creates and validates a ScalarFunctionImpl. */
  public static ScalarFunctionImpl create(List<String> path, String className) {
    final Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("UDF class '"
          + className + "' not found");
    }
    final Method method = findEvalMethod(clazz);
    if (method == null) {
      throw new RuntimeException("method not found");
    }
    if ((method.getModifiers() & Modifier.STATIC) == 0) {
      if (!classHasPublicZeroArgsConstructor(clazz)) {
        throw new RuntimeException("declaring class '" + clazz.getName()
            + "' of non-static UDF must have a public constructor with zero "
            + "parameters");
      }
    }
    return new ScalarFunctionImpl(path, method);
  }

  private static boolean classHasPublicZeroArgsConstructor(Class<?> clazz) {
    for (Constructor<?> constructor : clazz.getConstructors()) {
      if (constructor.getParameterTypes().length == 0
          && (constructor.getModifiers() & Modifier.PUBLIC) != 0) {
        return true;
      }
    }
    return false;
  }

  private static Method findEvalMethod(Class<?> clazz) {
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals("eval")) {
        return method;
      }
    }
    return null;
  }

  public List<FunctionParameter> getParameters() {
    final Class<?>[] parameterTypes = method.getParameterTypes();
    return new AbstractList<FunctionParameter>() {
      public FunctionParameter get(final int index) {
        return new FunctionParameter() {
          public int getOrdinal() {
            return index;
          }

          public String getName() {
            return "arg" + index;
          }

          public RelDataType getType(RelDataTypeFactory typeFactory) {
            return typeFactory.createJavaType(parameterTypes[index]);
          }
        };
      }

      public int size() {
        return parameterTypes.length;
      }
    };
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return typeFactory.createJavaType(method.getReturnType());
  }
}

// End ScalarFunctionImpl.java
