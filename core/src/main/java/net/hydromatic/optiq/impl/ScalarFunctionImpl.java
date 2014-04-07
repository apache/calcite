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

import java.lang.reflect.*;
import java.util.AbstractList;
import java.util.List;

import static org.eigenbase.util.Static.*;

/**
* Implementation of {@link net.hydromatic.optiq.ScalarFunction}.
*/
public class ScalarFunctionImpl implements ScalarFunction {
  public final Method method;

  /** Private constructor. */
  private ScalarFunctionImpl(Method method) {
    this.method = method;
  }

  /** Creates a scalar function.
   *
   * @see TableMacroImpl#create(Class)
   */
  public static ScalarFunction create(Class<?> clazz) {
    final Method method = findMethod(clazz, "eval");
    if (method != null) {
      if ((method.getModifiers() & Modifier.STATIC) == 0) {
        if (!classHasPublicZeroArgsConstructor(clazz)) {
          throw RESOURCE.requireDefaultConstructor(clazz.getName()).ex();
        }
      }
      // NOTE: scalar functions and table macros look similar. It is important
      // to check for table macros FIRST.
      return new ScalarFunctionImpl(method);
    }
    return null;
  }

  static boolean classHasPublicZeroArgsConstructor(Class<?> clazz) {
    for (Constructor<?> constructor : clazz.getConstructors()) {
      if (constructor.getParameterTypes().length == 0
          && (constructor.getModifiers() & Modifier.PUBLIC) != 0) {
        return true;
      }
    }
    return false;
  }

  static Method findMethod(Class<?> clazz, String name) {
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals(name)) {
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
