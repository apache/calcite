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
package net.hydromatic.optiq.impl;

import net.hydromatic.optiq.Function;
import net.hydromatic.optiq.FunctionParameter;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation of a function that is based on a method.
 * This class mainly solves conversion of method parameter types to {@code
 * List<FunctionParameter>} form.
 */
public abstract class ReflectiveFunctionBase implements Function {
  /** Method that implements the function. */
  public final Method method;
  /** Types of parameter for the function call. */
  public final List<FunctionParameter> parameters;

  /**
   * {@code ReflectiveFunctionBase} constructor
   * @param method method that is used to get type information from
   */
  public ReflectiveFunctionBase(Method method) {
    this.method = method;
    this.parameters = toFunctionParameters(method.getParameterTypes());
  }

  /**
   * Returns the parameters of this function.
   *
   * @return Parameters; never null
   */
  public List<FunctionParameter> getParameters() {
    return parameters;
  }


  public static ImmutableList<FunctionParameter> toFunctionParameters(
      Class... types) {
    return toFunctionParameters(Arrays.asList(types));
  }

  public static ImmutableList<FunctionParameter> toFunctionParameters(
      Iterable<? extends Class> types) {
    final ImmutableList.Builder<FunctionParameter> res =
        ImmutableList.builder();
    int i = 0;
    for (final Class type : types) {
      final int ordinal = i;
      res.add(new FunctionParameter() {
        public int getOrdinal() {
          return ordinal;
        }

        public String getName() {
          return "arg"  + ordinal;
        }

        public RelDataType getType(RelDataTypeFactory typeFactory) {
          return typeFactory.createJavaType(type);
        }
      });
      i++;
    }
    return res.build();
  }

  /**
   * Verifies if given class has public constructor with zero arguments.
   * @param clazz class to verify
   * @return true if given class has public constructor with zero arguments
   */
  static boolean classHasPublicZeroArgsConstructor(Class<?> clazz) {
    for (Constructor<?> constructor : clazz.getConstructors()) {
      if (constructor.getParameterTypes().length == 0
          && Modifier.isPublic(constructor.getModifiers())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Finds a method in a given class by name.
   * @param clazz class to search method in
   * @param name name of the method to find
   * @return the first method with matching name or null when no method found
   */
  static Method findMethod(Class<?> clazz, String name) {
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals(name)) {
        return method;
      }
    }
    return null;
  }
}

// End ReflectiveFunctionBase.java

