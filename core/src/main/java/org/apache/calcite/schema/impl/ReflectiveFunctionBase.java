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
package org.apache.calcite.schema.impl;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionContext;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.util.ReflectUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
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
   * Creates a ReflectiveFunctionBase.
   *
   * @param method Method that is used to get type information from
   */
  protected ReflectiveFunctionBase(Method method) {
    this.method = method;
    this.parameters = builder().addMethodParameters(method).build();
  }

  /**
   * Returns the parameters of this function.
   *
   * @return Parameters; never null
   */
  @Override public List<FunctionParameter> getParameters() {
    return parameters;
  }

  /**
   * Returns whether a class has a public constructor with zero arguments.
   *
   * @param clazz Class to verify
   * @return whether class has a public constructor with zero arguments
   */
  static boolean classHasPublicZeroArgsConstructor(Class<?> clazz) {
    for (Constructor<?> constructor : clazz.getConstructors()) {
      if (constructor.getParameterCount() == 0
          && Modifier.isPublic(constructor.getModifiers())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns whether a class has a public constructor with one argument
   * of type {@link FunctionContext}.
   *
   * @param clazz Class to verify
   * @return whether class has a public constructor with one FunctionContext
   * argument
   */
  static boolean classHasPublicFunctionContextConstructor(Class<?> clazz) {
    for (Constructor<?> constructor : clazz.getConstructors()) {
      if (constructor.getParameterCount() == 1
          && constructor.getParameterTypes()[0] == FunctionContext.class
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
  static @Nullable Method findMethod(Class<?> clazz, String name) {
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals(name) && !method.isBridge()) {
        return method;
      }
    }
    return null;
  }

  /** Creates a ParameterListBuilder. */
  public static ParameterListBuilder builder() {
    return new ParameterListBuilder();
  }

  /** Helps build lists of
   * {@link org.apache.calcite.schema.FunctionParameter}. */
  public static class ParameterListBuilder {
    final List<FunctionParameter> builder = new ArrayList<>();

    public ImmutableList<FunctionParameter> build() {
      return ImmutableList.copyOf(builder);
    }

    public ParameterListBuilder add(final Class<?> type, final String name) {
      return add(type, name, false);
    }

    public ParameterListBuilder add(final Class<?> type, final String name,
        final boolean optional) {
      final int ordinal = builder.size();
      builder.add(
          new FunctionParameter() {
            @Override public String toString() {
              return ordinal + ": " + name + " " + type.getSimpleName()
                  + (optional ? "?" : "");
            }

            @Override public int getOrdinal() {
              return ordinal;
            }

            @Override public String getName() {
              return name;
            }

            @Override public RelDataType getType(RelDataTypeFactory typeFactory) {
              return typeFactory.createJavaType(type);
            }

            @Override public boolean isOptional() {
              return optional;
            }
          });
      return this;
    }

    public ParameterListBuilder addMethodParameters(Method method) {
      final Class<?>[] types = method.getParameterTypes();
      for (int i = 0; i < types.length; i++) {
        add(types[i], ReflectUtil.getParameterName(method, i),
            ReflectUtil.isParameterOptional(method, i));
      }
      return this;
    }
  }
}
