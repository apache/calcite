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

import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.ReflectiveCallNotNullImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.linq4j.function.SemiStrict;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlOperatorBinding;

import com.google.common.collect.ImmutableMultimap;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.apache.calcite.util.Static.RESOURCE;

/**
* Implementation of {@link org.apache.calcite.schema.ScalarFunction}.
*/
public class ScalarFunctionImpl extends ReflectiveFunctionBase
    implements ScalarFunction, ImplementableFunction {
  private final CallImplementor implementor;

  /** Private constructor. */
  private ScalarFunctionImpl(Method method, CallImplementor implementor) {
    super(method);
    this.implementor = implementor;
  }

  /**
   * Creates {@link org.apache.calcite.schema.ScalarFunction} for each method in
   * a given class.
   */
  public static ImmutableMultimap<String, ScalarFunction> createAll(
      Class<?> clazz) {
    final ImmutableMultimap.Builder<String, ScalarFunction> builder =
        ImmutableMultimap.builder();
    for (Method method : clazz.getMethods()) {
      if (method.getDeclaringClass() == Object.class) {
        continue;
      }
      if (!Modifier.isStatic(method.getModifiers())
          && !classHasPublicZeroArgsConstructor(clazz)) {
        continue;
      }
      final ScalarFunction function = create(method);
      builder.put(method.getName(), function);
    }
    return builder.build();
  }

  /**
   * Creates {@link org.apache.calcite.schema.ScalarFunction} from given class.
   *
   * <p>If a method of the given name is not found or it does not suit,
   * returns {@code null}.
   *
   * @param clazz class that is used to implement the function
   * @param methodName Method name (typically "eval")
   * @return created {@link ScalarFunction} or null
   */
  public static ScalarFunction create(Class<?> clazz, String methodName) {
    final Method method = findMethod(clazz, methodName);
    if (method == null) {
      return null;
    }
    return create(method);
  }

  /**
   * Creates {@link org.apache.calcite.schema.ScalarFunction} from given method.
   * When {@code eval} method does not suit, {@code null} is returned.
   *
   * @param method method that is used to implement the function
   * @return created {@link ScalarFunction} or null
   */
  public static ScalarFunction create(Method method) {
    if (!Modifier.isStatic(method.getModifiers())) {
      Class clazz = method.getDeclaringClass();
      if (!classHasPublicZeroArgsConstructor(clazz)) {
        throw RESOURCE.requireDefaultConstructor(clazz.getName()).ex();
      }
    }
    CallImplementor implementor = createImplementor(method);
    return new ScalarFunctionImpl(method, implementor);
  }


  /**
   * Creates unsafe version of {@link ScalarFunction} from any method. The method
   * does not need to be static or belong to a class with default constructor. It is
   * the responsibility of the underlying engine to initialize the UDF object that
   * contain the method.
   *
   * @param method method that is used to implement the function
   */
  public static ScalarFunction createUnsafe(Method method) {
    CallImplementor implementor = createImplementor(method);
    return new ScalarFunctionImpl(method, implementor);
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return typeFactory.createJavaType(method.getReturnType());
  }

  public CallImplementor getImplementor() {
    return implementor;
  }

  private static CallImplementor createImplementor(final Method method) {
    final NullPolicy nullPolicy = getNullPolicy(method);
    return RexImpTable.createImplementor(
        new ReflectiveCallNotNullImplementor(method), nullPolicy, false);
  }

  private static NullPolicy getNullPolicy(Method m) {
    if (m.getAnnotation(Strict.class) != null) {
      return NullPolicy.STRICT;
    } else if (m.getAnnotation(SemiStrict.class) != null) {
      return NullPolicy.SEMI_STRICT;
    } else if (m.getDeclaringClass().getAnnotation(Strict.class) != null) {
      return NullPolicy.STRICT;
    } else if (m.getDeclaringClass().getAnnotation(SemiStrict.class) != null) {
      return NullPolicy.SEMI_STRICT;
    } else {
      return NullPolicy.NONE;
    }
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory,
      SqlOperatorBinding opBinding) {
    // Strict and semi-strict functions can return null even if their Java
    // functions return a primitive type. Because when one of their arguments
    // is null, they won't even be called.
    final RelDataType returnType = getReturnType(typeFactory);
    switch (getNullPolicy(method)) {
    case STRICT:
      for (RelDataType type : opBinding.collectOperandTypes()) {
        if (type.isNullable()) {
          return typeFactory.createTypeWithNullability(returnType, true);
        }
      }
      break;
    case SEMI_STRICT:
      return typeFactory.createTypeWithNullability(returnType, true);
    }
    return returnType;
  }
}

// End ScalarFunctionImpl.java
