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

import net.hydromatic.optiq.rules.java.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.lang.reflect.*;

import static org.eigenbase.util.Static.*;

/**
* Implementation of {@link net.hydromatic.optiq.ScalarFunction}.
*/
public class ScalarFunctionImpl extends ReflectiveFunctionBase implements
    ScalarFunction, ImplementableFunction {
  private final CallImplementor implementor;

  /** Private constructor. */
  private ScalarFunctionImpl(Method method, CallImplementor implementor) {
    super(method);
    this.implementor = implementor;
  }

  /**
   * Creates {@link net.hydromatic.optiq.ScalarFunction} from given class.
   * The class should contain {@code eval} method.
   * When {@code eval} method is not found or it does not suit,
   * {@code null} is returned.
   *
   * @param clazz class that is used to implement the function
   * @return created {@link ScalarFunction} or null
   */
  public static ScalarFunction create(Class<?> clazz) {
    final Method method = findMethod(clazz, "eval");
    if (method == null) {
      return null;
    }
    return create(method);
  }

  /**
   * Creates {@link net.hydromatic.optiq.ScalarFunction} from given method.
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

  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return typeFactory.createJavaType(method.getReturnType());
  }

  public CallImplementor getImplementor() {
    return implementor;
  }

  private static CallImplementor createImplementor(final Method method) {
    return RexImpTable.createImplementor(new ReflectiveCallNotNullImplementor(
        method), NullPolicy.ANY, false);
  }
}

// End ScalarFunctionImpl.java
