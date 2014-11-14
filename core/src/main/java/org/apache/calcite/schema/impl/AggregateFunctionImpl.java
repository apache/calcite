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

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.rules.java.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import static org.eigenbase.util.Static.*;

/**
 * Implementation of {@link AggregateFunction} via user-defined class.
 * The class should implement {@code A init()}, {@code A add(A, V)}, and
 * {@code R result(A)} methods.
 * All the methods should be either static or instance.
 * Bonus point: when using non-static implementation, the aggregate object is
 * reused through the calculation, thus it can have aggregation-related state.
 */
public class AggregateFunctionImpl implements AggregateFunction,
    ImplementableAggFunction {
  public final boolean isStatic;
  public final Method initMethod;
  public final Method addMethod;
  public final Method mergeMethod;
  public final Method resultMethod; // may be null
  public final ImmutableList<Class<?>> valueTypes;
  private final List<FunctionParameter> parameters;
  public final Class<?> accumulatorType;
  public final Class<?> resultType;
  public final Class<?> declaringClass;

  /** Private constructor; use {@link #create}. */
  private AggregateFunctionImpl(Class<?> declaringClass,
      List<Class<?>> valueTypes,
      Class<?> accumulatorType,
      Class<?> resultType,
      Method initMethod,
      Method addMethod,
      Method mergeMethod,
      Method resultMethod) {
    this.declaringClass = declaringClass;
    this.isStatic = Modifier.isStatic(initMethod.getModifiers());
    this.valueTypes = ImmutableList.copyOf(valueTypes);
    this.parameters = ReflectiveFunctionBase.toFunctionParameters(valueTypes);
    this.accumulatorType = accumulatorType;
    this.resultType = resultType;
    this.initMethod = initMethod;
    this.addMethod = addMethod;
    this.mergeMethod = mergeMethod;
    this.resultMethod = resultMethod;

    assert initMethod != null;
    assert addMethod != null;
    assert resultMethod != null || accumulatorType == resultType;
  }

  /** Creates an aggregate function, or returns null. */
  public static AggregateFunction create(Class<?> clazz) {
    final Method initMethod = ReflectiveFunctionBase.findMethod(clazz, "init");
    final Method addMethod = ReflectiveFunctionBase.findMethod(clazz, "add");
    final Method mergeMethod = null; // TODO:
    final Method resultMethod = ReflectiveFunctionBase.findMethod(
        clazz, "result");
    if (initMethod != null && addMethod != null) {
      // A is return type of init by definition
      final Class<?> accumulatorType = initMethod.getReturnType();

      // R is return type of result by definition
      final Class<?> resultType =
          resultMethod != null ? resultMethod.getReturnType() : accumulatorType;

      // V is remaining args of add by definition
      final List<Class<?>> addParamTypes =
          ImmutableList.copyOf(addMethod.getParameterTypes());
      if (addParamTypes.isEmpty() || addParamTypes.get(0) != accumulatorType) {
        throw RESOURCE.firstParameterOfAdd(clazz.getName()).ex();
      }
      final List<Class<?>> valueTypes = Util.skip(addParamTypes, 1);

      // A init()
      // A add(A, V)
      // A merge(A, A)
      // R result(A)

      // TODO: check add returns A
      // TODO: check merge returns A
      // TODO: check merge args are (A, A)
      // TODO: check result args are (A)

      return new AggregateFunctionImpl(clazz, valueTypes, accumulatorType,
          resultType, initMethod, addMethod, mergeMethod, resultMethod);
    }
    return null;
  }

  public List<FunctionParameter> getParameters() {
    return parameters;
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return typeFactory.createJavaType(resultType);
  }

  public AggImplementor getImplementor(boolean windowContext) {
    return new RexImpTable.UserDefinedAggReflectiveImplementor(this);
  }
}

// End AggregateFunctionImpl.java
