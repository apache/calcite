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
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

import static org.eigenbase.util.Static.*;

/**
* Implementation of {@link ScalarFunction}.
*/
public class AggregateFunctionImpl implements AggregateFunction {
  public final Method initMethod;
  public final Method initAddMethod; // may be null
  public final Method addMethod;
  public final Method mergeMethod;
  public final Method resultMethod; // may be null
  public final ImmutableList<Class<?>> valueTypes;
  public final Class<?> accumulatorType;
  public final Class<?> resultType;
  public final Class<?> declaringClass;

  /** Private constructor; use {@link #create}. */
  private AggregateFunctionImpl(List<Class<?>> valueTypes,
      Class<?> accumulatorType,
      Class<?> resultType,
      Method initMethod,
      Method initAddMethod,
      Method addMethod,
      Method mergeMethod,
      Method resultMethod) {
    this.valueTypes = ImmutableList.copyOf(valueTypes);
    this.accumulatorType = accumulatorType;
    this.resultType = resultType;
    this.initMethod = initMethod;
    this.initAddMethod = initAddMethod;
    this.addMethod = addMethod;
    this.mergeMethod = mergeMethod;
    this.resultMethod = resultMethod;
    this.declaringClass = initMethod.getDeclaringClass();

    assert initMethod != null;
    assert addMethod != null;
    assert resultMethod != null || accumulatorType == resultType;
    assert addMethod.getDeclaringClass() == declaringClass;
    assert initAddMethod == null
        || initAddMethod.getDeclaringClass() == declaringClass;
    assert mergeMethod == null
        || mergeMethod.getDeclaringClass() == declaringClass;
    assert resultMethod == null
        || resultMethod.getDeclaringClass() == declaringClass;
  }

  /** Creates an aggregate function, or returns null. */
  public static AggregateFunction create(Class<?> clazz) {
    final Method initMethod = ScalarFunctionImpl.findMethod(clazz, "init");
    final Method initAddMethod =
        ScalarFunctionImpl.findMethod(clazz, "initAdd");
    final Method addMethod = ScalarFunctionImpl.findMethod(clazz, "add");
    final Method mergeMethod = null; // TODO:
    final Method resultMethod = ScalarFunctionImpl.findMethod(clazz, "result");
    if (initMethod != null && addMethod != null) {
      // A is return type of init by definition
      final Class<?> accumulatorType = initMethod.getReturnType();

      // R is return type of result by definition
      final Class<?> resultType =
          resultMethod != null ? resultMethod.getReturnType() : accumulatorType;

      // V is remaining args of add by definition
      final List<Class<?>> addParamTypes =
          Arrays.asList(addMethod.getParameterTypes());
      if (addParamTypes.isEmpty() || addParamTypes.get(0) != accumulatorType) {
        throw RESOURCE.firstParameterOfAdd(clazz.getName()).ex();
      }
      final List<Class<?>> valueTypes = Util.skip(addParamTypes, 1);

      // A init()
      // A initAdd(V)
      // A add(A, V)
      // A merge(A, A)
      // R result(A)

      // TODO: check initAdd return is A
      // TODO: check initAdd args are (V)
      // TODO: check add returns A
      // TODO: check merge returns A
      // TODO: check merge args are (A, A)
      // TODO: check result args are (A)

      if (initAddMethod != null) {
        final List<Class<?>> initAddParams =
            Arrays.asList(initAddMethod.getParameterTypes());
        if (!initAddParams.equals(valueTypes)) {
          throw RESOURCE.initAddWrongParamTypes(clazz.getName()).ex();
        }
      }
      return new AggregateFunctionImpl(valueTypes, accumulatorType, resultType,
          initMethod, initAddMethod, addMethod, mergeMethod, resultMethod);
    }
    return null;
  }

  public List<FunctionParameter> getParameters() {
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
            return typeFactory.createJavaType(valueTypes.get(index));
          }
        };
      }

      public int size() {
        return valueTypes.size();
      }
    };
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return typeFactory.createJavaType(resultType);
  }
}

// End AggregateFunctionImpl.java
