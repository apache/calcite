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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.ReflectiveCallNotNullImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Implementation of {@link org.apache.calcite.schema.TableFunction} based on a
 * method.
*/
public class TableFunctionImpl extends ReflectiveFunctionBase implements
    TableFunction, ImplementableFunction {
  private final CallImplementor implementor;

  /** Private constructor; use {@link #create}. */
  private TableFunctionImpl(Method method, CallImplementor implementor) {
    super(method);
    this.implementor = implementor;
  }

  /** Creates a {@link TableFunctionImpl} from a class, looking for an "eval"
   * method. Returns null if there is no such method. */
  public static TableFunction create(Class<?> clazz) {
    return create(clazz, "eval");
  }

  /** Creates a {@link TableFunctionImpl} from a class, looking for a method
   * with a given name. Returns null if there is no such method. */
  public static TableFunction create(Class<?> clazz, String methodName) {
    final Method method = findMethod(clazz, methodName);
    if (method == null) {
      return null;
    }
    return create(method);
  }

  /** Creates a {@link TableFunctionImpl} from a method. */
  public static TableFunction create(final Method method) {
    if (!Modifier.isStatic(method.getModifiers())) {
      Class clazz = method.getDeclaringClass();
      if (!classHasPublicZeroArgsConstructor(clazz)) {
        throw RESOURCE.requireDefaultConstructor(clazz.getName()).ex();
      }
    }
    final Class<?> returnType = method.getReturnType();
    if (!QueryableTable.class.isAssignableFrom(returnType)
        && !ScannableTable.class.isAssignableFrom(returnType)) {
      return null;
    }
    CallImplementor implementor = createImplementor(method);
    return new TableFunctionImpl(method, implementor);
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory,
      List<Object> arguments) {
    return apply(arguments).getRowType(typeFactory);
  }

  public Type getElementType(List<Object> arguments) {
    final Table table = apply(arguments);
    if (table instanceof QueryableTable) {
      QueryableTable queryableTable = (QueryableTable) table;
      return queryableTable.getElementType();
    } else if (table instanceof ScannableTable) {
      return Object[].class;
    }
    throw new AssertionError("Invalid table class: " + table + " "
        + table.getClass());
  }

  public CallImplementor getImplementor() {
    return implementor;
  }

  private static CallImplementor createImplementor(final Method method) {
    return RexImpTable.createImplementor(
        new ReflectiveCallNotNullImplementor(method) {
          public Expression implement(RexToLixTranslator translator,
              RexCall call, List<Expression> translatedOperands) {
            Expression expr = super.implement(translator, call,
                translatedOperands);
            final Class<?> returnType = method.getReturnType();
            if (QueryableTable.class.isAssignableFrom(returnType)) {
              Expression queryable = Expressions.call(
                  Expressions.convert_(expr, QueryableTable.class),
                  BuiltInMethod.QUERYABLE_TABLE_AS_QUERYABLE.method,
                  Expressions.call(DataContext.ROOT,
                      BuiltInMethod.DATA_CONTEXT_GET_QUERY_PROVIDER.method),
                  Expressions.constant(null, SchemaPlus.class),
                  Expressions.constant(call.getOperator().getName(), String.class));
              expr = Expressions.call(queryable,
                  BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method);
            } else {
              expr = Expressions.call(expr,
                  BuiltInMethod.SCANNABLE_TABLE_SCAN.method, DataContext.ROOT);
            }
            return expr;
          }
        }, NullPolicy.ANY, false);
  }

  private Table apply(List<Object> arguments) {
    try {
      Object o = null;
      if (!Modifier.isStatic(method.getModifiers())) {
        final Constructor<?> constructor =
            method.getDeclaringClass().getConstructor();
        o = constructor.newInstance();
      }
      //noinspection unchecked
      final Object table = method.invoke(o, arguments.toArray());
      return (Table) table;
    } catch (IllegalArgumentException e) {
      throw RESOURCE.illegalArgumentForTableFunctionCall(
          method.toString(),
          Arrays.toString(method.getParameterTypes()),
          arguments.toString()).ex(e);
    } catch (IllegalAccessException | InvocationTargetException
        | InstantiationException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }
}

// End TableFunctionImpl.java
