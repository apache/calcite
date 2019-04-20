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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlUtil;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * A JavaTableFunction is a {@link TableFunction} that represent all the overload "eval" methods
 * in a java function class.
 */
public class JavaTableFunction implements TableFunction, ImplementableFunction {
  /** the function class **/
  private Class<?> functionClass;

  public JavaTableFunction(Class<?> functionClass) {
    this.functionClass = functionClass;
  }

  public List<FunctionParameter> getParameters() {
    return new ArrayList<>();
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory,
      List<RelDataType> argTypes, List<Object> arguments) {
    JavaTypeFactory javaTypeFactory = (JavaTypeFactory) typeFactory;
    Method method = SqlUtil.findBestMatchMethod(functionClass, "eval",
            argTypes, javaTypeFactory);
    assert method != null;
    Class<? extends Table> tableClass
         = (Class<? extends Table>) method.getReturnType();
    try {
      Table table = tableClass.newInstance();
      return table.getRowType(typeFactory);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Fail to create instance for " + tableClass
              + ": must have a default public constructor.");
    }
  }

  public Type getElementType(List<Object> arguments) {
    return Object[].class;
  }

  public CallImplementor getImplementor(List<RelDataType> argTypes,
      JavaTypeFactory typeFactory) {
    Method method = SqlUtil.findBestMatchMethod(functionClass, "eval",
          argTypes, typeFactory);
    assert method != null;
    return TableFunctionImpl.createImplementor(method);
  }

  public Class<?> getFunctionClass() {
    return functionClass;
  }
}

// End JavaTableFunction.java
