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
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.sql.SqlUtil;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * A JavaScalarFunction is a {@link org.apache.calcite.schema.ScalarFunction} that
 * represent all the overload "eval" methods in a java function class.
 */
public class JavaScalarFunction implements ImplementableFunction, Function {
  /** the function class **/
  private Class<?> functionClass;

  public JavaScalarFunction(Class<?> functionClass) {
    this.functionClass = functionClass;
  }

  public List<FunctionParameter> getParameters() {
    return new ArrayList<>();
  }

  public CallImplementor getImplementor(List<RelDataType> argTypes,
      JavaTypeFactory typeFactory) {
    Method method = SqlUtil.findBestMatchMethod(functionClass,
        "eval", argTypes, typeFactory);
    assert method != null;
    return ScalarFunctionImpl.createImplementor(method);
  }

  public Class<?> getFunctionClass() {
    return functionClass;
  }
}

// End JavaScalarFunction.java
