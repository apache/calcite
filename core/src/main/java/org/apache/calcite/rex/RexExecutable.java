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
package org.eigenbase.rex;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import org.eigenbase.util.Pair;

import net.hydromatic.linq4j.function.Function1;

import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.runtime.Hook;
import net.hydromatic.optiq.runtime.Utilities;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

/**
 * Result of compiling code generated from a {@link RexNode} expression.
 */
public class RexExecutable {
  public static final String GENERATED_CLASS_NAME = "Reducer";

  private final Function1<DataContext, Object[]> compiledFunction;
  private final String code;
  private DataContext dataContext;

  public RexExecutable(String code, Object reason) {
    try {
      //noinspection unchecked
      compiledFunction =
          (Function1) ClassBodyEvaluator.createFastClassBodyEvaluator(
              new Scanner(null, new StringReader(code)),
              GENERATED_CLASS_NAME,
              Utilities.class,
              new Class[] {Function1.class, Serializable.class},
              getClass().getClassLoader());
    } catch (CompileException e) {
      throw new RuntimeException("While compiling " + reason, e);
    } catch (IOException e) {
      throw new RuntimeException("While compiling " + reason, e);
    }
    this.code = code;
  }

  public void setDataContext(DataContext dataContext) {
    this.dataContext = dataContext;
  }

  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps,
      List<RexNode> reducedValues) {
    Object[] values = compiledFunction.apply(dataContext);
    assert values.length == constExps.size();
    final List<Object> valueList = Arrays.asList(values);
    for (Pair<RexNode, Object> value : Pair.zip(constExps, valueList)) {
      reducedValues.add(
          rexBuilder.makeLiteral(value.right, value.left.getType(), true));
    }
    Hook.EXPRESSION_REDUCER.run(Pair.of(code, values));
  }

  public Function1<DataContext, Object[]> getFunction() {
    return compiledFunction;
  }

  public Object[] execute() {
    return compiledFunction.apply(dataContext);
  }

  public String getSource() {
    return code;
  }
}

// End RexExecutable.java
