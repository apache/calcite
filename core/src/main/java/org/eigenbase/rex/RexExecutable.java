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
 * Contains executable code from {@link RexNode} expression.
 */
public class RexExecutable {
  public static final String GENERATED_CLASS_NAME = "Reducer";

  Function1<DataContext, Object[]> compiledFunction;
  private final String generatedCode;
  private DataContext dataContext;
  private final RexBuilder rexBuilder;

  public RexExecutable(String genCode, RexBuilder rexBuilder) {
    try {
      compiledFunction = (Function1) ClassBodyEvaluator
          .createFastClassBodyEvaluator(new Scanner(null, new StringReader(
              genCode)), GENERATED_CLASS_NAME, Utilities.class, new Class[] {
                Function1.class, Serializable.class
              }, getClass()
              .getClassLoader());
    } catch (CompileException e) {
      throw new RuntimeException("While compiling generated Rex code", e);
    } catch (IOException e) {
      throw new RuntimeException("While compiling generated Rex code", e);
    }
    this.rexBuilder = rexBuilder;
    this.generatedCode = genCode;
  }

  public void setDataContext(DataContext dataContext) {
    this.dataContext = dataContext;
  }

  public void reduce(List<RexNode> constExps, List<RexNode> reducedValues) {
    Object[] values = compiledFunction.apply(dataContext);
    assert values.length == constExps.size();
    final List<Object> valueList = Arrays.asList(values);
    for (Pair<RexNode, Object> value : Pair.zip(constExps, valueList)) {
      reducedValues.add(rexBuilder.makeLiteral(value.right,
          value.left.getType(), true));
    }
    Hook.EXPRESSION_REDUCER.run(Pair.of(generatedCode, values));
  }

  public Function1<DataContext, Object[]> getFunction() {
    return compiledFunction;
  }

  public Object[] execute() {
    return compiledFunction.apply(dataContext);
  }

  public String getSource() {
    return generatedCode;
  }
}
